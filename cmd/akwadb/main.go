package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/cluster"
	"github.com/akywaa/akwadb/config"
	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/server"
	"github.com/spf13/viper"
)

func main() {
	cfgPath := flag.String("config", "", "path to YAML config file")
	addrFlag := flag.String("addr", "", "TCP listen address")
	dataDirFlag := flag.String("data-dir", "", "Data directory")
	memMbFlag := flag.Int("memtable-mb", 0, "Memtable size in MB")
	raftIDFlag := flag.String("raft-id", "", "Raft node ID (enables clustered mode)")
	raftAddrFlag := flag.String("raft-addr", "", "Raft bind address")
	raftBootstrapFlag := flag.Bool("raft-bootstrap", false, "Bootstrap a new Raft cluster")
	archiveDirFlag := flag.String("archive-dir", "", "Directory for archived WAL/VLog segments")
	flag.Parse()

	// set up structured JSON logging
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Default()
	v := viper.GetViper()
	if *cfgPath != "" {
		v.SetConfigFile(*cfgPath)
		_ = v.ReadInConfig()
		if v.GetString("listen_addr") != "" {
			cfg.ListenAddr = v.GetString("listen_addr")
		}
		if v.GetString("data_dir") != "" {
			cfg.DataDir = v.GetString("data_dir")
		}
		if v.GetInt("memtable_mb") > 0 {
			cfg.MemTableMB = v.GetInt("memtable_mb")
		}
		if v.GetString("encryption_key") != "" {
			cfg.EncryptionKey = v.GetString("encryption_key")
		}
		if v.GetString("encryption_key_path") != "" {
			cfg.EncryptionKeyPath = v.GetString("encryption_key_path")
		}
		if v.GetString("raft_id") != "" {
			cfg.RaftID = v.GetString("raft_id")
		}
		if v.GetString("raft_addr") != "" {
			cfg.RaftAddr = v.GetString("raft_addr")
		}
		if v.GetBool("raft_bootstrap") {
			cfg.RaftBootstrap = true
		}
		if v.GetString("tls_cert_file") != "" {
			cfg.TLSCertFile = v.GetString("tls_cert_file")
		}
		if v.GetString("tls_key_file") != "" {
			cfg.TLSKeyFile = v.GetString("tls_key_file")
		}
		if v.GetString("archive_dir") != "" {
			cfg.ArchiveDir = v.GetString("archive_dir")
		}
		if v.GetString("checkpoint_dir") != "" {
			cfg.CheckpointDir = v.GetString("checkpoint_dir")
		}
		if v.GetInt("checkpoint_interval_seconds") > 0 {
			cfg.CheckpointInterval = v.GetInt("checkpoint_interval_seconds")
		}
		if v.GetInt("checkpoint_keep") > 0 {
			cfg.CheckpointKeep = v.GetInt("checkpoint_keep")
		}
		if v.GetString("s3_endpoint") != "" {
			cfg.S3Endpoint = v.GetString("s3_endpoint")
		}
		if v.GetString("s3_region") != "" {
			cfg.S3Region = v.GetString("s3_region")
		}
		if v.GetString("s3_bucket") != "" {
			cfg.S3Bucket = v.GetString("s3_bucket")
		}
		if v.GetString("s3_prefix") != "" {
			cfg.S3Prefix = v.GetString("s3_prefix")
		}
		if v.GetString("s3_access_key") != "" {
			cfg.S3AccessKey = v.GetString("s3_access_key")
		}
		if v.GetString("s3_secret_key") != "" {
			cfg.S3SecretKey = v.GetString("s3_secret_key")
		}
		if v.IsSet("s3_path_style") {
			cfg.S3PathStyle = v.GetBool("s3_path_style")
		}
		if v.IsSet("s3_use_tls") {
			cfg.S3UseTLS = v.GetBool("s3_use_tls")
		}
	}
	if *addrFlag != "" {
		cfg.ListenAddr = *addrFlag
	}
	if *dataDirFlag != "" {
		cfg.DataDir = *dataDirFlag
	}
	if *memMbFlag > 0 {
		cfg.MemTableMB = *memMbFlag
	}
	if *raftIDFlag != "" {
		cfg.RaftID = *raftIDFlag
	}
	if *raftAddrFlag != "" {
		cfg.RaftAddr = *raftAddrFlag
	}
	if *raftBootstrapFlag {
		cfg.RaftBootstrap = true
	}
	if *archiveDirFlag != "" {
		cfg.ArchiveDir = *archiveDirFlag
	}

	opts := akwadb.DefaultOptions(cfg.DataDir)
	opts.MemTableSize = cfg.MemTableMB * 1024 * 1024
	opts.CompactionThreshold = cfg.CompactionThreshold
	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.MaxDiskBytes = cfg.MaxDiskBytes
	opts.CheckpointDir = cfg.CheckpointDir
	opts.CheckpointInterval = time.Duration(cfg.CheckpointInterval) * time.Second
	opts.CheckpointKeep = cfg.CheckpointKeep

	keyHex := cfg.EncryptionKey
	if keyHex == "" && cfg.EncryptionKeyPath != "" {
		keyData, err := os.ReadFile(cfg.EncryptionKeyPath)
		if err != nil {
			slog.Error("failed to read encryption key file", "path", cfg.EncryptionKeyPath, "err", err)
			os.Exit(1)
		}
		keyHex = strings.TrimSpace(string(keyData))
	}
	if keyHex != "" {
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			slog.Error("encryption_key must be hex-encoded", "err", err)
			os.Exit(1)
		}
		reg, err := crypto.OpenKeyRegistry(cfg.DataDir, keyBytes)
		if err != nil {
			slog.Error("failed to open key registry", "err", err)
			os.Exit(1)
		}
		opts.KeyRegistry = reg
	}

	engine, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		slog.Error("engine init failed", "err", err)
		os.Exit(1)
	}
	defer engine.Close()

	switch {
	case cfg.S3Bucket != "":
		engine.SetArchiver(&akwadb.S3Archiver{
			Endpoint:  cfg.S3Endpoint,
			Region:    cfg.S3Region,
			Bucket:    cfg.S3Bucket,
			Prefix:    cfg.S3Prefix,
			AccessKey: cfg.S3AccessKey,
			SecretKey: cfg.S3SecretKey,
			PathStyle: cfg.S3PathStyle,
			UseTLS:    cfg.S3UseTLS,
		})
		slog.Info("s3 segment archiving enabled", "endpoint", cfg.S3Endpoint, "bucket", cfg.S3Bucket)
	case cfg.ArchiveDir != "":
		engine.SetArchiver(&akwadb.FileArchiver{Dir: cfg.ArchiveDir})
		slog.Info("segment archiving enabled", "dir", cfg.ArchiveDir)
	}

	srv := server.NewServerWithAuth(cfg.ListenAddr, engine, cfg.Password)

	engine.OnWrite = func(op byte, key, val []byte, expiresAt int64) {
		srv.ReplicateEntry(op, key, val, expiresAt)
	}

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		if err := srv.SetTLS(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
			slog.Error("tls init failed", "err", err)
			os.Exit(1)
		}
		slog.Info("tls enabled", "cert", cfg.TLSCertFile)
	}

	if cfg.RaftID != "" && cfg.RaftAddr != "" {
		raftNode, rerr := cluster.NewNode(cfg.RaftID, cfg.RaftAddr, filepath.Join(cfg.DataDir, "raft"), cfg.RaftBootstrap, engine)
		if rerr != nil {
			slog.Error("raft init failed", "err", rerr)
			os.Exit(1)
		}
		defer raftNode.Stop()
		srv.SetClusterNode(raftNode)
		slog.Info("raft enabled", "id", cfg.RaftID, "addr", cfg.RaftAddr, "bootstrap", cfg.RaftBootstrap)
	}

	// metrics endpoint
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			stats := engine.Stats()
			fmt.Fprintf(w, "# HELP akwadb_puts_total Total number of PUT operations\n")
			fmt.Fprintf(w, "# TYPE akwadb_puts_total counter\nakwadb_puts_total %d\n", stats.PutsTotal)
			fmt.Fprintf(w, "# HELP akwadb_gets_total Total number of GET operations\n")
			fmt.Fprintf(w, "# TYPE akwadb_gets_total counter\nakwadb_gets_total %d\n", stats.GetsTotal)
			fmt.Fprintf(w, "# HELP akwadb_flushes_total Total memtable flushes\n")
			fmt.Fprintf(w, "# TYPE akwadb_flushes_total counter\nakwadb_flushes_total %d\n", stats.FlushesTotal)
			fmt.Fprintf(w, "# HELP akwadb_compactions_total Total compactions done\n")
			fmt.Fprintf(w, "# TYPE akwadb_compactions_total counter\nakwadb_compactions_total %d\n", stats.CompactionsDone)
			fmt.Fprintf(w, "# HELP akwadb_deletes_total Total number of DELETE operations\n")
			fmt.Fprintf(w, "# TYPE akwadb_deletes_total counter\nakwadb_deletes_total %d\n", stats.DeletesTotal)
		})
		slog.Info("pprof and metrics listening", "addr", "localhost:6060")
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		slog.Info("server started", "addr", cfg.ListenAddr, "data", cfg.DataDir, "memtable_mb", cfg.MemTableMB)
		if err := srv.Start(); err != nil {
			slog.Error("server stopped", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("received shutdown signal, stopping...")
	_ = srv.Stop()
}
