package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/config"
	"github.com/akywaa/akwadb/server"
	"github.com/spf13/viper"
)

func main() {
	cfgPath := flag.String("config", "", "path to YAML config file")
	addrFlag := flag.String("addr", "", "TCP listen address")
	dataDirFlag := flag.String("data-dir", "", "Data directory")
	memMbFlag := flag.Int("memtable-mb", 0, "Memtable size in MB")
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

	opts := akwadb.DefaultOptions(cfg.DataDir)
	opts.MemTableSize = cfg.MemTableMB * 1024 * 1024
	opts.CompactionThreshold = cfg.CompactionThreshold
	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.MaxDiskBytes = cfg.MaxDiskBytes

	engine, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		slog.Error("engine init failed", "err", err)
		os.Exit(1)
	}
	defer engine.Close()

	srv := server.NewServerWithAuth(cfg.ListenAddr, engine, cfg.Password)

	engine.OnWrite = func(op byte, key, val []byte, expiresAt int64) {
		srv.ReplicateEntry(op, key, val, expiresAt)
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
