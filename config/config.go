package config

import (
	"github.com/spf13/viper"
	"log/slog"
)

type Config struct {
	ListenAddr string `mapstructure:"listen_addr"`
	DataDir    string `mapstructure:"data_dir"`
	Password   string `mapstructure:"password"`

	MemTableMB         int   `mapstructure:"memtable_mb"`
	CompactionThreshold int  `mapstructure:"compaction_threshold"`
	BlockCacheSize     int   `mapstructure:"block_cache_size"`
	MaxDiskBytes       int64 `mapstructure:"max_disk_bytes"`
	ReplBacklogSize    int   `mapstructure:"repl_backlog_size"`

	// EncryptionKey is a hex-encoded master key (32 bytes / 64 hex chars for AES-256).
	EncryptionKey     string `mapstructure:"encryption_key"`
	EncryptionKeyPath string `mapstructure:"encryption_key_path"`

	RaftID        string `mapstructure:"raft_id"`
	RaftAddr      string `mapstructure:"raft_addr"`
	RaftBootstrap bool   `mapstructure:"raft_bootstrap"`

	TLSCertFile string `mapstructure:"tls_cert_file"`
	TLSKeyFile  string `mapstructure:"tls_key_file"`

	ArchiveDir string `mapstructure:"archive_dir"`

	CheckpointDir      string `mapstructure:"checkpoint_dir"`
	CheckpointInterval int    `mapstructure:"checkpoint_interval_seconds"`
	CheckpointKeep     int    `mapstructure:"checkpoint_keep"`

	S3Endpoint  string `mapstructure:"s3_endpoint"`
	S3Region    string `mapstructure:"s3_region"`
	S3Bucket    string `mapstructure:"s3_bucket"`
	S3Prefix    string `mapstructure:"s3_prefix"`
	S3AccessKey string `mapstructure:"s3_access_key"`
	S3SecretKey string `mapstructure:"s3_secret_key"`
	S3PathStyle bool   `mapstructure:"s3_path_style"`
	S3UseTLS    bool   `mapstructure:"s3_use_tls"`
}

func Default() *Config {
	return &Config{
		ListenAddr:          ":6379",
		DataDir:             "./akwadata",
		MemTableMB:          4,
		CompactionThreshold: 4,
		BlockCacheSize:      1000,
		MaxDiskBytes:        10 * 1024 * 1024 * 1024, // 10 GB
		ReplBacklogSize:     10000,
	}
}

func Load(path string) *Config {
	cfg := Default()
	v := viper.GetViper()

	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	v.SetDefault("listen_addr", cfg.ListenAddr)
	v.SetDefault("data_dir", cfg.DataDir)
	v.SetDefault("password", "")
	v.SetDefault("memtable_mb", cfg.MemTableMB)
	v.SetDefault("compaction_threshold", cfg.CompactionThreshold)
	v.SetDefault("block_cache_size", cfg.BlockCacheSize)
	v.SetDefault("max_disk_bytes", cfg.MaxDiskBytes)
	v.SetDefault("repl_backlog_size", cfg.ReplBacklogSize)
	v.SetDefault("encryption_key", "")
	v.SetDefault("encryption_key_path", "")
	v.SetDefault("raft_id", "")
	v.SetDefault("raft_addr", "")
	v.SetDefault("raft_bootstrap", false)
	v.SetDefault("tls_cert_file", "")
	v.SetDefault("tls_key_file", "")
	v.SetDefault("archive_dir", "")
	v.SetDefault("checkpoint_dir", "")
	v.SetDefault("checkpoint_interval_seconds", 0)
	v.SetDefault("checkpoint_keep", 0)
	v.SetDefault("s3_endpoint", "")
	v.SetDefault("s3_region", "")
	v.SetDefault("s3_bucket", "")
	v.SetDefault("s3_prefix", "")
	v.SetDefault("s3_access_key", "")
	v.SetDefault("s3_secret_key", "")
	v.SetDefault("s3_path_style", false)
	v.SetDefault("s3_use_tls", true)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			slog.Warn("config file read error", "err", err)
		}
	}

	_ = v.Unmarshal(cfg)
	return cfg
}
