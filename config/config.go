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

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			slog.Warn("config file read error", "err", err)
		}
	}

	_ = v.Unmarshal(cfg)
	return cfg
}
