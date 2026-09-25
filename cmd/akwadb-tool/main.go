package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/internal/pitr"
)

func main() {
	baseDir := flag.String("base", "", "base data directory to restore (checkpoint)")
	archiveDir := flag.String("archive", "", "directory with archived .gz WAL/VLog segments")
	outDir := flag.String("out", "", "destination data directory for the restored copy")
	untilFlag := flag.String("restore-until", "", "replay only records at or before this time (RFC3339 or '2006-01-02 15:04:05')")
	keyHex := flag.String("encryption-key", "", "hex-encoded master key for encrypted WAL segments")
	keyPath := flag.String("encryption-key-path", "", "file containing the hex-encoded master key")
	flag.Parse()

	until, err := parseUntil(*untilFlag)
	if err != nil {
		slog.Error("invalid --restore-until", "err", err)
		os.Exit(2)
	}

	key := *keyHex
	if key == "" && *keyPath != "" {
		data, rerr := os.ReadFile(*keyPath)
		if rerr != nil {
			slog.Error("failed to read encryption key file", "err", rerr)
			os.Exit(1)
		}
		key = strings.TrimSpace(string(data))
	}
	var reg *crypto.KeyRegistry
	if key != "" {
		keyBytes, derr := hex.DecodeString(key)
		if derr != nil {
			slog.Error("encryption key must be hex-encoded", "err", derr)
			os.Exit(1)
		}
		reg, err = crypto.OpenKeyRegistry(*outDir, keyBytes)
		if err != nil {
			slog.Error("failed to open key registry", "err", err)
			os.Exit(1)
		}
	}

	stats, err := pitr.Restore(pitr.Options{
		BaseDir:    *baseDir,
		ArchiveDir: *archiveDir,
		OutDir:     *outDir,
		Until:      until,
		Registry:   reg,
	})
	if err != nil {
		slog.Error("restore failed", "err", err)
		os.Exit(1)
	}
	slog.Info("restore complete", "out", *outDir, "records", stats.Records, "skipped", stats.Skipped)
}

func parseUntil(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized format: %q", s)
}
