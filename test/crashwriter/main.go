package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/server"
)

func main() {
	dir := flag.String("dir", "", "data directory")
	progress := flag.String("progress", "", "path to progress log file")
	flag.Parse()

	if *dir == "" || *progress == "" {
		os.Exit(2)
	}

	opts := akwadb.DefaultOptions(*dir)
	opts.MemTableSize = 16 * 1024
	opts.CompactionThreshold = 2

	db, err := akwadb.OpenEngineWithOpts(opts)
	if err != nil {
		os.Exit(3)
	}

	logPath := *progress
	if dir := filepath.Dir(logPath); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		os.Exit(3)
	}

	for i := 0; ; i++ {
		key := fmt.Sprintf("k%06d", i)
		val := key + "_payload"

		if err := db.PutWithOptions(key, val, server.WriteOptions{Sync: true}); err != nil {
			os.Exit(4)
		}

		if _, err := fmt.Fprintf(f, "%s\n", key); err != nil {
			os.Exit(4)
		}
		if err := f.Sync(); err != nil {
			os.Exit(4)
		}
	}
}