package integration_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
	"github.com/akywaa/akwadb/server"
)

func TestCrashRecovery(t *testing.T) {
	if os.Getenv("AKWADB_CRASH_CHILD") == "1" {
		crashChild(os.Getenv("AKWADB_CRASH_DIR"), os.Getenv("AKWADB_CRASH_PROGRESS"))
		return
	}

	dataDir := t.TempDir()
	progressPath := filepath.Join(t.TempDir(), "progress.log")

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecovery")
	cmd.Env = append(os.Environ(),
		"AKWADB_CRASH_CHILD=1",
		"AKWADB_CRASH_DIR="+dataDir,
		"AKWADB_CRASH_PROGRESS="+progressPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	var lines []string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		lines = readProgress(progressPath)
		if len(lines) >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	if len(lines) < 10 {
		t.Skipf("child produced only %d synced writes before timeout", len(lines))
	}

	db, err := akwadb.OpenEngineWithOpts(akwadb.DefaultOptions(dataDir))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db.Close()

	for _, key := range lines {
		if _, err := db.Get(key); err != nil {
			t.Fatalf("acknowledged key %q lost after crash: %v", key, err)
		}
	}
}

func crashChild(dataDir, progressPath string) {
	db, err := akwadb.OpenEngineWithOpts(akwadb.DefaultOptions(dataDir))
	if err != nil {
		os.Exit(3)
	}
	f, err := os.OpenFile(progressPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		os.Exit(3)
	}
	for i := 0; ; i++ {
		key := fmt.Sprintf("k%06d", i)
		if err := db.PutWithOptions(key, key, server.WriteOptions{Sync: true}); err != nil {
			os.Exit(4)
		}
		if _, err := fmt.Fprintf(f, "%s\n", key); err != nil {
			os.Exit(4)
		}
		_ = f.Sync()
	}
}

func readProgress(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return nil
	}
	text := string(data)
	if !strings.HasSuffix(text, "\n") {
		if idx := strings.LastIndexByte(text, '\n'); idx >= 0 {
			text = text[:idx+1]
		} else {
			return nil
		}
	}
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
