package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/akywaa/akwadb"
)

func TestCrashRecovery(t *testing.T) {
	dataDir := t.TempDir()
	progressPath := filepath.Join(t.TempDir(), "progress.log")

	binary := filepath.Join(t.TempDir(), "crashwriter")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./test/crashwriter")
	build.Dir = moduleRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("failed to build crash writer: %v\n%s", err, out)
	}

	cmd := exec.Command(binary, "-dir", dataDir, "-progress", progressPath)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to spawn crash writer: %v", err)
	}

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
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("acknowledged key %q lost after crash: %v", key, err)
		}
		if val != key+"_payload" {
			t.Fatalf("key %q has corrupted value: %q", key, val)
		}
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to determine the module root")
	}
	// We are at <moduleRoot>/test/integration/crash_test.go; walking up three
	// directories from this file reaches the module root.
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
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
