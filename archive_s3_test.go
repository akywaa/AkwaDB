package akwadb

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestS3Archiver_Put(t *testing.T) {
	var gotPath, gotAuth, gotHash, gotBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotHash = r.Header.Get("x-amz-content-sha256")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	segment := filepath.Join(t.TempDir(), "wal_flush_000001.log")
	if err := os.WriteFile(segment, []byte("hello-archive"), 0644); err != nil {
		t.Fatal(err)
	}

	a := &S3Archiver{
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
		Bucket:    "bucket",
		Prefix:    "wal",
		AccessKey: "AKIDEXAMPLE",
		SecretKey: "secret",
		PathStyle: true,
	}
	if err := a.ArchiveSegment(segment); err != nil {
		t.Fatal(err)
	}

	if gotPath != "/bucket/wal/wal_flush_000001.log.gz" {
		t.Fatalf("path = %q, want /bucket/wal/wal_flush_000001.log.gz", gotPath)
	}
	if !strings.Contains(gotAuth, "AWS4-HMAC-SHA256") || !strings.Contains(gotAuth, "Credential=AKIDEXAMPLE/") {
		t.Fatalf("unexpected Authorization header: %q", gotAuth)
	}
	if len(gotHash) != 64 {
		t.Fatalf("x-amz-content-sha256 = %q, want 64 hex chars", gotHash)
	}

	gz, err := gzip.NewReader(strings.NewReader(gotBody))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello-archive" {
		t.Fatalf("archived body = %q, want hello-archive", data)
	}
}
