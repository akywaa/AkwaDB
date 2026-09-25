package pitr

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/akywaa/akwadb/internal/crypto"
	"github.com/akywaa/akwadb/wal"
)

type Options struct {
	BaseDir    string
	ArchiveDir string
	OutDir     string
	Until      time.Time
	Registry   *crypto.KeyRegistry
}

type Stats struct {
	Records int
	Skipped int
}

func Restore(opts Options) (Stats, error) {
	if opts.BaseDir == "" || opts.ArchiveDir == "" || opts.OutDir == "" {
		return Stats{}, fmt.Errorf("pitr: base, archive and out directories are required")
	}
	absBase, err := filepath.Abs(opts.BaseDir)
	if err != nil {
		return Stats{}, err
	}
	absOut, err := filepath.Abs(opts.OutDir)
	if err != nil {
		return Stats{}, err
	}
	if absBase == absOut {
		return Stats{}, fmt.Errorf("pitr: out directory must differ from base")
	}

	if err := os.MkdirAll(opts.OutDir, 0755); err != nil {
		return Stats{}, err
	}
	if err := copyTree(opts.BaseDir, opts.OutDir); err != nil {
		return Stats{}, fmt.Errorf("copy base: %w", err)
	}

	vlogDir := filepath.Join(opts.OutDir, "vlog")
	if err := os.MkdirAll(vlogDir, 0755); err != nil {
		return Stats{}, err
	}
	if err := restoreVlogSegments(opts.ArchiveDir, vlogDir); err != nil {
		return Stats{}, err
	}

	var records []wal.Record

	archived, err := archivedWALSegments(opts.ArchiveDir)
	if err != nil {
		return Stats{}, err
	}
	tmpDir := filepath.Join(opts.OutDir, ".pitr-tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return Stats{}, err
	}
	defer os.RemoveAll(tmpDir)

	for _, name := range archived {
		src := filepath.Join(opts.ArchiveDir, name)
		dst := filepath.Join(tmpDir, strings.TrimSuffix(name, ".gz"))
		if err := gunzipFile(src, dst); err != nil {
			return Stats{}, fmt.Errorf("decompress %s: %w", name, err)
		}
		recs, rerr := readWALRecords(dst, opts.Registry)
		if rerr != nil {
			return Stats{}, fmt.Errorf("read %s: %w", name, rerr)
		}
		records = append(records, recs...)
	}

	baseWalPath := filepath.Join(opts.OutDir, "wal.log")
	if _, err := os.Stat(baseWalPath); err == nil {
		recs, rerr := readWALRecords(baseWalPath, opts.Registry)
		if rerr != nil {
			return Stats{}, fmt.Errorf("read base wal: %w", rerr)
		}
		records = append(records, recs...)
		if err := os.Remove(baseWalPath); err != nil {
			return Stats{}, err
		}
	}

	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Version != records[j].Version {
			return records[i].Version < records[j].Version
		}
		return records[i].Timestamp < records[j].Timestamp
	})

	untilNano := int64(0)
	if !opts.Until.IsZero() {
		untilNano = opts.Until.UnixNano()
	}

	kept := records[:0]
	for _, rec := range records {
		if untilNano == 0 || rec.Timestamp == 0 || rec.Timestamp <= untilNano {
			kept = append(kept, rec)
		}
	}

	if err := writeWAL(baseWalPath, kept, opts.Registry); err != nil {
		return Stats{}, err
	}
	return Stats{Records: len(kept), Skipped: len(records) - len(kept)}, nil
}

func readWALRecords(path string, reg *crypto.KeyRegistry) ([]wal.Record, error) {
	w, err := wal.OpenWithOptionsAndRegistry(path, false, reg)
	if err != nil {
		return nil, err
	}
	recs, rerr := w.Recover()
	cerr := w.Close()
	if rerr != nil {
		return nil, rerr
	}
	return recs, cerr
}

func writeWAL(path string, records []wal.Record, reg *crypto.KeyRegistry) error {
	w, err := wal.OpenWithOptionsAndRegistry(path, false, reg)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if _, err := w.WriteVersion(rec.Op, rec.Key, rec.Value, rec.ExpiresAt, rec.Version); err != nil {
			w.Close()
			return err
		}
	}
	if err := w.Sync(); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func archivedWALSegments(archiveDir string) ([]string, error) {
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		return nil, err
	}
	type segment struct {
		name string
		seq  uint64
	}
	var segments []segment
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "wal_flush_") || !strings.HasSuffix(name, ".log.gz") {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, "wal_flush_"), ".log.gz")
		seq, perr := strconv.ParseUint(base, 10, 64)
		if perr != nil {
			continue
		}
		segments = append(segments, segment{name: name, seq: seq})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })
	names := make([]string, 0, len(segments))
	for _, s := range segments {
		names = append(names, s.name)
	}
	return names, nil
}

func restoreVlogSegments(archiveDir, vlogDir string) error {
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "vlog_") || !strings.HasSuffix(name, ".log.gz") {
			continue
		}
		dst := filepath.Join(vlogDir, strings.TrimSuffix(name, ".gz"))
		if _, serr := os.Stat(dst); serr == nil {
			continue
		}
		if err := gunzipFile(filepath.Join(archiveDir, name), dst); err != nil {
			return fmt.Errorf("restore %s: %w", name, err)
		}
	}
	return nil
}

func gunzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	gz, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer gz.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, gz); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		return copyFile(path, target)
	})
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
