//go:build windows

package fsutil

func SyncDir(string) error {
	return nil
}
