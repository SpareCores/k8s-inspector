// Package heartbeat implements the liveness signal between steps and the
// anchor: the mtime of a file in a hostPath directory. The anchor's
// `sc-run hold` exits when the file has not been touched for the idle timeout.
package heartbeat

import (
	"os"
	"path/filepath"
	"time"
)

const fileName = "heartbeat"

// Touch creates the heartbeat file in dir if needed and sets its mtime to now.
func Touch(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, fileName)
	now := time.Now()
	if err := os.Chtimes(path, now, now); err == nil {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

// Age returns the time since the last heartbeat in dir.
func Age(dir string) (time.Duration, error) {
	info, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		return 0, err
	}
	return time.Since(info.ModTime()), nil
}
