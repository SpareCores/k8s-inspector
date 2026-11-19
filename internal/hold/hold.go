// Package hold implements the anchor Pod's process: install the sc-run binary
// for the steps, then wait until no heartbeat has been seen for the idle timeout.
package hold

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sparecores/k8s-inspector/internal/heartbeat"
)

// BinaryName is the name of the installed binary.
const BinaryName = "sc-run"

// Install copies the running executable to dir/sc-run atomically (temp file in
// the same directory, then rename), so a step never sees a partial binary.
func Install(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	src, err := os.Open(self)
	if err != nil {
		return err
	}
	defer src.Close()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, BinaryName+".tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, BinaryName))
}

// Ready reports whether the binary is installed in dir (the anchor's readiness probe).
func Ready(dir string) error {
	info, err := os.Stat(filepath.Join(dir, BinaryName))
	if err != nil {
		return err
	}
	if info.Mode()&0o111 == 0 {
		return fmt.Errorf("%s is not executable", info.Name())
	}
	return nil
}

// Run installs the binary into installDir (if set), then blocks until ctx is
// cancelled or the heartbeat in hbDir is older than idle. The idle clock
// starts now, so a controller that never shows up cannot hold the node either.
func Run(ctx context.Context, installDir, hbDir string, idle, poll time.Duration) error {
	if installDir != "" {
		if err := Install(installDir); err != nil {
			return fmt.Errorf("install: %w", err)
		}
		log.Printf("installed %s into %s", BinaryName, installDir)
	}
	if err := heartbeat.Touch(hbDir); err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("hold: stopping on signal")
			return nil
		case <-ticker.C:
			age, err := heartbeat.Age(hbDir)
			if err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}
			if age > idle {
				log.Printf("hold: no heartbeat for %s (limit %s), releasing the node", age.Round(time.Second), idle)
				return nil
			}
		}
	}
}
