package hold

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sparecores/k8s-inspector/internal/heartbeat"
)

func TestInstallCopiesExecutableAtomically(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bin")
	if err := Ready(dir); err == nil {
		t.Fatal("ready before install")
	}
	if err := Install(dir); err != nil {
		t.Fatal(err)
	}
	if err := Ready(dir); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("leftover files: %v", entries)
	}
	if err := Install(dir); err != nil { // installing over an existing binary works
		t.Fatal(err)
	}
}

func TestExitsWithoutHeartbeat(t *testing.T) {
	hb := t.TempDir()
	start := time.Now()
	err := Run(context.Background(), "", hb, 150*time.Millisecond, 20*time.Millisecond)
	if err != nil || time.Since(start) > 3*time.Second {
		t.Fatalf("err=%v after %s", err, time.Since(start))
	}
}

func TestHeartbeatKeepsItAlive(t *testing.T) {
	hb := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, "", hb, 200*time.Millisecond, 20*time.Millisecond) }()
	for i := 0; i < 15; i++ { // 450 ms, longer than the idle timeout
		time.Sleep(30 * time.Millisecond)
		heartbeat.Touch(hb)
	}
	select {
	case <-done:
		t.Fatal("exited despite heartbeats")
	default:
	}
	cancel() // SIGTERM
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
