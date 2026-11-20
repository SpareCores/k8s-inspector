package task

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	mirrorPeriod = time.Second
	mirrorChunk  = 64 << 10

	mirrorMaxBacklog = 1 << 20   // more unread than this: skip ahead
	mirrorKeep       = 256 << 10 // what is still mirrored after skipping ahead
)

// mirrorStopWait bounds the final drain; a variable so tests can shorten it.
var mirrorStopWait = 2 * time.Second

// Where the mirrored output goes; variables so tests can capture it.
var (
	mirrorOut io.Writer = os.Stdout
	mirrorErr io.Writer = os.Stderr
)

// tailer copies what was appended to a spool file since the last drain to dst.
type tailer struct {
	path   string
	dst    io.Writer
	offset int64
	buf    []byte
	failed bool
}

// drain copies the bytes that exist when it starts (the size from fstat, so a
// process that keeps writing cannot keep it busy), in chunks of mirrorChunk.
// A backlog above mirrorMaxBacklog is skipped but its last mirrorKeep bytes.
// Errors are logged once and never reach the task: mirroring is best effort.
func (t *tailer) drain() {
	f, err := os.Open(t.path)
	if err != nil {
		t.fail(err)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.fail(err)
		return
	}
	size := fi.Size()
	if size < t.offset { // truncated: start over
		t.fail(errors.New("file shrank, mirroring from its start"))
		t.offset = 0
	}
	if skipped := size - t.offset - mirrorKeep; size-t.offset > mirrorMaxBacklog {
		t.write([]byte(fmt.Sprintf("sc-run: mirror skipped %d bytes; full output in the spool and object storage\n", skipped)))
		t.offset += skipped
	}
	if t.buf == nil {
		t.buf = make([]byte, mirrorChunk)
	}
	for t.offset < size {
		n, rerr := f.ReadAt(t.buf[:min(int64(len(t.buf)), size-t.offset)], t.offset)
		if n > 0 {
			t.offset += int64(n)
			t.write(t.buf[:n])
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				t.fail(rerr)
			}
			return
		}
	}
}

func (t *tailer) write(p []byte) {
	if _, err := t.dst.Write(p); err != nil {
		t.fail(err)
	}
}

func (t *tailer) fail(err error) {
	if !t.failed {
		t.failed = true
		log.Printf("mirror %s: %v", filepath.Base(t.path), err)
	}
}

// startMirror copies the spool's stdout and stderr to out and errw about once
// a second, so `kubectl logs` shows the task's output. The task itself still
// writes straight into the spool files. The returned stop function drains once
// more and returns, waiting at most mirrorStopWait for it (a stuck log writer
// must not delay the task's exit); call it after the task has exited.
func startMirror(spool string, out, errw io.Writer) (stop func()) {
	tailers := []*tailer{
		{path: filepath.Join(spool, "stdout"), dst: out},
		{path: filepath.Join(spool, "stderr"), dst: errw},
	}
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(mirrorPeriod)
		defer tick.Stop()
		for {
			select {
			case <-quit:
				for _, t := range tailers {
					t.drain()
				}
				return
			case <-tick.C:
				for _, t := range tailers {
					t.drain()
				}
			}
		}
	}()
	return func() {
		close(quit)
		select {
		case <-done:
		case <-time.After(mirrorStopWait):
			log.Printf("mirror: final drain abandoned after %s", mirrorStopWait)
		}
	}
}
