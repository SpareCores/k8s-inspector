package upload

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sparecores/k8s-inspector/internal/s3fake"
	"github.com/sparecores/k8s-inspector/internal/urls"
)

var standard = []string{"stdout", "stderr", "version", "result.json", "manifest.json"}

func newUploader(t *testing.T, s *s3fake.Server, large map[string]int, partSize int64) (*Uploader, string) {
	dir := t.TempDir()
	spool := filepath.Join(dir, "spool")
	if err := os.Mkdir(spool, 0o755); err != nil {
		t.Fatal(err)
	}
	u := New("t1", spool, s.URLs(t, dir, "t1", standard, large, partSize), 16)
	u.Backoff, u.MaxBackoff = time.Millisecond, 5*time.Millisecond
	return u, spool
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSyncReplacesGrowingFileAndSkipsUnchanged(t *testing.T) {
	s := s3fake.New(t)
	u, spool := newUploader(t, s, nil, 0)
	write(t, filepath.Join(spool, "stdout"), "one\n")

	if failed := u.Sync(context.Background(), 1); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}
	if b, _ := s.Object("t1", "stdout"); string(b) != "one\n" {
		t.Fatalf("object = %q", b)
	}
	before := s.Requests
	u.Sync(context.Background(), 1)
	if s.Requests != before {
		t.Fatalf("unchanged file was uploaded again")
	}

	f, _ := os.OpenFile(filepath.Join(spool, "stdout"), os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("two\n")
	f.Close()
	u.Sync(context.Background(), 1)
	if b, _ := s.Object("t1", "stdout"); string(b) != "one\ntwo\n" {
		t.Fatalf("object = %q", b)
	}
}

func TestMultipartUploadsPartsAndReplacesGrownTail(t *testing.T) {
	s := s3fake.New(t)
	u, spool := newUploader(t, s, map[string]int{"big": 8}, 8)
	big := filepath.Join(spool, "big")
	write(t, big, "aaaaaaaabbbbbbbbcccc") // 20 bytes > threshold 16: parts 8, 8, 4

	if failed := u.Sync(context.Background(), 1); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}
	if b, _ := s.Part("t1", "big", 3); string(b) != "cccc" {
		t.Fatalf("part 3 = %q", b)
	}
	if _, ok := s.Object("t1", "big"); ok {
		t.Fatal("multipart file was also PUT as a single object")
	}

	write(t, big, "aaaaaaaabbbbbbbbccccccccdd") // tail grows: part 3 replaced, part 4 new
	requests := s.Requests
	u.Sync(context.Background(), 1)
	if s.Requests-requests != 2 {
		t.Fatalf("expected 2 part uploads, got %d", s.Requests-requests)
	}
	if b, _ := s.Part("t1", "big", 3); string(b) != "cccccccc" {
		t.Fatalf("part 3 = %q", b)
	}
	if b, _ := s.Part("t1", "big", 4); string(b) != "dd" {
		t.Fatalf("part 4 = %q", b)
	}

	m, err := u.Finalize(context.Background(), context.Background())
	if err != nil || !m.Complete {
		t.Fatalf("finalize: %v %+v", err, m)
	}
	o := m.Objects["big"]
	if o.Method != "multipart" || o.UploadID != "U1" || len(o.Parts) != 4 || o.Parts[3].Size != 2 || o.Size != 26 {
		t.Fatalf("manifest object = %+v", o)
	}
}

func TestRetryOnServerError(t *testing.T) {
	s := s3fake.New(t)
	var calls atomic.Int32
	s.Fail = func(*http.Request) int {
		if calls.Add(1) <= 2 {
			return http.StatusInternalServerError
		}
		return 0
	}
	u, spool := newUploader(t, s, nil, 0)
	write(t, filepath.Join(spool, "stdout"), "x")
	if failed := u.Sync(context.Background(), 5); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestGivesUpAfterAttempts(t *testing.T) {
	s := s3fake.New(t)
	s.Fail = func(*http.Request) int { return http.StatusInternalServerError }
	u, spool := newUploader(t, s, nil, 0)
	write(t, filepath.Join(spool, "stdout"), "x")
	if failed := u.Sync(context.Background(), 2); len(failed) != 1 {
		t.Fatalf("failed = %v", failed)
	}
	if s.Requests != 2 {
		t.Fatalf("requests = %d, want 2", s.Requests)
	}
}

// An expired URL answers 403 until the controller re-signs: the new urls.json
// must be picked up on the next attempt.
func TestURLsAreReReadBetweenAttempts(t *testing.T) {
	s := s3fake.New(t)
	s.Fail = func(r *http.Request) int {
		if r.URL.Query().Get("sig") == "old" {
			return http.StatusForbidden
		}
		return 0
	}
	u, spool := newUploader(t, s, nil, 0)
	write(t, filepath.Join(spool, "stdout"), "x")
	data, _ := os.ReadFile(u.URLs)
	good := string(data)
	write(t, u.URLs, strings.ReplaceAll(good, "/t1/stdout", "/t1/stdout?sig=old"))

	go func() {
		time.Sleep(30 * time.Millisecond)
		write(t, u.URLs+".new", good)
		os.Rename(u.URLs+".new", u.URLs) // the kubelet swaps the file atomically
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if failed := u.Sync(ctx, 0); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}
	if _, ok := s.Object("t1", "stdout"); !ok {
		t.Fatal("stdout not uploaded after re-sign")
	}
}

func TestFinalizeListsMissingAndFlushRecovers(t *testing.T) {
	s := s3fake.New(t)
	u, spool := newUploader(t, s, nil, 0)
	write(t, filepath.Join(spool, "stdout"), "x")
	write(t, filepath.Join(spool, "extra.log"), "no url for this one")

	// A file without a URL entry is permanent: Finalize must not retry it until the budget ends.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	m, err := u.Finalize(ctx, ctx)
	if time.Since(start) > 3*time.Second {
		t.Fatalf("finalize retried a permanent failure for %s", time.Since(start))
	}
	if err != nil {
		t.Fatal(err)
	}
	if m.Complete || len(m.Missing) != 1 || m.Missing[0] != "extra.log" {
		t.Fatalf("manifest = %+v", m)
	}
	if b, ok := s.Object("t1", "manifest.json"); !ok || len(b) == 0 {
		t.Fatal("manifest.json not uploaded")
	}

	// A new Uploader (the flush step) with fresh URLs finds the old state in the spool.
	s.Requests = 0
	u2 := New("t1", spool, s.URLs(t, t.TempDir(), "t1", append(standard, "extra.log"), nil, 0), 16)
	u2.Backoff = time.Millisecond
	m, err = u2.Finalize(context.Background(), context.Background())
	if err != nil || !m.Complete {
		t.Fatalf("flush: %v %+v", err, m)
	}
	if s.Requests != 2 { // extra.log and the manifest only; stdout was confirmed before
		t.Fatalf("flush made %d requests, want 2", s.Requests)
	}
}

func TestClientErrorsAreNotRetriedExceptTransientOnes(t *testing.T) {
	for code, wantRequests := range map[int]int{
		http.StatusBadRequest:      1,
		http.StatusNotFound:        1,
		http.StatusForbidden:       3, // expired URL: may be re-signed
		http.StatusTooManyRequests: 3,
	} {
		s := s3fake.New(t)
		s.Fail = func(*http.Request) int { return code }
		u, spool := newUploader(t, s, nil, 0)
		write(t, filepath.Join(spool, "stdout"), "x")
		u.Sync(context.Background(), 3)
		if s.Requests != wantRequests {
			t.Errorf("status %d: %d requests, want %d", code, s.Requests, wantRequests)
		}
	}
}

// A part size change under the same upload id must not reuse the old parts.
func TestMultipartPartSizeChangeResetsState(t *testing.T) {
	s := s3fake.New(t)
	u, spool := newUploader(t, s, map[string]int{"big": 8}, 8)
	write(t, filepath.Join(spool, "big"), "aaaaaaaabbbbbbbbcccc") // 3 parts of 8
	if failed := u.Sync(context.Background(), 1); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}

	dir := t.TempDir()
	u.URLs = s.URLs(t, dir, "t1", standard, map[string]int{"big": 8}, 16) // same upload id U1, parts of 16
	s.Requests = 0
	if failed := u.Sync(context.Background(), 1); len(failed) != 0 {
		t.Fatalf("failed: %v", failed)
	}
	if s.Requests != 2 {
		t.Fatalf("part size change: %d part uploads, want 2", s.Requests)
	}
	m, err := u.Finalize(context.Background(), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if o := m.Objects["big"]; len(o.Parts) != 2 || o.PartSize != 16 {
		t.Fatalf("manifest object = %+v", o)
	}
}

// A file rewritten with the same size whose upload failed must not be listed as uploaded.
func TestManifestListsRewrittenSameSizeFileAsMissing(t *testing.T) {
	s := s3fake.New(t)
	u, spool := newUploader(t, s, nil, 0)
	path := filepath.Join(spool, "stdout")
	write(t, path, "aaaa")
	u.Sync(context.Background(), 1)
	write(t, path, "bbbb")
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	m, err := u.writeManifest()
	if err != nil {
		t.Fatal(err)
	}
	if m.Complete || len(m.Missing) != 1 || m.Missing[0] != "stdout" {
		t.Fatalf("manifest = %+v", m)
	}
}

func TestPartWithoutETagIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})) // 200, no ETag
	defer srv.Close()
	dir := t.TempDir()
	spool := filepath.Join(dir, "spool")
	os.Mkdir(spool, 0o755)
	f := urls.File{Version: 1, Objects: map[string]urls.Entry{
		"t1/big": {UploadID: "U1", PartSize: 8, Parts: []string{srv.URL + "/p1", srv.URL + "/p2"}},
	}}
	data, _ := json.Marshal(f)
	write(t, filepath.Join(dir, "urls.json"), string(data))
	u := New("t1", spool, filepath.Join(dir, "urls.json"), 4)
	u.Backoff = time.Millisecond
	write(t, filepath.Join(spool, "big"), "0123456789")
	if failed := u.Sync(context.Background(), 2); len(failed) != 1 {
		t.Fatalf("failed = %v", failed)
	}
	if st := u.state["big"]; len(st.Parts) != 0 {
		t.Fatalf("empty ETag recorded: %+v", st.Parts)
	}
}
