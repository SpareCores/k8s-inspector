// Package s3fake is a minimal S3 stand-in for tests: it accepts PUT and
// UploadPart requests on any path and keeps the bodies.
package s3fake

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sparecores/k8s-inspector/internal/urls"
)

// Server records uploaded objects and parts.
type Server struct {
	*httptest.Server

	// Fail may return a non-zero HTTP status to inject a failure for a request.
	Fail func(r *http.Request) int

	mu       sync.Mutex
	Objects  map[string][]byte         // path -> body of PutObject
	Parts    map[string]map[int][]byte // path -> part number -> body
	Requests int
}

// New starts a fake S3 server that is closed when the test ends.
func New(t *testing.T) *Server {
	s := &Server{Objects: map[string][]byte{}, Parts: map[string]map[int][]byte{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests++
	if r.Method != http.MethodPut {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if s.Fail != nil {
		if code := s.Fail(r); code != 0 {
			http.Error(w, "injected", code)
			return
		}
	}
	sum := md5.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	if r.URL.Query().Has("uploadId") {
		var n int
		fmt.Sscan(r.URL.Query().Get("partNumber"), &n)
		if s.Parts[r.URL.Path] == nil {
			s.Parts[r.URL.Path] = map[int][]byte{}
		}
		s.Parts[r.URL.Path][n] = body
		return
	}
	s.Objects[r.URL.Path] = body
}

// Object returns the body stored for task/name.
func (s *Server) Object(task, name string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.Objects["/"+task+"/"+name]
	return b, ok
}

// Part returns the body of one uploaded part of task/name.
func (s *Server) Part(task, name string, n int) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.Parts["/"+task+"/"+name][n]
	return b, ok
}

// URLs writes a urls.json for task into dir. Every name in puts gets a PUT URL;
// every name in large gets PUT URL plus nParts UploadPart URLs of partSize bytes.
func (s *Server) URLs(t *testing.T, dir, task string, puts []string, large map[string]int, partSize int64) string {
	f := urls.File{Version: 1, Objects: map[string]urls.Entry{}}
	for _, n := range puts {
		f.Objects[task+"/"+n] = urls.Entry{Put: s.URL + "/" + task + "/" + n}
	}
	for n, parts := range large {
		e := urls.Entry{Put: s.URL + "/" + task + "/" + n, UploadID: "U1", PartSize: partSize}
		for i := 1; i <= parts; i++ {
			e.Parts = append(e.Parts, fmt.Sprintf("%s/%s/%s?uploadId=U1&partNumber=%d", s.URL, task, n, i))
		}
		f.Objects[task+"/"+n] = e
	}
	path := filepath.Join(dir, "urls.json")
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
