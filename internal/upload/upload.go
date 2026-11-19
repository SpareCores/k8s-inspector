// Package upload sends a task's spool directory to object storage through
// presigned URLs. It never holds credentials: single objects go to a presigned
// PUT URL, large objects to presigned UploadPart URLs of a multipart upload
// that the controller created and will complete.
package upload

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sparecores/k8s-inspector/internal/urls"
)

const (
	stateFile    = ".uploaded.json"
	manifestFile = "manifest.json"
)

// Uploader syncs one task's spool directory. A mutex serialises the periodic
// and the final upload.
type Uploader struct {
	Task               string
	Spool              string
	URLs               string       // path of urls.json
	Client             *http.Client // per-request timeout lives here
	MultipartThreshold int64        // files above this use UploadPart when the entry has parts
	Backoff            time.Duration
	MaxBackoff         time.Duration
	ManifestTimeout    time.Duration // budget for the manifest, counted from the end of the sync phase

	mu    sync.Mutex
	state map[string]fileState
}

// fileState records what is confirmed uploaded for one file. It is persisted
// in the spool so a later `flush` step knows what is still missing.
type fileState struct {
	Mode     string `json:"mode"` // "put" or "multipart"
	Size     int64  `json:"size"`
	MtimeNS  int64  `json:"mtime_ns,omitempty"`
	MD5      string `json:"md5,omitempty"`
	ETag     string `json:"etag,omitempty"`
	UploadID string `json:"upload_id,omitempty"`
	PartSize int64  `json:"part_size,omitempty"`
	Parts    []Part `json:"parts,omitempty"`
}

// Part is one confirmed UploadPart.
type Part struct {
	N    int    `json:"n"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// Object is the manifest entry of an uploaded file.
type Object struct {
	Method   string `json:"method"`
	Size     int64  `json:"size"`
	MD5      string `json:"md5,omitempty"`
	ETag     string `json:"etag,omitempty"`
	UploadID string `json:"upload_id,omitempty"`
	PartSize int64  `json:"part_size,omitempty"`
	Parts    []Part `json:"parts,omitempty"`
}

// Manifest tells the controller what sc-run uploaded. It is written last.
type Manifest struct {
	Version  int               `json:"version"`
	Task     string            `json:"task"`
	Complete bool              `json:"complete"`
	Objects  map[string]Object `json:"objects"`
	Missing  []string          `json:"missing,omitempty"`
}

// errPermanent marks failures that retrying cannot fix (no URL entry, a bad
// urls.json, a 4xx answer other than an expired or throttled request).
var errPermanent = errors.New("permanent")

func permanent(err error) error { return fmt.Errorf("%w: %w", errPermanent, err) }

// New returns an Uploader with the default timeouts and retry delays.
func New(task, spool, urlsPath string, threshold int64) *Uploader {
	return &Uploader{
		Task:               task,
		Spool:              spool,
		URLs:               urlsPath,
		Client:             &http.Client{Timeout: 10 * time.Minute},
		MultipartThreshold: threshold,
		Backoff:            time.Second,
		MaxBackoff:         30 * time.Second,
		ManifestTimeout:    time.Minute,
	}
}

// Sync uploads every spool file whose content is not confirmed yet. Each
// request is tried up to attempts times (0 = until ctx ends). It returns the
// names of the files that failed and may succeed on a later pass. Permanent
// failures are logged and left to the manifest's missing list.
func (u *Uploader) Sync(ctx context.Context, attempts int) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.loadState(); err != nil {
		log.Printf("upload: %v", err)
	}
	names, err := u.files()
	if err != nil {
		log.Printf("upload: list spool: %v", err)
		return nil
	}
	var failed []string
	for _, name := range names {
		if err := u.syncFile(ctx, name, attempts); err != nil {
			log.Printf("upload %s: %v", name, err)
			if !errors.Is(err, errPermanent) {
				failed = append(failed, name)
			}
		}
	}
	return failed
}

// Finalize uploads everything, retrying whole passes with backoff until all
// retryable files are confirmed or syncCtx ends, then writes and uploads
// manifest.json under manifestCtx. The caller ends syncCtx earlier than
// manifestCtx, so a spent upload budget still leaves time for the manifest.
func (u *Uploader) Finalize(syncCtx, hardCtx context.Context) (Manifest, error) {
	delay := u.Backoff
	for {
		failed := u.Sync(syncCtx, 3)
		if len(failed) == 0 || syncCtx.Err() != nil {
			break
		}
		if !sleep(syncCtx, delay) {
			break
		}
		delay = min(delay*2, u.MaxBackoff)
	}
	// The manifest's own timeout starts now, not when the sync phase began.
	manifestCtx, cancel := context.WithTimeout(hardCtx, u.ManifestTimeout)
	defer cancel()
	m, err := u.writeManifest()
	if err != nil {
		return m, err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	err = u.retry(manifestCtx, 5, func() error { return u.putFile(manifestCtx, manifestFile) })
	return m, err
}

func (u *Uploader) files() ([]string, error) {
	entries, err := os.ReadDir(u.Spool)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if e.Type().IsRegular() && n != stateFile && n != manifestFile && !strings.HasPrefix(n, ".") && !strings.HasSuffix(n, ".tmp") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (u *Uploader) syncFile(ctx context.Context, name string, attempts int) error {
	info, err := os.Stat(filepath.Join(u.Spool, name))
	if err != nil {
		return err
	}
	entry, err := u.entry(name)
	if err != nil {
		return err
	}
	size := info.Size()
	st := u.state[name]
	if len(entry.Parts) > 0 && (size > u.MultipartThreshold || entry.Put == "") {
		return u.syncMultipart(ctx, name, size, attempts)
	}
	if st.Mode == "put" && st.Size == size && st.MtimeNS == info.ModTime().UnixNano() {
		return nil
	}
	return u.retry(ctx, attempts, func() error { return u.putFile(ctx, name) })
}

// putFile uploads the whole file to its presigned PUT URL and records it.
func (u *Uploader) putFile(ctx context.Context, name string) error {
	entry, err := u.entry(name)
	if err != nil {
		return err
	}
	if entry.Put == "" {
		return permanent(fmt.Errorf("no PUT URL for %s/%s", u.Task, name))
	}
	f, err := os.Open(filepath.Join(u.Spool, name))
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	h := md5.New()
	body := io.TeeReader(io.NewSectionReader(f, 0, info.Size()), h)
	etag, err := u.send(ctx, entry.Put, body, info.Size())
	if err != nil {
		return err
	}
	if name != manifestFile {
		u.state[name] = fileState{
			Mode: "put", Size: info.Size(), MtimeNS: info.ModTime().UnixNano(),
			MD5: hex.EncodeToString(h.Sum(nil)), ETag: etag,
		}
		return u.saveState()
	}
	return nil
}

// syncMultipart uploads every part whose current size differs from the
// confirmed one. A grown last part is simply uploaded again under its number,
// which replaces it.
func (u *Uploader) syncMultipart(ctx context.Context, name string, size int64, attempts int) error {
	entry, err := u.entry(name)
	if err != nil {
		return err
	}
	st := u.state[name]
	if st.Mode != "multipart" || st.UploadID != entry.UploadID || st.PartSize != entry.PartSize {
		st = fileState{Mode: "multipart", UploadID: entry.UploadID, PartSize: entry.PartSize}
	}
	n := int((size + entry.PartSize - 1) / entry.PartSize)
	if n > len(entry.Parts) {
		return fmt.Errorf("%s needs %d parts, URLs cover %d", name, n, len(entry.Parts))
	}
	st.Parts = st.Parts[:min(len(st.Parts), n)]
	f, err := os.Open(filepath.Join(u.Spool, name))
	if err != nil {
		return err
	}
	defer f.Close()
	for i := 0; i < n; i++ {
		off := int64(i) * entry.PartSize
		partLen := min(entry.PartSize, size-off)
		if len(st.Parts) > i && st.Parts[i].N == i+1 && st.Parts[i].Size == partLen {
			continue
		}
		var etag string
		err := u.retry(ctx, attempts, func() error {
			e, err := u.entry(name) // URLs may have been re-signed
			if err != nil {
				return err
			}
			if i >= len(e.Parts) || e.UploadID != entry.UploadID {
				return fmt.Errorf("part %d: upload changed in urls.json", i+1)
			}
			etag, err = u.send(ctx, e.Parts[i], io.NewSectionReader(f, off, partLen), partLen)
			if err == nil && etag == "" {
				err = fmt.Errorf("part %d: response has no ETag", i+1)
			}
			return err
		})
		if err != nil {
			u.state[name] = st
			_ = u.saveState()
			return fmt.Errorf("part %d: %w", i+1, err)
		}
		part := Part{N: i + 1, Size: partLen, ETag: etag}
		if len(st.Parts) > i {
			st.Parts[i] = part
		} else {
			st.Parts = append(st.Parts, part)
		}
	}
	st.Size = size
	u.state[name] = st
	return u.saveState()
}

// send PUTs size bytes from body to url and returns the ETag header.
func (u *Uploader) send(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	if size == 0 {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	resp, err := u.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("PUT: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
		if permanentStatus(resp.StatusCode, msg) {
			return "", permanent(err)
		}
		return "", err
	}
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// permanentStatus reports whether a 4xx answer will not change on a retry.
// 403 (expired URL, may be re-signed), 408 and 429 are transient, and so are
// S3's 400 answers with the error codes ExpiredToken, TokenRefreshRequired,
// RequestTimeout and IncompleteBody.
func permanentStatus(code int, body []byte) bool {
	switch code {
	case http.StatusForbidden, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	if code < 400 || code >= 500 {
		return false
	}
	for _, c := range []string{"ExpiredToken", "TokenRefreshRequired", "RequestTimeout", "IncompleteBody"} {
		if bytes.Contains(body, []byte("<Code>"+c+"</Code>")) {
			return false
		}
	}
	return true
}

// entry re-reads urls.json, so URLs re-signed during a step are picked up.
func (u *Uploader) entry(name string) (urls.Entry, error) {
	f, err := urls.Load(u.URLs)
	if errors.Is(err, fs.ErrNotExist) {
		return urls.Entry{}, err // the Secret mount may be mid-refresh: retry
	}
	if err != nil {
		return urls.Entry{}, permanent(err)
	}
	e, ok := f.Lookup(u.Task, name)
	if !ok {
		return e, permanent(fmt.Errorf("no URL for %s/%s", u.Task, name))
	}
	return e, nil
}

func (u *Uploader) retry(ctx context.Context, attempts int, op func() error) error {
	delay := u.Backoff
	for i := 1; ; i++ {
		err := op()
		if err == nil || errors.Is(err, errPermanent) {
			return err
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if attempts > 0 && i >= attempts {
			return err
		}
		if !sleep(ctx, delay) {
			return errors.Join(err, ctx.Err())
		}
		delay = min(delay*2, u.MaxBackoff)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// writeManifest lists what is confirmed uploaded and what is not, and writes
// manifest.json into the spool.
func (u *Uploader) writeManifest() (Manifest, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.loadState(); err != nil {
		return Manifest{}, err
	}
	names, err := u.files()
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Version: 1, Task: u.Task, Objects: map[string]Object{}}
	for _, name := range names {
		info, err := os.Stat(filepath.Join(u.Spool, name))
		if err != nil {
			return m, err
		}
		st, ok := u.state[name]
		if !ok || st.Size != info.Size() || (st.Mode == "put" && st.MtimeNS != info.ModTime().UnixNano()) {
			m.Missing = append(m.Missing, name)
			continue
		}
		m.Objects[name] = Object{
			Method: st.Mode, Size: st.Size, MD5: st.MD5, ETag: st.ETag,
			UploadID: st.UploadID, PartSize: st.PartSize, Parts: st.Parts,
		}
	}
	m.Complete = len(m.Missing) == 0
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, writeAtomic(filepath.Join(u.Spool, manifestFile), data)
}

func (u *Uploader) loadState() error {
	if u.state != nil {
		return nil
	}
	u.state = map[string]fileState{}
	data, err := os.ReadFile(filepath.Join(u.Spool, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &u.state)
}

func (u *Uploader) saveState() error {
	data, err := json.Marshal(u.state)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(u.Spool, stateFile), data)
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
