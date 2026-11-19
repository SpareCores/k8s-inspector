// Package urls reads the presigned URL file that the controller writes into
// the run's Secret. The kubelet refreshes the mounted file when the Secret is
// re-signed, so callers load it again before every request.
package urls

import (
	"encoding/json"
	"fmt"
	"os"
)

// Entry holds the URLs for one object. Put is a presigned PutObject URL.
// Parts, when present, are presigned UploadPart URLs (part numbers 1..n, in
// order) for the multipart upload UploadID, each PartSize bytes except the
// last one.
type Entry struct {
	Put      string   `json:"put,omitempty"`
	UploadID string   `json:"upload_id,omitempty"`
	PartSize int64    `json:"part_size,omitempty"`
	Parts    []string `json:"parts,omitempty"`
}

// File is the content of urls.json. Objects are keyed "<task>/<file name>",
// where the file name is relative to the task's spool directory.
type File struct {
	Version int              `json:"version"`
	Objects map[string]Entry `json:"objects"`
}

// Load reads and validates the file at path.
func Load(path string) (File, error) {
	var f File
	data, err := os.ReadFile(path)
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return f, fmt.Errorf("parse %s: %w", path, err)
	}
	if f.Version != 1 {
		return f, fmt.Errorf("%s: unsupported version %d", path, f.Version)
	}
	return f, nil
}

// Lookup returns the entry for a file of a task.
func (f File) Lookup(task, name string) (Entry, bool) {
	e, ok := f.Objects[task+"/"+name]
	return e, ok
}
