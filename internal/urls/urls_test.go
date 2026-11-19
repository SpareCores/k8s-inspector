package urls

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func load(t *testing.T, content string) error {
	path := filepath.Join(t.TempDir(), "urls.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	return err
}

func TestLoadRejectsPartsWithoutPartSize(t *testing.T) {
	err := load(t, `{"version":1,"objects":{"t/big":{"upload_id":"U","parts":["u1"]}}}`)
	if err == nil || !strings.Contains(err.Error(), "part_size") {
		t.Fatalf("err = %v", err)
	}
	if err := load(t, `{"version":1,"objects":{"t/big":{"upload_id":"U","part_size":8,"parts":["u1"]},"t/x":{"put":"u"}}}`); err != nil {
		t.Fatal(err)
	}
}
