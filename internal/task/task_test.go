package task

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/sparecores/k8s-inspector/internal/s3fake"
)

var files = []string{"stdout", "stderr", "version", "result.json", "manifest.json", "extra.txt"}

func setup(t *testing.T, cmd ...string) (Config, *s3fake.Server) {
	s := s3fake.New(t)
	dir := t.TempDir()
	return Config{
		Task:         "t1",
		Spool:        filepath.Join(dir, "spool"),
		URLs:         s.URLs(t, dir, "t1", files, nil, 0),
		Timeout:      10 * time.Second,
		Reput:        time.Hour,
		UploadBudget: 5 * time.Second,
		Grace:        2 * time.Second,
		MultipartMin: 1 << 20,
		TermLog:      filepath.Join(dir, "termination-log"),
		Cmd:          cmd,
	}, s
}

func termMsg(t *testing.T, cfg Config) termMessage {
	t.Helper()
	data, err := os.ReadFile(cfg.TermLog)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxTermMessage {
		t.Fatalf("termination message is %d bytes", len(data))
	}
	var m termMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSuccessUploadsOutputsAndManifest(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo out; echo err >&2; echo data > /dev/null")
	cfg.VersionCmd = "echo 1.2.3"
	if code := Run(cfg); code != 0 {
		t.Fatalf("sc-run exit = %d", code)
	}
	if b, _ := s.Object("t1", "stdout"); string(b) != "out\n" {
		t.Fatalf("stdout = %q", b)
	}
	if b, _ := s.Object("t1", "stderr"); string(b) != "err\n" {
		t.Fatalf("stderr = %q", b)
	}
	if b, _ := s.Object("t1", "version"); string(b) != "1.2.3" {
		t.Fatalf("version = %q", b)
	}
	b, ok := s.Object("t1", "manifest.json")
	if !ok {
		t.Fatal("no manifest")
	}
	var m struct {
		Complete bool
		Objects  map[string]struct{ Size int64 }
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if !m.Complete || m.Objects["stdout"].Size != 4 || m.Objects["result.json"].Size == 0 {
		t.Fatalf("manifest = %s", b)
	}
	tm := termMsg(t, cfg)
	if tm.ExitCode != 0 || !tm.UploadComplete || tm.Version != "1.2.3" {
		t.Fatalf("termination message = %+v", tm)
	}
}

func TestTaskExitCodeIsReportedNotReturned(t *testing.T) {
	cfg, _ := setup(t, "sh", "-c", "exit 3")
	if code := Run(cfg); code != 0 {
		t.Fatalf("sc-run exit = %d, want 0", code)
	}
	if tm := termMsg(t, cfg); tm.ExitCode != 3 {
		t.Fatalf("exit_code = %d", tm.ExitCode)
	}
}

func TestStartFailureIsHarnessError(t *testing.T) {
	cfg, _ := setup(t, "/nonexistent/binary")
	Run(cfg)
	if tm := termMsg(t, cfg); tm.ExitCode != CodeHarnessError || tm.ErrorMsg == "" {
		t.Fatalf("termination message = %+v", tm)
	}
}

func TestTimeoutKeepsPartialOutput(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo partial; sleep 30")
	cfg.Timeout = 300 * time.Millisecond
	start := time.Now()
	Run(cfg)
	if time.Since(start) > 5*time.Second {
		t.Fatal("timeout not enforced")
	}
	tm := termMsg(t, cfg)
	if tm.ExitCode != CodeTimeout || !tm.TimedOut || tm.ErrorMsg != "Execution timed out after 0s" {
		t.Fatalf("termination message = %+v", tm)
	}
	if b, _ := s.Object("t1", "stdout"); string(b) != "partial\n" {
		t.Fatalf("stdout = %q", b)
	}
}

func TestSIGTERMStopsTaskAndFlushes(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo started; sleep 30")
	go func() {
		time.Sleep(500 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
	start := time.Now()
	Run(cfg)
	if time.Since(start) > 5*time.Second {
		t.Fatal("task not stopped")
	}
	tm := termMsg(t, cfg)
	if !tm.Cancelled || tm.ExitCode != 143 || !tm.UploadComplete {
		t.Fatalf("termination message = %+v", tm)
	}
	if b, _ := s.Object("t1", "stdout"); string(b) != "started\n" {
		t.Fatalf("stdout = %q", b)
	}
}

func TestSIGTERMReachesGrandchildren(t *testing.T) {
	cfg, _ := setup(t, "sh", "-c", "sleep 30 & wait")
	go func() {
		time.Sleep(300 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
	start := time.Now()
	Run(cfg)
	if time.Since(start) > 5*time.Second {
		t.Fatal("process group not stopped")
	}
}

func TestCollectAndHeartbeat(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo extra > "+filepath.Join(t.TempDir(), "x")+"; sleep 0.1")
	src := filepath.Join(t.TempDir(), "pg.log")
	os.WriteFile(src, []byte("log line\n"), 0o644)
	cfg.Collect = []string{src + ":extra.txt"}
	cfg.HeartbeatDir = filepath.Join(t.TempDir(), "hb")
	Run(cfg)
	if b, _ := s.Object("t1", "extra.txt"); string(b) != "log line\n" {
		t.Fatalf("extra.txt = %q", b)
	}
	if _, err := os.Stat(filepath.Join(cfg.HeartbeatDir, "heartbeat")); err != nil {
		t.Fatal("no heartbeat file")
	}
}

func TestOOMKilledMessage(t *testing.T) {
	code, msg := exitStatus(nil, Result{OOMKills: 0}, time.Second)
	if code != 0 || msg != "" {
		t.Fatal(code, msg)
	}
	cmd := killedProcessErr(t)
	if code, msg := exitStatus(cmd, Result{OOMKills: 1}, time.Second); code != 137 || msg != "OOMKilled" {
		t.Fatalf("got %d %q", code, msg)
	}
	if code, msg := exitStatus(cmd, Result{}, time.Second); code != 137 || msg != "" {
		t.Fatalf("got %d %q", code, msg)
	}
}

// killedProcessErr returns the Wait error of a process that died from SIGKILL.
func killedProcessErr(t *testing.T) error {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cmd.Process.Kill()
	return cmd.Wait()
}
