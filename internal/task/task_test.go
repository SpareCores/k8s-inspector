package task

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
		KillAfter:    time.Second,
		Grace:        2 * time.Second,
		MultipartMin: 1 << 20,
		TermLog:      filepath.Join(dir, "termination-log"),
		Cmd:          cmd,
	}, s
}

// runSignalled runs the task and delivers SIGTERM after delay on a test-owned channel.
func runSignalled(cfg Config, delay time.Duration) int {
	sigs := make(chan os.Signal, 1)
	go func() {
		time.Sleep(delay)
		sigs <- syscall.SIGTERM
	}()
	return run(cfg, sigs)
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
	start := time.Now()
	runSignalled(cfg, 500*time.Millisecond)
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
	start := time.Now()
	runSignalled(cfg, 300*time.Millisecond)
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
	cmd := killedProcessErr(t)
	for _, c := range []struct {
		name     string
		err      error
		res      Result
		wantCode int
		wantMsg  string
	}{
		{"success", nil, Result{}, 0, ""},
		{"killed, no oom kill", cmd, Result{}, 137, ""},
		{"killed, oom kill", cmd, Result{OOMKills: 1}, 137, "OOMKilled"},
		{"oom kill below the direct child", &exec.ExitError{ProcessState: exitedWith(t, 1)}, Result{OOMKills: 2}, 137, "OOMKilled"},
		{"oom kill, child exited 0", nil, Result{OOMKills: 1}, 0, ""},
	} {
		code, msg := exitStatus(c.err, c.res, time.Second)
		if code != c.wantCode || msg != c.wantMsg {
			t.Errorf("%s: got %d %q, want %d %q", c.name, code, msg, c.wantCode, c.wantMsg)
		}
	}
}

func exitedWith(t *testing.T, code int) *os.ProcessState {
	cmd := exec.Command("sh", "-c", "exit 1")
	cmd.Run()
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != code {
		t.Fatal("cannot build a process state")
	}
	return cmd.ProcessState
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

// The oom_kill counter comes from the container's own cgroup (/proc/self/cgroup),
// not from the root of /sys/fs/cgroup.
func TestOOMKillsReadsOwnCgroup(t *testing.T) {
	dir := t.TempDir()
	cg := filepath.Join(dir, "kubepods", "pod1", "ctr")
	os.MkdirAll(cg, 0o755)
	os.WriteFile(filepath.Join(cg, "memory.events"), []byte("low 0\nmax 3\noom 2\noom_kill 2\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "memory.events"), []byte("oom_kill 99\n"), 0o644)
	self := filepath.Join(dir, "cgroup")
	os.WriteFile(self, []byte("0::/kubepods/pod1/ctr\n"), 0o644)
	oldRoot, oldSelf := cgroupRoot, selfCgroup
	cgroupRoot, selfCgroup = dir, self
	defer func() { cgroupRoot, selfCgroup = oldRoot, oldSelf }()
	if n := oomKills(); n != 2 {
		t.Fatalf("oomKills = %d, want 2", n)
	}
}

func TestProtectFromOOMWritesScoreOnly(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "cgroup"), []byte("0::/\n"), 0o644)
	oldRoot, oldSelf, oldAdj := cgroupRoot, selfCgroup, selfOOMAdj
	cgroupRoot, selfCgroup, selfOOMAdj = dir, filepath.Join(dir, "cgroup"), filepath.Join(dir, "adj")
	defer func() { cgroupRoot, selfCgroup, selfOOMAdj = oldRoot, oldSelf, oldAdj }()
	protectFromOOM()
	if _, err := os.Stat(filepath.Join(dir, "memory.oom.group")); err == nil {
		t.Fatal("memory.oom.group must not be written")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "adj")); string(b) != "-1000" {
		t.Fatalf("oom_score_adj = %q", b)
	}
}

// A background process that keeps the output files open must not turn a
// successful task into a harness error.
func TestBackgroundGrandchildDoesNotFailTask(t *testing.T) {
	cfg, _ := setup(t, "sh", "-c", "echo hi; (sleep 20 &); exit 0")
	start := time.Now()
	Run(cfg)
	if time.Since(start) > 4*time.Second {
		t.Fatalf("Run took %s", time.Since(start))
	}
	if tm := termMsg(t, cfg); tm.ExitCode != 0 || tm.ErrorMsg != "" {
		t.Fatalf("termination message = %+v", tm)
	}
}

func TestSIGTERMDuringVersionCommandStopsAtOnce(t *testing.T) {
	cfg, _ := setup(t, "sh", "-c", "touch "+filepath.Join(t.TempDir(), "ran"))
	cfg.VersionCmd = "sleep 30"
	start := time.Now()
	runSignalled(cfg, 200*time.Millisecond)
	if time.Since(start) > 4*time.Second {
		t.Fatalf("version command not interrupted: %s", time.Since(start))
	}
	if tm := termMsg(t, cfg); !tm.Cancelled {
		t.Fatalf("termination message = %+v", tm)
	}
	if _, err := os.Stat(filepath.Join(cfg.Spool, "stdout")); err == nil {
		t.Fatal("the task was started after SIGTERM")
	}
}

// SIGTERM after the task ended, while the final upload keeps failing: the
// shutdown deadline counts from the signal, not from UploadBudget.
func TestSIGTERMDuringFinalUploadEndsWithinGrace(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo x")
	s.Fail = func(*http.Request) int { return http.StatusInternalServerError }
	cfg.UploadBudget = time.Minute
	cfg.Grace = 800 * time.Millisecond
	start := time.Now()
	runSignalled(cfg, 300*time.Millisecond)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Run took %s", d)
	}
	tm := termMsg(t, cfg)
	if tm.UploadComplete {
		t.Fatalf("termination message = %+v", tm)
	}
	if _, err := os.Stat(filepath.Join(cfg.Spool, "manifest.json")); err != nil {
		t.Fatal("manifest.json not written locally")
	}
}

func TestStaleSpoolFromEarlierAttemptIsRemoved(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "echo new")
	os.MkdirAll(cfg.Spool, 0o755)
	os.WriteFile(filepath.Join(cfg.Spool, ".uploaded.json"), []byte(`{"stdout":{"mode":"put","size":4}}`), 0o644)
	os.WriteFile(filepath.Join(cfg.Spool, "extra.txt"), []byte("old run"), 0o644)
	Run(cfg)
	var m struct {
		Objects map[string]struct{ Size int64 }
	}
	b, _ := s.Object("t1", "manifest.json")
	json.Unmarshal(b, &m)
	if _, ok := m.Objects["extra.txt"]; ok {
		t.Fatalf("stale file in manifest: %s", b)
	}
	if b, _ := s.Object("t1", "stdout"); string(b) != "new\n" {
		t.Fatalf("stdout = %q", b)
	}
}

// A large collected file goes through UploadPart and is listed as multipart.
func TestLargeCollectedFileUsesMultipart(t *testing.T) {
	cfg, s := setup(t, "sh", "-c", "true")
	cfg.URLs = s.URLs(t, t.TempDir(), "t1", files, map[string]int{"extra.txt": 4}, 1<<20)
	src := filepath.Join(t.TempDir(), "big")
	os.WriteFile(src, make([]byte, 2<<20+512<<10), 0o644) // 2.5 MiB: 3 parts
	cfg.Collect = []string{src + ":extra.txt"}
	Run(cfg)
	b, _ := s.Object("t1", "manifest.json")
	var m struct {
		Complete bool
		Objects  map[string]struct {
			Method string
			Parts  []struct{ N int }
		}
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if o := m.Objects["extra.txt"]; !m.Complete || o.Method != "multipart" || len(o.Parts) != 3 {
		t.Fatalf("manifest = %s", b)
	}
}

func TestTermMessageStaysWithinLimit(t *testing.T) {
	cfg, _ := setup(t, "true")
	m := termMessage{Result: Result{Task: "t1", ExitCode: 3, Version: strings.Repeat("v", 20000), ErrorMsg: "boom"}}
	writeTermMessage(cfg.TermLog, m)
	got := termMsg(t, cfg)
	if got.ExitCode != 3 || got.ErrorMsg != "boom" || got.Version == "" {
		t.Fatalf("termination message = %+v", got)
	}
}

func TestParseRlimit(t *testing.T) {
	const inf = ^uint64(0)
	good := map[string]Rlimit{
		"nofile=4096":         {"nofile", 4096, 4096},
		"nofile=1024:1048576": {"nofile", 1024, 1048576},
		"memlock=unlimited":   {"memlock", inf, inf},
		"memlock=0:unlimited": {"memlock", 0, inf},
	}
	for spec, want := range good {
		got, err := ParseRlimit(spec)
		if err != nil || got != want {
			t.Errorf("%s: got %+v, %v; want %+v", spec, got, err, want)
		}
	}
	for _, spec := range []string{"", "nofile", "bogus=1", "core=0", "nproc=10", "cpu=10", "nofile=-1", "nofile=x", "nofile=", "nofile=1:", "nofile=10:5", "nofile=1:2:3"} {
		if _, err := ParseRlimit(spec); err == nil {
			t.Errorf("%q: want error", spec)
		}
	}
}

func TestInvalidRlimitIsHarnessError(t *testing.T) {
	cfg, _ := setup(t, "true")
	cfg.Rlimits = []string{"nofile=abc"}
	run(cfg, make(chan os.Signal))
	if tm := termMsg(t, cfg); tm.ExitCode != CodeHarnessError || !strings.Contains(tm.ErrorMsg, "rlimit") {
		t.Fatalf("got %+v", tm.Result)
	}
}

func TestRlimitAppliesToChildOnly(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	var before syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &before); err != nil {
		t.Fatal(err)
	}
	cfg, _ := setup(t, "sh", "-c", "ulimit -n; ulimit -Hn")
	// Keep the hard limit so that sc-run's soft limit can be restored.
	cfg.Rlimits = []string{fmt.Sprintf("nofile=4096:%d", before.Max)}
	run(cfg, make(chan os.Signal))
	out, err := os.ReadFile(filepath.Join(cfg.Spool, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(out)); got != "4096\n"+strconv.FormatUint(before.Max, 10) {
		t.Fatalf("child limits: %q", got)
	}
	var after syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &after); err != nil {
		t.Fatal(err)
	}
	if after.Cur != before.Cur {
		t.Fatalf("soft nofile of the test process changed: %d -> %d", before.Cur, after.Cur)
	}
}
