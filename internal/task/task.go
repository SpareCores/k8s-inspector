// Package task runs one benchmark task for `sc-run exec`: it spools the
// output to node disk, uploads it while the task runs, enforces the timeout,
// forwards SIGTERM, and reports the result in result.json, manifest.json and
// the Kubernetes termination message.
package task

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/sparecores/k8s-inspector/internal/heartbeat"
	"github.com/sparecores/k8s-inspector/internal/upload"
)

// Exit codes written to result.json (docs/reference/exit-codes.md).
const (
	CodeHarnessError = 256
	CodeTimeout      = -6
)

// Range of the Config.Nice value.
const (
	MinNice = -20
	MaxNice = 19
)

const (
	versionTimeout  = 60 * time.Second
	heartbeatPeriod = 10 * time.Second
	maxTermMessage  = 4096
	maxTermText     = 256 // cap of version and error text in the termination message once it is too big
)

// cgroup v2 and procfs locations; variables so tests can point them at a temp dir.
var (
	cgroupRoot = "/sys/fs/cgroup"
	selfCgroup = "/proc/self/cgroup"
	selfOOMAdj = "/proc/self/oom_score_adj"
)

// Config describes one task invocation.
type Config struct {
	Task         string
	Spool        string
	URLs         string
	HeartbeatDir string // empty: no heartbeat
	Timeout      time.Duration
	VersionCmd   string   // run with sh -c before the task; stdout becomes the "version" file
	Collect      []string // "src:name": copy src into the spool as name after the task
	Reput        time.Duration
	UploadBudget time.Duration // final upload budget after the task ends
	KillAfter    time.Duration // SIGTERM to SIGKILL for the task
	// Grace is the whole shutdown, counted from the first SIGTERM: stopping the
	// task, the final upload and the manifest. Keep it below the Pod's
	// terminationGracePeriodSeconds. The last quarter is reserved for the manifest.
	Grace        time.Duration
	MultipartMin int64
	TermLog      string // termination message path
	Cmd          []string
	Rlimits      []string // "NAME=SOFT[:HARD]" limits for the task process only; see ParseRlimit
	Nice         int      // nice value of the task process, MinNice..MaxNice (see withNice); 0 leaves it unchanged
}

// Result is result.json and, with Upload, the termination message.
type Result struct {
	Task          string `json:"task"`
	ExitCode      int    `json:"exit_code"`
	ErrorMsg      string `json:"error_msg,omitempty"`
	TimedOut      bool   `json:"timed_out,omitempty"`
	Cancelled     bool   `json:"cancelled,omitempty"`
	OOMKills      int    `json:"oom_kills"`
	Start         string `json:"start"`
	End           string `json:"end"`
	Version       string `json:"version,omitempty"`
	KernelVersion string `json:"kernel_version,omitempty"`
	BootTime      int64  `json:"btime,omitempty"`
	Nice          *int   `json:"nice"` // nice the task process runs at; null when disabled or it could not be set
}

type termMessage struct {
	Result
	UploadComplete bool `json:"upload_complete"`
	Missing        int  `json:"missing"`
}

// Run executes the task and returns sc-run's own exit code: 0 whenever sc-run
// did its job (the task's exit code is in the result), 1 on a harness error.
func Run(cfg Config) int {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	return run(cfg, sigs)
}

func run(cfg Config, sigs <-chan os.Signal) int {
	if err := os.MkdirAll(cfg.Spool, 0o755); err != nil {
		log.Printf("spool: %v", err)
		return 1
	}
	if err := cleanSpool(cfg.Spool); err != nil {
		log.Printf("spool: %v", err)
		return 1
	}
	up := upload.New(cfg.Task, cfg.Spool, cfg.URLs, cfg.MultipartMin)

	// The first signal stops the task and starts one shutdown clock: syncCtx
	// ends a quarter of Grace before hardCtx, which leaves time for the manifest.
	stopCtx, stop := context.WithCancel(context.Background())
	hardCtx, hardCancel := context.WithCancel(context.Background())
	syncCtx, syncCancel := context.WithCancel(hardCtx)
	defer stop()
	defer hardCancel()
	defer syncCancel()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case sig := <-sigs:
			log.Printf("received %s, shutting down within %s", sig, cfg.Grace)
			stop()
			time.AfterFunc(cfg.Grace*3/4, syncCancel)
			time.AfterFunc(cfg.Grace, hardCancel)
		case <-finished:
		}
	}()

	bg, stopBG := context.WithCancel(context.Background())
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		background(bg, cfg, up)
	}()

	res := execute(stopCtx, cfg)

	stopBG()
	<-bgDone
	collect(cfg)
	if err := writeJSON(filepath.Join(cfg.Spool, "result.json"), res); err != nil {
		log.Printf("result.json: %v", err)
	}
	// What is known goes out first; the upload outcome updates it if time allows.
	writeTermMessage(cfg.TermLog, termMessage{Result: res})

	m, err := up.FinalizeWithin(syncCtx, hardCtx, cfg.UploadBudget)
	if err != nil {
		log.Printf("finalize: %v", err)
	}
	writeTermMessage(cfg.TermLog, termMessage{Result: res, UploadComplete: m.Complete && err == nil, Missing: len(m.Missing)})
	return 0
}

// cleanSpool removes the files of an earlier attempt, so a task never mixes
// its output or upload state with a previous run's.
func cleanSpool(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Type().IsRegular() {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// background touches the heartbeat and re-uploads growing files until ctx ends.
func background(ctx context.Context, cfg Config, up *upload.Uploader) {
	hb := time.NewTicker(heartbeatPeriod)
	defer hb.Stop()
	put := time.NewTicker(cfg.Reput)
	defer put.Stop()
	beat := func() {
		if cfg.HeartbeatDir != "" {
			if err := heartbeat.Touch(cfg.HeartbeatDir); err != nil {
				log.Printf("heartbeat: %v", err)
			}
		}
	}
	beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			beat()
		case <-put.C:
			up.Sync(ctx, 1)
		}
	}
}

// execute runs the version command and the task. stopCtx ends when SIGTERM or
// SIGINT arrived.
func execute(stopCtx context.Context, cfg Config) (res Result) {
	res = Result{Task: cfg.Task, Start: now(), KernelVersion: kernelVersion(), BootTime: bootTime()}
	defer func() { res.End = now() }()

	if cfg.VersionCmd != "" {
		res.Version = runVersion(stopCtx, cfg.VersionCmd)
		writeFile(filepath.Join(cfg.Spool, "version"), res.Version)
	}
	if stopCtx.Err() != nil {
		res.Cancelled, res.ExitCode, res.ErrorMsg = true, 128+int(syscall.SIGTERM), "cancelled"
		return res
	}

	// The child writes straight into the spool files: no pipes, so a background
	// grandchild holding them open cannot delay or fail the exit.
	stdout, err1 := os.Create(filepath.Join(cfg.Spool, "stdout"))
	stderr, err2 := os.Create(filepath.Join(cfg.Spool, "stderr"))
	if err := errors.Join(err1, err2); err != nil {
		res.ExitCode, res.ErrorMsg = CodeHarnessError, err.Error()
		return res
	}
	defer stdout.Close()
	defer stderr.Close()

	var limits []Rlimit
	for _, spec := range cfg.Rlimits {
		l, err := ParseRlimit(spec)
		if err != nil {
			res.ExitCode, res.ErrorMsg = CodeHarnessError, err.Error()
			return res
		}
		limits = append(limits, l)
	}

	if cfg.Nice < MinNice || cfg.Nice > MaxNice {
		res.ExitCode, res.ErrorMsg = CodeHarnessError, fmt.Sprintf("nice %d: want %d to %d", cfg.Nice, MinNice, MaxNice)
		return res
	}

	cmd := exec.Command(cfg.Cmd[0], cfg.Cmd[1:]...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	oomBefore := oomKills()
	startNiced := func() (err error) {
		res.Nice, err = withNice(cfg.Nice, cmd.Start)
		return err
	}
	if err := withRlimits(limits, startNiced); err != nil {
		res.ExitCode, res.ErrorMsg = CodeHarnessError, "cannot start command: "+err.Error()
		return res
	}
	protectFromOOM() // after Start: the task must not inherit oom_score_adj -1000
	pgid := cmd.Process.Pid
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	timeout := time.NewTimer(cfg.Timeout)
	defer timeout.Stop()
	var waitErr error
	select {
	case waitErr = <-done:
	case <-timeout.C:
		res.TimedOut = true
		waitErr = terminate(pgid, done, cfg.KillAfter)
	case <-stopCtx.Done():
		res.Cancelled = true
		waitErr = terminate(pgid, done, cfg.KillAfter)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL) // stragglers
	reap()

	res.OOMKills = oomKills() - oomBefore
	res.ExitCode, res.ErrorMsg = exitStatus(waitErr, res, cfg.Timeout)
	return res
}

// terminate sends SIGTERM to the task's process group, then SIGKILL after killAfter.
func terminate(pgid int, done <-chan error, killAfter time.Duration) error {
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	select {
	case err := <-done:
		return err
	case <-time.After(killAfter):
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return <-done
	}
}

// exitStatus maps the wait result to the exit code and message of
// docs/reference/exit-codes.md.
func exitStatus(waitErr error, res Result, timeout time.Duration) (int, string) {
	if res.TimedOut {
		return CodeTimeout, fmt.Sprintf("Execution timed out after %ds", int(timeout.Seconds()))
	}
	code := 0
	var ee *exec.ExitError
	if errors.As(waitErr, &ee) {
		code = ee.ExitCode()
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			code = 128 + int(ws.Signal())
		}
	} else if waitErr != nil {
		return CodeHarnessError, waitErr.Error()
	}
	switch {
	case res.Cancelled:
		return code, "cancelled"
	case code != 0 && res.OOMKills > 0:
		// The kernel killed something in the container's cgroup. The direct child
		// is usually resource-tracker, whose own exit code says little.
		return 137, "OOMKilled"
	}
	return code, ""
}

func runVersion(ctx context.Context, cmdline string) string {
	ctx, cancel := context.WithTimeout(ctx, versionTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) } // the whole group
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err != nil {
		log.Printf("version command: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// collect copies the files the task leaves elsewhere ("src:name") into the spool.
func collect(cfg Config) {
	for _, spec := range cfg.Collect {
		src, name, ok := strings.Cut(spec, ":")
		if !ok || name == "" || strings.Contains(name, "/") {
			log.Printf("collect %q: want src:name", spec)
			continue
		}
		if err := copyFile(src, filepath.Join(cfg.Spool, name)); err != nil {
			log.Printf("collect %s: %v", src, err)
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// writeTermMessage writes the compact result for the Kubernetes API (4 KiB
// max, result.json keeps the full text). Version text goes first, then the
// error text; if that is still too big, both are dropped.
func writeTermMessage(path string, m termMessage) {
	data, _ := json.Marshal(m)
	if len(data) > maxTermMessage {
		m.Version = truncate(m.Version, maxTermText)
		data, _ = json.Marshal(m)
	}
	if len(data) > maxTermMessage {
		m.ErrorMsg = truncate(m.ErrorMsg, maxTermText)
		data, _ = json.Marshal(m)
	}
	if len(data) > maxTermMessage {
		m.Version, m.ErrorMsg = "", ""
		data, _ = json.Marshal(m)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("termination message: %v", err)
	}
}

// truncate cuts s to at most n bytes without splitting a UTF-8 character.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeFile(path, s string) {
	if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
		log.Printf("%s: %v", path, err)
	}
}

// timeLayout is RFC 3339 in UTC with microseconds: sub-second tasks need a non-zero duration.
const timeLayout = "2006-01-02T15:04:05.000000Z07:00"

func now() string { return time.Now().UTC().Format(timeLayout) }

func kernelVersion() string {
	data, _ := os.ReadFile("/proc/sys/kernel/osrelease")
	return strings.TrimSpace(string(data))
}

// bootTime is the kernel boot time (btime in /proc/stat) as a Unix timestamp.
func bootTime() int64 {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "btime "); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return 0
}

// cgroupFile returns the path of a file in this container's own cgroup (v2),
// resolved from /proc/self/cgroup so it also works when the container sees the
// host hierarchy. It is "" on cgroup v1.
func cgroupFile(name string) string {
	data, err := os.ReadFile(selfCgroup)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			return filepath.Join(cgroupRoot, p, name)
		}
	}
	return ""
}

// oomKills reads the oom_kill counter of the container's cgroup, which counts
// every kill in it, whatever process the kernel picked.
func oomKills() int {
	path := cgroupFile("memory.events")
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) || path != "" {
			log.Printf("oom counter unavailable, OOM kills will not be reported: %v", err)
		}
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v, ok := strings.CutPrefix(line, "oom_kill "); ok {
			n, _ := strconv.Atoi(v)
			return n
		}
	}
	return 0
}

// protectFromOOM keeps an OOM kill of the task from taking sc-run with it:
// the kernel skips tasks with oom_score_adj -1000, also in a group kill
// (memory.oom.group=1, the kubelet's default). A failure only means the
// controller falls back to the container's OOMKilled reason.
func protectFromOOM() {
	if err := os.WriteFile(selfOOMAdj, []byte("-1000"), 0o644); err != nil {
		log.Printf("warning: oom_score_adj: %v", err)
	}
}

// reap collects orphaned children when sc-run is the container's PID 1.
func reap() {
	if os.Getpid() != 1 {
		return
	}
	for {
		var ws syscall.WaitStatus
		if pid, _ := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil); pid <= 0 {
			return
		}
	}
}
