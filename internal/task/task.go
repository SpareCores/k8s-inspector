// Package task runs one benchmark task for `sc-run exec`: it spools the
// output to node disk, uploads it while the task runs, enforces the timeout,
// forwards SIGTERM, and reports the result in result.json, manifest.json and
// the Kubernetes termination message.
package task

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sparecores/k8s-inspector/internal/heartbeat"
	"github.com/sparecores/k8s-inspector/internal/upload"
)

// Exit codes written to result.json (docs/reference/exit-codes.md).
const (
	CodeHarnessError = 256
	CodeTimeout      = -6
)

const (
	killAfter       = 10 * time.Second // SIGTERM to SIGKILL for the task
	versionTimeout  = 60 * time.Second
	heartbeatPeriod = 10 * time.Second
	maxTermMessage  = 4096
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
	Grace        time.Duration // final upload budget after SIGTERM
	MultipartMin int64
	TermLog      string // termination message path
	Cmd          []string
	Uploader     *upload.Uploader // optional, for tests
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

	if err := os.MkdirAll(cfg.Spool, 0o755); err != nil {
		log.Printf("spool: %v", err)
		return 1
	}
	up := cfg.Uploader
	if up == nil {
		up = upload.New(cfg.Task, cfg.Spool, cfg.URLs, cfg.MultipartMin)
	}

	bg, stopBG := context.WithCancel(context.Background())
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		background(bg, cfg, up)
	}()

	res, signalled := execute(cfg, sigs)

	stopBG()
	<-bgDone
	collect(cfg)
	if err := writeJSON(filepath.Join(cfg.Spool, "result.json"), res); err != nil {
		log.Printf("result.json: %v", err)
	}

	budget := cfg.UploadBudget
	if signalled {
		budget = cfg.Grace
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	m, err := up.Finalize(ctx)
	if err != nil {
		log.Printf("finalize: %v", err)
	}
	writeTermMessage(cfg.TermLog, termMessage{Result: res, UploadComplete: m.Complete && err == nil, Missing: len(m.Missing)})
	return 0
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

// execute runs the version command and the task. signalled is true when
// SIGTERM or SIGINT arrived, which shortens the final upload budget.
func execute(cfg Config, sigs <-chan os.Signal) (res Result, signalled bool) {
	res = Result{Task: cfg.Task, Start: now(), KernelVersion: kernelVersion(), BootTime: bootTime()}
	defer func() { res.End = now() }()

	if cfg.VersionCmd != "" {
		res.Version = runVersion(cfg.VersionCmd)
		writeFile(filepath.Join(cfg.Spool, "version"), res.Version)
	}

	stdout, err1 := os.Create(filepath.Join(cfg.Spool, "stdout"))
	stderr, err2 := os.Create(filepath.Join(cfg.Spool, "stderr"))
	if err := errors.Join(err1, err2); err != nil {
		res.ExitCode, res.ErrorMsg = CodeHarnessError, err.Error()
		return res, false
	}
	defer stdout.Close()
	defer stderr.Close()

	cmd := exec.Command(cfg.Cmd[0], cfg.Cmd[1:]...)
	cmd.Stdout = io.MultiWriter(stdout, os.Stdout)
	cmd.Stderr = io.MultiWriter(stderr, os.Stderr)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 5 * time.Second // do not hang on pipes held by stray grandchildren

	oomBefore := oomKills()
	if err := cmd.Start(); err != nil {
		res.ExitCode, res.ErrorMsg = CodeHarnessError, "cannot start command: "+err.Error()
		return res, false
	}
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
		waitErr = terminate(pgid, done)
	case sig := <-sigs:
		log.Printf("received %s, stopping the task", sig)
		signalled, res.Cancelled = true, true
		waitErr = terminate(pgid, done)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL) // stragglers
	reap()

	res.OOMKills = oomKills() - oomBefore
	res.ExitCode, res.ErrorMsg = exitStatus(waitErr, res, cfg.Timeout)
	return res, signalled
}

// terminate sends SIGTERM to the task's process group, then SIGKILL after killAfter.
func terminate(pgid int, done <-chan error) error {
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
	case code == 137 && res.OOMKills > 0:
		return code, "OOMKilled"
	case res.Cancelled:
		return code, "cancelled"
	}
	return code, ""
}

func runVersion(cmdline string) string {
	ctx, cancel := context.WithTimeout(context.Background(), versionTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sh", "-c", cmdline).Output()
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

// writeTermMessage writes the compact result for the Kubernetes API (4 KiB max).
func writeTermMessage(path string, m termMessage) {
	data, _ := json.Marshal(m)
	if len(data) > maxTermMessage {
		m.ErrorMsg = ""
		data, _ = json.Marshal(m)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("termination message: %v", err)
	}
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

func now() string { return time.Now().UTC().Format(time.RFC3339) }

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

// oomKills reads the oom_kill counter of the container's cgroup (cgroup v2).
func oomKills() int {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.events")
	if err != nil {
		return 0
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if v, ok := strings.CutPrefix(string(line), "oom_kill "); ok {
			n, _ := strconv.Atoi(v)
			return n
		}
	}
	return 0
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
