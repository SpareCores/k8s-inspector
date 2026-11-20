// Command sc-run is the single static binary that runs inside every
// k8s-inspector Pod on a benchmark node.
//
//	sc-run hold  [--install DIR] --heartbeat-dir DIR [--idle-timeout 45m]   anchor process
//	sc-run exec  --task T --spool DIR --urls FILE [flags] -- CMD...         one task
//	sc-run flush --task T --spool DIR --urls FILE                           re-upload the spool
//	sc-run install DIR | ready DIR | heartbeat DIR                          helpers
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sparecores/k8s-inspector/internal/heartbeat"
	"github.com/sparecores/k8s-inspector/internal/hold"
	"github.com/sparecores/k8s-inspector/internal/task"
	"github.com/sparecores/k8s-inspector/internal/upload"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetPrefix("sc-run: ")
	if len(os.Args) < 2 {
		fatalf("usage: sc-run hold|exec|flush|install|ready|heartbeat ...")
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "hold":
		runHold(args)
	case "exec":
		os.Exit(runExec(args))
	case "flush":
		runFlush(args)
	case "install":
		check(hold.Install(oneArg(args)))
	case "ready":
		check(hold.Ready(oneArg(args)))
	case "heartbeat":
		check(heartbeat.Touch(oneArg(args)))
	default:
		fatalf("unknown subcommand %q", os.Args[1])
	}
}

func runHold(args []string) {
	fs := flag.NewFlagSet("hold", flag.ExitOnError)
	install := fs.String("install", "", "directory to install the sc-run binary into")
	hb := fs.String("heartbeat-dir", "", "directory holding the heartbeat file")
	idle := fs.Duration("idle-timeout", 45*time.Minute, "exit when no heartbeat was seen for this long")
	poll := fs.Duration("poll", 10*time.Second, "how often to check the heartbeat")
	check(fs.Parse(args))
	if *hb == "" {
		fatalf("hold: --heartbeat-dir is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	check(hold.Run(ctx, *install, *hb, *idle, *poll))
}

func runExec(args []string) int {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	cfg := task.Config{}
	var collect, rlimits stringList
	fs.StringVar(&cfg.Task, "task", "", "task name (prefix of its keys in urls.json)")
	fs.StringVar(&cfg.Spool, "spool", "", "spool directory on the node")
	fs.StringVar(&cfg.URLs, "urls", "", "presigned URL file (urls.json)")
	fs.StringVar(&cfg.HeartbeatDir, "heartbeat-dir", "", "directory of the anchor's heartbeat file")
	fs.DurationVar(&cfg.Timeout, "timeout", 30*time.Minute, "task timeout")
	fs.StringVar(&cfg.VersionCmd, "version-cmd", "", "shell command whose output is the tool version (60 s cap)")
	fs.Var(&collect, "collect", "extra file to upload, src:name (repeatable)")
	fs.Var(&rlimits, "rlimit", "resource limit of the task, NAME=SOFT[:HARD] (repeatable; nofile or memlock; integer or unlimited)")
	fs.DurationVar(&cfg.Reput, "reput", 60*time.Second, "interval of re-uploading growing files")
	fs.DurationVar(&cfg.UploadBudget, "upload-budget", 10*time.Minute, "final upload budget")
	fs.DurationVar(&cfg.KillAfter, "kill-after", 5*time.Second, "SIGTERM to SIGKILL for the task")
	fs.DurationVar(&cfg.Grace, "grace", 15*time.Second, "whole shutdown after SIGTERM (task, upload, manifest); below terminationGracePeriodSeconds")
	mib := fs.Int64("multipart-mib", 32, "files above this size use UploadPart URLs (parts are at least 5 MiB)")
	fs.StringVar(&cfg.TermLog, "termination-log", "/dev/termination-log", "termination message path")
	check(fs.Parse(args))
	cfg.Collect = collect
	cfg.Rlimits = rlimits
	cfg.MultipartMin = *mib << 20
	cfg.Cmd = fs.Args()
	if cfg.Task == "" || cfg.Spool == "" || cfg.URLs == "" || len(cfg.Cmd) == 0 {
		fatalf("exec: --task, --spool, --urls and a command after -- are required")
	}
	return task.Run(cfg)
}

func runFlush(args []string) {
	fs := flag.NewFlagSet("flush", flag.ExitOnError)
	taskName := fs.String("task", "", "task name")
	spool := fs.String("spool", "", "spool directory")
	urls := fs.String("urls", "", "presigned URL file")
	mib := fs.Int64("multipart-mib", 32, "files above this size use UploadPart URLs")
	budget := fs.Duration("upload-budget", 10*time.Minute, "upload budget")
	check(fs.Parse(args))
	if *taskName == "" || *spool == "" || *urls == "" {
		fatalf("flush: --task, --spool and --urls are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *budget)
	defer cancel()
	m, err := upload.New(*taskName, *spool, *urls, *mib<<20).Finalize(ctx, context.Background())
	check(err)
	if !m.Complete {
		fatalf("flush: %d file(s) still missing: %s", len(m.Missing), strings.Join(m.Missing, ", "))
	}
}

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func oneArg(args []string) string {
	if len(args) != 1 {
		fatalf("want exactly one directory argument")
	}
	return args[0]
}

func check(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "sc-run: "+format+"\n", a...)
	os.Exit(1)
}
