package task

import (
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
)

// Linux asm-generic resource number that package syscall does not export.
const rlimitMemlock = 8

// rlimitResource maps the two names --rlimit accepts (the DB tasks need exactly these).
func rlimitResource(name string) (int, bool) {
	switch name {
	case "nofile":
		return syscall.RLIMIT_NOFILE, true
	case "memlock":
		return rlimitMemlock, true
	}
	return 0, false
}

// withRlimits runs start with the limits applied to sc-run, so the child
// inherits them, and puts sc-run's own limits back afterwards.
//
// Go's os/exec has no hook between fork and exec, and prlimit(2) on the child
// after Start would race with the child's first instructions. So the limits are
// set on sc-run for the duration of start (milliseconds) and restored right
// after; sc-run needs very few descriptors, so even a lower value is safe.
// Restoring works for soft limits and for hard limits that were not lowered. A
// lowered hard limit cannot be raised again without CAP_SYS_RESOURCE and stays
// on sc-run; that is harmless for the intended use, raising limits.
func withRlimits(specs []Rlimit, start func() error) error {
	type saved struct {
		name string
		res  int
		lim  syscall.Rlimit
	}
	var restore []saved
	defer func() {
		for _, s := range restore {
			if err := syscall.Setrlimit(s.res, &s.lim); err != nil {
				log.Printf("restore rlimit %s: %v (a lowered hard limit stays on sc-run)", s.name, err)
			}
		}
	}()
	for _, r := range specs {
		res, _ := rlimitResource(r.Name)
		var old syscall.Rlimit
		if err := syscall.Getrlimit(res, &old); err != nil {
			return fmt.Errorf("rlimit %s: %w", r.Name, err)
		}
		if err := syscall.Setrlimit(res, &syscall.Rlimit{Cur: r.Soft, Max: r.Hard}); err != nil {
			return fmt.Errorf("rlimit %s=%s:%s: %w%s", r.Name, fmtLimit(r.Soft), fmtLimit(r.Hard), err, rlimitHint(r, err))
		}
		restore = append(restore, saved{r.Name, res, old})
	}
	return start()
}

func rlimitHint(r Rlimit, err error) string {
	if err != syscall.EPERM {
		return ""
	}
	hint := "; raising a hard limit needs CAP_SYS_RESOURCE"
	if r.Name == "nofile" {
		if b, e := os.ReadFile("/proc/sys/fs/nr_open"); e == nil {
			hint += fmt.Sprintf(", and nofile cannot exceed the node's fs.nr_open (%s)", strings.TrimSpace(string(b)))
		}
	}
	return hint
}

func fmtLimit(v uint64) string {
	if v == rlimitInfinity {
		return "unlimited"
	}
	return fmt.Sprint(v)
}
