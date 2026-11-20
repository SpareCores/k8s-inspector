package task

import (
	"fmt"
	"strconv"
	"strings"
)

// Rlimit is one parsed --rlimit NAME=SOFT[:HARD] flag.
type Rlimit struct {
	Name       string
	Soft, Hard uint64
}

const rlimitInfinity = ^uint64(0)

// ParseRlimit parses "NAME=SOFT[:HARD]". Values are non-negative integers or
// "unlimited"; HARD defaults to SOFT.
func ParseRlimit(spec string) (Rlimit, error) {
	name, val, ok := strings.Cut(spec, "=")
	if !ok {
		return Rlimit{}, fmt.Errorf("rlimit %q: want NAME=SOFT[:HARD]", spec)
	}
	if _, known := rlimitResource(name); !known {
		return Rlimit{}, fmt.Errorf("rlimit %q: unknown name %q (nofile, memlock, nproc, stack, core, as, fsize, data, cpu)", spec, name)
	}
	softS, hardS, hasHard := strings.Cut(val, ":")
	if !hasHard {
		hardS = softS
	}
	soft, err := parseRlimitValue(softS)
	if err != nil {
		return Rlimit{}, fmt.Errorf("rlimit %q: soft: %w", spec, err)
	}
	hard, err := parseRlimitValue(hardS)
	if err != nil {
		return Rlimit{}, fmt.Errorf("rlimit %q: hard: %w", spec, err)
	}
	if soft > hard {
		return Rlimit{}, fmt.Errorf("rlimit %q: soft limit above hard limit", spec)
	}
	return Rlimit{Name: name, Soft: soft, Hard: hard}, nil
}

func parseRlimitValue(s string) (uint64, error) {
	if s == "unlimited" {
		return rlimitInfinity, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == rlimitInfinity {
		return 0, fmt.Errorf("%q is not a non-negative integer or \"unlimited\"", s)
	}
	return v, nil
}
