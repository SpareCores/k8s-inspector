//go:build !linux

package task

import "errors"

func rlimitResource(name string) (int, bool) { return 0, true }

func withRlimits(specs []Rlimit, start func() error) error {
	if len(specs) > 0 {
		return errors.New("--rlimit is only supported on Linux")
	}
	return start()
}
