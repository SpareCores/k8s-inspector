//go:build !linux

package task

func withNice(nice int, start func() error) error { return start() }
