//go:build !linux

package task

func withNice(nice int, start func() error) (*int, error) { return nil, start() }
