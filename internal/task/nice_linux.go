package task

import (
	"log"
	"runtime"
	"syscall"
)

// withNice runs start with the calling thread at the given nice value, so the
// child that start forks inherits it, and puts the thread's nice back afterwards.
//
// On Linux nice is per thread: setpriority(PRIO_PROCESS, 0, ...) changes only
// the calling thread, so sc-run's other threads (uploader, heartbeat) keep their
// priority. The child inherits the nice of the thread that forks it. Go 1.27
// documents this for a goroutine locked to its thread (os/exec Cmd.Run: "the new
// process will inherit the caller's thread state"), and the code agrees:
// exec.Cmd.Start -> os.StartProcess -> syscall.forkExec -> forkAndExecInChild1
// (syscall/exec_linux.go) issues clone/clone3 directly through rawVforkSyscall on
// the calling goroutine, between runtime_BeforeFork and runtime_AfterFork, with
// no hand-off to another thread.
//
// Failing to set the nice value (no CAP_SYS_NICE) is only a warning: the
// benchmark still runs, just without the priority boost. 0 disables nice.
// It returns the nice value actually applied, nil when it was disabled or failed.
//
// If putting the thread's nice back fails, the thread is NOT unlocked: unlocked,
// the Go scheduler would hand the -20 thread to arbitrary goroutines. A locked
// goroutine that ends takes its thread with it.
func withNice(nice int, start func() error) (applied *int, err error) {
	if nice == 0 {
		return nil, start()
	}
	runtime.LockOSThread()
	old, err := syscall.Getpriority(syscall.PRIO_PROCESS, 0)
	if err == nil {
		// The raw syscall returns 20-nice; convert back.
		old = 20 - old
		err = syscall.Setpriority(syscall.PRIO_PROCESS, 0, nice)
	}
	if err != nil {
		runtime.UnlockOSThread() // the thread's nice is unchanged
		log.Printf("warning: cannot set nice %d for the task: %v", nice, err)
		return nil, start()
	}
	defer func() {
		if err := syscall.Setpriority(syscall.PRIO_PROCESS, 0, old); err != nil {
			log.Printf("warning: cannot restore nice %d: %v (this sc-run thread stays at %d and stays locked to its goroutine)", old, err, nice)
			return
		}
		runtime.UnlockOSThread()
	}()
	return &nice, start()
}
