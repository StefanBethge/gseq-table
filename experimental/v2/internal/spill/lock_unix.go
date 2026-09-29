//go:build unix

package spill

import (
	"os"
	"syscall"
)

// lockOwn locks the lock file of a new run directory without waiting. The
// system gives the lock up when f is closed or the process ends (D103).
func lockOwn(f *os.File) bool { return flock(f) }

// lockOrphan locks the lock file of another run's directory without
// waiting; it succeeds only if no one holds the lock.
func lockOrphan(f *os.File) bool { return flock(f) }

func flock(f *os.File) bool {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}
