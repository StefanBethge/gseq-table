//go:build !linux && !darwin

package memlimit

// Physical returns 0: the physical memory is unknown on this system.
func Physical() int64 { return 0 }
