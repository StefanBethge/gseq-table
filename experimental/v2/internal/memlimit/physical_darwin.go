package memlimit

import (
	"encoding/binary"
	"syscall"
)

// Physical returns the physical memory in bytes from sysctl hw.memsize, or
// 0.
func Physical() int64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0
	}
	// Sysctl drops a trailing zero byte; pad the value to eight bytes.
	var b [8]byte
	copy(b[:], s)
	return int64(binary.LittleEndian.Uint64(b[:]))
}
