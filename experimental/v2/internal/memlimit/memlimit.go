// Package memlimit detects the memory the process may use: the limit of its
// cgroup, as in a container, or else the physical memory (design decision
// D90). The engine takes its default memory budget and the value for
// GOMEMLIMIT from it (D90, D91).
package memlimit

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Detect returns the memory limit of the process in bytes, or 0 if it is
// unknown.
func Detect() int64 { return DetectFrom("/sys/fs/cgroup", "/proc/self/cgroup", Physical()) }

// DetectFrom returns the limit of the cgroup found under root for the
// process described by the cgroup file self (as /proc/self/cgroup), if it
// is below phys, and else phys. phys 0 means unknown physical memory.
func DetectFrom(root, self string, phys int64) int64 {
	if l, ok := cgroupLimit(root, self); ok && (phys <= 0 || l < phys) {
		return l
	}
	return phys
}

// cgroupLimit reads the memory limit of cgroup v2 (memory.max) or v1
// (memory.limit_in_bytes). "max" and v1's huge "no limit" are no limit.
func cgroupLimit(root, self string) (int64, bool) {
	var v2, v1 []string
	if data, err := os.ReadFile(self); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) != 3 {
				continue
			}
			switch {
			case parts[0] == "0" && parts[1] == "":
				v2 = append(v2, filepath.Join(root, parts[2], "memory.max"))
			case hasController(parts[1], "memory"):
				v1 = append(v1, filepath.Join(root, "memory", parts[2], "memory.limit_in_bytes"))
			}
		}
	}
	v2 = append(v2, filepath.Join(root, "memory.max"))
	v1 = append(v1, filepath.Join(root, "memory", "memory.limit_in_bytes"))
	for _, path := range append(v2, v1...) {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(data))
		if s == "max" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n >= 1<<62 {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

func hasController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name {
			return true
		}
	}
	return false
}
