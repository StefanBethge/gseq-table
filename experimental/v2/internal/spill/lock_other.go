//go:build !unix

package spill

import "os"

// Without flock a run holds no lock on its directory, and no run removes
// another run's directory (D96).

func lockOwn(*os.File) bool    { return true }
func lockOrphan(*os.File) bool { return false }
