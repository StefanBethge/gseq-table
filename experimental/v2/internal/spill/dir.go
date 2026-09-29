// Package spill holds what the v2 engine needs to spill to disk (design
// decisions D6, D28): a directory per run that is marked in use by a lock
// (D38, D96) and only accessible to the running user (D56), and an encoding
// of blocks for spill files.
package spill

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// prefix starts the names of the run directories and their lock files in
// the spill root.
const prefix = "gtable-spill-"

// Dir is the spill directory of one run. Its lock file sits next to it and
// is locked while the directory is in use: by the run, and then by a result
// that holds spilled rejects until Close (D96).
type Dir struct {
	path string
	lock *os.File
	once sync.Once
	err  error
}

// Create creates a run directory in root, root itself if needed. The
// directory is 0700 and the files in it 0600 (D56).
func Create(root string) (*Dir, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	for range 10 {
		f, err := os.CreateTemp(root, prefix+"*.lock")
		if err != nil {
			return nil, fmt.Errorf("spill: %w", err)
		}
		// Another run cleaning up may have taken the new lock file before
		// this one locked it; it then removes it. Try a new one.
		if !lockOwn(f) || !linked(f) {
			f.Close()
			continue
		}
		path := strings.TrimSuffix(f.Name(), ".lock")
		if err := os.Mkdir(path, 0o700); err != nil {
			os.Remove(f.Name())
			f.Close()
			return nil, fmt.Errorf("spill: %w", err)
		}
		return &Dir{path: path, lock: f}, nil
	}
	return nil, errors.New("spill: no lock file could be locked")
}

// linked reports whether f is still the file at its path.
func linked(f *os.File) bool {
	a, err := f.Stat()
	if err != nil {
		return false
	}
	b, err := os.Stat(f.Name())
	return err == nil && os.SameFile(a, b)
}

// Path returns the path of the directory.
func (d *Dir) Path() string { return d.path }

// CreateFile creates a new file in the directory, readable and writable
// only by the running user.
func (d *Dir) CreateFile(pattern string) (*os.File, error) {
	f, err := os.CreateTemp(d.path, pattern)
	if err != nil {
		return nil, fmt.Errorf("spill: %w", err)
	}
	return f, nil
}

// Remove removes the directory with everything in it, then its lock file,
// and gives up the lock. Calling it again returns the first result.
func (d *Dir) Remove() error {
	d.once.Do(func() {
		d.err = os.RemoveAll(d.path)
		if err := os.Remove(d.lock.Name()); err != nil && d.err == nil {
			d.err = err
		}
		d.lock.Close() // closing gives up the lock
	})
	return d.err
}

// CleanOrphans removes the run directories in root whose lock no one holds:
// their process has ended, or their result was closed (D38, D96). It is
// best effort; a directory it cannot remove stays for the next run.
func CleanOrphans(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		path := filepath.Join(root, name)
		switch {
		case !e.IsDir() && strings.HasSuffix(name, ".lock"):
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				continue
			}
			if lockOrphan(f) {
				os.RemoveAll(strings.TrimSuffix(path, ".lock"))
				os.Remove(path)
			}
			f.Close()
		case e.IsDir():
			// A run creates its lock file first, so a directory without one
			// is left over from a clean-up that ended half way.
			if _, err := os.Stat(path + ".lock"); errors.Is(err, os.ErrNotExist) {
				os.RemoveAll(path)
			}
		}
	}
}
