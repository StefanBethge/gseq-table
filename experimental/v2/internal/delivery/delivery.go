// Package delivery holds what the readers of the formats share: the
// fingerprint of a delivery file (design decisions D61, D81).
package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// prefixLen is the number of bytes at the start of a file that go into its
// fingerprint (D61).
const prefixLen = 64 << 10

// Fingerprint returns the fingerprint of the delivery in f under the source
// name: the first 16 hex characters of SHA-256 over name, size,
// modification time and the first 64 KiB (D61, D81). It reads with ReadAt
// and leaves the offset of f unchanged.
func Fingerprint(name string, f *os.File) (string, error) {
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "%d:%s;%d;%d;", len(name), name, st.Size(), st.ModTime().UnixNano())
	buf := make([]byte, min(st.Size(), prefixLen))
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
		return "", err
	}
	h.Write(buf)
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}
