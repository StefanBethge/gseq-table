//go:build !strict

package table

import (
	"errors"
	"fmt"
)

// addErrf appends a formatted error to m's error list.
// When m.source is set, the error message is prefixed with "[source] ".
// Default (lenient) build: errors accumulate silently; use Errs() to inspect them.
// Build with -tags strict to panic instead and get a stack trace.
func (m *MutableTable) addErrf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if m.source != "" {
		msg = "[" + m.source + "] " + msg
	}
	m.errs = append(m.errs, errors.New(msg))
}
