//go:build !strict

package table

import (
	"errors"
	"fmt"
)

// withErrf returns a shallow copy of t with one more error appended.
// When t.source is set, the error message is prefixed with "[source] ".
// Default (lenient) build: errors accumulate silently; use Errs() to inspect them.
// Build with -tags strict to panic instead and get a stack trace.
func (t Table) withErrf(format string, args ...any) Table {
	msg := fmt.Sprintf(format, args...)
	if t.source != "" {
		msg = "[" + t.source + "] " + msg
	}
	errs := make([]error, len(t.errs)+1)
	copy(errs, t.errs)
	errs[len(t.errs)] = errors.New(msg)
	t.errs = errs
	return t
}
