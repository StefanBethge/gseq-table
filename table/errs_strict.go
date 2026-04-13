//go:build strict

package table

import "fmt"

// withErrf panics immediately with the formatted message.
// When t.source is set, the message is prefixed with "[source] ".
// Strict build: every invalid column access or unsupported operation panics so you
// get a full stack trace. Build normally (no tag) for silent accumulation.
func (t Table) withErrf(format string, args ...any) Table {
	msg := fmt.Sprintf(format, args...)
	if t.source != "" {
		msg = "[" + t.source + "] " + msg
	}
	panic(msg)
}
