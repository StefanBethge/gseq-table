//go:build strict

package table

import "fmt"

// addErrf panics immediately with the formatted message.
// When m.source is set, the message is prefixed with "[source] ".
// Strict build: every invalid column access or unsupported operation panics so you
// get a full stack trace. Build normally (no tag) for silent accumulation.
func (m *MutableTable) addErrf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if m.source != "" {
		msg = "[" + m.source + "] " + msg
	}
	panic(msg)
}
