package gtable

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Error codes of rejected rows, from the fixed core of D14 and D54. Closures
// can set their own code under "custom:" (see CustomError).
const (
	CodeParse  = "parse"  // a value could not be cast (D29)
	CodeExpr   = "expr"   // an expression failed at run time (D54)
	CodeCustom = "custom" // a closure returned an error or panicked (D32, D39)

	CodeUnparseableLine = "unparseable_line" // a reader could not split a line into cells (D10)
	CodeFieldTooLarge   = "field_too_large"  // a field is over the limit (D56)

	// Delivery errors (D42).
	CodeMissingColumn = "missing_column"
	CodeMissingSheet  = "missing_sheet"
	CodeMissingFile   = "missing_file"
	CodeUnreadable    = "unreadable"
	CodeTruncated     = "truncated"
)

// Reject is one error of a rejected row: the step that rejected it, the
// affected column, the value at the time of the error, the reason and the
// code (D14). Errors of one row in one step share the ID, the reject_id
// (D15). The rejected rows themselves, with raw state and location, are
// tables; see Table.RejectedRows.
type Reject struct {
	ID     string
	Step   string
	Column string
	// Value is the value of Column when the row failed; HasValue is false if
	// there was none (null, or a column the step was about to derive).
	Value    string
	HasValue bool
	Reason   string
	Code     string
}

func (r Reject) String() string {
	v := "null"
	if r.HasValue {
		v = strconv.Quote(r.Value)
	}
	return fmt.Sprintf("step=%s column=%s value=%s code=%s reason=%s", r.Step, r.Column, v, r.Code, r.Reason)
}

// ErrorMode is the error behavior for delivery and data errors (D3, D19):
// reject the rows and go on, or stop at the first error. The default is
// ModeReject (D5).
type ErrorMode uint8

const (
	ModeReject ErrorMode = iota
	ModeStop
)

func (m ErrorMode) String() string {
	if m == ModeStop {
		return "stop"
	}
	return "reject"
}

// ErrorKind is the kind of an error whose behavior is configurable (D19).
// Plan errors are not: the run never starts.
type ErrorKind uint8

const (
	KindData     ErrorKind = iota // one row: a value that does not parse, a failed expression
	KindDelivery                  // the build of a delivery, such as a missing column (D42)
)

func (k ErrorKind) String() string {
	if k == KindDelivery {
		return "delivery"
	}
	return "data"
}

// kindOfCode returns the kind of the errors with the given code.
func kindOfCode(code string) ErrorKind {
	switch code {
	case CodeMissingColumn, CodeMissingSheet, CodeMissingFile, CodeUnreadable, CodeTruncated:
		return KindDelivery
	}
	return KindData
}

// errorPolicy is the error behavior of a pipeline or table: a mode per
// code, else per kind, else the general mode (D19). It is changed by copy,
// so tables that share one stay independent.
type errorPolicy struct {
	mode  ErrorMode
	kinds map[ErrorKind]ErrorMode
	codes map[string]ErrorMode
}

func (p errorPolicy) modeFor(code string) ErrorMode {
	if m, ok := p.codes[code]; ok {
		return m
	}
	if m, ok := p.kinds[kindOfCode(code)]; ok {
		return m
	}
	return p.mode
}

func (p errorPolicy) withKind(k ErrorKind, m ErrorMode) errorPolicy {
	kinds := make(map[ErrorKind]ErrorMode, len(p.kinds)+1)
	for kk, mm := range p.kinds {
		kinds[kk] = mm
	}
	kinds[k] = m
	p.kinds = kinds
	return p
}

func (p errorPolicy) withCode(code string, m ErrorMode) errorPolicy {
	codes := make(map[string]ErrorMode, len(p.codes)+1)
	for c, mm := range p.codes {
		codes[c] = mm
	}
	codes[code] = m
	p.codes = codes
	return p
}

// PlanError is an error in the pipeline itself: an unknown column, a type
// conflict or an invalid parameter. It is found before any row is read, and
// nothing runs (D19).
type PlanError struct {
	Step string
	Err  error
}

func (e *PlanError) Error() string { return fmt.Sprintf("plan error in step %s: %v", e.Step, e.Err) }
func (e *PlanError) Unwrap() error { return e.Err }

// DataError is the first error of a run whose code is set to ModeStop (D3,
// D19, D50).
type DataError struct {
	Reject Reject
}

func (e *DataError) Error() string { return "data error: " + e.Reject.String() }

// CustomError is a typed error a closure returns to reject a row with its
// own code "custom:<Code>" instead of "custom" (D54).
type CustomError struct {
	Code string
	Err  error
}

func (e *CustomError) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return e.Err.Error()
}

func (e *CustomError) Unwrap() error { return e.Err }

// customCode returns the reject code for an error returned by a closure.
func customCode(err error) string {
	var ce *CustomError
	if errors.As(err, &ce) && ce.Code != "" {
		return CodeCustom + ":" + ce.Code
	}
	return CodeCustom
}

// rejectEntry is one error of a rejected row with what the reject tables
// need: the run, the origin of the row, the failing step and, for an
// aggregated row, its values (D77).
type rejectEntry struct {
	Reject
	runID string
	orig  origin
	step  *stepRef
	snap  *snapshot
}

// stepRef identifies one step of one run; steps of the same name stay
// apart (D77).
type stepRef struct{ name string }

// snapshot holds the values of an aggregated row as it went into the
// failing step: one row with its schema.
type snapshot struct {
	s   schema
	blk block.Block
}

// rejector collects the rejects of one run under its error policy. An
// error whose code is set to ModeStop becomes a DataError, or a
// DeliveryError for a delivery error.
type rejector struct {
	policy  errorPolicy
	run     *runInfo
	entries []rejectEntry
}

func (rx *rejector) add(e rejectEntry) error {
	if rx.policy.modeFor(e.Code) == ModeStop {
		if kindOfCode(e.Code) == KindDelivery {
			src := ""
			if len(e.orig.refs) > 0 {
				src = e.orig.refs[0].src.name
			}
			return &DeliveryError{Source: src, Code: e.Code, Err: errors.New(e.Reason)}
		}
		return &DataError{Reject: e.Reject}
	}
	rx.entries = append(rx.entries, e)
	return nil
}

// formatCell returns cell i of v as text: integers in decimal, floats in the
// shortest form, timestamps in RFC 3339.
func formatCell(k block.Kind, v *vec, i int) string {
	switch k {
	case block.Text:
		return v.texts[i]
	case block.Int:
		return strconv.FormatInt(v.ints[i], 10)
	case block.Float:
		return strconv.FormatFloat(v.flts[i], 'f', -1, 64)
	case block.Bool:
		return strconv.FormatBool(v.bools[i])
	case block.Timestamp:
		return v.times[i].Format(time.RFC3339Nano)
	}
	return ""
}

// panicReason turns a recovered panic into the reason of a reject (D39).
func panicReason(p any) string {
	return strings.TrimSpace(fmt.Sprintf("panic: %v", p))
}
