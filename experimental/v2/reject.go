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
)

// Reject describes one rejected row in the minimal form of this prototype
// slice: the step that rejected it, the affected column, the value at the
// time of the error, the reason and the code (D14). The full reject model
// with raw state and location follows in slice 3 (#47).
type Reject struct {
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

// ErrorMode is the error behavior for data errors (D3): reject the row and
// go on, or stop at the first data error. The default is ModeReject (D5).
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

// PlanError is an error in the pipeline itself: an unknown column, a type
// conflict or an invalid parameter. It is found before any row is read, and
// nothing runs (D19).
type PlanError struct {
	Step string
	Err  error
}

func (e *PlanError) Error() string { return fmt.Sprintf("plan error in step %s: %v", e.Step, e.Err) }
func (e *PlanError) Unwrap() error { return e.Err }

// DataError is the first data error of a run in ModeStop (D3, D50).
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

// rejector collects the rejects of one run in its error mode. In ModeStop
// the first reject becomes a DataError.
type rejector struct {
	mode    ErrorMode
	rejects []Reject
}

func (rx *rejector) add(r Reject) error {
	if rx.mode == ModeStop {
		return &DataError{Reject: r}
	}
	rx.rejects = append(rx.rejects, r)
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
