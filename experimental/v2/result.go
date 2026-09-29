package gtable

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Status is the status of a run (D21). The statuses are ordered by
// precedence: if several apply, the highest counts (D63).
type Status uint8

const (
	StatusOK              Status = iota
	StatusFailedThreshold        // more rows rejected than the threshold allows (D4, D20)
	StatusAborted                // stopped at a data error, or the context ended (D3, D63)
	StatusDeliveryError          // a delivery error, in any mode (D42)
	StatusSinkError              // a target failed (D40)
	StatusPlanError              // an error in the pipeline itself; nothing ran (D19)
)

var statusNames = [...]string{"ok", "failed_threshold", "aborted", "delivery_error", "sink_error", "plan_error"}

func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return "Status(" + strconv.Itoa(int(s)) + ")"
}

// ExitCode returns the exit code of the status for a scheduler: 0 for ok,
// rising with the precedence to 5 for plan_error (D21, D86).
func (s Status) ExitCode() int { return int(s) }

// Cause is one status that applies to a run, with the error behind it
// (D63): a *PlanError, a *DeliveryError, a *DataError, a *ThresholdError,
// a *SinkError (D40) or the error of the context.
type Cause struct {
	Status Status
	Err    error
}

func (c Cause) String() string { return c.Status.String() + ": " + c.Err.Error() }

// Counts counts source rows, not entries (D43, D84). Every source row
// falls in exactly one category, so Read = Passed + Rejected + Dropped +
// Unprocessed. A row is rejected if one of its result rows was rejected,
// else passed if one reached the end of the plan, else dropped. Unprocessed
// rows were read but the run ended before they were done. ByCode counts
// the rejected source rows per error code; a row with two codes counts for
// both.
//
// For a step, Read are the source rows that went into the step, and Passed
// those that came out of it. A row that failed in a step and that its fail
// branch returned passed the step; Rescued counts these rows apart, and
// for the run the rows any fail branch returned (D46). Only rows that stay
// rejected count as Rejected and for the threshold.
type Counts struct {
	Read, Passed, Rejected, Dropped, Unprocessed int
	Rescued                                      int
	ByCode                                       map[string]int
}

// StepCounts are the counts of one step (D21). The rows a Reader rejects
// while reading count for the step "read".
type StepCounts struct {
	Step string
	Counts
}

// Result is the result of a run (D21, D88): the result table, the status
// with every cause that applies (D63), the counts for the run and per step
// (D43), and the change report (D23).
type Result struct {
	// Table holds the rows that passed, the rejected rows and the findings
	// of the header check. After a run that ended early, it holds what was
	// done until then, and the error that ended the run as its sticky
	// error.
	Table  Table
	Status Status
	// Causes are all statuses that apply, highest first.
	Causes []Cause
	Counts Counts
	Steps  []StepCounts
	// Report is the change report: the findings of the header check
	// (D22), unreadable deliveries (D42) and format changes (D23, D85).
	Report []Finding
	// Trace notes where the engine copied data in the mode CopyInPlace:
	// at branches and at the first change of a column the raw state or a
	// table holds (D9, D64, D93).
	Trace []TraceEntry

	mem    *runMem
	closer *resultCloser // spilled rejected rows until Close (D49)
}

// Close releases the rejected rows the result holds (D49, D98). After
// Close, the tables of RejectedRows of the result table, and of the
// tables derived from it, carry the sticky error ErrClosed. Status,
// causes, counts and the change report stay. Close may be called more
// than once.
func (r Result) Close() error {
	if r.Table.rs != nil {
		r.Table.rs.close(r.Table.rejects)
	}
	// The spilled copies go with the run's spill directory (D28, D103).
	if r.closer != nil {
		return r.closer.close()
	}
	return nil
}

// ExitCode returns the exit code of the status (D86).
func (r Result) ExitCode() int { return r.Status.ExitCode() }

// Step returns the counts of the first step with the given name.
func (r Result) Step(name string) (StepCounts, bool) {
	for _, s := range r.Steps {
		if s.Step == name {
			return s, true
		}
	}
	return StepCounts{}, false
}

// ChangeReport returns the change report as a table with a row per finding
// (D23): kind, source, column, detail, count and examples. Examples are
// quoted and separated by ", ". Count is null for a finding without a
// count.
func (r Result) ChangeReport() Table {
	n := len(r.Report)
	b := map[string]*block.Builder{}
	names := []string{"kind", "source", "column", "detail", "examples"}
	for _, name := range names {
		b[name] = block.NewBuilder(block.Text, n)
	}
	count := block.NewBuilder(block.Int, n)
	for _, f := range r.Report {
		b["kind"].AppendText(f.Kind)
		b["source"].AppendText(f.Source)
		b["column"].AppendText(f.Column)
		b["detail"].AppendText(f.Detail)
		ex := make([]string, len(f.Examples))
		for i, e := range f.Examples {
			ex[i] = strconv.Quote(e)
		}
		b["examples"].AppendText(strings.Join(ex, ", "))
		if f.Count > 0 {
			count.AppendInt(int64(f.Count))
		} else {
			count.AppendNull()
		}
	}
	return NewTable(
		newColumn("kind", b["kind"].Build()),
		newColumn("source", b["source"].Build()),
		newColumn("column", b["column"].Build()),
		newColumn("detail", b["detail"].Build()),
		newColumn("count", count.Build()),
		newColumn("examples", b["examples"].Build()),
	)
}

// Limit is one limit of a threshold (D20): a number of rejected source
// rows, or a share of the rows read.
type Limit struct {
	rows  int
	share float64
	isShr bool
}

// MaxRejected is exceeded when more than n source rows are rejected (D4).
func MaxRejected(n int) Limit { return Limit{rows: n} }

// MaxRejectedShare is exceeded when the share of rejected source rows is
// larger than share, between 0 and 1. The share is over the rows read so
// far for the run, and over the rows that went into the step for a step
// threshold (D45).
func MaxRejectedShare(share float64) Limit { return Limit{share: share, isShr: true} }

func (l Limit) String() string {
	if l.isShr {
		return "share " + strconv.FormatFloat(l.share, 'f', -1, 64)
	}
	return strconv.Itoa(l.rows) + " rows"
}

func (l Limit) check() error {
	switch {
	case l.isShr && (math.IsNaN(l.share) || l.share < 0 || l.share > 1):
		return fmt.Errorf("threshold share %v is not between 0 and 1", l.share)
	case !l.isShr && l.rows < 0:
		return fmt.Errorf("threshold of %d rows is negative", l.rows)
	}
	return nil
}

// exceeded reports whether rejected of read rows exceed l. A share only
// counts from min rows read (D45).
func (l Limit) exceeded(rejected, read, min int) bool {
	if !l.isShr {
		return rejected > l.rows
	}
	return read > 0 && read >= min && float64(rejected)/float64(read) > l.share
}

// ThresholdError says that the rejected rows exceed a threshold (D4, D20).
// Step is empty for the threshold of the run.
type ThresholdError struct {
	Step     string
	Rejected int
	Read     int
	Limit    Limit
}

func (e *ThresholdError) Error() string {
	where := "the run"
	if e.Step != "" {
		where = "step " + e.Step
	}
	return fmt.Sprintf("threshold exceeded in %s: %d of %d rows rejected, limit %v", where, e.Rejected, e.Read, e.Limit)
}

// causeOf returns the status an error that ended a run stands for (D63).
func causeOf(err error) Cause {
	var pe *PlanError
	var de *DeliveryError
	var te *ThresholdError
	var se *SinkError
	switch {
	case errors.As(err, &se):
		return Cause{StatusSinkError, err}
	case errors.As(err, &pe):
		return Cause{StatusPlanError, err}
	case errors.As(err, &de):
		return Cause{StatusDeliveryError, err}
	case errors.As(err, &te):
		return Cause{StatusFailedThreshold, err}
	}
	return Cause{StatusAborted, err}
}

// status returns the highest status of the causes, and sorts them highest
// first (D63).
func status(causes []Cause) Status {
	for i := 1; i < len(causes); i++ {
		for j := i; j > 0 && causes[j].Status > causes[j-1].Status; j-- {
			causes[j], causes[j-1] = causes[j-1], causes[j]
		}
	}
	if len(causes) == 0 {
		return StatusOK
	}
	return causes[0].Status
}
