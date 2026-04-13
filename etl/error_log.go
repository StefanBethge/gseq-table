package etl

import (
	"fmt"
	"sort"
	"sync"

	"github.com/stefanbethge/gseq-table/table"
)

// ErrorEntry captures one error with full context, including the original row
// before any transformation was applied.
type ErrorEntry struct {
	// Source is the dataset name set via table.Table.WithSource ("" if not set).
	Source string
	// Step is the name of the operation that produced the error.
	Step string
	// Row is the zero-based row index in the source table. -1 for non-row errors.
	Row int
	// Err is the underlying error.
	Err error
	// OriginalRow is a snapshot of the full row before any transformation.
	// Use ErrorLog.ToTable() to export all rejected rows as a table.Table.
	OriginalRow map[string]string
}

// ErrorLog collects row-level errors from pipeline steps without stopping
// pipeline execution. Attach it to a Pipeline via Pipeline.WithErrorLog.
//
// ErrorLog is safe for concurrent use.
type ErrorLog struct {
	mu      sync.Mutex
	entries []ErrorEntry
}

// NewErrorLog creates a new, empty ErrorLog.
func NewErrorLog() *ErrorLog { return &ErrorLog{} }

func (l *ErrorLog) add(e ErrorEntry) {
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
}

// Entries returns a snapshot of all collected error entries.
func (l *ErrorLog) Entries() []ErrorEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ErrorEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// HasErrors reports whether any errors have been collected.
func (l *ErrorLog) HasErrors() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries) > 0
}

// Len returns the number of collected error entries.
func (l *ErrorLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// ToTable returns all error entries as an immutable Table suitable for CSV export.
//
// The table contains the following columns:
//   - _source   – dataset name (from WithSource)
//   - _step     – pipeline step name
//   - _row      – original zero-based row index
//   - _error    – error message
//   - one column per field from the original rows, merged across all entries
//     (empty string for missing values)
//
//	csv.NewWriter().WriteFile("rejected.csv", log.ToTable())
func (l *ErrorLog) ToTable() table.Table {
	l.mu.Lock()
	entries := make([]ErrorEntry, len(l.entries))
	copy(entries, l.entries)
	l.mu.Unlock()

	metaCols := []string{"_source", "_step", "_row", "_error"}

	if len(entries) == 0 {
		return table.New(metaCols, nil)
	}

	// Collect all original row column names, then sort for deterministic output.
	seen := make(map[string]bool)
	extraCols := make([]string, 0)
	for _, e := range entries {
		for k := range e.OriginalRow {
			if !seen[k] {
				seen[k] = true
				extraCols = append(extraCols, k)
			}
		}
	}
	sort.Strings(extraCols)

	headers := append(metaCols, extraCols...)
	records := make([][]string, len(entries))
	for i, e := range entries {
		row := make([]string, len(headers))
		row[0] = e.Source
		row[1] = e.Step
		row[2] = fmt.Sprintf("%d", e.Row)
		row[3] = e.Err.Error()
		for j, col := range extraCols {
			row[4+j] = e.OriginalRow[col]
		}
		records[i] = row
	}
	return table.New(headers, records)
}
