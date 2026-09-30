package gtable

import (
	"fmt"
	"strings"
	"sync"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// DefaultInfoPrefix is the reserved prefix of the info columns of rejected
// rows (D14). A pipeline can set another one with InfoPrefix.
const DefaultInfoPrefix = "_gseq_"

func infoPrefix(p string) string {
	if p == "" {
		return DefaultInfoPrefix
	}
	return p
}

// prefixClash returns an error if a raw column of src starts with the info
// prefix, which is reserved (D14).
func prefixClash(src *rawSource, prefix string) error {
	for _, f := range src.s {
		if strings.HasPrefix(f.name, prefix) {
			return fmt.Errorf("column %q of source %q starts with the reserved info prefix %q; set another prefix", f.name, src.name, prefix)
		}
	}
	return nil
}

// The info columns of a table per source and of the overview, in order
// (D14). Columns that a source cannot fill, such as sheet or cell for a
// table built in code, are null. The optional record_hash and display are
// added by infoFields where a source fills them.
var sourceInfo = []field{
	{"reject_id", block.Text},
	{"run_id", block.Text},
	{"record_key", block.Text},
	{"row_key", block.Text},
	{"error_count", block.Int},
	{"source", block.Text},
	{"sheet", block.Text},
	{"line", block.Int},
	{"offset", block.Int},
	{"cell", block.Text},
	{"step", block.Text},
	{"column", block.Text},
	{"value", block.Text},
	{"reason", block.Text},
	{"prev_reason", block.Text},
	{"code", block.Text},
	{"raw_line", block.Text},
}

// infoFields returns the info columns for rows of the given sources: the
// fixed ones, with record_hash after row_key if a source has hashes (D18)
// and display after cell if a source locates cells (D62).
func infoFields(srcs []*rawSource) []field {
	hash, display := false, false
	for _, s := range srcs {
		hash = hash || s.loc != nil && s.loc.hashes
		display = display || s.loc != nil && s.loc.cells
	}
	if !hash && !display {
		return sourceInfo
	}
	out := make([]field, 0, len(sourceInfo)+2)
	for _, f := range sourceInfo {
		out = append(out, f)
		switch {
		case f.name == "row_key" && hash:
			out = append(out, field{"record_hash", block.Text})
		case f.name == "cell" && display:
			out = append(out, field{"display", block.Text})
		}
	}
	return out
}

// The info columns of a table of aggregated rejected rows (D77).
var aggregatedInfo = []field{
	{"reject_id", block.Text},
	{"run_id", block.Text},
	{"step", block.Text},
	{"column", block.Text},
	{"value", block.Text},
	{"reason", block.Text},
	{"code", block.Text},
	{"error_count", block.Int},
	{"source_rows", block.Int},
}

// RejectedRows are the rejected rows of a table or a run as tables (D1): a
// table per source with the raw columns of the source and the info columns
// (D13, D44), the overview with one entry per error (D15), and a table per
// step in which aggregated rows failed (D48, D77). After Result.Close, every
// table carries the sticky error ErrClosed (D98).
type RejectedRows struct {
	prefix  string
	entries []rejectEntry
	st      *rejectState
}

// rejectState is shared by the table of a result and the tables derived
// from it: the source rows a writer in the plan took (D49), and whether
// the result is closed (D98).
type rejectState struct {
	mu     sync.Mutex
	sunk   map[srcRef]bool
	closed bool
}

func (st *rejectState) sink(r srcRef) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.sunk == nil {
		st.sunk = map[srcRef]bool{}
	}
	// The copy of the raw state stays until Close: the overview, which
	// the result keeps (D95), shows the displayed text of a cell from it.
	st.sunk[r] = true
}

func (st *rejectState) written(r srcRef) bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.sunk[r]
}

func (st *rejectState) isClosed() bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

// close marks the state closed and frees the copies of the raw state of
// the rejected rows (D98).
func (st *rejectState) close(entries []rejectEntry) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return
	}
	st.closed = true
	for _, e := range entries {
		for _, r := range e.orig.refs {
			if r.src.rel != nil {
				r.src.rel.kept = nil
			}
		}
	}
}

// SourceRejects are the rejected rows of one source: each source row once,
// with the info columns of its first error and error_count (D44).
type SourceRejects struct {
	Source string
	Rows   Table
}

// StepRejects are the aggregated rows that failed in one step (D77).
type StepRejects struct {
	Step string
	Rows Table
}

// RejectedRows returns the rejected rows of the table's operations as
// tables.
func (t Table) RejectedRows() RejectedRows {
	return RejectedRows{prefix: infoPrefix(t.prefix), entries: t.rejects, st: t.rs}
}

// closedTable is a table of rejected rows after Close.
func closedTable() Table { return Table{err: ErrClosed} }

// sourceGroup are the rejected rows of one source: the source rows in the
// order of their first error, that entry and the number of errors of each.
type sourceGroup struct {
	src    *rawSource
	rows   []int
	first  map[srcRef]int
	counts map[srcRef]int
}

// groups returns the rejected rows per source, in the order of their first
// rejected row, without the rows a writer took (D49).
func (r RejectedRows) groups() []sourceGroup {
	var out []sourceGroup
	idx := map[*rawSource]int{}
	for i, e := range r.entries {
		for _, ref := range e.orig.refs {
			if r.st.written(ref) {
				continue
			}
			j, ok := idx[ref.src]
			if !ok {
				j = len(out)
				idx[ref.src] = j
				out = append(out, sourceGroup{src: ref.src, first: map[srcRef]int{}, counts: map[srcRef]int{}})
			}
			g := &out[j]
			if _, ok := g.first[ref]; !ok {
				g.first[ref] = i
				g.rows = append(g.rows, ref.row)
			}
			g.counts[ref]++
		}
	}
	return out
}

// Sources returns a table per source that has rejected rows, in the order
// of their first rejected row. Source names are unique (D75). A table whose
// raw columns clash with the info prefix carries a sticky plan error. The
// rows a writer in the plan took are not in it (D49).
func (r RejectedRows) Sources() []SourceRejects {
	gs := r.groups()
	out := make([]SourceRejects, len(gs))
	for i, g := range gs {
		if r.st.isClosed() {
			out[i] = SourceRejects{g.src.name, closedTable()}
			continue
		}
		out[i] = SourceRejects{g.src.name, r.sourceTable(g)}
	}
	return out
}

// Source returns the rejected rows of the named source.
func (r RejectedRows) Source(name string) (Table, bool) {
	for _, s := range r.Sources() {
		if s.Source == name {
			return s.Rows, true
		}
	}
	return Table{}, false
}

func (r RejectedRows) sourceTable(g sourceGroup) Table {
	src, rows := g.src, g.rows
	if err := prefixClash(src, r.prefix); err != nil {
		return Table{err: &PlanError{Step: "rejected_rows", Err: err}}
	}
	cols := make([]Column, 0, len(src.s)+len(sourceInfo))
	for j, f := range src.s {
		cols = append(cols, newColumn(f.name, src.rawColumn(j, rows)))
	}
	ib := newInfoBuilder(infoFields([]*rawSource{src}), len(rows))
	for _, row := range rows {
		ref := srcRef{src, row}
		ib.sourceRow(r.entries[g.first[ref]], ref, g.counts[ref])
	}
	return NewTable(append(cols, ib.columns(r.prefix)...)...)
}

// Overview returns one entry per error with the info columns only: one per
// source row of the failed row, sharing its reject_id (D11, D15), and one
// with an empty location for an aggregated row (D77).
func (r RejectedRows) Overview() Table {
	if r.st.isClosed() {
		return closedTable()
	}
	perID := r.errorsPerID()
	var srcs []*rawSource
	for _, e := range r.entries {
		for _, ref := range e.orig.refs {
			srcs = append(srcs, ref.src)
		}
	}
	ib := newInfoBuilder(infoFields(srcs), len(r.entries))
	for _, e := range r.entries {
		for _, ref := range e.orig.refs {
			ib.sourceRow(e, ref, perID[e.ID])
		}
		if e.orig.aggregated() > 0 {
			ib.aggregatedRow(e, perID[e.ID])
		}
	}
	return NewTable(ib.columns(r.prefix)...)
}

// Aggregated returns a table per step in which aggregated rows failed, in
// the order of their first failure. Each row stands once, with its values
// as it went into the step and the info columns of its first error (D77).
func (r RejectedRows) Aggregated() []StepRejects {
	perID := r.errorsPerID()
	var steps []*stepRef
	byStep := map[*stepRef][]rejectEntry{}
	seen := map[string]bool{}
	for _, e := range r.entries {
		if e.snap == nil || seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		if _, ok := byStep[e.step]; !ok {
			steps = append(steps, e.step)
		}
		byStep[e.step] = append(byStep[e.step], e)
	}
	out := make([]StepRejects, len(steps))
	for i, st := range steps {
		es := byStep[st]
		s := es[0].snap.s
		blks := make([]block.Block, len(es))
		for j, e := range es {
			blks[j] = e.snap.blk
		}
		b := concatBlocks(blks, s)
		cols := make([]Column, 0, len(s)+len(aggregatedInfo))
		for j, f := range s {
			cols = append(cols, newColumn(f.name, b.Column(j)))
		}
		ib := newInfoBuilder(aggregatedInfo, len(es))
		for _, e := range es {
			ib.aggregatedRow(e, perID[e.ID])
		}
		rows := NewTable(append(cols, ib.columns(r.prefix)...)...)
		if r.st.isClosed() {
			rows = closedTable()
		}
		out[i] = StepRejects{st.name, rows}
	}
	return out
}

// errorsPerID counts the errors of each rejected row in its step.
func (r RejectedRows) errorsPerID() map[string]int {
	n := map[string]int{}
	for _, e := range r.entries {
		n[e.ID]++
	}
	return n
}

// infoBuilder builds the info columns of a reject table row by row.
type infoBuilder struct {
	fields []field
	b      map[string]*block.Builder
}

func newInfoBuilder(fields []field, n int) *infoBuilder {
	ib := &infoBuilder{fields: fields, b: make(map[string]*block.Builder, len(fields))}
	for _, f := range fields {
		ib.b[f.name] = block.NewBuilder(f.kind, n)
	}
	return ib
}

// set sets the info columns of the current row from vals; the others are
// null. It ends the row.
func (ib *infoBuilder) set(vals map[string]any) {
	for _, f := range ib.fields {
		b := ib.b[f.name]
		switch v := vals[f.name].(type) {
		case string:
			b.AppendText(v)
		case int:
			b.AppendInt(int64(v))
		case int64:
			b.AppendInt(v)
		default:
			b.AppendNull()
		}
	}
}

func entryValues(e rejectEntry, errors int) map[string]any {
	vals := map[string]any{
		"reject_id":   e.ID,
		"run_id":      e.runID,
		"error_count": errors,
		"step":        e.Step,
		"reason":      e.Reason,
		"code":        e.Code,
	}
	if e.Column != "" {
		vals["column"] = e.Column
	}
	if e.HasValue {
		vals["value"] = e.Value
	}
	if e.PrevReason != "" {
		vals["prev_reason"] = e.PrevReason
	}
	return vals
}

// sourceRow adds the entry for one source row of e.
func (ib *infoBuilder) sourceRow(e rejectEntry, ref srcRef, errors int) {
	vals := entryValues(e, errors)
	src := ref.src
	vals["record_key"] = src.recordKey(ref.row)
	vals["row_key"] = e.orig.rowKey()
	vals["source"] = src.name
	vals["line"] = src.line(ref.row)
	if src.sheet != "" {
		vals["sheet"] = src.sheet
	}
	if loc := src.loc; loc != nil {
		rl := src.locOf(ref.row)
		if rl.offset >= 0 {
			vals["offset"] = rl.offset
		}
		if loc.hashes {
			vals["record_hash"] = rl.hash
		}
		if raw, ok := loc.rawLines[ref.row]; ok {
			vals["raw_line"] = raw
		}
		if addr, display, ok := src.cell(ref.row, e.Column); ok {
			vals["cell"], vals["display"] = addr, display
		}
	}
	ib.set(vals)
}

// aggregatedRow adds the entry for the aggregated part of e.
func (ib *infoBuilder) aggregatedRow(e rejectEntry, errors int) {
	vals := entryValues(e, errors)
	vals["source_rows"] = e.orig.aggregated()
	ib.set(vals)
}

func (ib *infoBuilder) columns(prefix string) []Column {
	out := make([]Column, len(ib.fields))
	for i, f := range ib.fields {
		out[i] = newColumn(prefix+f.name, ib.b[f.name].Build())
	}
	return out
}
