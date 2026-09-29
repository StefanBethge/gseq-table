package gtable

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// rawSource is the raw state of one source (D9): its columns as they were
// read or created, held apart from the working data. No step changes them;
// the working data shares them until a step replaces a column (D55). A
// source read by a Reader holds its rows in chunks as they are read; each
// chunk carries the location, keys and hashes of its rows as columns after
// the raw columns, and they go with it (D10, D111).
type rawSource struct {
	name   string // location source; "code" for a table built in code (D50, D75)
	sheet  string
	s      schema
	chunks [][]block.Column // nil once released (D87)
	starts []int            // first row of every chunk
	n      int
	loc    *readLoc  // nil for a table built in code
	rel    *release  // nil unless the raw state is released during a run
	sp     *rawSpill // spilled raw state; nil unless spilled (D6, D49)

	fpOnce sync.Once
	fp     string
}

// readLoc describes the location columns of a source read by a Reader (D10,
// D18, D81, D111).
type readLoc struct {
	id       string // delivery identifier (D61)
	cells    bool   // cells located by address (D52)
	noRaw    bool   // the chunks hold no raw columns (D106)
	hashes   bool   // the rows have a record_hash
	layouts  []chunkLayout
	rawLines map[int]string // row: raw bytes of a line rejected at read time
}

// chunkLayout describes the location of the rows of a chunk. The lines
// stay in memory as runs of consecutive lines, one run for a chunk without
// empty lines or records over several lines, so that record_key needs no
// spilled chunk; so do keys (G74). Offset, hash and displayed texts are
// columns after the raw columns, which only rejected rows need; hash and
// display only where a row of the chunk has one, else -1.
type chunkLayout struct {
	lines                 []lineRun
	keys                  block.Column // record_key per row where taken or formed; unset for none
	hasKeys               bool
	offset, hash, display int
}

// lineRun says that from row row of a chunk on, the lines count up from
// line.
type lineRun struct{ row, line int }

// line returns the line of row i of the chunk.
func (l chunkLayout) line(i int) int {
	k := sort.Search(len(l.lines), func(j int) bool { return l.lines[j].row > i }) - 1
	return l.lines[k].line + i - l.lines[k].row
}

// key returns the key of row i of the chunk, or "".
func (l chunkLayout) key(i int) string {
	if !l.hasKeys {
		return ""
	}
	k, _ := l.keys.Text(i)
	return k
}

// rowLoc is the location and the keys of one read row.
type rowLoc struct {
	line    int
	offset  int64 // -1: none
	key     string
	hash    string
	display string // displayed texts by raw column, see encodeDisplay (D62)
}

// newRawSource returns the raw state of a source with the given columns,
// sharing their values.
func newRawSource(name string, s schema, cols []block.Column) *rawSource {
	shared := make([]block.Column, len(cols))
	n := 0
	for i, c := range cols {
		shared[i] = c.Share()
		n = c.Len()
	}
	return &rawSource{name: name, s: s, chunks: [][]block.Column{shared}, starts: []int{0}, n: n}
}

// release tracks which chunks of a source read in a run the plan still
// needs, so that a chunk no working row needs is freed (D43, D87, D110). A
// rejected row keeps a copy of its raw state and location.
type release struct {
	owner     *tally  // the run that read the rows
	chunkLive []int   // working rows per chunk that hold its raw state
	bytes     []int64 // counted bytes per chunk (D28)
	kept      map[int]*keptRow
	keptBytes int64
}

// keptRow is the copy of the raw state and the location of a rejected row.
type keptRow struct {
	cells []keptCell
	loc   rowLoc
}

type keptCell struct {
	s  string
	ok bool
}

// addChunk stores cols, the raw columns and then the location columns
// described by lay, as the chunk of the next rows. With release, every row
// counts as held by one working row.
func (r *rawSource) addChunk(cols []block.Column, lay chunkLayout, rows int) {
	r.chunks = append(r.chunks, cols)
	r.starts = append(r.starts, r.n)
	r.n += rows
	if r.loc != nil {
		r.loc.layouts = append(r.loc.layouts, lay)
	}
	if r.rel != nil {
		r.rel.chunkLive = append(r.rel.chunkLive, rows)
		// The location counts in the budget, but only the raw columns are
		// raw state read (D106).
		var n, raw int64
		for i, c := range cols {
			n += c.Bytes()
			if r.loc == nil || i < r.dataCols() {
				raw += c.Bytes()
			}
		}
		if lay.hasKeys {
			n += lay.keys.Bytes()
		}
		r.rel.bytes = append(r.rel.bytes, n)
		if m := r.mem(); m != nil {
			m.grow(n)
			m.read += raw
		}
	}
}

// chunkOf returns the chunk that holds row.
func (r *rawSource) chunkOf(row int) int {
	return sort.Search(len(r.starts), func(i int) bool { return r.starts[i] > row }) - 1
}

// acquire notes one more working row that holds the raw state of row.
func (r *rawSource) acquire(row int) {
	if r.rel != nil {
		r.rel.chunkLive[r.chunkOf(row)]++
	}
}

// drop notes that a working row that held the raw state of row left the
// plan. When no working row holds a chunk any more, it is freed (D43).
func (r *rawSource) drop(row int) {
	if r.rel == nil {
		return
	}
	c := r.chunkOf(row)
	if r.rel.chunkLive[c] <= 0 {
		return
	}
	r.rel.chunkLive[c]--
	if r.rel.chunkLive[c] > 0 {
		return
	}
	if r.chunks[c] != nil {
		r.mem().shrink(r.rel.bytes[c])
	} else if lay := &r.loc.layouts[c]; lay.hasKeys {
		r.mem().shrink(lay.keys.Bytes()) // they stayed when the chunk was spilled
	}
	r.chunks[c] = nil
	if r.loc != nil {
		r.loc.layouts[c].keys, r.loc.layouts[c].hasKeys = block.Column{}, false
	}
	if r.sp != nil {
		delete(r.sp.at, c)
	}
}

// chunk returns chunk c from memory or disk, or nil if it was freed.
func (r *rawSource) chunk(c int) []block.Column {
	if cols := r.chunks[c]; cols != nil {
		return cols
	}
	return r.spilledChunk(c)
}

// keep copies the raw state and the location of a rejected row, so that
// the row keeps them after its chunk is freed (D87, D111). The copy counts
// against the budget and is spilled over it (D49).
func (r *rawSource) keep(row int) {
	if r.rel == nil {
		return
	}
	if _, ok := r.rel.kept[row]; ok {
		return
	}
	if _, ok := r.spilledKept(row); ok {
		return
	}
	c := r.chunkOf(row)
	cols := r.chunk(c)
	if cols == nil {
		return
	}
	k := &keptRow{loc: r.chunkLoc(c, cols, row)}
	if !r.loc.noRaw {
		k.cells = make([]keptCell, len(r.s))
		for j := range r.s {
			k.cells[j].s, k.cells[j].ok = cols[j].Text(row - r.starts[c])
		}
	}
	if r.rel.kept == nil {
		r.rel.kept = map[int]*keptRow{}
	}
	r.rel.kept[row] = k
	n := keptBytes(k)
	r.rel.keptBytes += n
	r.mem().grow(n)
}

// keptOf returns the copy of a rejected row, in memory or spilled.
func (r *rawSource) keptOf(row int) (*keptRow, bool) {
	if r.rel == nil {
		return nil, false
	}
	if k, ok := r.rel.kept[row]; ok {
		return k, true
	}
	return r.spilledKept(row)
}

// text returns the raw value of column j in row as text: from its chunk,
// in memory or spilled, or from the copy of a rejected row.
func (r *rawSource) text(row, j int) (string, bool) {
	if r.loc != nil && r.loc.noRaw {
		return "", false // D106
	}
	c := r.chunkOf(row)
	if cols := r.chunks[c]; cols != nil {
		return cols[j].Text(row - r.starts[c])
	}
	if k, ok := r.keptOf(row); ok {
		if k.cells == nil {
			return "", false
		}
		return k.cells[j].s, k.cells[j].ok
	}
	if cols := r.spilledChunk(c); cols != nil {
		return cols[j].Text(row - r.starts[c])
	}
	return "", false // released by Result.Close or a writer (D49, D98)
}

// dataCols returns the number of raw columns in a chunk.
func (r *rawSource) dataCols() int {
	if r.loc != nil && r.loc.noRaw {
		return 0
	}
	return len(r.s)
}

// chunkLoc reads the location of row from the columns of its chunk c.
func (r *rawSource) chunkLoc(c int, cols []block.Column, row int) rowLoc {
	i := row - r.starts[c]
	lay := r.loc.layouts[c]
	loc := rowLoc{line: lay.line(i), offset: -1, key: lay.key(i)}
	if off, ok := cols[lay.offset].Int(i); ok {
		loc.offset = off
	}
	if lay.hash >= 0 {
		loc.hash, _ = cols[lay.hash].Text(i)
	}
	if lay.display >= 0 {
		loc.display, _ = cols[lay.display].Text(i)
	}
	return loc
}

// locOf returns the location of a read row: from the copy of a rejected
// row, or from its chunk. A freed row has none (D111).
func (r *rawSource) locOf(row int) rowLoc {
	if k, ok := r.keptOf(row); ok {
		return k.loc
	}
	c := r.chunkOf(row)
	if cols := r.chunk(c); cols != nil {
		return r.chunkLoc(c, cols, row)
	}
	return rowLoc{offset: -1}
}

// rawColumn returns raw column j for the given rows.
func (r *rawSource) rawColumn(j int, rows []int) block.Column {
	if r.rel == nil {
		return r.column(j).Take(rows)
	}
	b := block.NewBuilder(block.Text, len(rows))
	for _, row := range rows {
		if s, ok := r.text(row, j); ok {
			b.AppendText(s)
		} else {
			b.AppendNull()
		}
	}
	return b.Build()
}

// rows returns the number of rows of the raw state.
func (r *rawSource) rows() int { return r.n }

// column returns raw column j over all rows.
func (r *rawSource) column(j int) block.Column {
	switch len(r.chunks) {
	case 0:
		return block.NewBuilder(r.s[j].kind, 0).Build()
	case 1:
		return r.chunks[0][j]
	}
	parts := make([]block.Column, len(r.chunks))
	for i, c := range r.chunks {
		parts[i] = c[j]
	}
	return block.Concat(r.s[j].kind, parts...)
}

// fingerprint identifies a source built in code: the first 16 hex characters
// of SHA-256 over the column names and the values at creation (D76).
func (r *rawSource) fingerprint() string {
	r.fpOnce.Do(func() {
		h := sha256.New()
		field := func(s string) { fmt.Fprintf(h, "%d:%s;", len(s), s) }
		for i, f := range r.s {
			field(f.name)
			v := vecOf(r.column(i))
			for j := range v.n {
				if s, ok := v.format(j); ok {
					field(s)
				} else {
					h.Write([]byte("n;"))
				}
			}
		}
		r.fp = hex.EncodeToString(h.Sum(nil))[:16]
	})
	return r.fp
}

// recordKey is the record_key of a row: for a table built in code the
// fingerprint and the row index (D76), for a read row the delivery
// identifier and the line (D81), or its business key or the key taken from
// a rejects table (D82, D97).
func (r *rawSource) recordKey(row int) string {
	if r.loc == nil {
		return r.fingerprint() + ":" + strconv.Itoa(row)
	}
	line, key := r.lineKey(row)
	if key != "" {
		return key
	}
	return r.loc.id + ":" + strconv.Itoa(line)
}

// lineKey returns the line and the key of a read row without reading its
// chunk, from the copy of a rejected row or from the layout.
func (r *rawSource) lineKey(row int) (int, string) {
	if k, ok := r.keptOf(row); ok {
		return k.loc.line, k.loc.key
	}
	c := r.chunkOf(row)
	lay := r.loc.layouts[c]
	return lay.line(row - r.starts[c]), lay.key(row - r.starts[c])
}

// line returns the line of a row: its index for a table built in code.
func (r *rawSource) line(row int) int {
	if r.loc == nil {
		return row
	}
	line, _ := r.lineKey(row)
	return line
}

// cell returns the address and the displayed text of the cell of raw
// column col in a row, for a source whose cells are located by address
// (D52, D80).
func (r *rawSource) cell(row int, col string) (addr, display string, ok bool) {
	if r.loc == nil || !r.loc.cells {
		return "", "", false
	}
	j := r.s.index(col)
	if j < 0 {
		return "", "", false
	}
	loc := r.locOf(row)
	addr = columnLetters(j) + strconv.Itoa(loc.line)
	if d, ok := displayOf(loc.display, j); ok {
		return addr, d, true
	}
	display, _ = r.text(row, j)
	return addr, display, true
}

// encodeDisplay encodes the displayed texts of a row by raw column as one
// text, "<column>:<length>:<text>" for each (D62).
func encodeDisplay(d map[int]string) string {
	cols := make([]int, 0, len(d))
	for j := range d {
		cols = append(cols, j)
	}
	sort.Ints(cols)
	var sb strings.Builder
	for _, j := range cols {
		fmt.Fprintf(&sb, "%d:%d:%s", j, len(d[j]), d[j])
	}
	return sb.String()
}

// displayOf returns the displayed text of raw column j from an encoded
// display.
func displayOf(s string, j int) (string, bool) {
	for s != "" {
		a := strings.IndexByte(s, ':')
		col, _ := strconv.Atoi(s[:a])
		s = s[a+1:]
		b := strings.IndexByte(s, ':')
		n, _ := strconv.Atoi(s[:b])
		v := s[b+1 : b+1+n]
		s = s[b+1+n:]
		if col == j {
			return v, true
		}
	}
	return "", false
}

// srcRef is one source row.
type srcRef struct {
	src *rawSource
	row int
}

// origin says where a working row comes from: its source rows, more than
// one after a join (D11), and for a row that a step over all rows computed
// from others, what went into it (D12). A row that went into a fail branch
// carries its history (D46).
type origin struct {
	refs []srcRef
	x    *originExt // nil for a row that is neither aggregated nor in a branch
}

// originExt is the rarer part of an origin. It is never changed once set,
// so origins that share it stay independent.
type originExt struct {
	agg  *aggOrigin
	hist *history
}

// aggOrigin is what went into an aggregated row: the number of rows for
// the weight (D77), and the source rows it stands for (D84), counted per
// source instead of listed (D110). Source rows and aggregated rows that
// several working rows hold keep their identity, because they are counted
// with a state of their own.
type aggOrigin struct {
	n     int
	srcs  []srcCount   // source rows per source counted without a state
	multi []srcRef     // source rows with a state, each once
	units []*aggOrigin // aggregated rows that went into it or were joined, each once
}

type srcCount struct {
	src *rawSource
	n   int
}

func (o origin) agg() *aggOrigin {
	if o.x == nil {
		return nil
	}
	return o.x.agg
}

func (o origin) history() *history {
	if o.x == nil {
		return nil
	}
	return o.x.hist
}

// withHistory returns o with the history h.
func (o origin) withHistory(h *history) origin {
	x := &originExt{hist: h}
	if o.x != nil {
		x.agg = o.x.agg
	}
	o.x = x
	return o
}

// aggOf returns an origin for a row aggregated from a.
func aggOf(a *aggOrigin) origin { return origin{x: &originExt{agg: a}} }

// rowKey is the row_key of the row: the record_keys of its source rows,
// joined with "+" (D47, D76).
func (o origin) rowKey() string {
	if len(o.refs) == 1 {
		r := o.refs[0]
		return r.src.recordKey(r.row)
	}
	keys := make([]string, len(o.refs))
	for i, r := range o.refs {
		keys[i] = r.src.recordKey(r.row)
	}
	return strings.Join(keys, "+")
}

// weight is the number of rows the row stands for when it goes into a
// group (D77).
func (o origin) weight() int {
	if a := o.agg(); a != nil {
		return a.n
	}
	return 1
}

// aggregated returns the number of rows that went into an aggregated row,
// or 0 for another row.
func (o origin) aggregated() int {
	if a := o.agg(); a != nil {
		return a.n
	}
	return 0
}

// detached returns o with its own copy of the source rows, so that a
// reject does not hold the source rows of a whole block.
func (o origin) detached() origin {
	o.refs = append([]srcRef(nil), o.refs...)
	return o
}

// join is the origin of a join row: the source rows of both sides, left
// first. An aggregated side keeps its identity (D110).
func (o origin) join(r origin) origin {
	refs := make([]srcRef, 0, len(o.refs)+len(r.refs))
	refs = append(append(refs, o.refs...), r.refs...)
	a, b := o.agg(), r.agg()
	switch {
	case a == nil:
		a = b
	case b != nil:
		a = &aggOrigin{n: a.n + b.n, units: []*aggOrigin{a, b}}
	}
	hist := o.history()
	if hist == nil {
		hist = r.history()
	}
	out := origin{refs: refs}
	if a != nil || hist != nil {
		out.x = &originExt{agg: a, hist: hist}
	}
	return out
}

// sourceOrigins returns the origins of the rows of src, row by row.
func sourceOrigins(src *rawSource, n int) []origin {
	refs := make([]srcRef, n)
	out := make([]origin, n)
	for i := range n {
		refs[i] = srcRef{src, i}
		out[i] = origin{refs: refs[i : i+1 : i+1]}
	}
	return out
}

// pick returns the origins of the given rows.
func pick(orig []origin, idx []int) []origin {
	out := make([]origin, len(idx))
	for i, j := range idx {
		out[i] = orig[j]
	}
	return out
}

// sourceClash returns an error if a and b hold two different sources of
// the same name (D75).
func sourceClash(a, b []*rawSource) error {
	for _, x := range a {
		for _, y := range b {
			if x != y && x.name == y.name {
				return fmt.Errorf("both sides have a source named %q; name them with AsSource", x.name)
			}
		}
	}
	return nil
}

// unionSources returns the sources of a and b, each once, in order.
func unionSources(a, b []*rawSource) []*rawSource {
	out := append([]*rawSource(nil), a...)
	for _, y := range b {
		found := false
		for _, x := range out {
			found = found || x == y
		}
		if !found {
			out = append(out, y)
		}
	}
	return out
}

// runInfo identifies a run and numbers its rejects (D76).
type runInfo struct {
	id   string
	next atomic.Int64
}

func newRun() *runInfo {
	b := make([]byte, 8)
	rand.Read(b) // never returns an error
	return &runInfo{id: hex.EncodeToString(b)}
}

func (r *runInfo) nextID() string { return r.id + "-" + strconv.FormatInt(r.next.Add(1), 10) }
