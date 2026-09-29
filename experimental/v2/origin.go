package gtable

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// rawSource is the raw state of one source (D9): its columns as they were
// read or created, held apart from the working data. No step changes them;
// the working data shares them until a step replaces a column (D55). A
// source read by a Reader holds its rows in chunks as they are read, with
// their location (D10).
type rawSource struct {
	name   string // location source; "code" for a table built in code (D50, D75)
	sheet  string
	s      schema
	chunks [][]block.Column
	n      int
	loc    *readLoc // nil for a table built in code

	fpOnce sync.Once
	fp     string
}

// readLoc is the location and the keys of the rows of a source read by a
// Reader, by row (D10, D18, D81).
type readLoc struct {
	id       string // delivery identifier (D61)
	cells    bool   // cells located by address (D52)
	lines    []int
	offsets  []int64                // -1: none
	display  map[int]map[int]string // row, raw column: displayed text (D62)
	rawLines map[int]string         // row: raw bytes of a rejected line
	keys     []string               // record_key taken from a rejects table (D82); nil if formed
	hashes   []string               // record_hash; nil unless the source has them
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
	return &rawSource{name: name, s: s, chunks: [][]block.Column{shared}, n: n}
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
// identifier and the line (D81), or the key taken from a rejects table
// (D82).
func (r *rawSource) recordKey(row int) string {
	if r.loc == nil {
		return r.fingerprint() + ":" + strconv.Itoa(row)
	}
	if r.loc.keys != nil && r.loc.keys[row] != "" {
		return r.loc.keys[row]
	}
	return r.loc.id + ":" + strconv.Itoa(r.loc.lines[row])
}

// line returns the line of a row: its index for a table built in code.
func (r *rawSource) line(row int) int {
	if r.loc == nil {
		return row
	}
	return r.loc.lines[row]
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
	addr = columnLetters(j) + strconv.Itoa(r.loc.lines[row])
	if d, ok := r.loc.display[row][j]; ok {
		return addr, d, true
	}
	c := r.column(j)
	display, _ = c.Text(row)
	return addr, display, true
}

// srcRef is one source row.
type srcRef struct {
	src *rawSource
	row int
}

// origin says where a working row comes from: its source rows, more than
// one after a join (D11), or, for a row that a step over all rows computed
// from others, the number of rows that went into it (D12).
type origin struct {
	refs []srcRef
	agg  int
}

// rowKey is the row_key of the row: the record_keys of its source rows,
// joined with "+" (D47, D76).
func (o origin) rowKey() string {
	keys := make([]string, len(o.refs))
	for i, r := range o.refs {
		keys[i] = r.src.recordKey(r.row)
	}
	return strings.Join(keys, "+")
}

// weight is the number of rows the row stands for when it goes into a
// group (D77).
func (o origin) weight() int {
	if o.agg > 0 {
		return o.agg
	}
	return 1
}

// join is the origin of a join row: the source rows of both sides, left
// first.
func (o origin) join(r origin) origin {
	refs := make([]srcRef, 0, len(o.refs)+len(r.refs))
	refs = append(append(refs, o.refs...), r.refs...)
	return origin{refs: refs, agg: o.agg + r.agg}
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
