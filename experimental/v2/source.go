package gtable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/benchknob"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Reader reads one delivery for a Source (F8). The packages csv and excel
// provide readers for their formats (D57); a Reader only splits the
// delivery into records, and the Source keeps raw state, location and keys
// (D10, D18). A Source opens its reader when a run starts and closes it at
// the end, so Open may be called again after Close.
type Reader interface {
	// Open opens the delivery and returns its header. An error that is
	// neither a *DeliveryError nor a *PlanError is reported as unreadable
	// (D42).
	Open() (Header, error)
	// Next returns the next record, or io.EOF at the end. Another error
	// ends the run as unreadable.
	Next() (Record, error)
	Close() error
}

// Header describes an opened delivery.
type Header struct {
	// Source names the source: the file name, for Excel with the sheet, as
	// in lieferung.xlsx#Kunden (D53, D81).
	Source string
	Sheet  string
	// Columns are the column names of the header, or nil for a delivery
	// without header, whose names come from Source.Expect (D53).
	Columns []string
	// ID identifies the delivery for record_key: its fingerprint (D61,
	// D81). Source.DeliveryID replaces it.
	ID string
	// Cells says that the fields are cells located by address, column
	// letters from A and the line, as in Excel (D52).
	Cells bool
}

// Record is one row of a delivery as the reader read it (D10).
type Record struct {
	// Fields are the cell values as read. A record with another number of
	// fields than the header cannot be split into the columns and is
	// rejected with CodeUnparseableLine.
	Fields []string
	// Line is the line number (D81, D52), Offset the byte offset of the
	// start of the record, or -1 where the format has none (D52).
	Line   int
	Offset int64
	// Raw holds the raw bytes of the record, where the format has them. It
	// is only valid until the next call of Next.
	Raw []byte
	// Display holds the displayed text of fields whose display differs
	// from the stored value, by field index (D62).
	Display map[int]string
	// Code, if set, rejects the record at read time with Reason:
	// CodeUnparseableLine for a record the reader could not split, or
	// CodeFieldTooLarge for a field over the limit (D10, D56). Column is
	// the index of the affected field, or -1. The raw state is Fields,
	// null if nil, and raw_line holds Raw.
	Code   string
	Reason string
	Column int

	key, hash string // taken from a rejects table (D82)
}

// NewColumnMode says what a source does with columns that its expected
// layout does not name (D22).
type NewColumnMode uint8

const (
	// ReportNewColumns reports new columns and passes them on (default).
	ReportNewColumns NewColumnMode = iota
	// PassNewColumns passes new columns on without a finding.
	PassNewColumns
	// IgnoreNewColumns leaves new columns out of the working data, without
	// a finding; the raw state still holds them.
	IgnoreNewColumns
)

// Kinds of findings of the change report (D22, D23, D42).
const (
	FindingMissingColumn   = "missing_column"
	FindingNewColumn       = "new_column"
	FindingProbablyRenamed = "probably_renamed"
	FindingFormatChange    = "format_change"
	FindingUnreadable      = "unreadable"
)

// Finding is one finding of the change report (D23): a missing, a new or a
// probably renamed column (D22), an unreadable delivery (D42), or a format
// change (D85). For a rename, Column is the missing column and Detail the
// new one (D79). For a format change, Count is the number of source rows
// whose value failed and Examples are up to five of the failed values, as
// they look now.
type Finding struct {
	Kind     string
	Source   string
	Column   string
	Detail   string
	Count    int
	Examples []string
}

// DeliveryError is a delivery error that ends a run (D42): a delivery that
// cannot be read, or the first delivery error whose code is set to
// ModeStop.
type DeliveryError struct {
	Source string
	Code   string
	Err    error
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("delivery error %s in %s: %v", e.Code, e.Source, e.Err)
}

func (e *DeliveryError) Unwrap() error { return e.Err }

// Source is the input of a pipeline read by a Reader (F8). The methods
// return a changed copy.
type Source struct {
	r       Reader
	expect  []string
	newCols NewColumnMode
	id      string
	hash    bool
	key     []string
}

// NewSource returns a source that reads with r. Readers come from the
// packages csv and excel, or from FromRejects.
func NewSource(r Reader) Source { return Source{r: r} }

// Expect sets the expected layout of the source: the columns the delivery
// should have (D22). The header is checked against it when the source is
// opened. A missing column is the delivery error missing_column, which
// rejects every row in reject mode (D19). New columns are reported and
// passed on, see OnNewColumns. A missing and a similar new column are
// reported as probably renamed (D79). For a delivery without header, the
// expected columns name the fields by position (D53).
func (s Source) Expect(cols ...string) Source {
	s.expect = append([]string(nil), cols...)
	return s
}

// OnNewColumns sets what the source does with columns the expected layout
// does not name; the default is ReportNewColumns (D22).
func (s Source) OnNewColumns(m NewColumnMode) Source {
	s.newCols = m
	return s
}

// DeliveryID sets the identifier of the delivery that record_key is formed
// from, in place of the fingerprint the reader takes at open, for example
// for a delivery over HTTP (D61).
func (s Source) DeliveryID(id string) Source {
	s.id = id
	return s
}

// WithRecordHash adds record_hash to every row: a hash over its cell
// values, or over the raw bytes of a line that could not be split (D18,
// D81). It helps to recognize a delivery loaded twice.
func (s Source) WithRecordHash() Source {
	s.hash = true
	return s
}

// Key sets a business key from the given columns: record_key is then the
// first 16 hex characters of SHA-256 over the names of the columns and
// their values as read, the same across deliveries (D18, D97). A line
// without cells keeps the key from fingerprint and line (D81). A key
// column that is neither in the header nor expected is a plan error.
func (s Source) Key(cols ...string) Source {
	s.key = append([]string(nil), cols...)
	return s
}

// Table reads the whole delivery into a table (D31). The table carries the
// rows rejected while reading and the findings of the header check. It
// returns a *DeliveryError if the delivery cannot be read, or a *PlanError
// if the source has no column names.
func (s Source) Table(ctx context.Context) (Table, error) {
	res, err := FromSource(s, DefaultBlockLen).Run(ctx)
	return res.Table, err
}

// FromSource returns a pipeline over the rows of src, run in blocks of
// blockLen rows; 0 means DefaultBlockLen (see From).
func FromSource(src Source, blockLen int) *Pipeline {
	return &Pipeline{src: readerSource{src}, blockLen: blockLen}
}

// readerSource is a Source as the input of a pipeline.
type readerSource struct{ s Source }

func (rs readerSource) open() (opened, error) {
	if rs.s.r == nil {
		return nil, &PlanError{Step: "source", Err: errors.New("no reader")}
	}
	h, err := rs.s.r.Open()
	if err != nil {
		rs.s.r.Close()
		var de *DeliveryError
		var pe *PlanError
		if errors.As(err, &de) || errors.As(err, &pe) {
			return nil, err
		}
		return nil, &DeliveryError{Source: h.Source, Code: CodeUnreadable, Err: err}
	}
	o, err := newOpenedReader(rs.s, h)
	if err != nil {
		rs.s.r.Close()
		return nil, err
	}
	return o, nil
}

// openedReader is an opened delivery: the raw state as it is read, the
// working schema after the header check, and the findings.
type openedReader struct {
	src      Source
	h        Header
	raw      *rawSource
	width    int   // raw columns
	work     []int // raw column per working column, -1 for a missing one
	s        schema
	missing  []string
	findings []Finding
	keyCols  []int // raw column per business key column, -1 for a missing one (D97)
}

func newOpenedReader(src Source, h Header) (*openedReader, error) {
	cols := h.Columns
	if cols == nil {
		if src.expect == nil {
			return nil, &PlanError{Step: "source", Err: fmt.Errorf("source %q has no header and no expected layout", h.Source)}
		}
		cols = src.expect
	}
	seen := make(map[string]bool, len(cols))
	for i, c := range cols {
		switch {
		case c == "":
			return nil, &DeliveryError{Source: h.Source, Code: CodeUnreadable, Err: fmt.Errorf("column %d of the header has no name", i+1)}
		case seen[c]:
			return nil, &DeliveryError{Source: h.Source, Code: CodeUnreadable, Err: fmt.Errorf("column %q appears twice in the header", c)}
		}
		seen[c] = true
	}
	if src.id != "" {
		h.ID = src.id
	}
	o := &openedReader{src: src, h: h, width: len(cols)}
	rs := make(schema, len(cols))
	for i, c := range cols {
		rs[i] = field{c, block.Text}
	}
	o.raw = &rawSource{name: h.Source, sheet: h.Sheet, s: rs, rel: &release{},
		loc: &readLoc{id: h.ID, cells: h.Cells, hashes: src.hash, noRaw: benchknob.NoRawState.Load()}}
	for _, k := range src.key {
		j := slices.Index(cols, k)
		if j < 0 && !slices.Contains(src.expect, k) {
			return nil, &PlanError{Step: "source", Err: fmt.Errorf("key column %q is neither in the header of %q nor expected", k, h.Source)}
		}
		o.keyCols = append(o.keyCols, j)
	}

	expected := make(map[string]bool, len(src.expect))
	for _, c := range src.expect {
		expected[c] = true
	}
	var added []string
	for i, c := range cols {
		isNew := src.expect != nil && !expected[c]
		if isNew {
			added = append(added, c)
		}
		if isNew && src.newCols == IgnoreNewColumns {
			continue
		}
		o.work = append(o.work, i)
		o.s = append(o.s, field{c, block.Text})
	}
	for _, c := range src.expect {
		if !seen[c] {
			o.missing = append(o.missing, c)
			o.work = append(o.work, -1)
			o.s = append(o.s, field{c, block.Text})
			o.findings = append(o.findings, Finding{Kind: FindingMissingColumn, Source: h.Source, Column: c})
		}
	}
	if src.newCols == ReportNewColumns {
		for _, c := range added {
			o.findings = append(o.findings, Finding{Kind: FindingNewColumn, Source: h.Source, Column: c})
		}
	}
	for _, p := range probablyRenamed(o.missing, added) {
		o.findings = append(o.findings, Finding{Kind: FindingProbablyRenamed, Source: h.Source, Column: p[0], Detail: p[1]})
	}
	return o, nil
}

func (o *openedReader) schema() schema         { return o.s }
func (o *openedReader) rejects() []rejectEntry { return nil }
func (o *openedReader) sources() []*rawSource  { return []*rawSource{o.raw} }
func (o *openedReader) findingList() []Finding { return o.findings }
func (o *openedReader) close() error           { return o.src.r.Close() }
func (o *openedReader) stopAtOpen(p errorPolicy) error {
	if len(o.missing) > 0 && p.modeFor(CodeMissingColumn) == ModeStop {
		return &DeliveryError{Source: o.h.Source, Code: CodeMissingColumn,
			Err: fmt.Errorf("column %q is missing from the delivery", o.missing[0])}
	}
	return nil
}

// blocks reads the delivery in blocks of at most n records. Records the
// reader could not split, and every record while a column is missing, are
// rejected at read time; the others go on as working rows that share the
// raw columns (D55).
func (o *openedReader) blocks(n int, sc *stepCtx, yield func(batch) error) error {
	for {
		chunk, done, err := o.readChunk(n)
		if err != nil {
			return err
		}
		if len(chunk.recs) > 0 {
			if err := o.emit(chunk, sc, yield); err != nil {
				return err
			}
		}
		if done {
			return nil
		}
	}
}

// rawChunk is a run of records stored in the raw state: rows first to first
// plus len(recs)-1, their raw columns, and why a record was rejected.
type rawChunk struct {
	first int
	cols  []block.Column
	recs  []chunkRec
}

type chunkRec struct {
	code, reason string
	column       int
}

func (o *openedReader) readChunk(n int) (rawChunk, bool, error) {
	c := rawChunk{first: o.raw.rows()}
	builders := make([]*block.Builder, o.width)
	for i := range builders {
		builders[i] = block.NewBuilder(block.Text, n)
	}
	// The location, keys and hashes go with the raw state (D111).
	offsets := block.NewBuilder(block.Int, n)
	var lines []lineRun
	var keys, hashes, displays []string
	hasKey, hasDisplay := false, false
	loc := o.raw.loc
	done := false
	for len(c.recs) < n {
		r, err := o.src.r.Next()
		if err == io.EOF {
			done = true
			break
		}
		if err != nil {
			return rawChunk{}, false, &DeliveryError{Source: o.h.Source, Code: CodeUnreadable, Err: err}
		}
		row := o.raw.rows() + len(c.recs)
		cr := chunkRec{code: r.Code, reason: r.Reason, column: r.Column}
		if cr.code == "" && len(r.Fields) != o.width {
			cr = chunkRec{CodeUnparseableLine, fmt.Sprintf("line has %d fields, want %d", len(r.Fields), o.width), -1}
			r.Fields = nil
		}
		if cr.code == "" {
			cr.column = -1
		}
		fields := r.Fields
		if cr.code != "" && len(fields) != o.width {
			fields = nil
		}
		for i, b := range builders {
			if fields == nil {
				b.AppendNull()
			} else {
				b.AppendText(fields[i])
			}
		}
		if i := len(c.recs); len(lines) == 0 || lines[len(lines)-1].line+i-lines[len(lines)-1].row != r.Line {
			lines = append(lines, lineRun{i, r.Line})
		}
		if r.Offset >= 0 {
			offsets.AppendInt(r.Offset)
		} else {
			offsets.AppendNull()
		}
		if cr.code != "" && len(r.Raw) > 0 {
			if loc.rawLines == nil {
				loc.rawLines = map[int]string{}
			}
			loc.rawLines[row] = string(r.Raw)
		}
		disp := ""
		if len(r.Display) > 0 && fields != nil {
			disp, hasDisplay = encodeDisplay(r.Display), true
		}
		displays = append(displays, disp)
		key := r.key
		if key == "" && o.keyCols != nil && fields != nil {
			key = businessKey(o.src.key, o.keyCols, fields)
		}
		keys, hasKey = append(keys, key), hasKey || key != ""
		if loc.hashes {
			h := r.hash
			if h == "" {
				h = recordHash(r.Fields, r.Raw)
			}
			hashes = append(hashes, h)
		}
		c.recs = append(c.recs, cr)
	}
	c.cols = make([]block.Column, o.width)
	for i, b := range builders {
		c.cols[i] = b.Build()
	}
	chunk := c.cols
	if loc.noRaw {
		chunk = nil // D106
	}
	lay := chunkLayout{lines: lines, offset: len(chunk), hash: -1, display: -1}
	if hasKey {
		lay.keys, lay.hasKeys = textColumn(keys), true
	}
	chunk = append(chunk[:len(chunk):len(chunk)], offsets.Build())
	for _, extra := range []struct {
		vals []string
		on   bool
		at   *int
	}{{hashes, loc.hashes, &lay.hash}, {displays, hasDisplay, &lay.display}} {
		if extra.on {
			*extra.at = len(chunk)
			chunk = append(chunk, textColumn(extra.vals))
		}
	}
	o.raw.addChunk(chunk, lay, len(c.recs))
	return c, done, nil
}

// textColumn returns a text column of vals.
func textColumn(vals []string) block.Column {
	b := block.NewBuilder(block.Text, len(vals))
	for _, v := range vals {
		b.AppendText(v)
	}
	return b.Build()
}

// emit rejects the records of c that failed at read time and yields the
// others as one batch.
func (o *openedReader) emit(c rawChunk, sc *stepCtx, yield func(batch) error) error {
	var keep []int
	var rejected []origin
	sc.begin(block.Block{}, nil)
	for i, r := range c.recs {
		row := c.first + i
		if r.code == "" && len(o.missing) == 0 {
			keep = append(keep, i)
			continue
		}
		orig := origin{refs: []srcRef{{o.raw, row}}}
		sc.rx.tally.see([]origin{orig})
		sc.cnt.enter(orig)
		if r.code != "" || len(o.missing) > 0 {
			rejected = append(rejected, orig)
		}
		switch {
		case r.code != "":
			col := ""
			if r.column >= 0 && r.column < o.width {
				col = o.raw.s[r.column].name
			}
			if err := sc.rejectRow(row, orig, nil, col, "", false, r.reason, r.code); err != nil {
				return err
			}
		case len(o.missing) > 0:
			for _, m := range o.missing {
				reason := fmt.Sprintf("column %q is missing from the delivery", m)
				if err := sc.rejectRow(row, orig, nil, m, "", false, reason, CodeMissingColumn); err != nil {
					return err
				}
			}
		}
	}
	// Rows rejected while reading leave the plan here (D43).
	sc.rx.tally.release(rejected)
	if len(keep) == 0 {
		return nil
	}
	cols := make([]block.Column, len(o.work))
	for i, j := range o.work {
		if j < 0 {
			b := block.NewBuilder(block.Text, len(keep))
			for range keep {
				b.AppendNull()
			}
			cols[i] = b.Build()
			continue
		}
		col := c.cols[j]
		if !benchknob.NoRawState.Load() {
			col = col.Share() // with the raw state (D55)
		}
		if len(keep) < len(c.recs) {
			col = col.Take(keep)
		}
		cols[i] = col
	}
	orig := make([]origin, len(keep))
	refs := make([]srcRef, len(keep))
	for i, k := range keep {
		refs[i] = srcRef{o.raw, c.first + k}
		orig[i] = origin{refs: refs[i : i+1 : i+1]}
	}
	sc.rx.tally.enter(sc.cnt, orig)
	sc.rx.tally.passed(sc.cnt, orig, false)
	return yield(batch{newBlock(cols, len(keep)), orig})
}

// recordHash is the record_hash of a row: the first 16 hex characters of
// SHA-256 over its cell values, or over the raw bytes of a line that could
// not be split (D81).
func recordHash(fields []string, raw []byte) string {
	h := sha256.New()
	if fields == nil {
		h.Write([]byte("raw:"))
		h.Write(raw)
	} else {
		for _, f := range fields {
			fmt.Fprintf(h, "%d:%s;", len(f), f)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// businessKey is the record_key of a row with a business key: the first 16
// hex characters of SHA-256 over the names of the key columns and their
// values (D97).
func businessKey(names []string, cols []int, fields []string) string {
	h := sha256.New()
	for i, name := range names {
		fmt.Fprintf(h, "%d:%s;", len(name), name)
		if j := cols[i]; j >= 0 {
			fmt.Fprintf(h, "%d:%s;", len(fields[j]), fields[j])
		} else {
			h.Write([]byte("n;"))
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// probablyRenamed pairs missing with new columns whose names are similar
// (D79): equal after normalizing, or a Levenshtein distance of at most 2
// and at most a third of the longer normalized name. Each missing column,
// in order, takes the closest new column not taken yet; a tie is no pair.
func probablyRenamed(missing, added []string) [][2]string {
	var out [][2]string
	taken := make([]bool, len(added))
	for _, m := range missing {
		nm := normalizeName(m)
		best, bestDist, tie := -1, 0, false
		for j, a := range added {
			if taken[j] {
				continue
			}
			na := normalizeName(a)
			d := levenshtein(nm, na)
			if d > 0 && (d > 2 || 3*d > max(len([]rune(nm)), len([]rune(na)))) {
				continue
			}
			switch {
			case best < 0 || d < bestDist:
				best, bestDist, tie = j, d, false
			case d == bestDist:
				tie = true
			}
		}
		if best >= 0 && !tie {
			taken[best] = true
			out = append(out, [2]string{m, added[best]})
		}
	}
	return out
}

// normalizeName lowers a column name and keeps only letters and digits.
func normalizeName(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

// columnLetters returns the letters of the i-th column from A (0 is A).
func columnLetters(i int) string {
	s := ""
	for i++; i > 0; i = (i - 1) / 26 {
		s = string(rune('A'+(i-1)%26)) + s
	}
	return s
}
