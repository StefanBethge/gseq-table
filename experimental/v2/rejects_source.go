package gtable

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RejectsOption configures FromRejects.
type RejectsOption func(*rejectsReader)

// RejectsPrefix sets the prefix of the info columns of the rejects table;
// the default is DefaultInfoPrefix (D16).
func RejectsPrefix(prefix string) RejectsOption {
	return func(r *rejectsReader) { r.prefix = prefix }
}

// Reparse sets how lines that could not be split into cells are read
// again: with the reader of the original source in its current
// configuration (D16), such as csv.Reparse. Without it, such lines are
// rejected again with their raw bytes.
func Reparse(split func(rawLine string) ([]string, error)) RejectsOption {
	return func(r *rejectsReader) { r.reparse = split }
}

// FromRejects returns a source over the rejected rows of one source, as
// RejectedRows.Source returns them or as a reader reads them back (F10,
// D16). The raw columns become the columns of the source, the info columns
// do not. Each row keeps the location, record_key and record_hash of its
// info columns, so a row that fails again points to the original delivery
// and a row that passes keeps its key (D82). Aggregated rejected rows
// cannot be processed again this way (D48).
func FromRejects(rejects Table, opts ...RejectsOption) Source {
	r := &rejectsReader{t: rejects, prefix: DefaultInfoPrefix}
	for _, opt := range opts {
		opt(r)
	}
	s := NewSource(r)
	_, s.hash = rejects.Column(r.prefix + "record_hash")
	return s
}

type rejectsReader struct {
	t       Table
	prefix  string
	reparse func(string) ([]string, error)

	raw  []Column
	info map[string]Column
	row  int
}

func (r *rejectsReader) Open() (Header, error) {
	if r.t.err != nil {
		return Header{}, &PlanError{Step: "source", Err: fmt.Errorf("rejects table has an error: %w", r.t.err)}
	}
	if r.prefix == "" {
		return Header{}, &PlanError{Step: "source", Err: errors.New("empty info prefix")}
	}
	r.raw, r.info, r.row = nil, map[string]Column{}, 0
	var h Header
	for _, name := range r.t.Columns() {
		c, _ := r.t.Column(name)
		if n, ok := strings.CutPrefix(name, r.prefix); ok {
			r.info[n] = c
			continue
		}
		r.raw = append(r.raw, c)
		h.Columns = append(h.Columns, name)
	}
	if _, ok := r.info["source"]; !ok {
		return Header{}, &PlanError{Step: "source", Err: fmt.Errorf("no column %ssource: not a table of rejected rows", r.prefix)}
	}
	for i := range r.t.Len() {
		src, _ := r.text("source", i)
		switch {
		case h.Source == "":
			h.Source = src
		case src != h.Source:
			return Header{}, &PlanError{Step: "source", Err: fmt.Errorf("rejected rows of two sources, %q and %q", h.Source, src)}
		}
		if sheet, ok := r.text("sheet", i); ok {
			h.Sheet = sheet
		}
		if _, ok := r.text("cell", i); ok {
			h.Cells = true
		}
	}
	if h.Columns == nil {
		h.Columns = []string{}
	}
	return h, nil
}

// text returns cell i of info column name as text.
func (r *rejectsReader) text(name string, i int) (string, bool) {
	c, ok := r.info[name]
	if !ok {
		return "", false
	}
	return vecOf(c.col).format(i)
}

func (r *rejectsReader) int(name string, i int) (int64, bool) {
	s, ok := r.text(name, i)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func (r *rejectsReader) Next() (Record, error) {
	if r.row >= r.t.Len() {
		return Record{}, io.EOF
	}
	i := r.row
	r.row++
	rec := Record{Offset: -1, Column: -1}
	if n, ok := r.int("line", i); ok {
		rec.Line = int(n)
	}
	if n, ok := r.int("offset", i); ok {
		rec.Offset = n
	}
	rec.key, _ = r.text("record_key", i)
	rec.hash, _ = r.text("record_hash", i)

	fields := make([]string, len(r.raw))
	complete := true
	for j, c := range r.raw {
		v, ok := vecOf(c.col).format(i)
		fields[j], complete = v, complete && ok
	}
	raw, hasRaw := r.text("raw_line", i)
	if complete || !hasRaw {
		rec.Fields = fields
		return rec, nil
	}
	// A line without cells goes through the original reader again (D16).
	rec.Raw = []byte(raw)
	if r.reparse != nil {
		fs, err := r.reparse(raw)
		if err == nil {
			rec.Fields = fs
			return rec, nil
		}
		rec.Code, rec.Reason = CodeUnparseableLine, err.Error()
		return rec, nil
	}
	rec.Code, _ = r.text("code", i)
	if rec.Code != CodeFieldTooLarge {
		rec.Code = CodeUnparseableLine
	}
	rec.Reason, _ = r.text("reason", i)
	return rec, nil
}

func (r *rejectsReader) Close() error { return nil }
