package gtable

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// fakeReader delivers fixed records, the way a format package does.
type fakeReader struct {
	h       Header
	recs    []Record
	openErr error
	next    int
	opened  int
	closed  int
}

func (r *fakeReader) Open() (Header, error) {
	r.opened++
	r.next = 0
	return r.h, r.openErr
}

func (r *fakeReader) Next() (Record, error) {
	if r.next >= len(r.recs) {
		return Record{}, io.EOF
	}
	r.next++
	return r.recs[r.next-1], nil
}

func (r *fakeReader) Close() error { r.closed++; return nil }

func rec(line int, fields ...string) Record {
	return Record{Fields: fields, Line: line, Offset: -1}
}

func findingKinds(fs []Finding) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Kind + ":" + f.Column + ":" + f.Detail
	}
	return out
}

func TestProbablyRenamedColumns(t *testing.T) {
	testutil.Proves(t, "T55")

	for _, c := range []struct {
		expect, got []string
		want        []string
	}{
		// Normalized equal and a small edit distance are renames (D79).
		{[]string{"Kunden Nr"}, []string{"kunden_nr"}, []string{"Kunden Nr:kunden_nr"}},
		{[]string{"Kundennr"}, []string{"Kundenr"}, []string{"Kundennr:Kundenr"}},
		// Short names are not paired by the relative limit.
		{[]string{"id"}, []string{"nr"}, nil},
		// Two new columns at the same smallest distance: no guess.
		{[]string{"abcd"}, []string{"abce", "abcf"}, nil},
		// A new column pairs with one missing column only.
		{[]string{"name1", "name2"}, []string{"name3"}, []string{"name1:name3"}},
	} {
		got := probablyRenamed(c.expect, c.got)
		var pairs []string
		for _, p := range got {
			pairs = append(pairs, p[0]+":"+p[1])
		}
		if !slices.Equal(pairs, c.want) {
			t.Errorf("probablyRenamed(%q, %q) = %q, want %q", c.expect, c.got, pairs, c.want)
		}
	}

	// Through a source: missing and new columns are reported in any case.
	r := &fakeReader{
		h:    Header{Source: "d.csv", Columns: []string{"id", "kunden_nr", "extra"}, ID: "fp"},
		recs: []Record{rec(2, "1", "K1", "x")},
	}
	tbl, err := NewSource(r).Expect("id", "Kunden Nr").Table(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"missing_column:Kunden Nr:",
		"new_column:kunden_nr:",
		"new_column:extra:",
		"probably_renamed:Kunden Nr:kunden_nr",
	}
	if got := findingKinds(tbl.Findings()); !slices.Equal(got, want) {
		t.Errorf("findings = %q, want %q", got, want)
	}
}

func TestSourceChecksTheHeader(t *testing.T) {
	ctx := context.Background()
	reader := func() *fakeReader {
		return &fakeReader{
			h:    Header{Source: "d.csv", Columns: []string{"id", "new", "gone_not"}, ID: "fp"},
			recs: []Record{rec(2, "1", "a", "b"), rec(3, "2", "c", "d")},
		}
	}

	// A missing column rejects every row with missing_column in reject
	// mode (D19, D22); new columns are reported and passed on by default.
	tbl, err := NewSource(reader()).Expect("id", "amount").Table(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Len() != 0 {
		t.Errorf("rows = %d, want 0", tbl.Len())
	}
	if !slices.Equal(tbl.Columns(), []string{"id", "new", "gone_not", "amount"}) {
		t.Errorf("columns = %q", tbl.Columns())
	}
	if got := codes(tbl.Rejects()); !slices.Equal(got, []string{"missing_column", "missing_column"}) {
		t.Errorf("codes = %q", got)
	}
	rows := sourceRows(t, tbl.RejectedRows(), "d.csv")
	wantCells(t, rows, "id", "1", "2")
	wantCells(t, rows, DefaultInfoPrefix+"column", "amount", "amount")
	wantCells(t, rows, DefaultInfoPrefix+"line", "2", "3")

	// Ignored new columns do not reach the working data.
	tbl, err = NewSource(reader()).Expect("id").OnNewColumns(IgnoreNewColumns).Table(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tbl.Columns(), []string{"id"}) || len(tbl.Findings()) != 0 {
		t.Errorf("columns = %q, findings = %v", tbl.Columns(), tbl.Findings())
	}

	// Stop mode stops at the missing column before any row (D19).
	_, err = FromSource(NewSource(reader()).Expect("id", "amount"), 10).
		OnErrorCode(CodeMissingColumn, ModeStop).Run(ctx)
	var de *DeliveryError
	if !errors.As(err, &de) || de.Code != CodeMissingColumn || de.Source != "d.csv" {
		t.Errorf("err = %v, want a missing_column *DeliveryError", err)
	}

	// Without a header the names come from the expected layout (D53).
	r := reader()
	r.h.Columns = nil
	tbl, err = NewSource(r).Expect("a", "b", "c").Table(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantCells(t, tbl, "b", "a", "c")
	if _, err := NewSource(&fakeReader{h: Header{Source: "x"}}).Table(ctx); err == nil {
		t.Error("no error for a source without header and expected layout")
	}

	// An empty or repeated name in the header is unreadable (D81).
	for _, cols := range [][]string{{"a", "a"}, {"a", ""}} {
		_, err := NewSource(&fakeReader{h: Header{Source: "x", Columns: cols}}).Table(ctx)
		if !errors.As(err, &de) || de.Code != CodeUnreadable {
			t.Errorf("header %q: err = %v, want unreadable", cols, err)
		}
	}
}

func TestSourceRejectsWhatTheReaderCannotSplit(t *testing.T) {
	r := &fakeReader{
		h: Header{Source: "d.csv", Columns: []string{"a", "b"}, ID: "fp"},
		recs: []Record{
			{Fields: []string{"1", "2"}, Line: 2, Offset: 4, Raw: []byte("1,2")},
			{Fields: []string{"1", "2", "3"}, Line: 3, Offset: 8, Raw: []byte("1,2,3")},
			{Line: 4, Offset: 14, Raw: []byte(`"x`), Code: CodeUnparseableLine, Reason: "bare quote"},
			{Fields: []string{"5", "6"}, Line: 5, Offset: 17, Raw: []byte("5,6")},
		},
	}
	res, err := FromSource(NewSource(r).WithRecordHash(), 2).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tbl := res.Table
	wantCells(t, tbl, "a", "1", "5")
	if r.opened != 1 || r.closed != 1 {
		t.Errorf("opened %d, closed %d, want 1 and 1", r.opened, r.closed)
	}
	rows := sourceRows(t, tbl.RejectedRows(), "d.csv")
	wantCells(t, rows, "a", "<null>", "<null>")
	wantCells(t, rows, DefaultInfoPrefix+"raw_line", "1,2,3", `"x`)
	wantCells(t, rows, DefaultInfoPrefix+"line", "3", "4")
	wantCells(t, rows, DefaultInfoPrefix+"offset", "8", "14")
	wantCells(t, rows, DefaultInfoPrefix+"code", "unparseable_line", "unparseable_line")
	wantCells(t, rows, DefaultInfoPrefix+"record_key", "fp:3", "fp:4")
	wantCells(t, rows, DefaultInfoPrefix+"step", "read", "read")
	if h := info(t, rows, "record_hash"); h[0] == "<null>" || h[0] == h[1] {
		t.Errorf("record_hash = %q", h)
	}

	// A delivery that cannot be opened is unreadable, and nothing runs.
	r = &fakeReader{h: Header{Source: "gone.csv"}, openErr: errors.New("no such file")}
	_, err = FromSource(NewSource(r), 2).Run(context.Background())
	var de *DeliveryError
	if !errors.As(err, &de) || de.Code != CodeUnreadable {
		t.Errorf("err = %v, want unreadable", err)
	}
}
