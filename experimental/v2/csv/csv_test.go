package csv_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/delivery"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

var ctx = context.Background()

// write writes a delivery into a temporary directory and returns its path.
func write(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// cells returns the cells of a column as text, with <null> for null.
func cells(t *testing.T, tbl gtable.Table, col string) []string {
	t.Helper()
	if tbl.Err() != nil {
		t.Fatalf("sticky error: %v", tbl.Err())
	}
	c, ok := tbl.Column(col)
	if !ok {
		t.Fatalf("no column %q in %v", col, tbl.Columns())
	}
	out := make([]string, c.Len())
	for i := range out {
		var v string
		var ok bool
		switch c.Type() {
		case gtable.TypeInt:
			var n int64
			n, ok = c.Int(i)
			v = strconv.FormatInt(n, 10)
		default:
			v, ok = c.Text(i)
		}
		if !ok {
			v = "<null>"
		}
		out[i] = v
	}
	return out
}

func want(t *testing.T, tbl gtable.Table, col string, vals ...string) {
	t.Helper()
	if got := cells(t, tbl, col); !slices.Equal(got, vals) {
		t.Errorf("%s = %q, want %q", col, got, vals)
	}
}

func info(name string) string { return gtable.DefaultInfoPrefix + name }

func rejected(t *testing.T, tbl gtable.Table, source string) gtable.Table {
	t.Helper()
	rows, ok := tbl.RejectedRows().Source(source)
	if !ok {
		t.Fatalf("no rejected rows of %q", source)
	}
	return rows
}

func run(t *testing.T, src gtable.Source, blockLen int, ops ...gtable.Op) gtable.Table {
	t.Helper()
	p := gtable.FromSource(src, blockLen)
	for _, op := range ops {
		p.Then(op)
	}
	res, err := p.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res.Table
}

func offsetOf(t *testing.T, content, line string) string {
	t.Helper()
	i := strings.Index(content, line)
	if i < 0 {
		t.Fatalf("%q not in the delivery", line)
	}
	return strconv.Itoa(i)
}

func TestRejectsCarryRawStateNotWorkingState(t *testing.T) {
	testutil.Proves(t, "T1")

	path := write(t, "orders.csv", "id,amount\n1,12\n2,x7\n3,8\n")
	tbl := run(t, csv.File(path), 2,
		// An early step changes the column, a later one lets the row fail.
		gtable.With("amount", gtable.Col("amount").Upper()),
		gtable.Cast("amount", gtable.TypeInt),
	)
	want(t, tbl, "amount", "12", "8")

	rows := rejected(t, tbl, "orders.csv")
	// The raw state is the value from the delivery (D1, D10) ...
	want(t, rows, "amount", "x7")
	want(t, rows, "id", "2")
	// ... and value is the value at the time of the error.
	want(t, rows, info("value"), "X7")
	want(t, rows, info("line"), "3")
}

func TestUnparseableLinesAreRejectedWithRawBytes(t *testing.T) {
	testutil.Proves(t, "T10")

	content := "id,name,amount\n" +
		"1,Anna,10\n" +
		"2,\"Bo\"b,20\n" + // extraneous quote in a quoted field
		"3,Carl\n" + // too few fields
		"4,\"Dora \"\"D\"\"\",40\n" +
		"5,Eve,50,extra\r\n" + // too many fields
		"6,Gr\"eg,60\n" + // bare quote
		"7,Finn,70"
	path := write(t, "d.csv", content)
	tbl := run(t, csv.File(path), 3)

	// The other rows pass.
	want(t, tbl, "id", "1", "4", "7")
	want(t, tbl, "name", "Anna", `Dora "D"`, "Finn")

	rows := rejected(t, tbl, "d.csv")
	lines := []string{"2,\"Bo\"b,20", "3,Carl", "5,Eve,50,extra", "6,Gr\"eg,60"}
	want(t, rows, info("raw_line"), lines...)
	want(t, rows, info("line"), "3", "4", "6", "7")
	var offsets []string
	for _, l := range lines {
		offsets = append(offsets, offsetOf(t, content, l))
	}
	want(t, rows, info("offset"), offsets...)
	want(t, rows, info("code"), "unparseable_line", "unparseable_line", "unparseable_line", "unparseable_line")
	// A line without cells has no raw cell values (D10).
	want(t, rows, "id", "<null>", "<null>", "<null>", "<null>")
}

func TestLocationStaysRightOverSortAndFilter(t *testing.T) {
	testutil.Proves(t, "T11")

	content := "id,amount,keep\n" +
		"1,10,y\n" +
		"2,bad2,y\n" +
		"3,30,n\n" +
		"4,bad4,y\n" +
		"5,bad5,n\n" +
		"6,60,y\n"
	tbl := run(t, csv.File(write(t, "d.csv", content)), 2,
		gtable.Cast("id", gtable.TypeInt),
		gtable.Sort(gtable.Desc("id")),
		gtable.Where(gtable.Col("keep").Eq(gtable.Lit("y"))),
		gtable.Cast("amount", gtable.TypeInt),
	)
	want(t, tbl, "id", "6", "1")

	// Sorted descending, the rows with ids 4 and 2 fail; each points to the
	// line its values come from (D10).
	rows := rejected(t, tbl, "d.csv")
	want(t, rows, "id", "4", "2")
	want(t, rows, "amount", "bad4", "bad2")
	want(t, rows, info("line"), "5", "3")
	want(t, rows, info("offset"), offsetOf(t, content, "4,bad4"), offsetOf(t, content, "2,bad2"))
}

// keys runs a delivery whose rows all fail and returns their record_keys.
func keys(t *testing.T, src gtable.Source) []string {
	t.Helper()
	tbl := run(t, src, 100, gtable.Cast("n", gtable.TypeInt))
	return cells(t, rejected(t, tbl, "big.csv"), info("record_key"))
}

func TestDeliveryIDIsFixedAtOpen(t *testing.T) {
	testutil.Proves(t, "T37")

	// A delivery over 64 KiB whose rows all fail the cast.
	var sb strings.Builder
	sb.WriteString("n,pad\n")
	for i := 0; sb.Len() < 100<<10; i++ {
		sb.WriteString("x" + strconv.Itoa(i) + "," + strings.Repeat("p", 50) + "\n")
	}
	content := sb.String()
	path := write(t, "big.csv", content)
	mtime := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	touch := func() {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	touch()
	first := keys(t, csv.File(path))

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := delivery.Fingerprint("big.csv", f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if first[0] != fp+":2" {
		t.Errorf("first record_key = %q, want %q", first[0], fp+":2")
	}

	// The same unchanged file gives the same keys.
	if again := keys(t, csv.File(path)); !slices.Equal(again, first) {
		t.Error("the same file gives other keys")
	}
	// The key is fixed from the start of the file: a change behind the first
	// 64 KiB, at the same size and time, leaves it unchanged (D61) ...
	tail := []byte(content)
	i := len(tail) - 10
	tail[i] = 'q'
	if err := os.WriteFile(path, tail, 0o600); err != nil {
		t.Fatal(err)
	}
	touch()
	if got := keys(t, csv.File(path)); got[0] != first[0] {
		t.Errorf("a change behind 64 KiB changed the key: %q, want %q", got[0], first[0])
	}
	// ... and a changed file is another delivery with other keys.
	if err := os.WriteFile(path, []byte(content+"x,y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	touch()
	if got := keys(t, csv.File(path)); got[0] == first[0] {
		t.Error("a changed file keeps its keys")
	}
	// An identifier of the pipeline replaces the fingerprint.
	if got := keys(t, csv.File(path).DeliveryID("lieferung-42")); got[0] != "lieferung-42:2" {
		t.Errorf("record_key = %q, want lieferung-42:2", got[0])
	}
}

func TestLocationAndKeyFollowThePhysicalLine(t *testing.T) {
	testutil.Proves(t, "T54")

	content := "id,note\n" +
		"1,a\n" +
		"\n" +
		"2,\"multi\nline\"\n" +
		"3,b\n"
	path := write(t, "d.csv", content)
	tbl := run(t, csv.File(path).WithRecordHash(), 10, gtable.Cast("note", gtable.TypeInt))
	rows := rejected(t, tbl, "d.csv")
	want(t, rows, "note", "a", "multi\nline", "b")
	want(t, rows, info("line"), "2", "4", "6")
	want(t, rows, info("offset"), offsetOf(t, content, "1,a"), offsetOf(t, content, "2,\"multi"), offsetOf(t, content, "3,b"))

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := delivery.Fingerprint("d.csv", f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	want(t, rows, info("record_key"), fp+":2", fp+":4", fp+":6")

	// The same cell values under another name have the same record_hash.
	other := write(t, "copy.csv", content)
	tbl2 := run(t, csv.File(other).WithRecordHash(), 10, gtable.Cast("note", gtable.TypeInt))
	if a, b := cells(t, rows, info("record_hash")), cells(t, rejected(t, tbl2, "copy.csv"), info("record_hash")); !slices.Equal(a, b) {
		t.Errorf("record_hash differs by name: %q, %q", a, b)
	}

	// A header with a repeated name is unreadable (D81).
	_, err = csv.File(write(t, "dup.csv", "a,b,a\n1,2,3\n")).Table(ctx)
	var de *gtable.DeliveryError
	if !errors.As(err, &de) || de.Code != gtable.CodeUnreadable {
		t.Errorf("err = %v, want unreadable", err)
	}
}

func TestRejectsAsSourceKeepKeysAndLocation(t *testing.T) {
	testutil.Proves(t, "T56")

	content := "id,name,amount\n" +
		"1,A,10\n" +
		"2,B, 20\n" + // passes with Lenient
		"3,C,x\n" + // fails again
		"4,Do\"ra,40\n" // readable with LazyQuotes
	path := write(t, "d.csv", content)
	first := run(t, csv.File(path), 10, gtable.Cast("amount", gtable.TypeInt))
	want(t, first, "id", "1")
	rej := rejected(t, first, "d.csv")
	// The line rejected at read time comes first, then those of the cast.
	want(t, rej, info("line"), "5", "3", "4")
	firstKeys := cells(t, rej, info("record_key"))

	// Reprocess with a fixed pipeline and a changed reader (D16). A last
	// step rejects every row, to show the keys of the rows that passed.
	src := gtable.FromRejects(rej, csv.Reparse(csv.LazyQuotes()))
	second := run(t, src, 10,
		gtable.Cast("amount", gtable.TypeInt, gtable.Lenient()),
		gtable.WhereFunc(func(gtable.Row) (bool, error) { return false, errors.New("check") }),
	)
	if !slices.Equal(second.Columns(), []string{"id", "name", "amount"}) {
		t.Errorf("columns = %q, want the raw columns only", second.Columns())
	}
	again := rejected(t, second, "d.csv")
	want(t, again, "id", "3", "4", "2")
	want(t, again, info("code"), "parse", "custom", "custom")
	// Keys and location are those of the first run (D82).
	want(t, again, info("record_key"), firstKeys[2], firstKeys[0], firstKeys[1])
	want(t, again, info("line"), "4", "5", "3")
	want(t, again, info("offset"), offsetOf(t, content, "3,C"), offsetOf(t, content, "4,Do"), offsetOf(t, content, "2,B"))
	want(t, again, info("source"), "d.csv", "d.csv", "d.csv")
	// The line that could not be split before has cells now.
	want(t, again, "name", "C", `Do"ra`, "B")
}

// A field over the limit rejects its line with field_too_large and a cut
// raw_line (D56; prepares T35). A UTF-8 byte order mark is not part of the
// first column name.
func TestFieldSizeLimit(t *testing.T) {
	path := write(t, "d.csv", "\ufeffid,note\n1,short\n2,far too long\n")
	tbl := run(t, csv.File(path, csv.MaxFieldSize(6)), 10)
	want(t, tbl, "id", "1")
	rows := rejected(t, tbl, "d.csv")
	want(t, rows, info("code"), "field_too_large")
	want(t, rows, info("column"), "note")
	want(t, rows, info("raw_line"), "2,far ")
	want(t, rows, "note", "<null>")
}
