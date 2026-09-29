package csv_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// keySink keeps the row keys and one column of the blocks it gets.
type keySink struct {
	col  string
	keys []string
	vals []string
}

func (s *keySink) Write(_ context.Context, b gtable.Block) error {
	s.keys = append(s.keys, b.RowKeys...)
	if c, ok := b.Rows.Column(s.col); ok {
		for i := range c.Len() {
			v, _ := c.Format(i)
			s.vals = append(s.vals, v)
		}
	}
	return nil
}

func (s *keySink) Close() error { return nil }

// readBack reads a file of rejected rows written by a writer. Its columns
// carry the info prefix, which is reserved in a delivery, so it is read
// under another prefix (D14, G66).
func readBack(t *testing.T, path string) gtable.Table {
	t.Helper()
	res, err := gtable.FromSource(csv.File(path), 100).InfoPrefix("_read_").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res.Table
}

// texts returns the cells of every column of tbl as text, null as empty
// text, which is what a CSV file holds.
func texts(t *testing.T, tbl gtable.Table) map[string][]string {
	t.Helper()
	if tbl.Err() != nil {
		t.Fatalf("sticky error: %v", tbl.Err())
	}
	out := map[string][]string{}
	for _, name := range tbl.Columns() {
		c, _ := tbl.Column(name)
		vals := make([]string, c.Len())
		for i := range vals {
			vals[i], _ = c.Format(i)
		}
		out[name] = vals
	}
	return out
}

const dirtyOrders = "order,amount,date\n" +
	"1,5,2026-09-01\n" +
	"2,x,2026-09-02\n" + // amount fails
	"3;7;2026-09-03\n" + // one field only; readable with ';'
	"4,y,someday\n" + // fails in amount and date
	"5,8,2026-09-05\n"

func castOrders(amountNulls ...string) gtable.Op {
	return gtable.CastAll(
		gtable.Cast("amount", gtable.TypeInt, gtable.NullTexts(amountNulls...)),
		gtable.Cast("date", gtable.TypeTimestamp, gtable.DateFormat("2006-01-02"), gtable.NullTexts("someday")),
	)
}

func TestRejectsWrittenWithTheCSVWriterReadBack(t *testing.T) {
	testutil.Proves(t, "T2")
	path := write(t, "orders.csv", dirtyOrders)
	out := filepath.Join(t.TempDir(), "rejects.csv")

	// The rejected rows go through the same writer as results (D2).
	res, err := gtable.FromSource(csv.File(path), 2).Then(castOrders()).
		RejectsTo("orders.csv", csv.Create(out)).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inMemory, err := gtable.FromSource(csv.File(path), 2).Then(castOrders()).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantRows := rejected(t, inMemory.Table, "orders.csv")
	if _, ok := res.Table.RejectedRows().Source("orders.csv"); ok {
		t.Error("the result still holds the rows written")
	}

	// Read back, raw and info columns are those of the table; a null, as
	// in value or offset, comes back as empty text, since CSV has no null.
	back := readBack(t, out)
	if !slices.Equal(back.Columns(), wantRows.Columns()) {
		t.Fatalf("columns = %q, want %q", back.Columns(), wantRows.Columns())
	}
	got, want := texts(t, back), texts(t, wantRows)
	for _, name := range wantRows.Columns() {
		if name == info("run_id") || name == info("reject_id") {
			continue // two runs
		}
		if !slices.Equal(got[name], want[name]) {
			t.Errorf("%s = %q, want %q", name, got[name], want[name])
		}
	}
	if raw := got[info("raw_line")]; !slices.Contains(raw, "3;7;2026-09-03") {
		t.Errorf("raw_line = %q", raw)
	}
}

func TestReprocessingKeepsTheOriginalLocationAndKeys(t *testing.T) {
	testutil.Proves(t, "T12")
	path := write(t, "orders.csv", dirtyOrders)
	out := filepath.Join(t.TempDir(), "rejects.csv")

	first := &keySink{col: "order"}
	if _, err := gtable.FromSource(csv.File(path), 2).Then(castOrders()).
		To(first).RejectsTo("orders.csv", csv.Create(out)).Run(ctx); err != nil {
		t.Fatal(err)
	}
	written := readBack(t, out)
	firstKeys := texts(t, written)[info("record_key")]
	if len(firstKeys) != 3 {
		t.Fatalf("%d rows written, want 3: the row with two errors once (D44)", len(firstKeys))
	}

	// The fixed pipeline, with a reader that splits the broken line, runs
	// the rows read back from the file (D16).
	fixed := gtable.FromSource(csv.File(path), 2).Then(castOrders("y"))
	second := &keySink{col: "order"}
	res, err := fixed.ReplaceSource(gtable.FromRejects(written, csv.Reparse(csv.Comma(';')))).
		To(second).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Rows that pass keep their record_key (D18, D82); the one with two
	// errors is processed once.
	if !slices.Equal(second.vals, []string{"3", "4"}) || !slices.Equal(second.keys, firstKeys[1:]) {
		t.Errorf("passed %q with keys %q, want [3 4] with %q", second.vals, second.keys, firstKeys[1:])
	}
	// A row that fails again points to the original delivery.
	again := rejected(t, res.Table, "orders.csv")
	want(t, again, "order", "2")
	want(t, again, info("line"), "3")
	want(t, again, info("offset"), offsetOf(t, dirtyOrders, "2,x"))
	want(t, again, info("record_key"), firstKeys[0])
}

func TestRecordKeyIsStableWithinADeliveryAndABusinessKeyBeyond(t *testing.T) {
	testutil.Proves(t, "T13")
	dir := t.TempDir()
	put := func(name, content string, mtime time.Time) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		return p
	}
	keysOf := func(src gtable.Source) []string {
		t.Helper()
		s := &keySink{col: "order"}
		if _, err := gtable.FromSource(src, 2).To(s).Run(ctx); err != nil {
			t.Fatal(err)
		}
		return s.keys
	}
	day := time.Date(2026, 9, 28, 6, 0, 0, 0, time.UTC)
	content := "order,amount\n1,5\n2,6\n3,7\n"
	old := put("orders.csv", content, day)

	// The same delivery gives the same keys in two runs ...
	first := keysOf(csv.File(old))
	if again := keysOf(csv.File(old)); !slices.Equal(again, first) {
		t.Errorf("second run: %q, want %q", again, first)
	}
	// ... and in reprocessing (D16, D82).
	res, err := gtable.FromSource(csv.File(old), 2).
		Then(gtable.WhereFunc(func(gtable.Row) (bool, error) { return false, errors.New("check") })).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(gtable.FromRejects(rejected(t, res.Table, "orders.csv"))); !slices.Equal(got, first) {
		t.Errorf("reprocessing: %q, want %q", got, first)
	}

	// A row inserted at the top, and a new delivery under the same name,
	// overwrite no key of the old delivery (D61).
	inserted := keysOf(csv.File(put("orders.csv", "order,amount\n0,4\n1,5\n2,6\n3,7\n", day.Add(time.Hour))))
	renewed := keysOf(csv.File(put("orders.csv", "order,amount\n7,1\n8,2\n9,3\n", day.Add(2*time.Hour))))
	for _, k := range append(inserted, renewed...) {
		if slices.Contains(first, k) {
			t.Errorf("key %q of a later delivery is a key of the old one", k)
		}
	}

	// With a business key, record_key is the same across deliveries (D18).
	a := keysOf(csv.File(put("orders-0928.csv", content, day)).Key("order"))
	b := keysOf(csv.File(put("orders-0929.csv", "order,amount\n0,4\n1,5\n2,6\n3,7\n", day.Add(24*time.Hour))).Key("order"))
	if !slices.Equal(a, b[1:]) {
		t.Errorf("business keys %q and %q differ", a, b[1:])
	}

	// The same delivery under another name has the same record_hash.
	hashes := func(p string) []string {
		t.Helper()
		res, err := gtable.FromSource(csv.File(p).WithRecordHash(), 2).
			Then(gtable.Cast("order", gtable.TypeBool)).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return cells(t, rejected(t, res.Table, filepath.Base(p)), info("record_hash"))
	}
	if h1, h2 := hashes(put("x.csv", content, day)), hashes(put("y.csv", content, day)); !slices.Equal(h1, h2) {
		t.Errorf("record_hash %q under another name, want %q", h2, h1)
	}
}

func TestBusinessKeyFormsRecordKeyFromTheValues(t *testing.T) {
	testutil.Proves(t, "T64")
	content := "region,order,amount\nN,1,5\nS,1,6\nN,2,7\n\"broken,1,1\n"
	path := write(t, "a.csv", content)
	other := write(t, "b.csv", "order,region,amount\n2,N,9\n1,S,8\n1,N,0\n")

	keys := func(p string) (map[string]string, gtable.Table) {
		t.Helper()
		s := &keySink{col: "amount"}
		res, err := gtable.FromSource(csv.File(p).Key("region", "order"), 10).To(s).Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for i, k := range s.keys {
			out[s.vals[i]] = k
		}
		return out, res.Table
	}
	ka, tbl := keys(path)
	kb, _ := keys(other)
	// A key of 16 hex characters from the values, the same in both
	// deliveries and different for other values (D97).
	hex16 := regexp.MustCompile(`^[0-9a-f]{16}$`)
	for _, k := range ka {
		if !hex16.MatchString(k) {
			t.Errorf("record_key %q is not 16 hex characters", k)
		}
	}
	if ka["5"] != kb["0"] || ka["6"] != kb["8"] || ka["7"] != kb["9"] {
		t.Errorf("keys of the same values differ: %v and %v", ka, kb)
	}
	if ka["5"] == ka["6"] || ka["5"] == ka["7"] {
		t.Errorf("keys of other values are equal: %v", ka)
	}
	// A line without cells keeps the key from fingerprint and line (D81).
	positional, err := gtable.FromSource(csv.File(path), 10).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want(t, rejected(t, tbl, "a.csv"), info("record_key"), cells(t, rejected(t, positional.Table, "a.csv"), info("record_key"))...)

	// A key column neither in the header nor expected is a plan error.
	_, err = gtable.FromSource(csv.File(path).Key("customer"), 10).Run(ctx)
	var pe *gtable.PlanError
	if !errors.As(err, &pe) {
		t.Errorf("unknown key column: err %v, want a *PlanError", err)
	}
}
