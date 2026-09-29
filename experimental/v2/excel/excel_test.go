package excel_test

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
	x "github.com/stefanbethge/gseq-table/experimental/v2/internal/xlsxtest"
)

var ctx = context.Background()

// serial returns the Excel serial number of t (1900 date system).
func serial(t time.Time) string {
	d := t.Sub(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC))
	return strconv.FormatFloat(d.Hours()/24, 'f', -1, 64)
}

func book(t *testing.T, b x.Book) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "d.xlsx")
	if err := x.Write(p, b); err != nil {
		t.Fatal(err)
	}
	return p
}

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
		if c.Type() == gtable.TypeInt {
			var n int64
			n, ok = c.Int(i)
			v = strconv.FormatInt(n, 10)
		} else {
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

func run(t *testing.T, src gtable.Source, ops ...gtable.Op) gtable.Table {
	t.Helper()
	p := gtable.FromSource(src, 2)
	for _, op := range ops {
		p.Then(op)
	}
	res, err := p.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res.Table
}

func rejected(t *testing.T, tbl gtable.Table, source string) gtable.Table {
	t.Helper()
	rows, ok := tbl.RejectedRows().Source(source)
	if !ok {
		t.Fatalf("no rejected rows of %q in %d rejects", source, len(tbl.Rejects()))
	}
	return rows
}

func header(cols ...string) x.Row {
	r := x.Row{Num: 1}
	for i, c := range cols {
		r.Cells = append(r.Cells, x.Text(string(rune('A'+i))+"1", c))
	}
	return r
}

func TestExcelCellsCarryStoredValueAndDisplay(t *testing.T) {
	testutil.Proves(t, "T34")

	day := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	path := book(t, x.Book{
		Formats: []string{"dd.mm.yyyy", "#,##0.00"},
		Sheets: []x.Sheet{{Name: "Kunden", Rows: []x.Row{
			header("id", "datum", "betrag"),
			{Num: 2, Cells: []x.Cell{x.Num("A2", "1", 0), x.Num("B2", serial(day), 1), x.Num("C2", "1234.5", 2)}},
			{Num: 3, Cells: []x.Cell{x.Num("A3", "2", 0), x.Text("B3", "gestern"), x.Num("C3", "7", 2)}},
		}}},
	})

	// Raw state and working column carry the stored value in a fixed text
	// form (D62).
	all := run(t, excel.Sheet(path, "Kunden"))
	want(t, all, "datum", "2026-09-27", "gestern")
	want(t, all, "betrag", "1234.5", "7")

	tbl := run(t, excel.Sheet(path, "Kunden"), gtable.Cast("betrag", gtable.TypeInt))
	want(t, tbl, "id", "2")
	rows := rejected(t, tbl, "d.xlsx#Kunden")
	want(t, rows, "datum", "2026-09-27")
	want(t, rows, "betrag", "1234.5")
	// The rejected row carries the displayed text, the cell and the line as
	// Excel shows it, counting the header (D52).
	want(t, rows, info("display"), "1,234.50")
	want(t, rows, info("cell"), "C2")
	want(t, rows, info("line"), "2")
	want(t, rows, info("sheet"), "Kunden")
	want(t, rows, info("offset"), "<null>")
	want(t, rows, info("raw_line"), "<null>")
}

func TestExcelCellTypesHaveAFixedTextForm(t *testing.T) {
	testutil.Proves(t, "T53")

	at := time.Date(2026, 9, 27, 14, 30, 0, 0, time.UTC)
	path := book(t, x.Book{
		Formats: []string{"yyyy-mm-dd hh:mm", "hh:mm", "#22"},
		Sheets: []x.Sheet{{Name: "Typen", Rows: []x.Row{
			header("kind", "value"),
			{Num: 2, Cells: []x.Cell{x.Text("A2", "datetime"), x.Num("B2", serial(at), 1)}},
			{Num: 3, Cells: []x.Cell{x.Text("A3", "time"), x.Num("B3", strconv.FormatFloat(14.5/24, 'f', -1, 64), 2)}},
			{Num: 4, Cells: []x.Cell{x.Text("A4", "big"), x.Num("B4", "1E+05", 0)}},
			// Row 5 is missing, row 6 has only empty cells.
			{Num: 6, Cells: []x.Cell{{Ref: "A6", Type: "inlineStr"}, x.Num("B6", "", 0)}},
			{Num: 7, Cells: []x.Cell{x.Text("A7", "bool"), {Ref: "B7", Type: "b", Value: "1"}}},
			{Num: 8, Cells: []x.Cell{x.Text("A8", "error"), {Ref: "B8", Type: "e", Value: "#DIV/0!"}}},
			{Num: 9, Cells: []x.Cell{{Ref: "A9", Type: "inlineStr", Value: "formula"}, {Ref: "B9", Formula: "2*21", Value: "42"}}},
			{Num: 10, Cells: []x.Cell{x.Text("A10", "builtin"), x.Num("B10", serial(at), 3)}},
		}}},
	})
	tbl := run(t, excel.Sheet(path, "Typen"),
		// A derived column is not a raw column: its reject has no cell.
		gtable.With("derived", gtable.Col("value").Upper()),
		gtable.Cast("derived", gtable.TypeInt),
	)
	rows := rejected(t, tbl, "d.xlsx#Typen")
	want(t, rows, "kind", "datetime", "time", "bool", "error", "builtin")
	want(t, rows, "value", "2026-09-27T14:30:00", "14:30:00", "true", "#DIV/0!", "2026-09-27T14:30:00")
	// The empty rows are skipped but counted (D80).
	want(t, rows, info("line"), "2", "3", "7", "8", "10")
	want(t, rows, info("cell"), "<null>", "<null>", "<null>", "<null>", "<null>")
	want(t, rows, info("display"), "<null>", "<null>", "<null>", "<null>", "<null>")
	want(t, tbl, "kind", "big", "formula")
	want(t, tbl, "value", "100000", "42")
}

// A row with values right of the header is rejected (G64), a missing
// sheet is a delivery error (D42), and an archive over the
// unpacked size limit is unreadable (D56, D83; prepares T35).
func TestExcelDeliveryErrors(t *testing.T) {
	path := book(t, x.Book{Sheets: []x.Sheet{{Name: "S", Rows: []x.Row{
		header("a", "b"),
		{Num: 2, Cells: []x.Cell{x.Text("A2", "1"), x.Text("B2", "2")}},
		{Num: 3, Cells: []x.Cell{x.Text("A3", "3"), x.Text("B3", "4"), x.Text("D3", "extra")}},
	}}}})
	tbl := run(t, excel.Sheet(path, "S"))
	want(t, tbl, "a", "1")
	rows := rejected(t, tbl, "d.xlsx#S")
	want(t, rows, info("code"), "unparseable_line")
	want(t, rows, "b", "4")
	want(t, rows, info("line"), "3")

	var de *gtable.DeliveryError
	_, err := excel.Sheet(path, "Auftraege").Table(ctx)
	if !errors.As(err, &de) || de.Code != gtable.CodeMissingSheet || de.Source != "d.xlsx#Auftraege" {
		t.Errorf("err = %v, want missing_sheet", err)
	}
	_, err = excel.Sheet(path, "S", excel.MaxUnpackedSize(100)).Table(ctx)
	if !errors.As(err, &de) || de.Code != gtable.CodeUnreadable {
		t.Errorf("err = %v, want unreadable", err)
	}
	_, err = excel.Sheet(path, "S", excel.MaxRatio(1)).Table(ctx)
	if !errors.As(err, &de) || de.Code != gtable.CodeUnreadable {
		t.Errorf("err = %v, want unreadable", err)
	}
	_, err = excel.Sheet(filepath.Join(t.TempDir(), "gone.xlsx"), "S").Table(ctx)
	if !errors.As(err, &de) || de.Code != gtable.CodeUnreadable {
		t.Errorf("err = %v, want unreadable", err)
	}

	// Without a sheet name the first sheet is read.
	first, err := excel.Sheet(path, "").Table(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := first.RejectedRows().Source("d.xlsx#S"); !ok {
		t.Error("first sheet not named d.xlsx#S")
	}
}

// A file written by excelize itself, with its own styles and relations,
// reads the same way.
func TestExcelFileWrittenByExcelize(t *testing.T) {
	f := excelize.NewFile()
	defer f.Close()
	if _, err := f.NewSheet("Daten"); err != nil {
		t.Fatal(err)
	}
	date, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		ref string
		v   any
	}{
		{"A1", "name"}, {"B1", "seit"}, {"C1", "menge"},
		{"A2", "Nordlicht"}, {"B2", time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)}, {"C2", 12},
		{"A3", "Sonne"}, {"B3", "unbekannt"}, {"C3", 2.5},
	} {
		if err := f.SetCellValue("Daten", c.ref, c.v); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.SetCellStyle("Daten", "B2", "B2", date); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "e.xlsx")
	if err := f.SaveAs(p); err != nil {
		t.Fatal(err)
	}
	tbl := run(t, excel.Sheet(p, "Daten"), gtable.Cast("seit", gtable.TypeTimestamp))
	want(t, tbl, "name", "Nordlicht")
	rows := rejected(t, tbl, "e.xlsx#Daten")
	want(t, rows, "menge", "2.5")
	want(t, rows, info("cell"), "B3")
	want(t, rows, info("display"), "unbekannt")
}

func TestTwoSheetsAreTwoSourcesAndAMissingSheetIsADeliveryError(t *testing.T) {
	testutil.Proves(t, "T42")
	path := book(t, x.Book{Sheets: []x.Sheet{
		{Name: "Kunden", Rows: []x.Row{
			header("kunde", "limit"),
			{Num: 2, Cells: []x.Cell{x.Text("A2", "K1"), x.Text("B2", "100")}},
			{Num: 3, Cells: []x.Cell{x.Text("A3", "K2"), x.Text("B3", "viel")}},
		}},
		{Name: "Auftraege", Rows: []x.Row{
			header("auftrag", "kunde", "betrag"),
			{Num: 2, Cells: []x.Cell{x.Text("A2", "A1"), x.Text("B2", "K1"), x.Text("C2", "5")}},
			{Num: 3, Cells: []x.Cell{x.Text("A3", "A2"), x.Text("B3", "K1"), x.Text("C3", "fünf")}},
		}},
	}})

	// Each sheet is a source of its own, with its own table of rejected
	// rows (D53).
	kunden, err := excel.Sheet(path, "Kunden").Table(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := gtable.FromSource(excel.Sheet(path, "Auftraege"), 2).
		Then(gtable.Cast("betrag", gtable.TypeInt)).
		Then(gtable.InnerJoin(kunden.Cast("limit", gtable.TypeInt), gtable.On("kunde"))).
		Run(ctx)
	if err != nil || res.Status != gtable.StatusOK {
		t.Fatalf("err %v, status %v", err, res.Status)
	}
	want(t, res.Table, "auftrag", "A1")
	var names []string
	for _, s := range res.Table.RejectedRows().Sources() {
		names = append(names, s.Source)
	}
	if !slices.Equal(names, []string{"d.xlsx#Auftraege", "d.xlsx#Kunden"}) {
		t.Errorf("sources = %q", names)
	}
	want(t, rejected(t, res.Table, "d.xlsx#Auftraege"), "betrag", "fünf")
	want(t, rejected(t, res.Table, "d.xlsx#Kunden"), "limit", "viel")

	// A missing sheet is the delivery error missing_sheet (D42).
	res, err = gtable.FromSource(excel.Sheet(path, "Lieferungen"), 2).Run(ctx)
	var de *gtable.DeliveryError
	if !errors.As(err, &de) || de.Code != gtable.CodeMissingSheet || de.Source != "d.xlsx#Lieferungen" {
		t.Errorf("err = %v, want missing_sheet", err)
	}
	if res.Status != gtable.StatusDeliveryError || res.ExitCode() != 3 {
		t.Errorf("status %v, exit %d", res.Status, res.ExitCode())
	}
}

// An archive over the limit for its unpacked size or for the ratio of
// unpacked to packed size is unreadable, and the run ends with
// delivery_error (D56, D83).
func TestExcelArchiveLimitsAreDeliveryErrors(t *testing.T) {
	testutil.Proves(t, "T35")
	path := book(t, x.Book{Sheets: []x.Sheet{{Name: "S", Rows: []x.Row{
		header("a"),
		{Num: 2, Cells: []x.Cell{x.Text("A2", "1")}},
	}}}})
	for name, opt := range map[string]excel.Option{
		"unpacked size": excel.MaxUnpackedSize(100),
		"ratio":         excel.MaxRatio(1),
	} {
		res, err := gtable.FromSource(excel.Sheet(path, "S", opt), 10).Run(ctx)
		var de *gtable.DeliveryError
		if !errors.As(err, &de) || de.Code != gtable.CodeUnreadable || res.Status != gtable.StatusDeliveryError {
			t.Errorf("%s: err %v, status %v, want unreadable and delivery_error", name, err, res.Status)
		}
	}
	if _, err := gtable.FromSource(excel.Sheet(path, "S"), 10).Run(ctx); err != nil {
		t.Errorf("within the default limits: %v", err)
	}
}
