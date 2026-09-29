package excel_test

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
	x "github.com/stefanbethge/gseq-table/experimental/v2/internal/xlsxtest"
)

// texts returns the cells of every column of tbl as text, null as empty
// text, which is what an empty cell reads back as (D80).
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

// readBack reads a sheet written by the writer. Columns of rejected rows
// carry the info prefix, so it is read under another prefix (D14, G66).
func readBack(t *testing.T, path, sheet string) gtable.Table {
	t.Helper()
	res, err := gtable.FromSource(excel.Sheet(path, sheet), 100).InfoPrefix("_read_").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res.Table
}

func TestRejectsWrittenWithTheExcelWriterReadBack(t *testing.T) {
	testutil.Proves(t, "T2")
	path := book(t, x.Book{
		Formats: []string{"#,##0.00"},
		Sheets: []x.Sheet{{Name: "Kunden", Rows: []x.Row{
			header("id", "betrag", "notiz"),
			{Num: 2, Cells: []x.Cell{x.Num("A2", "1", 0), x.Num("B2", "1234.5", 1), x.Text("C2", "ok")}},
			{Num: 3, Cells: []x.Cell{x.Num("A3", "2", 0), x.Text("B3", "auf Anfrage"), x.Text("C3", "=SUM(A1)")}},
			{Num: 4, Cells: []x.Cell{x.Num("A4", "3", 0), x.Num("B4", "7", 1)}},
		}}},
	})
	out := filepath.Join(t.TempDir(), "rejects.xlsx")
	cast := gtable.Cast("betrag", gtable.TypeInt)

	// The rejected rows go through the same writer as results (D2).
	if _, err := gtable.FromSource(excel.Sheet(path, "Kunden"), 2).Then(cast).
		RejectsTo("d.xlsx#Kunden", excel.Create(out, "Rejects")).Run(ctx); err != nil {
		t.Fatal(err)
	}
	wantRows := rejected(t, run(t, excel.Sheet(path, "Kunden"), cast), "d.xlsx#Kunden")

	// Read back, raw and info columns are those of the table, with the
	// cell and the displayed text; a null comes back as empty text.
	back := readBack(t, out, "Rejects")
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
	if got[info("display")][0] != "1,234.50" || got["notiz"][1] != "=SUM(A1)" {
		t.Errorf("display %q, notiz %q", got[info("display")], got["notiz"])
	}
}

func TestExcelWriterWritesTypedCellsAndTextNeverAsFormula(t *testing.T) {
	testutil.Proves(t, "T66")
	at := time.Date(2026, 9, 27, 14, 30, 0, 0, time.UTC)
	tbl := gtable.NewTable(
		gtable.Ints("n", 5, 6),
		gtable.Floats("f", 2.5, 0),
		gtable.Bools("b", true, false),
		gtable.Timestamps("at", at, at.Truncate(24*time.Hour)),
		gtable.Texts("s", "=1+1", "12").WithNulls(1),
	)
	out := filepath.Join(t.TempDir(), "out.xlsx")
	if _, err := gtable.From(tbl, 1).To(excel.Create(out, "")).Run(ctx); err != nil {
		t.Fatal(err)
	}

	f, err := excelize.OpenFile(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sheet := f.GetSheetName(0)
	for cell, typ := range map[string]excelize.CellType{
		"A2": excelize.CellTypeUnset, // a number has no type attribute
		"B2": excelize.CellTypeUnset,
		"C2": excelize.CellTypeBool,
		"D2": excelize.CellTypeUnset,
		"E2": excelize.CellTypeInlineString,
	} {
		if got, err := f.GetCellType(sheet, cell); err != nil || got != typ {
			t.Errorf("%s: type %v (%v), want %v", cell, got, err, typ)
		}
	}
	// A date has a date format; text is never a formula (D99).
	if st, _ := f.GetCellStyle(sheet, "D2"); st == 0 {
		t.Error("D2 has no date format")
	}
	if formula, _ := f.GetCellFormula(sheet, "E2"); formula != "" {
		t.Errorf("E2 holds the formula %q", formula)
	}
	// A null is an empty cell.
	if v, _ := f.GetCellValue(sheet, "E3"); v != "" {
		t.Errorf("E3 = %q, want empty", v)
	}

	// Read back, the cells have the text forms of D80.
	back := run(t, excel.Sheet(out, ""))
	want(t, back, "n", "5", "6")
	want(t, back, "f", "2.5", "0")
	want(t, back, "b", "true", "false")
	want(t, back, "at", "2026-09-27T14:30:00", "2026-09-27")
	want(t, back, "s", "=1+1", "")
}
