package excel

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/xuri/excelize/v2"
)

func makeWriteTable() table.Table {
	return table.New(
		[]string{"name", "city", "age"},
		[][]string{
			{"Alice", "Berlin", "30"},
			{"Bob", "Munich", "25"},
		},
	)
}

// assertTableEqual compares headers and cell values of two tables.
func assertTableEqual(t *testing.T, got, want table.Table) {
	t.Helper()
	if strings.Join(got.Headers, "|") != strings.Join(want.Headers, "|") {
		t.Fatalf("headers: got %v, want %v", got.Headers, want.Headers)
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("rows: got %d, want %d", len(got.Rows), len(want.Rows))
	}
	for i := range want.Rows {
		g, w := got.Rows[i].Values(), want.Rows[i].Values()
		if strings.Join(g, "|") != strings.Join(w, "|") {
			t.Errorf("row %d: got %q, want %q", i, g, w)
		}
	}
}

// openWritten opens the workbook in buf with excelize for low-level checks.
func openWritten(t *testing.T, buf *bytes.Buffer) *excelize.File {
	t.Helper()
	f, err := excelize.OpenReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestWriter_Write_Roundtrip(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	res := New().Read(&buf)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	assertTableEqual(t, res.Unwrap(), makeWriteTable())
}

func TestWriter_Write_DefaultSheetName(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	names, err := SheetNamesFromReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(names), 1)
	assertEqual(t, names[0], DefaultSheetName)
}

func TestWriter_WithWriteSheet(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(WithWriteSheet("Sales")).Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	res := New(WithSheet("Sales")).Read(bytes.NewReader(buf.Bytes()))
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	assertTableEqual(t, res.Unwrap(), makeWriteTable())

	names, _ := SheetNamesFromReader(&buf)
	assertEqual(t, strings.Join(names, ","), "Sales")
}

func TestWriter_WithoutHeader(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(WithoutHeader()).Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	res := New(WithHeaderNames("name", "city", "age")).Read(&buf)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	assertTableEqual(t, res.Unwrap(), makeWriteTable())
}

func TestWriter_WithoutHeader_NoHeaderReader(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(WithoutHeader()).Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	tb := New(WithNoHeader()).Read(&buf).Unwrap()
	assertEqual(t, strings.Join(tb.Headers, ","), "col_0,col_1,col_2")
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("col_0").UnwrapOr(""), "Alice")
}

func TestWriter_WriteFile_Roundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.xlsx")
	if err := NewWriter().WriteFile(path, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	res := New().ReadFile(path)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	assertTableEqual(t, res.Unwrap(), makeWriteTable())
}

func TestWriter_WriteFile_BadPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "out.xlsx")
	if err := NewWriter().WriteFile(path, makeWriteTable()); err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestWriter_WriteFile_ErrorLeavesNoPartialWorkbook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.xlsx")
	err := NewWriter().WriteFileSheets(path,
		Sheet{Name: "A", Table: makeWriteTable()},
		Sheet{Name: "a", Table: makeWriteTable()},
	)
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	assertEqual(t, info.Size(), int64(0))
}

func TestWriter_Roundtrip_SpecialValues(t *testing.T) {
	want := table.New(
		[]string{"text", "formula", "spaces", "unicode", "numeric"},
		[][]string{
			{"a,b;c", "=SUM(A1:A2)", "  padded  ", "Grüße 日本", "007"},
			{"line1\nline2", "'quoted", "\ttab", "emoji 🎉", "1.50"},
			{`"double"`, "<xml>&amp;", " ", "ß", "-0"},
		},
	)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, New().Read(&buf).Unwrap(), want)
}

func TestWriter_Formula_WrittenAsText(t *testing.T) {
	var buf bytes.Buffer
	tb := table.New([]string{"f"}, [][]string{{"=1+1"}})
	if err := NewWriter().Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	formula, err := f.GetCellFormula(DefaultSheetName, "A2")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, formula, "")
}

func TestWriter_Roundtrip_EmptyCells(t *testing.T) {
	want := table.New(
		[]string{"a", "b", "c"},
		[][]string{
			{"1", "", "3"},
			{"", "", "6"},
			{"7", "8", ""},
		},
	)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, New().Read(&buf).Unwrap(), want)
}

func TestWriter_EmptyCells_NotEmitted(t *testing.T) {
	var buf bytes.Buffer
	tb := table.New([]string{"a", "b"}, [][]string{{"", "x"}})
	if err := NewWriter().Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	typ, err := f.GetCellType(DefaultSheetName, "A2")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, typ, excelize.CellTypeUnset)
}

func TestWriter_HeadersOnly(t *testing.T) {
	var buf bytes.Buffer
	want := table.New([]string{"a", "b"}, nil)
	if err := NewWriter().Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	got := New().Read(&buf).Unwrap()
	assertEqual(t, strings.Join(got.Headers, ","), "a,b")
	assertEqual(t, len(got.Rows), 0)
}

func TestWriter_EmptyTable(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, table.Table{}); err != nil {
		t.Fatal(err)
	}
	got := New().Read(&buf).Unwrap()
	assertEqual(t, len(got.Headers), 0)
	assertEqual(t, len(got.Rows), 0)
}

func TestWriter_ShortRowsPadded_LongRowsTruncated(t *testing.T) {
	tb := table.Table{
		Headers: []string{"a", "b"},
		Rows: []table.Row{
			table.NewRow([]string{"a", "b"}, []string{"1"}),
			table.NewRow([]string{"a", "b"}, []string{"2", "3", "extra"}),
		},
	}
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	rows, err := f.GetRows(DefaultSheetName)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(rows), 3)
	assertEqual(t, strings.Join(rows[1], ","), "1")
	assertEqual(t, strings.Join(rows[2], ","), "2,3")
}

func TestWriter_NoHeaders_UsesWidestRow(t *testing.T) {
	tb := table.Table{
		Rows: []table.Row{
			table.NewRow(nil, []string{"1"}),
			table.NewRow(nil, []string{"2", "3", "4"}),
		},
	}
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	rows, _ := f.GetRows(DefaultSheetName)
	assertEqual(t, len(rows), 2)
	assertEqual(t, strings.Join(rows[1], ","), "2,3,4")
}

func TestWriter_TypedCells(t *testing.T) {
	tb := table.New(
		[]string{"int", "float", "text", "padded", "nan", "inf", "empty"},
		[][]string{{"42", "3.25", "abc", " 7 ", "NaN", "+Inf", ""}},
	)
	var buf bytes.Buffer
	if err := NewWriter(WithTypedCells()).Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)

	// Numeric cells carry no type attribute, so excelize reports them as
	// CellTypeUnset; text cells are written as inline strings.
	cases := []struct {
		axis    string
		numeric bool
		val     string
	}{
		{"A2", true, "42"},
		{"B2", true, "3.25"},
		{"C2", false, "abc"},
		{"D2", true, "7"},
		{"E2", false, "NaN"},
		{"F2", false, "+Inf"},
	}
	for _, tc := range cases {
		assertEqual(t, isNumericCell(t, f, tc.axis), tc.numeric)
		val, _ := f.GetCellValue(DefaultSheetName, tc.axis)
		assertEqual(t, val, tc.val)
	}
	val, _ := f.GetCellValue(DefaultSheetName, "G2")
	assertEqual(t, val, "")
}

// isNumericCell reports whether the cell at axis holds a number.
func isNumericCell(t *testing.T, f *excelize.File, axis string) bool {
	t.Helper()
	typ, err := f.GetCellType(DefaultSheetName, axis)
	if err != nil {
		t.Fatal(err)
	}
	return typ == excelize.CellTypeNumber || typ == excelize.CellTypeUnset
}

func TestWriter_TypedCells_HeaderStaysText(t *testing.T) {
	tb := table.New([]string{"2024"}, [][]string{{"1"}})
	var buf bytes.Buffer
	if err := NewWriter(WithTypedCells()).Write(&buf, tb); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	if isNumericCell(t, f, "A1") {
		t.Error("header cell must not be written as a number")
	}
}

func TestWriter_WithoutTypedCells_NumbersAreText(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, makeWriteTable()); err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	if isNumericCell(t, f, "C2") {
		t.Error("expected text cell by default")
	}
}

func TestWriter_TypedCells_Roundtrip(t *testing.T) {
	want := table.New(
		[]string{"id", "amount", "label"},
		[][]string{
			{"1", "10.5", "a"},
			{"2", "-3", "b"},
		},
	)
	var buf bytes.Buffer
	if err := NewWriter(WithTypedCells()).Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, New().Read(&buf).Unwrap(), want)
}

func TestWriter_WriteSheets_Roundtrip(t *testing.T) {
	sales := makeWriteTable()
	costs := table.New([]string{"item", "cost"}, [][]string{{"rent", "1000"}})

	var buf bytes.Buffer
	err := NewWriter().WriteSheets(&buf,
		Sheet{Name: "Sales", Table: sales},
		Sheet{Name: "Costs", Table: costs},
	)
	if err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	names, _ := SheetNamesFromReader(bytes.NewReader(data))
	assertEqual(t, strings.Join(names, ","), "Sales,Costs")

	assertTableEqual(t, New(WithSheet("Sales")).Read(bytes.NewReader(data)).Unwrap(), sales)
	assertTableEqual(t, New(WithSheet("Costs")).Read(bytes.NewReader(data)).Unwrap(), costs)
	// The first sheet is also the default for the reader.
	assertTableEqual(t, New().Read(bytes.NewReader(data)).Unwrap(), sales)
}

func TestWriter_WriteSheets_FirstSheetActive(t *testing.T) {
	var buf bytes.Buffer
	err := NewWriter().WriteSheets(&buf,
		Sheet{Name: "One", Table: makeWriteTable()},
		Sheet{Name: "Two", Table: makeWriteTable()},
	)
	if err != nil {
		t.Fatal(err)
	}
	f := openWritten(t, &buf)
	assertEqual(t, f.GetSheetName(f.GetActiveSheetIndex()), "One")
}

func TestWriter_WriteSheets_DefaultNames(t *testing.T) {
	var buf bytes.Buffer
	err := NewWriter().WriteSheets(&buf,
		Sheet{Table: makeWriteTable()},
		Sheet{Name: "Named", Table: makeWriteTable()},
		Sheet{Table: makeWriteTable()},
	)
	if err != nil {
		t.Fatal(err)
	}
	names, _ := SheetNamesFromReader(&buf)
	assertEqual(t, strings.Join(names, ","), "Sheet1,Named,Sheet3")
}

func TestWriter_WriteSheets_OptionsApplyToAll(t *testing.T) {
	var buf bytes.Buffer
	err := NewWriter(WithoutHeader(), WithWriteSheet("ignored")).WriteSheets(&buf,
		Sheet{Name: "A", Table: makeWriteTable()},
		Sheet{Name: "B", Table: makeWriteTable()},
	)
	if err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	names, _ := SheetNamesFromReader(bytes.NewReader(data))
	assertEqual(t, strings.Join(names, ","), "A,B")
	for _, name := range names {
		got := New(WithSheet(name), WithHeaderNames("name", "city", "age")).Read(bytes.NewReader(data)).Unwrap()
		assertTableEqual(t, got, makeWriteTable())
	}
}

func TestWriter_WriteSheets_NoSheets(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter().WriteSheets(&buf); err == nil {
		t.Fatal("expected error for no sheets")
	}
	assertEqual(t, buf.Len(), 0)
}

func TestWriter_WriteSheets_DuplicateNames(t *testing.T) {
	cases := [][]Sheet{
		{{Name: "Data"}, {Name: "Data"}},
		{{Name: "Data"}, {Name: "DATA"}},
		{{}, {Name: "sheet1"}},
	}
	for i, sheets := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var buf bytes.Buffer
			err := NewWriter().WriteSheets(&buf, sheets...)
			if err == nil || !strings.Contains(err.Error(), "duplicate sheet name") {
				t.Fatalf("expected duplicate error, got %v", err)
			}
			assertEqual(t, buf.Len(), 0)
		})
	}
}

func TestWriter_InvalidSheetName(t *testing.T) {
	cases := []string{"bad/name", "a:b", "[x]", "'quoted'", strings.Repeat("x", 32)}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := NewWriter(WithWriteSheet(name)).Write(&buf, makeWriteTable())
			if err == nil {
				t.Fatalf("expected error for sheet name %q", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q should mention sheet name", err)
			}
			assertEqual(t, buf.Len(), 0)
		})
	}
}

func TestWriter_InvalidSecondSheetName(t *testing.T) {
	var buf bytes.Buffer
	err := NewWriter().WriteSheets(&buf,
		Sheet{Name: "ok", Table: makeWriteTable()},
		Sheet{Name: "not?ok", Table: makeWriteTable()},
	)
	if err == nil {
		t.Fatal("expected error for invalid second sheet name")
	}
}

type failWriter struct{}

var errFailWrite = errors.New("write failed")

func (failWriter) Write([]byte) (int, error) { return 0, errFailWrite }

func TestWriter_Write_PropagatesWriterError(t *testing.T) {
	err := NewWriter().Write(failWriter{}, makeWriteTable())
	if !errors.Is(err, errFailWrite) {
		t.Fatalf("got %v, want %v", err, errFailWrite)
	}
}

func TestWriter_Roundtrip_ManyRows(t *testing.T) {
	want := makeBenchTable(2500, 4)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, New().Read(&buf).Unwrap(), want)
}

func TestWriter_Roundtrip_Stream(t *testing.T) {
	want := makeBenchTable(250, 3)
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, want); err != nil {
		t.Fatal(err)
	}
	total := 0
	for chunk, err := range New().ReadStream(&buf, 100) {
		if err != nil {
			t.Fatal(err)
		}
		for i, row := range chunk.Rows {
			w := want.Rows[total+i].Values()
			assertEqual(t, strings.Join(row.Values(), "|"), strings.Join(w, "|"))
		}
		total += len(chunk.Rows)
	}
	assertEqual(t, total, 250)
}

// --- benchmarks ---

func makeBenchTable(rows, cols int) table.Table {
	headers := make([]string, cols)
	for c := range cols {
		headers[c] = fmt.Sprintf("col_%d", c)
	}
	records := make([][]string, rows)
	for r := range rows {
		rec := make([]string, cols)
		for c := range cols {
			if c%2 == 0 {
				rec[c] = strconv.Itoa(r * c)
			} else {
				rec[c] = fmt.Sprintf("value_%d_%d", r, c)
			}
		}
		records[r] = rec
	}
	return table.New(headers, records)
}

func BenchmarkWrite(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		tb := makeBenchTable(size, 8)
		b.Run(fmt.Sprintf("rows=%d", size), func(b *testing.B) {
			w := NewWriter()
			var buf bytes.Buffer
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				if err := w.Write(&buf, tb); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWrite_TypedCells(b *testing.B) {
	tb := makeBenchTable(1_000, 8)
	w := NewWriter(WithTypedCells())
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		if err := w.Write(&buf, tb); err != nil {
			b.Fatal(err)
		}
	}
}
