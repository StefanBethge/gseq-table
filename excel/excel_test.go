package excel

import (
	"os"
	"testing"

	"github.com/xuri/excelize/v2"
)

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// createTestFile creates a temporary .xlsx file with the given sheet data
// and returns its path. Caller must defer os.Remove(path).
func createTestFile(t *testing.T, sheetName string, rows [][]string) string {
	t.Helper()
	f := excelize.NewFile()
	defer f.Close()

	// Rename default sheet to desired name.
	defaultSheet := f.GetSheetName(0)
	if err := f.SetSheetName(defaultSheet, sheetName); err != nil {
		t.Fatal(err)
	}

	for i, row := range rows {
		for j, val := range row {
			cell, _ := excelize.CoordinatesToCellName(j+1, i+1)
			if err := f.SetCellValue(sheetName, cell, val); err != nil {
				t.Fatal(err)
			}
		}
	}

	tmp, err := os.CreateTemp("", "test*.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	path := tmp.Name()
	tmp.Close()
	if err := f.SaveAs(path); err != nil {
		t.Fatal(err)
	}
	return path
}

// createMultiSheetFile creates a file with multiple named sheets.
func createMultiSheetFile(t *testing.T, sheets map[string][][]string) string {
	t.Helper()
	f := excelize.NewFile()
	defer f.Close()

	first := true
	for name, rows := range sheets {
		if first {
			defaultSheet := f.GetSheetName(0)
			if err := f.SetSheetName(defaultSheet, name); err != nil {
				t.Fatal(err)
			}
			first = false
		} else {
			if _, err := f.NewSheet(name); err != nil {
				t.Fatal(err)
			}
		}
		for i, row := range rows {
			for j, val := range row {
				cell, _ := excelize.CoordinatesToCellName(j+1, i+1)
				if err := f.SetCellValue(name, cell, val); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	tmp, err := os.CreateTemp("", "test*.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	path := tmp.Name()
	tmp.Close()
	if err := f.SaveAs(path); err != nil {
		t.Fatal(err)
	}
	return path
}

// --- ReadFile tests ---

func TestReadFile_WithHeader(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"name", "city", "age"},
		{"Alice", "Berlin", "30"},
		{"Bob", "Munich", "25"},
	})
	defer os.Remove(path)

	res := New().ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Headers), 3)
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Headers[0], "name")
	assertEqual(t, tb.Headers[1], "city")
	assertEqual(t, tb.Headers[2], "age")
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

func TestReadFile_NoHeader_AutoNames(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"Alice", "Berlin", "30"},
		{"Bob", "Munich", "25"},
	})
	defer os.Remove(path)

	res := New(WithNoHeader()).ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Headers[0], "col_0")
	assertEqual(t, tb.Headers[1], "col_1")
	assertEqual(t, tb.Headers[2], "col_2")
	assertEqual(t, tb.Rows[0].Get("col_0").UnwrapOr(""), "Alice")
}

func TestReadFile_WithHeaderNames(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"Alice", "Berlin", "30"},
		{"Bob", "Munich", "25"},
	})
	defer os.Remove(path)

	res := New(WithHeaderNames("first", "location", "years")).ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Headers[0], "first")
	assertEqual(t, tb.Rows[0].Get("first").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("location").UnwrapOr(""), "Berlin")
}

func TestReadFile_WithShortHeaderNames_ClampsExtraFields(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"Alice", "Berlin", "30"},
	})
	defer os.Remove(path)

	res := New(WithHeaderNames("first", "location")).ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, len(tb.Rows[0].Values()), 2)
	assertEqual(t, tb.Rows[0].Get("first").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[0].Get("location").UnwrapOr(""), "Berlin")
}

func TestReadFile_Empty(t *testing.T) {
	path := createTestFile(t, "Empty", [][]string{})
	defer os.Remove(path)

	res := New().ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Headers), 0)
	assertEqual(t, len(tb.Rows), 0)
}

func TestReadFile_HeaderOnly(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"name", "city"},
	})
	defer os.Remove(path)

	res := New().ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, len(tb.Rows), 0)
}

func TestReadFile_NotFound(t *testing.T) {
	res := New().ReadFile("/nonexistent/file.xlsx")
	assertEqual(t, res.IsErr(), true)
}

// --- Sheet selection tests ---

func TestReadFile_SheetByName(t *testing.T) {
	path := createMultiSheetFile(t, map[string][][]string{
		"Sales": {
			{"product", "revenue"},
			{"Widget", "100"},
		},
		"Costs": {
			{"item", "amount"},
			{"Rent", "500"},
		},
	})
	defer os.Remove(path)

	res := New(WithSheet("Costs")).ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, tb.Headers[0], "item")
	assertEqual(t, tb.Rows[0].Get("item").UnwrapOr(""), "Rent")
}

func TestReadFile_SheetByIndex(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"name"},
		{"Alice"},
	})
	defer os.Remove(path)

	res := New(WithSheetIndex(0)).ReadFile(path)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestReadFile_SheetNotFound(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{{"a"}, {"1"}})
	defer os.Remove(path)

	res := New(WithSheet("NonexistentSheet")).ReadFile(path)
	assertEqual(t, res.IsErr(), true)
}

func TestReadFile_SheetIndexOutOfRange(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{{"a"}, {"1"}})
	defer os.Remove(path)

	res := New(WithSheetIndex(99)).ReadFile(path)
	assertEqual(t, res.IsErr(), true)
}

// --- Read from io.Reader ---

func TestRead_FromReader(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"name", "city"},
		{"Alice", "Berlin"},
	})
	defer os.Remove(path)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	res := New().Read(f)
	assertEqual(t, res.IsOk(), true)
	tb := res.Unwrap()
	assertEqual(t, len(tb.Rows), 1)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// --- Streaming tests ---

func TestReadFileStream_Basic(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"name", "value"},
		{"A", "1"},
		{"B", "2"},
		{"C", "3"},
		{"D", "4"},
		{"E", "5"},
	})
	defer os.Remove(path)

	var totalRows int
	var chunks int
	for tb, err := range New().ReadFileStream(path, 2) {
		if err != nil {
			t.Fatal(err)
		}
		chunks++
		totalRows += tb.Len()
		assertEqual(t, tb.Headers[0], "name")
	}
	assertEqual(t, totalRows, 5)
	assertEqual(t, chunks, 3) // [A,B], [C,D], [E]
}

func TestReadFileStream_SmallChunks(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"v"},
		{"1"},
		{"2"},
		{"3"},
	})
	defer os.Remove(path)

	var chunks int
	for _, err := range New().ReadFileStream(path, 1) {
		if err != nil {
			t.Fatal(err)
		}
		chunks++
	}
	assertEqual(t, chunks, 3) // one row per chunk
}

func TestReadFileStream_NoHeader(t *testing.T) {
	path := createTestFile(t, "Data", [][]string{
		{"Alice", "30"},
		{"Bob", "25"},
	})
	defer os.Remove(path)

	var totalRows int
	for tb, err := range New(WithNoHeader()).ReadFileStream(path, 10) {
		if err != nil {
			t.Fatal(err)
		}
		totalRows += tb.Len()
		assertEqual(t, tb.Headers[0], "col_0")
	}
	assertEqual(t, totalRows, 2)
}

// --- SheetNames ---

func TestSheetNames(t *testing.T) {
	path := createMultiSheetFile(t, map[string][][]string{
		"Sales":     {{"a"}, {"1"}},
		"Inventory": {{"b"}, {"2"}},
	})
	defer os.Remove(path)

	names, err := SheetNames(path)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(names), 2)
	// Check both names are present (order may vary due to map iteration)
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	assertEqual(t, found["Sales"], true)
	assertEqual(t, found["Inventory"], true)
}

func TestSheetNames_NotFound(t *testing.T) {
	_, err := SheetNames("/nonexistent/file.xlsx")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}
