package excel

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/stefanbethge/gseq-table/internal/cell"
	"github.com/stefanbethge/gseq-table/table"
	"github.com/xuri/excelize/v2"
)

// DefaultSheetName is the sheet name used when none is configured.
const DefaultSheetName = "Sheet1"

// WriterConfig holds the resolved settings for a Writer.
type WriterConfig struct {
	// HasHeader controls whether the column headers are written as the first
	// row. Defaults to true.
	HasHeader bool

	// Sheet is the name of the sheet Write and WriteFile write to.
	// Defaults to DefaultSheetName.
	Sheet string

	// TypedCells writes cells that parse as integers or floats as native
	// Excel numbers instead of text. Defaults to false.
	TypedCells bool
}

// WriterOption is a functional option for configuring a Writer.
type WriterOption func(*WriterConfig)

// WithoutHeader suppresses the header row in the output.
func WithoutHeader() WriterOption { return func(c *WriterConfig) { c.HasHeader = false } }

// WithWriteSheet sets the sheet name used by Write and WriteFile
// (default "Sheet1").
//
//	excel.NewWriter(excel.WithWriteSheet("Sales"))
func WithWriteSheet(name string) WriterOption {
	return func(c *WriterConfig) { c.Sheet = name }
}

// WithTypedCells writes cells that parse as integers or floats as native
// Excel numbers, so they can be summed and sorted in Excel. All other cells,
// including NaN and ±Inf, stay text.
//
// Number parsing follows the shared cell rules: surrounding whitespace is
// trimmed, and formatting such as leading zeros ("007") or trailing zeros
// ("1.50") is not preserved. Leave this option off when a lossless round trip
// through Reader is required.
func WithTypedCells() WriterOption { return func(c *WriterConfig) { c.TypedCells = true } }

// Sheet pairs a table with the name of the worksheet it is written to.
// It is the unit of WriteSheets and WriteFileSheets.
type Sheet struct {
	// Name is the worksheet name. If empty, "Sheet<N>" is used, where N is
	// the 1-based position of the sheet.
	Name string

	// Table is the data written to the worksheet.
	Table table.Table
}

// Writer serialises one or more table.Table values to an Excel (.xlsx)
// workbook.
//
//	err := excel.NewWriter(excel.WithWriteSheet("Sales")).WriteFile("out.xlsx", t)
type Writer struct{ config WriterConfig }

// NewWriter constructs a Writer with the supplied options. Unset options keep
// their defaults: HasHeader=true, Sheet="Sheet1", TypedCells=false.
func NewWriter(opts ...WriterOption) *Writer {
	cfg := WriterConfig{HasHeader: true, Sheet: DefaultSheetName}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Writer{config: cfg}
}

// WriteFile creates (or truncates) the file at path and writes t as a
// single-sheet workbook.
func (w *Writer) WriteFile(path string, t table.Table) error {
	return w.WriteFileSheets(path, Sheet{Name: w.config.Sheet, Table: t})
}

// Write serialises t as a single-sheet workbook to wr.
func (w *Writer) Write(wr io.Writer, t table.Table) error {
	return w.WriteSheets(wr, Sheet{Name: w.config.Sheet, Table: t})
}

// WriteFileSheets creates (or truncates) the file at path and writes each
// sheet as a separate worksheet, in order.
//
//	err := excel.NewWriter().WriteFileSheets("report.xlsx",
//	    excel.Sheet{Name: "Sales", Table: sales},
//	    excel.Sheet{Name: "Costs", Table: costs},
//	)
func (w *Writer) WriteFileSheets(path string, sheets ...Sheet) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := w.WriteSheets(f, sheets...); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// WriteSheets serialises each sheet as a separate worksheet to wr, in order.
// The first sheet becomes the active one. Returns an error if no sheets are
// given, a sheet name is invalid for Excel, or two sheets share a name
// (compared case-insensitively, as Excel does).
func (w *Writer) WriteSheets(wr io.Writer, sheets ...Sheet) error {
	if len(sheets) == 0 {
		return errors.New("excel: no sheets to write")
	}
	names, err := sheetNames(sheets)
	if err != nil {
		return err
	}

	f := excelize.NewFile()
	defer f.Close()
	if err := f.SetSheetName(f.GetSheetName(0), names[0]); err != nil {
		return fmt.Errorf("excel: sheet %q: %w", names[0], err)
	}
	for i, s := range sheets {
		if i > 0 {
			if _, err := f.NewSheet(names[i]); err != nil {
				return fmt.Errorf("excel: sheet %q: %w", names[i], err)
			}
		}
		if err := w.writeSheet(f, names[i], s.Table); err != nil {
			return err
		}
	}
	return f.Write(wr)
}

// --- internal helpers ---

// sheetNames resolves the worksheet name for each sheet and rejects names
// that collide. Name validity is checked by excelize when the sheet is created.
func sheetNames(sheets []Sheet) ([]string, error) {
	names := make([]string, len(sheets))
	seen := make(map[string]struct{}, len(sheets))
	for i, s := range sheets {
		name := s.Name
		if name == "" {
			name = fmt.Sprintf("Sheet%d", i+1)
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("excel: duplicate sheet name %q", name)
		}
		seen[key] = struct{}{}
		names[i] = name
	}
	return names, nil
}

// writeSheet streams t into the named sheet. Rows are padded or truncated
// to the header width, mirroring csv.Writer; a headerless table uses the
// width of its widest row.
func (w *Writer) writeSheet(f *excelize.File, sheet string, t table.Table) error {
	sw, err := f.NewStreamWriter(sheet)
	if err != nil {
		return err
	}

	width := len(t.Headers)
	if width == 0 {
		for _, row := range t.Rows {
			if len(row.Values()) > width {
				width = len(row.Values())
			}
		}
	}

	rowNum := 1
	record := make([]any, width)
	if w.config.HasHeader && len(t.Headers) > 0 {
		for i, h := range t.Headers {
			record[i] = h
		}
		if err := sw.SetRow("A1", record); err != nil {
			return err
		}
		rowNum++
	}
	for _, row := range t.Rows {
		values := row.Values()
		for i := range record {
			record[i] = nil
			if i < len(values) {
				record[i] = w.cellValue(values[i])
			}
		}
		axis, err := excelize.CoordinatesToCellName(1, rowNum)
		if err != nil {
			return err
		}
		if err := sw.SetRow(axis, record); err != nil {
			return err
		}
		rowNum++
	}
	return sw.Flush()
}

// cellValue converts a cell string to the value handed to excelize. Empty
// strings become nil so no empty cell is emitted.
func (w *Writer) cellValue(s string) any {
	if s == "" {
		return nil
	}
	if !w.config.TypedCells {
		return s
	}
	if n, err := cell.ParseInt(s, 64); err == nil {
		return n
	}
	if f, err := cell.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return f
	}
	return s
}
