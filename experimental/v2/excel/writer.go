package excel

import (
	"context"
	"errors"

	"github.com/xuri/excelize/v2"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
)

// Create returns a sink that writes one sheet of an Excel file at path
// (F19, D35), for results or rejected rows (D2); an empty sheet name is
// "Sheet1". The first row is the header. Integers and floats are written as
// numbers, booleans as booleans, timestamps as date cells, and text always
// as text, never as a formula; a null is an empty cell (D99).
//
// The file is created, or replaced, when the sink is closed. A run closes
// its sinks also after it ended early, so the blocks written before stay
// (D51, D100); a run without a block leaves no file.
func Create(path, sheet string) gtable.Sink {
	if sheet == "" {
		sheet = "Sheet1"
	}
	return &writer{path: path, sheet: sheet}
}

// writer implements gtable.Sink over one sheet, with the stream writer of
// excelize.
type writer struct {
	path, sheet string

	f   *excelize.File
	sw  *excelize.StreamWriter
	row int
}

func (w *writer) Write(_ context.Context, b gtable.Block) error {
	cols := b.Rows.Columns()
	if w.f == nil {
		if err := w.open(cols); err != nil {
			return err
		}
	}
	vals := make([]gtable.Column, len(cols))
	for j, name := range cols {
		vals[j], _ = b.Rows.Column(name)
	}
	cells := make([]any, len(cols))
	for i := range b.Rows.Len() {
		for j, c := range vals {
			cells[j] = value(c, i)
		}
		w.row++
		ref, err := excelize.CoordinatesToCellName(1, w.row)
		if err != nil {
			return err
		}
		if err := w.sw.SetRow(ref, cells); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) open(cols []string) error {
	f := excelize.NewFile()
	if w.sheet != "Sheet1" {
		if err := f.SetSheetName("Sheet1", w.sheet); err != nil {
			f.Close()
			return err
		}
	}
	sw, err := f.NewStreamWriter(w.sheet)
	if err != nil {
		f.Close()
		return err
	}
	header := make([]any, len(cols))
	for i, c := range cols {
		header[i] = c
	}
	if err := sw.SetRow("A1", header); err != nil {
		f.Close()
		return err
	}
	w.f, w.sw, w.row = f, sw, 1
	return nil
}

// value returns cell i of c as the value of an Excel cell: nil for null,
// which leaves the cell empty.
func value(c gtable.Column, i int) any {
	switch c.Type() {
	case gtable.TypeInt:
		if v, ok := c.Int(i); ok {
			return v
		}
	case gtable.TypeFloat:
		if v, ok := c.Float(i); ok {
			return v
		}
	case gtable.TypeBool:
		if v, ok := c.Bool(i); ok {
			return v
		}
	case gtable.TypeTimestamp:
		if v, ok := c.Timestamp(i); ok {
			return v
		}
	default:
		if v, ok := c.Text(i); ok {
			return v // an inline string, never a formula (D99)
		}
	}
	return nil
}

func (w *writer) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.sw.Flush()
	if err == nil {
		err = w.f.SaveAs(w.path)
	}
	err = errors.Join(err, w.f.Close())
	w.f, w.sw, w.row = nil, nil, 0
	return err
}
