package csv

import (
	"bufio"
	"context"
	stdcsv "encoding/csv"
	"errors"
	"os"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
)

// Create returns a sink that writes CSV to the file at path (F19, D35),
// for results or rejected rows (D2). The file is created, or truncated,
// with the first block; a run without a block leaves no file (D100). The
// first line is the header, unless NoHeader is given, and Comma sets the
// separator. Values are written in the text form of Column.Format; a null
// is an empty field. Values that a spreadsheet would read as a formula are
// written as they are (P8).
//
// Every block is flushed to the file before Write returns, so the blocks
// written before a run ends early stay (D51).
func Create(path string, opts ...Option) gtable.Sink {
	return &writer{path: path, cfg: newConfig(opts)}
}

// writer implements gtable.Sink over a CSV file.
type writer struct {
	path string
	cfg  config

	f  *os.File
	bw *bufio.Writer
	cw *stdcsv.Writer
}

func (w *writer) Write(_ context.Context, b gtable.Block) error {
	cols := b.Rows.Columns()
	if w.f == nil {
		f, err := os.Create(w.path)
		if err != nil {
			return err
		}
		w.f, w.bw = f, bufio.NewWriter(f)
		w.cw = stdcsv.NewWriter(w.bw)
		w.cw.Comma = w.cfg.comma
		if !w.cfg.noHeader {
			if err := w.cw.Write(cols); err != nil {
				return err
			}
		}
	}
	vals := make([]gtable.Column, len(cols))
	for j, name := range cols {
		vals[j], _ = b.Rows.Column(name)
	}
	rec := make([]string, len(cols))
	for i := range b.Rows.Len() {
		for j, c := range vals {
			rec[j], _ = c.Format(i)
		}
		if err := w.cw.Write(rec); err != nil {
			return err
		}
	}
	w.cw.Flush()
	if err := w.cw.Error(); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *writer) Close() error {
	if w.f == nil {
		return nil
	}
	w.cw.Flush()
	err := errors.Join(w.cw.Error(), w.bw.Flush(), w.f.Close())
	w.f, w.bw, w.cw = nil, nil, nil
	return err
}
