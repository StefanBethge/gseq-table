// Package excel reads Excel (.xlsx) files into a table.Table.
//
// This package is a separate Go module so that its dependency on excelize
// is not pulled in by consumers who only need csv or table.
//
// The reader is configured via functional options, mirroring the csv
// package API:
//
//	r := excel.New()                                    // first sheet, header row
//	r := excel.New(excel.WithSheet("Sales"))            // specific sheet
//	r := excel.New(excel.WithNoHeader())                // auto-generated col_0, col_1, …
//	r := excel.New(excel.WithHeaderNames("id", "name")) // explicit header names
//
// Reading returns a result.Result so errors are handled in the caller's
// preferred style:
//
//	res := excel.New().ReadFile("report.xlsx")
//	if res.IsErr() {
//	    log.Fatal(res.UnwrapErr())
//	}
//	t := res.Unwrap()
package excel

import (
	"fmt"
	"io"
	"iter"
	"path/filepath"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
	"github.com/xuri/excelize/v2"
)

// Config holds the resolved configuration for a Reader.
type Config struct {
	// HasHeader controls whether the first row is treated as a header.
	HasHeader bool

	// HeaderNames provides explicit column names when HasHeader is false.
	// If set, these names are used instead of auto-generated col_N names.
	HeaderNames slice.Slice[string]

	// Sheet is the name of the sheet to read. If empty, SheetIndex is used.
	Sheet string

	// SheetIndex is the zero-based index of the sheet to read.
	// Only used when Sheet is empty. Defaults to 0 (first sheet).
	SheetIndex int

	// Password is the workbook password for encrypted files.
	Password string
}

// Option is a functional option for configuring a Reader.
type Option func(*Config)

// WithHeader treats the first row as a header row (default).
func WithHeader() Option { return func(c *Config) { c.HasHeader = true } }

// WithNoHeader signals that the input has no header row. Column names will be
// auto-generated as col_0, col_1, … unless WithHeaderNames is also applied.
func WithNoHeader() Option { return func(c *Config) { c.HasHeader = false } }

// WithHeaderNames provides explicit column names. Implies WithNoHeader.
//
//	excel.New(excel.WithHeaderNames("id", "name", "amount"))
func WithHeaderNames(names ...string) Option {
	return func(c *Config) {
		c.HasHeader = false
		c.HeaderNames = slice.Slice[string](names)
	}
}

// WithSheet selects a sheet by name.
//
//	excel.New(excel.WithSheet("Sales"))
func WithSheet(name string) Option {
	return func(c *Config) { c.Sheet = name }
}

// WithSheetIndex selects a sheet by zero-based index (default 0).
//
//	excel.New(excel.WithSheetIndex(2)) // third sheet
func WithSheetIndex(idx int) Option {
	return func(c *Config) {
		c.Sheet = ""
		c.SheetIndex = idx
	}
}

// WithPassword sets the password for encrypted workbooks.
func WithPassword(pw string) Option {
	return func(c *Config) { c.Password = pw }
}

// Reader parses Excel data into a table.Table.
type Reader struct{ config Config }

// New constructs a Reader with the supplied options. Unset options keep their
// defaults: HasHeader=true, SheetIndex=0.
func New(opts ...Option) *Reader {
	cfg := Config{HasHeader: true, SheetIndex: 0}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Reader{config: cfg}
}

// ReadFile opens the Excel file at path and parses the configured sheet.
// Returns Err if the file cannot be opened or the sheet is missing.
// The basename of path is attached as the table's source name so error
// messages include "[filename.xlsx] ..." context.
func (r *Reader) ReadFile(path string) result.Result[table.Table, error] {
	f, err := excelize.OpenFile(path, r.excelizeOpts()...)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	defer f.Close()
	return result.Map(r.readFromFile(f), func(t table.Table) table.Table {
		return t.WithSource(filepath.Base(path))
	})
}

// Read parses Excel data from rd. The entire content is consumed because
// the .xlsx format (ZIP) requires random access.
func (r *Reader) Read(rd io.Reader) result.Result[table.Table, error] {
	f, err := excelize.OpenReader(rd, r.excelizeOpts()...)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	defer f.Close()
	return r.readFromFile(f)
}

// ReadFileStream opens the file at path and streams it in chunks of chunkSize
// rows. Uses excelize's streaming Rows iterator for memory efficiency.
//
//	for t, err := range excel.New().ReadFileStream("big.xlsx", 1000) {
//	    if err != nil { log.Fatal(err) }
//	    process(t)
//	}
func (r *Reader) ReadFileStream(path string, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		f, err := excelize.OpenFile(path, r.excelizeOpts()...)
		if err != nil {
			yield(table.Table{}, err)
			return
		}
		defer f.Close()
		r.streamFromFile(f, chunkSize, yield)
	}
}

// ReadStream reads Excel data from rd and streams in chunks.
func (r *Reader) ReadStream(rd io.Reader, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		f, err := excelize.OpenReader(rd, r.excelizeOpts()...)
		if err != nil {
			yield(table.Table{}, err)
			return
		}
		defer f.Close()
		r.streamFromFile(f, chunkSize, yield)
	}
}

// SheetNames opens the Excel file at path and returns the names of all sheets.
//
//	sheets, err := excel.SheetNames("report.xlsx")
func SheetNames(path string) ([]string, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.GetSheetList(), nil
}

// SheetNamesFromReader returns the names of all sheets from an io.Reader.
func SheetNamesFromReader(rd io.Reader) ([]string, error) {
	f, err := excelize.OpenReader(rd)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.GetSheetList(), nil
}

// --- internal helpers ---

// excelizeOpts returns excelize options derived from our Config.
func (r *Reader) excelizeOpts() []excelize.Options {
	if r.config.Password != "" {
		return []excelize.Options{{Password: r.config.Password}}
	}
	return nil
}

// resolveSheet returns the sheet name to read, given the config and the file.
func (r *Reader) resolveSheet(f *excelize.File) (string, error) {
	if r.config.Sheet != "" {
		idx, err := f.GetSheetIndex(r.config.Sheet)
		if err != nil {
			return "", err
		}
		if idx == -1 {
			return "", fmt.Errorf("excel: sheet %q not found", r.config.Sheet)
		}
		return r.config.Sheet, nil
	}
	name := f.GetSheetName(r.config.SheetIndex)
	if name == "" {
		return "", fmt.Errorf("excel: no sheet at index %d", r.config.SheetIndex)
	}
	return name, nil
}

// readFromFile contains the shared reading logic for ReadFile and Read.
func (r *Reader) readFromFile(f *excelize.File) result.Result[table.Table, error] {
	sheet, err := r.resolveSheet(f)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return result.Err[table.Table, error](err)
	}
	rows = normalizeRows(rows)
	headers, dataRows := r.resolveHeaders(rows)
	return result.Ok[table.Table, error](table.New(headers, dataRows))
}

// resolveHeaders separates the header row (or generates names) from data rows,
// mirroring the csv package logic.
func (r *Reader) resolveHeaders(records [][]string) (slice.Slice[string], [][]string) {
	if r.config.HasHeader {
		if len(records) == 0 {
			return slice.Slice[string]{}, nil
		}
		return slice.Slice[string](records[0]), records[1:]
	}
	if len(r.config.HeaderNames) > 0 {
		return r.config.HeaderNames, records
	}
	if len(records) == 0 {
		return slice.Slice[string]{}, nil
	}
	cols := len(records[0])
	names := make(slice.Slice[string], cols)
	for i := range cols {
		names[i] = fmt.Sprintf("col_%d", i)
	}
	return names, records
}

// normalizeRows pads short rows so every row has the same width.
// Excel often omits trailing empty cells.
func normalizeRows(rows [][]string) [][]string {
	maxCols := 0
	for _, row := range rows {
		if len(row) > maxCols {
			maxCols = len(row)
		}
	}
	for i, row := range rows {
		if len(row) < maxCols {
			padded := make([]string, maxCols)
			copy(padded, row)
			rows[i] = padded
		}
	}
	return rows
}

// streamFromFile is the shared streaming logic using excelize's Rows iterator.
func (r *Reader) streamFromFile(f *excelize.File, chunkSize int, yield func(table.Table, error) bool) {
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	sheet, err := r.resolveSheet(f)
	if err != nil {
		yield(table.Table{}, err)
		return
	}
	rows, err := f.Rows(sheet)
	if err != nil {
		yield(table.Table{}, err)
		return
	}
	defer rows.Close()

	var headers slice.Slice[string]
	headersResolved := false
	chunk := make([][]string, 0, chunkSize)

	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			yield(table.Table{}, err)
			return
		}

		if !headersResolved {
			if r.config.HasHeader {
				headers = slice.Slice[string](cols)
				headersResolved = true
				continue
			}
			if len(r.config.HeaderNames) > 0 {
				headers = r.config.HeaderNames
			} else {
				names := make(slice.Slice[string], len(cols))
				for i := range cols {
					names[i] = fmt.Sprintf("col_%d", i)
				}
				headers = names
			}
			headersResolved = true
			chunk = append(chunk, cols)
		} else {
			chunk = append(chunk, cols)
		}

		if len(chunk) >= chunkSize {
			padChunk(chunk, len(headers))
			if !yield(table.New(headers, chunk), nil) {
				return
			}
			chunk = make([][]string, 0, chunkSize)
		}
	}
	if err := rows.Error(); err != nil {
		yield(table.Table{}, err)
		return
	}
	if len(chunk) > 0 {
		padChunk(chunk, len(headers))
		yield(table.New(headers, chunk), nil)
	}
}

// padChunk ensures every row in the chunk has at least width cells.
func padChunk(chunk [][]string, width int) {
	for i, row := range chunk {
		if len(row) < width {
			padded := make([]string, width)
			copy(padded, row)
			chunk[i] = padded
		}
	}
}
