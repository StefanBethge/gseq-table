package excel

import (
	"archive/zip"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/delivery"
)

// Defaults of the limits for Excel deliveries (D56, D83).
const (
	DefaultMaxFieldSize    = 16 << 20 // one cell
	DefaultMaxUnpackedSize = 1 << 30  // the unpacked archive
	DefaultMaxRatio        = 100      // unpacked to packed size
)

// Option configures the Excel reader.
type Option func(*config)

type config struct {
	maxField    int
	maxUnpacked int64
	maxRatio    float64
}

// MaxFieldSize sets the limit for the size of one cell in bytes; the
// default is DefaultMaxFieldSize. A larger cell rejects its row with code
// field_too_large (D56).
func MaxFieldSize(n int) Option { return func(c *config) { c.maxField = n } }

// MaxUnpackedSize sets the limit for the unpacked size of the archive in
// bytes; the default is DefaultMaxUnpackedSize (D83). A larger archive is
// unreadable (D56).
func MaxUnpackedSize(n int64) Option { return func(c *config) { c.maxUnpacked = n } }

// MaxRatio sets the limit for the ratio of unpacked to packed size; the
// default is DefaultMaxRatio. An archive over it is unreadable (D56).
func MaxRatio(r float64) Option { return func(c *config) { c.maxRatio = r } }

// Sheet returns a source over one sheet of the Excel file at path (F8),
// named file#sheet (D53); an empty sheet name reads the first sheet. The
// first row with a value is the header. Raw state and working columns
// carry the stored value of every cell in a fixed text form (D62, D80), and
// rejected rows carry the cell address and the displayed text (D52). A
// missing sheet is the delivery error missing_sheet (D42).
func Sheet(path, sheet string, opts ...Option) gtable.Source {
	c := config{maxField: DefaultMaxFieldSize, maxUnpacked: DefaultMaxUnpackedSize, maxRatio: DefaultMaxRatio}
	for _, opt := range opts {
		opt(&c)
	}
	return gtable.NewSource(&reader{path: path, sheet: sheet, cfg: c})
}

// reader implements gtable.Reader over one sheet. It streams the sheet XML
// itself for the stored values and their types, and takes the displayed
// text row by row from excelize.
type reader struct {
	path, sheet string
	cfg         config

	zr       *zip.ReadCloser
	xf       *excelize.File
	part     io.ReadCloser
	dec      *xml.Decoder
	disp     *excelize.Rows
	dispRow  int
	sst      []string
	isDate   map[int]bool
	date1904 bool
	width    int
	lastRow  int
}

func (r *reader) Open() (gtable.Header, error) {
	name := filepath.Base(r.path)
	h := gtable.Header{Source: name + "#" + r.sheet, Sheet: r.sheet, Cells: true}
	st, err := os.Stat(r.path)
	if err != nil {
		return h, err
	}
	if r.zr, err = zip.OpenReader(r.path); err != nil {
		return h, err
	}
	var unpacked uint64
	for _, f := range r.zr.File {
		unpacked += f.UncompressedSize64
	}
	switch {
	case unpacked > uint64(r.cfg.maxUnpacked):
		return h, fmt.Errorf("the archive unpacks to %d bytes, over the limit of %d", unpacked, r.cfg.maxUnpacked)
	case st.Size() > 0 && float64(unpacked)/float64(st.Size()) > r.cfg.maxRatio:
		return h, fmt.Errorf("the archive unpacks to %.0f times its size, over the limit of %g", float64(unpacked)/float64(st.Size()), r.cfg.maxRatio)
	}

	sheets, date1904, err := r.workbook()
	if err != nil {
		return h, err
	}
	r.date1904 = date1904
	if r.sheet == "" && len(sheets) > 0 {
		r.sheet = sheets[0].name
		h.Source, h.Sheet = name+"#"+r.sheet, r.sheet
	}
	part := ""
	for _, s := range sheets {
		if s.name == r.sheet {
			part = s.part
		}
	}
	if part == "" {
		return h, &gtable.DeliveryError{Source: h.Source, Code: gtable.CodeMissingSheet, Err: fmt.Errorf("no sheet %q", r.sheet)}
	}
	if r.sst, err = r.sharedStrings(); err != nil {
		return h, err
	}
	if h.ID, err = r.fingerprint(h.Source); err != nil {
		return h, err
	}
	if r.xf, err = excelize.OpenFile(r.path, excelize.Options{
		UnzipSizeLimit:    r.cfg.maxUnpacked,
		UnzipXMLSizeLimit: min(r.cfg.maxUnpacked, excelize.StreamChunkSize),
	}); err != nil {
		return h, err
	}
	if r.disp, err = r.xf.Rows(r.sheet); err != nil {
		return h, err
	}
	r.isDate, r.dispRow, r.lastRow = map[int]bool{}, 0, 0
	if r.part, err = r.open(part); err != nil {
		return h, err
	}
	r.dec = xml.NewDecoder(r.part)

	// The first row with a value is the header.
	for {
		num, cells, err := r.row()
		if err == io.EOF {
			return h, errors.New("sheet has no header row")
		}
		if err != nil {
			return h, err
		}
		if len(cells) == 0 {
			continue
		}
		last := cells[len(cells)-1].col
		h.Columns = make([]string, last+1)
		for _, c := range cells {
			h.Columns[c.col] = c.val
		}
		r.width = len(h.Columns)
		r.display(num)
		return h, nil
	}
}

func (r *reader) Next() (gtable.Record, error) {
	for {
		num, cells, err := r.row()
		if err != nil {
			return gtable.Record{}, err
		}
		if len(cells) == 0 {
			continue // an empty row is skipped but counted (D80)
		}
		rec := gtable.Record{Line: num, Offset: -1, Column: -1, Fields: make([]string, r.width)}
		for _, c := range cells {
			if c.col < r.width {
				rec.Fields[c.col] = c.val
				continue
			}
			if rec.Code == "" {
				rec.Code = gtable.CodeUnparseableLine
				rec.Reason = fmt.Sprintf("row has a value in %s%d, right of the header", columnLetters(c.col), num)
			}
		}
		for j, f := range rec.Fields {
			if len(f) > r.cfg.maxField {
				rec.Fields, rec.Column, rec.Code = nil, j, gtable.CodeFieldTooLarge
				rec.Reason = fmt.Sprintf("cell is larger than %d bytes", r.cfg.maxField)
				return rec, nil
			}
		}
		disp := r.display(num)
		for j, f := range rec.Fields {
			if j < len(disp) && disp[j] != f {
				if rec.Display == nil {
					rec.Display = map[int]string{}
				}
				rec.Display[j] = disp[j]
			}
		}
		return rec, nil
	}
}

func (r *reader) Close() error {
	var errs []error
	if r.disp != nil {
		errs = append(errs, r.disp.Close())
	}
	if r.xf != nil {
		errs = append(errs, r.xf.Close())
	}
	if r.part != nil {
		errs = append(errs, r.part.Close())
	}
	if r.zr != nil {
		errs = append(errs, r.zr.Close())
	}
	r.disp, r.xf, r.part, r.zr, r.dec = nil, nil, nil, nil, nil
	return errors.Join(errs...)
}

// display returns the displayed text of the cells of row num, as excelize
// formats them.
func (r *reader) display(num int) []string {
	for r.dispRow < num {
		if !r.disp.Next() {
			return nil
		}
		r.dispRow++
	}
	cols, err := r.disp.Columns()
	if err != nil {
		return nil
	}
	return cols
}

func (r *reader) fingerprint(source string) (string, error) {
	f, err := os.Open(r.path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return delivery.Fingerprint(source, f)
}

func (r *reader) open(name string) (io.ReadCloser, error) {
	f, err := r.zr.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type sheetRef struct{ name, part string }

// workbook returns the sheets with the paths of their parts, and whether
// the workbook uses the 1904 date system.
func (r *reader) workbook() ([]sheetRef, bool, error) {
	var wb struct {
		Pr struct {
			Date1904 string `xml:"date1904,attr"`
		} `xml:"workbookPr"`
		Sheets []struct {
			Name string `xml:"name,attr"`
			RID  string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		} `xml:"sheets>sheet"`
	}
	var rels struct {
		Rels []struct {
			ID     string `xml:"Id,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if err := r.decode("xl/workbook.xml", &wb); err != nil {
		return nil, false, err
	}
	if err := r.decode("xl/_rels/workbook.xml.rels", &rels); err != nil {
		return nil, false, err
	}
	targets := map[string]string{}
	for _, rel := range rels.Rels {
		t := rel.Target
		if strings.HasPrefix(t, "/") {
			t = strings.TrimPrefix(t, "/")
		} else {
			t = path.Join("xl", t)
		}
		targets[rel.ID] = t
	}
	out := make([]sheetRef, len(wb.Sheets))
	for i, s := range wb.Sheets {
		out[i] = sheetRef{s.Name, targets[s.RID]}
	}
	return out, wb.Pr.Date1904 == "1" || wb.Pr.Date1904 == "true", nil
}

func (r *reader) decode(name string, v any) error {
	f, err := r.open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return xml.NewDecoder(f).Decode(v)
}

// richText is the text of a shared or inline string: plain or in runs.
type richText struct {
	T string `xml:"t"`
	R []struct {
		T string `xml:"t"`
	} `xml:"r"`
}

func (t richText) text() string {
	s := t.T
	for _, r := range t.R {
		s += r.T
	}
	return s
}

func (r *reader) sharedStrings() ([]string, error) {
	f, err := r.open("xl/sharedStrings.xml")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	dec := xml.NewDecoder(f)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if el, ok := tok.(xml.StartElement); ok && el.Name.Local == "si" {
			var t richText
			if err := dec.DecodeElement(&t, &el); err != nil {
				return nil, err
			}
			out = append(out, t.text())
		}
	}
}

// cell is a cell with a value in text form.
type cell struct {
	col int
	val string
}

// row reads the next row of the sheet XML and returns its number and its
// cells with a non-empty value, in column order. A row the file leaves out
// is counted by its number.
func (r *reader) row() (int, []cell, error) {
	var (
		num     int
		cells   []cell
		inRow   bool
		col     int
		typ     string
		style   int
		v, text string
		lastCol = -1
	)
	for {
		tok, err := r.dec.Token()
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		if err != nil {
			return 0, nil, err
		}
		switch el := tok.(type) {
		case xml.StartElement:
			switch el.Name.Local {
			case "row":
				inRow, num, cells, lastCol = true, r.lastRow+1, nil, -1
				if n, err := strconv.Atoi(attr(el, "r")); err == nil {
					num = n
				}
			case "c":
				col, typ, v, text = lastCol+1, attr(el, "t"), "", ""
				if ref := attr(el, "r"); ref != "" {
					if c, ok := refColumn(ref); ok {
						col = c
					}
				}
				style, _ = strconv.Atoi(attr(el, "s"))
			case "v":
				if err := r.dec.DecodeElement(&v, &el); err != nil {
					return 0, nil, err
				}
			case "is":
				var t richText
				if err := r.dec.DecodeElement(&t, &el); err != nil {
					return 0, nil, err
				}
				text = t.text()
			case "f", "extLst":
				if err := r.dec.Skip(); err != nil {
					return 0, nil, err
				}
			}
		case xml.EndElement:
			switch el.Name.Local {
			case "c":
				lastCol = col
				val, err := r.value(typ, style, v, text)
				if err != nil {
					return 0, nil, fmt.Errorf("cell %s%d: %w", columnLetters(col), num, err)
				}
				if val != "" {
					cells = append(cells, cell{col, val})
				}
			case "row":
				if inRow {
					r.lastRow = num
					return num, cells, nil
				}
			case "sheetData":
				return 0, nil, io.EOF
			}
		}
	}
}

// value returns the stored value of a cell in its fixed text form (D62,
// D80).
func (r *reader) value(typ string, style int, v, inline string) (string, error) {
	switch typ {
	case "s":
		i, err := strconv.Atoi(v)
		if err != nil || i < 0 || i >= len(r.sst) {
			return "", fmt.Errorf("no shared string %q", v)
		}
		return r.sst[i], nil
	case "inlineStr":
		return inline, nil
	case "str", "e":
		return v, nil
	case "b":
		return strconv.FormatBool(v == "1" || v == "true"), nil
	case "d":
		t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSuffix(v, "Z"))
		if err != nil {
			return v, nil
		}
		return dateForm(t, false), nil
	}
	if v == "" {
		return "", nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return v, nil
	}
	if r.dateStyle(style) && f >= 0 {
		t, err := excelize.ExcelDateToTime(f, r.date1904)
		if err == nil {
			return dateForm(t.Round(time.Second), f < 1), nil
		}
	}
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

// dateForm formats a date (D80): without a time as 2026-09-27, with one as
// 2026-09-27T14:30:00, a time of day alone as 14:30:00.
func dateForm(t time.Time, timeOnly bool) string {
	switch {
	case timeOnly:
		return t.Format("15:04:05")
	case t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0:
		return t.Format("2006-01-02")
	}
	return t.Format("2006-01-02T15:04:05")
}

// dateStyle reports whether cells of the given style have a date or time
// number format.
func (r *reader) dateStyle(style int) bool {
	if style == 0 {
		return false
	}
	d, ok := r.isDate[style]
	if !ok {
		st, err := r.xf.GetStyle(style)
		switch {
		case err != nil:
		case st.CustomNumFmt != nil:
			d = dateFormat(*st.CustomNumFmt)
		default:
			n := st.NumFmt
			d = 14 <= n && n <= 22 || 27 <= n && n <= 36 || 45 <= n && n <= 47 || 50 <= n && n <= 58
		}
		r.isDate[style] = d
	}
	return d
}

// dateFormat reports whether a custom number format shows a date or time:
// it has a d, m, y, h or s outside quoted text, escapes and brackets, or an
// elapsed time such as [h].
func dateFormat(code string) bool {
	quoted := false
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case c == '"':
			quoted = !quoted
		case quoted:
		case c == '\\' || c == '_' || c == '*':
			i++
		case c == '[':
			end := strings.IndexByte(code[i:], ']')
			if end < 0 {
				return false
			}
			inner := strings.ToLower(code[i+1 : i+end])
			if inner != "" && strings.Trim(inner, "hms") == "" {
				return true
			}
			i += end
		case strings.IndexByte("dmyhsDMYHS", c) >= 0:
			return true
		}
	}
	return false
}

func attr(el xml.StartElement, name string) string {
	for _, a := range el.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// refColumn returns the column index of a cell reference such as C17.
func refColumn(ref string) (int, bool) {
	n := 0
	i := 0
	for ; i < len(ref) && 'A' <= ref[i] && ref[i] <= 'Z'; i++ {
		n = n*26 + int(ref[i]-'A'+1)
	}
	return n - 1, i > 0
}

// columnLetters returns the letters of the i-th column from A (0 is A).
func columnLetters(i int) string {
	s := ""
	for i++; i > 0; i = (i - 1) / 26 {
		s = string(rune('A'+(i-1)%26)) + s
	}
	return s
}
