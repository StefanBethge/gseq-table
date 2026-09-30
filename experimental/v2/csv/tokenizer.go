package csv

import (
	"bytes"
	stdcsv "encoding/csv"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/delivery"
)

// errInvalidDelim is the error of encoding/csv for an invalid separator.
var errInvalidDelim = errors.New("csv: invalid field or comment delimiter")

// tokenizer splits CSV input into records like encoding/csv with
// FieldsPerRecord -1, but into a reused delivery.Fields instead of strings,
// and it knows the raw bytes of every record without a copy of its own
// (D10, D113, G75). It reads the input in large pieces into a window and
// never changes the bytes in it: encoding/csv turns \r\n into \n in its
// buffer, the tokenizer only in the fields.
type tokenizer struct {
	r     io.Reader
	comma rune
	lazy  bool

	buf     []byte // window of the input; buf[0] is at offset base
	base    int64
	pos     int // start of the next line in buf
	eof     bool
	readErr error
	numLine int
}

func newTokenizer(r io.Reader, comma rune, lazy bool) *tokenizer {
	return &tokenizer{r: r, comma: comma, lazy: lazy}
}

// record is what next returns besides the fields: the line the record
// starts on (D81), the offset of its start, its raw bytes without the line
// break, valid until the next call, and the parse error of a record that
// could not be split.
type record struct {
	line   int
	offset int64
	raw    []byte
	perr   *stdcsv.ParseError
}

// readLine returns the next line without its line break and whether it had
// one, like readLine of encoding/csv: a \r before the line break and before
// the end of the input is not part of the line. It returns io.EOF only when
// no byte is left.
func (t *tokenizer) readLine() (line []byte, nl bool, err error) {
	for {
		if i := bytes.IndexByte(t.buf[t.pos:], '\n'); i >= 0 {
			line = t.buf[t.pos : t.pos+i]
			t.pos += i + 1
			t.numLine++
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			return line, true, nil
		}
		if t.eof {
			break
		}
		t.fill()
	}
	t.numLine++
	line = t.buf[t.pos:]
	t.pos = len(t.buf)
	if len(line) == 0 {
		if t.readErr != nil {
			return nil, false, t.readErr
		}
		return nil, false, io.EOF
	}
	if n := len(line); line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, false, nil
}

// readSize is the size of the reads from the input.
const readSize = 1 << 20

// fill reads more input into the window, growing it if the current record
// fills it.
func (t *tokenizer) fill() {
	if cap(t.buf)-len(t.buf) < readSize/2 {
		nb := make([]byte, len(t.buf), max(2*cap(t.buf), len(t.buf)+readSize))
		copy(nb, t.buf)
		t.buf = nb
	}
	n, err := t.r.Read(t.buf[len(t.buf):cap(t.buf)])
	t.buf = t.buf[:len(t.buf)+n]
	switch {
	case err == io.EOF:
		t.eof = true
	case err != nil:
		t.eof, t.readErr = true, err
	}
}

// startRecord drops the bytes before the next line once they are many, so
// that the window does not grow with the input.
func (t *tokenizer) startRecord() {
	if t.pos >= readSize/2 || (t.pos > 0 && t.pos == len(t.buf)) {
		n := copy(t.buf, t.buf[t.pos:])
		t.buf, t.base, t.pos = t.buf[:n], t.base+int64(t.pos), 0
	}
}

// next reads the next record into f; it returns io.EOF at the end. A record
// that cannot be split has perr set, and f holds the fields up to the
// error.
func (t *tokenizer) next(f *delivery.Fields) (record, error) {
	if !validDelim(t.comma) {
		return record{}, errInvalidDelim
	}
	t.startRecord()
	f.Reset()

	// Skip empty lines; the record starts at the first other line.
	var line []byte
	var nl bool
	var errRead error
	start := t.pos
	for {
		start = t.pos
		line, nl, errRead = t.readLine()
		if errRead == nil && len(line) == 0 {
			continue
		}
		break
	}
	if errRead == io.EOF {
		return record{}, io.EOF
	}
	rec := record{line: t.numLine, offset: t.base + int64(start)}

	const quoteLen = len(`"`)
	commaLen := utf8.RuneLen(t.comma)
	col, lineNo := 1, t.numLine
	fail := func(line, col int, err error) {
		rec.perr = &stdcsv.ParseError{StartLine: rec.line, Line: line, Column: col, Err: err}
	}
parseField:
	for {
		if len(line) == 0 || line[0] != '"' {
			// A field without quotes.
			i := indexRune(line, t.comma)
			field := line
			if i >= 0 {
				field = field[:i]
			}
			if !t.lazy {
				if j := bytes.IndexByte(field, '"'); j >= 0 {
					fail(t.numLine, col+j, stdcsv.ErrBareQuote)
					break parseField
				}
			}
			f.Buf = append(f.Buf, field...)
			f.End()
			if i >= 0 {
				line = line[i+commaLen:]
				col += i + commaLen
				continue parseField
			}
			break parseField
		}
		// A quoted field.
		line = line[quoteLen:]
		col += quoteLen
		for {
			i := bytes.IndexByte(line, '"')
			switch {
			case i >= 0:
				f.Buf = append(f.Buf, line[:i]...)
				line = line[i+quoteLen:]
				col += i + quoteLen
				switch rn := nextRune(line); {
				case rn == '"':
					f.Buf = append(f.Buf, '"')
					line = line[quoteLen:]
					col += quoteLen
				case rn == t.comma:
					line = line[commaLen:]
					col += commaLen
					f.End()
					continue parseField
				case len(line) == 0:
					f.End()
					break parseField
				case t.lazy:
					f.Buf = append(f.Buf, '"')
				default:
					fail(t.numLine, col-quoteLen, stdcsv.ErrQuote)
					break parseField
				}
			case len(line) > 0 || nl:
				// The field goes on in the next line.
				f.Buf = append(f.Buf, line...)
				if nl {
					f.Buf = append(f.Buf, '\n')
				}
				if errRead != nil {
					break parseField
				}
				col += len(line)
				if nl {
					col++
				}
				line, nl, errRead = t.readLine()
				if len(line) > 0 || nl {
					lineNo++
					col = 1
				}
				if errRead == io.EOF {
					errRead = nil
				}
			default:
				// The input ends inside the quotes.
				if !t.lazy && errRead == nil {
					fail(lineNo, col, stdcsv.ErrQuote)
					break parseField
				}
				f.End()
				break parseField
			}
		}
	}
	if rec.perr == nil && errRead != nil {
		return record{}, errRead
	}
	raw := t.buf[start:t.pos]
	raw = bytes.TrimSuffix(bytes.TrimSuffix(raw, []byte("\n")), []byte("\r"))
	rec.raw = raw
	return rec, nil
}

// indexRune is bytes.IndexRune with a fast path for a separator of one byte.
func indexRune(b []byte, r rune) int {
	if r < utf8.RuneSelf {
		return bytes.IndexByte(b, byte(r))
	}
	return bytes.IndexRune(b, r)
}

func nextRune(b []byte) rune {
	r, _ := utf8.DecodeRune(b)
	return r
}

func validDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}
