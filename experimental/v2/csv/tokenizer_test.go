package csv

import (
	"bytes"
	stdcsv "encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/delivery"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// token is what a reader yields for one record: its fields, or the parse
// error, with line, offset and raw bytes (D10, D81).
type token struct {
	Fields    []string
	Err       string // ParseError.Error(), or ""
	StartLine int
	Line      int
	Offset    int64
	Raw       string
}

// referenceTokens reads content the way the reader did before #69:
// encoding/csv with a recorder of the bytes it read, and the blank lines
// and the line break cut from the raw bytes.
func referenceTokens(content string, comma rune, lazy bool) ([]token, error) {
	rec := &recorder{r: strings.NewReader(content)}
	cr := stdcsv.NewReader(rec)
	cr.Comma, cr.LazyQuotes, cr.FieldsPerRecord = comma, lazy, -1
	var out []token
	for {
		start := cr.InputOffset()
		rec.drop(start)
		fields, err := cr.Read()
		if err == io.EOF {
			return out, nil
		}
		raw, off := trimRaw(rec.take(start, cr.InputOffset()), start)
		tk := token{Offset: off, Raw: string(raw)}
		var pe *stdcsv.ParseError
		switch {
		case errors.As(err, &pe):
			tk.Err, tk.StartLine = pe.Error(), pe.StartLine
		case err != nil:
			return out, err
		default:
			tk.Fields = fields
			tk.Line, _ = cr.FieldPos(0)
		}
		out = append(out, tk)
	}
}

// recorder keeps the bytes encoding/csv has read, as the reader did before
// #69.
type recorder struct {
	r    io.Reader
	buf  []byte
	base int64
}

func (rc *recorder) Read(p []byte) (int, error) {
	n, err := rc.r.Read(p)
	rc.buf = append(rc.buf, p[:n]...)
	return n, err
}

func (rc *recorder) take(from, to int64) []byte { return rc.buf[from-rc.base : to-rc.base] }

func (rc *recorder) drop(to int64) {
	rc.buf, rc.base = rc.buf[:copy(rc.buf, rc.buf[to-rc.base:])], to
}

// trimRaw is trimLine of the reader before #69.
func trimRaw(raw []byte, offset int64) ([]byte, int64) {
	for {
		switch {
		case len(raw) > 0 && raw[0] == '\n':
			raw, offset = raw[1:], offset+1
			continue
		case len(raw) > 1 && raw[0] == '\r' && raw[1] == '\n':
			raw, offset = raw[2:], offset+2
			continue
		}
		break
	}
	return bytes.TrimSuffix(bytes.TrimSuffix(raw, []byte("\n")), []byte("\r")), offset
}

// tokens reads content with the tokenizer, in reads of the given sizes.
func tokens(content string, comma rune, lazy bool, r io.Reader) ([]token, error) {
	if r == nil {
		r = strings.NewReader(content)
	}
	tz := newTokenizer(r, comma, lazy)
	var f delivery.Fields
	var out []token
	for {
		rec, err := tz.next(&f)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		tk := token{Offset: rec.offset, Raw: string(rec.raw)}
		if rec.perr != nil {
			tk.Err, tk.StartLine = rec.perr.Error(), rec.perr.StartLine
		} else {
			tk.Fields = f.Strings()
			tk.Line = rec.line
		}
		out = append(out, tk)
	}
}

// T75: the CSV reader splits a delivery into the same records as
// encoding/csv, with the same errors, lines, offsets and raw bytes (D10,
// D81), and reads a line without allocating (D113, G75).
func TestCSVTokenizerReadsLikeEncodingCSV(t *testing.T) {
	testutil.Proves(t, "T75")

	fixed := []string{
		"", "\n", "\r\n", "a", "a\n", "a\r", "a\r\n", "\r", "\n\n\na,b\n\r\n\r\nc,d",
		"a,b,c\n1,2,3\n", `a,"b ""c"" d",e` + "\n", "\"multi\r\nline\",x\ny,z", "\"a\"b,c\n1,2\n",
		"a\"b,c\nd,e\n", "\"open,\nnever closed", "\"\"\n", ",\n,,\n", "\ufeffid,name\n1,Ä\n",
		"\"a\"\n\"b\"\r\n", "x,\"y\"\r", "\"a\"\"\"\n", "a, \"b\"\n", "\"a\" ,b\n", "\"\n\"\n",
		"1;2;3\n\"4;5\";6\n", "a\tb\n",
	}
	alphabet := []string{"a", "b", "ü", ",", ";", "\t", "\"", "\"\"", "\n", "\r", "\r\n", " "}
	rng := rand.New(rand.NewPCG(1, 69))
	inputs := slices.Clone(fixed)
	for range 3000 {
		var sb strings.Builder
		for range rng.IntN(40) {
			sb.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		inputs = append(inputs, sb.String())
	}
	for _, in := range inputs {
		for _, comma := range []rune{',', ';', '\t', 'ü'} {
			for _, lazy := range []bool{false, true} {
				want, err := referenceTokens(in, comma, lazy)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range []io.Reader{nil, iotest.OneByteReader(strings.NewReader(in)), iotest.HalfReader(strings.NewReader(in))} {
					got, err := tokens(in, comma, lazy, r)
					if err != nil {
						t.Fatalf("%q: %v", in, err)
					}
					if !slices.EqualFunc(got, want, func(a, b token) bool { return fmt.Sprint(a) == fmt.Sprint(b) }) {
						t.Fatalf("%q, comma %q, lazy %v:\n got %+v\nwant %+v", in, comma, lazy, got, want)
					}
				}
			}
		}
	}

	// Reading a line allocates nothing once the buffers have grown.
	var sb strings.Builder
	for i := range 20000 {
		fmt.Fprintf(&sb, "%d,C%04d,\"Müller, Anna\",%d.25\n", i, i%1000, i)
	}
	content := sb.String()
	tz := newTokenizer(strings.NewReader(content), ',', false)
	var f delivery.Fields
	tz.next(&f)
	if a := testing.AllocsPerRun(10000, func() {
		if _, err := tz.next(&f); err != nil {
			t.Fatal(err)
		}
	}); a != 0 {
		t.Errorf("reading a line allocates %v times, want 0", a)
	}
}
