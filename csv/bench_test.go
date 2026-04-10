package csv

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// csvBenchSizes lists the row-count variants used across all sub-benchmarks.
var csvBenchSizes = []struct {
	name string
	n    int
}{
	{"1k", 1_000},
	{"10k", 10_000},
	{"100k", 100_000},
}

// buildCSV generates a CSV string with n data rows and the given number of cols.
func buildCSV(n, cols int, sep rune) string {
	var sb strings.Builder
	// header
	for c := 0; c < cols; c++ {
		if c > 0 {
			sb.WriteRune(sep)
		}
		sb.WriteString("col" + strconv.Itoa(c))
	}
	sb.WriteByte('\n')
	// data rows
	for i := 0; i < n; i++ {
		for c := 0; c < cols; c++ {
			if c > 0 {
				sb.WriteRune(sep)
			}
			sb.WriteString(fmt.Sprintf("val_%d_%d", i, c))
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

// ── Read (comma, header) ─────────────────────────────────────────────────────

func BenchmarkRead(b *testing.B) {
	for _, sz := range csvBenchSizes {
		data := buildCSV(sz.n, 5, ',')
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := New().Read(strings.NewReader(data))
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── Read – semicolon separator ───────────────────────────────────────────────

func BenchmarkRead_Semicolon(b *testing.B) {
	for _, sz := range csvBenchSizes {
		data := buildCSV(sz.n, 5, ';')
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := New(WithSeparator(';')).Read(strings.NewReader(data))
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── Read – no header ─────────────────────────────────────────────────────────

func BenchmarkRead_NoHeader(b *testing.B) {
	for _, sz := range csvBenchSizes {
		data := buildCSV(sz.n, 5, ',')
		// strip the header line
		idx := strings.Index(data, "\n")
		data = data[idx+1:]
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := New(WithNoHeader()).Read(strings.NewReader(data))
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── Read – wide table (many columns) ─────────────────────────────────────────

func BenchmarkRead_Wide(b *testing.B) {
	colCounts := []struct {
		name string
		cols int
	}{
		{"5cols", 5},
		{"20cols", 20},
		{"50cols", 50},
	}
	const rows = 10_000
	for _, cc := range colCounts {
		data := buildCSV(rows, cc.cols, ',')
		b.Run(cc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := New().Read(strings.NewReader(data))
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── ReadStream ───────────────────────────────────────────────────────────────

func BenchmarkReadStream(b *testing.B) {
	chunkSizes := []struct {
		name  string
		chunk int
	}{
		{"chunk100", 100},
		{"chunk1k", 1_000},
		{"chunk5k", 5_000},
	}
	const rows = 10_000
	data := buildCSV(rows, 5, ',')
	for _, cs := range chunkSizes {
		b.Run(cs.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, err := range New().ReadStream(strings.NewReader(data), cs.chunk) {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// ── Write – size variants ────────────────────────────────────────────────────
// BenchmarkWriter_Write in writer_test.go covers 10k rows; add more sizes here.

func BenchmarkWrite(b *testing.B) {
	headers := []string{"name", "city", "age", "id", "tag"}
	for _, sz := range csvBenchSizes {
		records := make([][]string, sz.n)
		for i := range records {
			records[i] = []string{
				"user_" + strconv.Itoa(i),
				"city_" + strconv.Itoa(i%100),
				strconv.Itoa(20 + i%50),
				strconv.Itoa(i % 1000),
				"tag_" + strconv.Itoa(i%10),
			}
		}
		tb := table.New(headers, records)
		w := NewWriter()
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := w.Write(&buf, tb); err != nil {
					b.Fatal(err)
				}
				_ = buf
			}
		})
	}
}

// ── Write – separator variants ───────────────────────────────────────────────

func BenchmarkWrite_Separators(b *testing.B) {
	seps := []struct {
		name string
		sep  rune
	}{
		{"comma", ','},
		{"semicolon", ';'},
		{"tab", '\t'},
	}
	headers := []string{"name", "city", "id"}
	const rows = 10_000
	records := make([][]string, rows)
	for i := range records {
		records[i] = []string{"user_" + strconv.Itoa(i), "city_" + strconv.Itoa(i%100), strconv.Itoa(i)}
	}
	tb := table.New(headers, records)
	for _, s := range seps {
		w := NewWriter(WithWriteSeparator(s.sep))
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if err := w.Write(&buf, tb); err != nil {
					b.Fatal(err)
				}
				_ = buf
			}
		})
	}
}

// ── ToString ─────────────────────────────────────────────────────────────────

func BenchmarkToString(b *testing.B) {
	headers := []string{"name", "city", "id"}
	for _, sz := range csvBenchSizes {
		records := make([][]string, sz.n)
		for i := range records {
			records[i] = []string{"user_" + strconv.Itoa(i), "city_" + strconv.Itoa(i%100), strconv.Itoa(i)}
		}
		tb := table.New(headers, records)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ToString(tb)
			}
		})
	}
}
