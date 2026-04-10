package schema

import (
	"strconv"
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/table"
)

// schemaBenchSizes lists the row-count variants used across all sub-benchmarks.
var schemaBenchSizes = []struct {
	name string
	n    int
}{
	{"1k", 1_000},
	{"10k", 10_000},
	{"100k", 100_000},
}

// schemaNumTable returns a numeric table with n rows and columns
// name/age/price/active/created_at – suitable for Infer, Apply, and stats.
func schemaNumTable(n int) table.Table {
	records := make([][]string, n)
	for i := range records {
		records[i] = []string{
			"user_" + strconv.Itoa(i),
			strconv.Itoa(18 + i%70),
			strconv.FormatFloat(float64(i%10_000)/100, 'f', 2, 64),
			[]string{"true", "false"}[i%2],
			"2024-01-15",
		}
	}
	return table.New(
		[]string{"name", "age", "price", "active", "created_at"},
		records,
	)
}

// ── Infer – size variants ─────────────────────────────────────────────────────
// BenchmarkInfer in schema_test.go covers 20k rows; add finer size variants here.

func BenchmarkInfer_Sizes(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Infer(tb)
			}
		})
	}
}

// ── Apply ─────────────────────────────────────────────────────────────────────

func BenchmarkApply(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		s := Infer(tb)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := s.Apply(tb)
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── ApplyStrict ───────────────────────────────────────────────────────────────

func BenchmarkApplyStrict(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		s := Infer(tb)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				res := s.ApplyStrict(tb)
				if res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
	}
}

// ── SumCol ────────────────────────────────────────────────────────────────────

func BenchmarkSumCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = SumCol(tb, "price")
			}
		})
	}
}

// ── MeanCol ───────────────────────────────────────────────────────────────────

func BenchmarkMeanCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MeanCol(tb, "price")
			}
		})
	}
}

// ── MinCol / MaxCol ───────────────────────────────────────────────────────────

func BenchmarkMinCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MinCol(tb, "price")
			}
		})
	}
}

func BenchmarkMaxCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MaxCol(tb, "price")
			}
		})
	}
}

// ── StdDevCol ─────────────────────────────────────────────────────────────────

func BenchmarkStdDevCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = StdDevCol(tb, "price")
			}
		})
	}
}

// ── MedianCol ────────────────────────────────────────────────────────────────

func BenchmarkMedianCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MedianCol(tb, "price")
			}
		})
	}
}

// ── CountCol / CountWhere ─────────────────────────────────────────────────────

func BenchmarkCountCol(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CountCol(tb, "price")
			}
		})
	}
}

func BenchmarkCountWhere(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CountWhere(tb, "active", "true")
			}
		})
	}
}

// ── Describe ─────────────────────────────────────────────────────────────────

func BenchmarkDescribe(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Describe(tb)
			}
		})
	}
}

// ── FreqMap ───────────────────────────────────────────────────────────────────

func BenchmarkFreqMap(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = FreqMap(tb, "active")
			}
		})
	}
}

// ── MinMaxNorm ────────────────────────────────────────────────────────────────

func BenchmarkMinMaxNorm(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = MinMaxNorm(tb, "price")
			}
		})
	}
}

// ── Arithmetic helpers: Add / Sub / Mul / Div ────────────────────────────────

func BenchmarkAdd(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		fn := Add("age", "price")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColFloat("sum", fn)
			}
		})
	}
}

func BenchmarkDiv(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		fn := Div("price", "age")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColFloat("ratio", fn)
			}
		})
	}
}

// ── Date helpers ──────────────────────────────────────────────────────────────

func BenchmarkDateYear(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		fn := DateYear("created_at")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddCol("year", fn)
			}
		})
	}
}

func BenchmarkDateDiffDays(b *testing.B) {
	records := make([][]string, 0)
	base, _ := time.Parse("2006-01-02", "2024-01-01")
	_ = base
	for _, sz := range schemaBenchSizes {
		recs := make([][]string, sz.n)
		for i := range recs {
			recs[i] = []string{"2024-01-15", "2024-06-30"}
		}
		tb := table.New([]string{"from", "to"}, recs)
		_ = records
		fn := DateDiffDays("from", "to")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColFloat("days", fn)
			}
		})
	}
}

// ── Pct ───────────────────────────────────────────────────────────────────────

func BenchmarkPct(b *testing.B) {
	for _, sz := range schemaBenchSizes {
		tb := schemaNumTable(sz.n)
		fn := Pct("price", "age")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColFloat("pct", fn)
			}
		})
	}
}
