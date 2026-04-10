package etl

import (
	"strconv"
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// etlBenchSizes lists the row-count variants used across all sub-benchmarks.
var etlBenchSizes = []struct {
	name string
	n    int
}{
	{"1k", 1_000},
	{"10k", 10_000},
	{"100k", 100_000},
}

// etlTable returns a Table with n rows and columns id/city/revenue/name.
func etlTable(n int) table.Table {
	headers := []string{"id", "city", "revenue", "name"}
	records := make([][]string, n)
	for i := 0; i < n; i++ {
		city := "Berlin"
		if i%3 == 1 {
			city = "Munich"
		}
		if i%3 == 2 {
			city = "Hamburg"
		}
		records[i] = []string{
			strconv.Itoa(i),
			city,
			strconv.Itoa(100 + i%1000),
			"name_" + strconv.Itoa(i),
		}
	}
	return table.New(headers, records)
}

// etlJoinTable returns a table keyed by id with an extra column.
func etlJoinTable(n int) table.Table {
	records := make([][]string, n)
	for i := 0; i < n; i++ {
		records[i] = []string{strconv.Itoa(i), "group_" + strconv.Itoa(i%100)}
	}
	return table.New([]string{"id", "group"}, records)
}

// ── Single-step operations ────────────────────────────────────────────────────

func BenchmarkPipeline_Where(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Where(func(r table.Row) bool {
					return r.Get("city").UnwrapOr("") == "Berlin"
				}).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Select(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Select("city", "revenue").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Map(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Map("city", func(v string) string { return "[" + v + "]" }).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Sort(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Sort("revenue", true).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_GroupByAgg(b *testing.B) {
	aggs := []table.AggDef{
		{Col: "total", Agg: table.Sum("revenue")},
		{Col: "count", Agg: table.Count("revenue")},
	}
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).GroupByAgg([]string{"city"}, aggs).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Join(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		other := etlJoinTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Join(other, "id", "id").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_LeftJoin(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		other := etlJoinTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).LeftJoin(other, "id", "id").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Distinct(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Distinct("city").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_ValueCounts(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).ValueCounts("city").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_FillEmpty(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).FillEmpty("city", "unknown").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_DropEmpty(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).DropEmpty("city").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_RollingAgg(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).RollingAgg("roll", 10, table.Sum("revenue")).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_CumSum(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).CumSum("revenue", "cum").Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Rank(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Rank("revenue", "rank", true).Unwrap()
			}
		})
	}
}

func BenchmarkPipeline_Lag(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).Lag("revenue", "prev", 7).Unwrap()
			}
		})
	}
}

// ── Multi-step pipeline ───────────────────────────────────────────────────────
// Simulates a realistic ETL chain: filter → enrich → aggregate.

func BenchmarkPipeline_ETLChain(b *testing.B) {
	aggs := []table.AggDef{
		{Col: "total", Agg: table.Sum("revenue")},
		{Col: "count", Agg: table.Count("revenue")},
	}
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).
					DropEmpty("revenue").
					FillEmpty("city", "unknown").
					Where(func(r table.Row) bool {
						return r.Get("city").UnwrapOr("") != "unknown"
					}).
					AddCol("label", func(r table.Row) string {
						return r.Get("city").UnwrapOr("") + "_" + r.Get("revenue").UnwrapOr("0")
					}).
					SortMulti(table.Desc("revenue"), table.Asc("city")).
					GroupByAgg([]string{"city"}, aggs).
					Unwrap()
			}
		})
	}
}

// ── Join-then-aggregate pipeline ─────────────────────────────────────────────

func BenchmarkPipeline_JoinAgg(b *testing.B) {
	aggs := []table.AggDef{
		{Col: "total", Agg: table.Sum("revenue")},
	}
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		other := etlJoinTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).
					Join(other, "id", "id").
					GroupByAgg([]string{"group"}, aggs).
					Unwrap()
			}
		})
	}
}

// ── Melt → Pivot round-trip ───────────────────────────────────────────────────

func BenchmarkPipeline_MeltPivot(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"1k", 1_000},
		{"5k", 5_000},
		{"10k", 10_000},
	}
	for _, sz := range sizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				melted := From(tb).Melt([]string{"id"}, "var", "val").Unwrap()
				_ = From(melted).Pivot("id", "var", "val").Unwrap()
			}
		})
	}
}

// ── Error-propagation overhead ────────────────────────────────────────────────
// Verifies that a pipeline that hits an early error short-circuits cheaply.

func BenchmarkPipeline_ErrPropagation(b *testing.B) {
	// A pipeline on a zero-row table where AssertNoEmpty returns an error.
	tb := etlTable(10)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := From(tb).
			AssertNoEmpty("nonexistent_col"). // always errors
			Sort("revenue", true).            // should be skipped
			GroupByAgg([]string{"city"}, []table.AggDef{{Col: "n", Agg: table.Count("revenue")}})
		if p.IsOk() {
			b.Fatal("expected error")
		}
	}
}
