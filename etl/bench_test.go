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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Where(func(r table.Row) bool {
						return r.Get("city").UnwrapOr("") == "Berlin"
					})
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Select("city", "revenue")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Map("city", func(v string) string { return "[" + v + "]" })
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Sort("revenue", true)
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.GroupByAgg([]string{"city"}, aggs)
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Join(other, "id", "id")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.LeftJoin(other, "id", "id")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Distinct("city")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.ValueCounts("city")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.FillEmpty("city", "unknown")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.DropEmpty("city")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.RollingAgg("roll", 10, table.Sum("revenue"))
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.CumSum("revenue", "cum")
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Rank("revenue", "rank", true)
				}).Unwrap()
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
				_ = From(tb).Then(func(t table.Table) table.Table {
					return t.Lag("revenue", "prev", 7)
				}).Unwrap()
			}
		})
	}
}

// ── Multi-step pipeline ───────────────────────────────────────────────────────

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
					Then(func(t table.Table) table.Table { return t.DropEmpty("revenue") }).
					Then(func(t table.Table) table.Table { return t.FillEmpty("city", "unknown") }).
					Then(func(t table.Table) table.Table {
						return t.Where(func(r table.Row) bool {
							return r.Get("city").UnwrapOr("") != "unknown"
						})
					}).
					Then(func(t table.Table) table.Table {
						return t.AddCol("label", func(r table.Row) string {
							return r.Get("city").UnwrapOr("") + "_" + r.Get("revenue").UnwrapOr("0")
						})
					}).
					Then(func(t table.Table) table.Table {
						return t.SortMulti(table.Desc("revenue"), table.Asc("city"))
					}).
					Then(func(t table.Table) table.Table { return t.GroupByAgg([]string{"city"}, aggs) }).
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
					Then(func(t table.Table) table.Table { return t.Join(other, "id", "id") }).
					Then(func(t table.Table) table.Table { return t.GroupByAgg([]string{"group"}, aggs) }).
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
				melted := From(tb).Then(func(t table.Table) table.Table {
					return t.Melt([]string{"id"}, "var", "val")
				}).Unwrap()
				_ = From(melted).Then(func(t table.Table) table.Table {
					return t.Pivot("id", "var", "val")
				}).Unwrap()
			}
		})
	}
}

// ── Error-propagation overhead ────────────────────────────────────────────────

func BenchmarkPipeline_ErrPropagation(b *testing.B) {
	tb := etlTable(10)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := From(tb).
			AssertNoEmpty("nonexistent_col"). // always errors
			Then(func(t table.Table) table.Table { return t.Sort("revenue", true) }).
			Then(func(t table.Table) table.Table {
				return t.GroupByAgg([]string{"city"}, []table.AggDef{{Col: "n", Agg: table.Count("revenue")}})
			})
		if p.IsOk() {
			b.Fatal("expected error")
		}
	}
}

// ── Step tracing overhead ────────────────────────────────────────────────────

func BenchmarkPipeline_Step(b *testing.B) {
	for _, sz := range etlBenchSizes {
		tb := etlTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = From(tb).WithTracing().
					Step("where", func(t table.Table) table.Table {
						return t.Where(func(r table.Row) bool {
							return r.Get("city").UnwrapOr("") == "Berlin"
						})
					}).
					Step("select", func(t table.Table) table.Table {
						return t.Select("city", "revenue")
					}).
					Unwrap()
			}
		})
	}
}
