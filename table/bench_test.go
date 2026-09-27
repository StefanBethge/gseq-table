package table

import (
	"strconv"
	"testing"
)

// benchSizes lists the row-count variants used across all sub-benchmarks.
var benchSizes = []struct {
	name string
	n    int
}{
	{"1k", 1_000},
	{"10k", 10_000},
	{"100k", 100_000},
}

// benchCacheEntry holds pre-generated fixtures for a specific row count.
type benchCacheEntry struct {
	headers []string
	records [][]string
	table   Table
	join    Table
}

// benchCache pre-generates all test fixtures once at package init to avoid
// regenerating on every calibration round Go's testing framework issues before
// actual measurement starts.
var benchCache = map[int]benchCacheEntry{}

func init() {
	for _, n := range []int{100, 500, 1_000, 5_000, 10_000, 50_000, 100_000} {
		h, r := benchmarkMutableRecords(n)
		benchCache[n] = benchCacheEntry{
			headers: h,
			records: r,
			table:   New(h, r),
			join:    benchmarkMutableJoinTable(n),
		}
	}
}

// benchTable returns a pre-generated immutable Table with n rows.
func benchTable(n int) Table {
	if e, ok := benchCache[n]; ok {
		return e.table
	}
	return benchmarkMutableBaseTable(n)
}

// benchRecords returns pre-generated headers and records for n rows.
func benchRecords(n int) ([]string, [][]string) {
	if e, ok := benchCache[n]; ok {
		return e.headers, e.records
	}
	return benchmarkMutableRecords(n)
}

// benchMutableView returns a cheap MutableTable that shares row data with the
// cached immutable table. Suitable only for operations that either rearrange the
// outer rows-slice (Sort, Where, Select, Join) or only read values without writing
// (FillForward with no-empty test data). Never use for ops that write to row[i][j]
// (Transform, Lag, Lead, Bin, etc.) — they would corrupt the shared cache.
func benchMutableView(n int) *MutableTable {
	return benchTable(n).MutableView()
}

// benchFixture is a prepared MutableTable from which fresh tables are stamped
// for benchmarks whose op modifies the receiver. By default only the outer rows
// slice and the headers are copied and the row values are shared: the
// benchmarked ops replace m.rows / m.headers instead of writing cells, and rows
// are capped, so appends reallocate. Set copyCells for ops that write into row
// values (Transform, MapJSON, ...) to get a packed deep copy per table.
type benchFixture struct {
	src       *MutableTable
	copyCells bool
}

func (f benchFixture) fresh() *MutableTable {
	var rows [][]string
	if f.copyCells {
		rows = cloneRecordsPacked(f.src.rows)
	} else {
		rows = make([][]string, len(f.src.rows))
		copy(rows, f.src.rows)
	}
	return &MutableTable{
		headers:   copyHeaders(f.src.headers),
		rows:      rows,
		headerIdx: buildHeaderIndex(f.src.headers),
	}
}

// benchPoolRows bounds the number of rows prepared per batch, which keeps the
// pool (and the op results it retains) at a few tens of MB for 100k-row tables.
const benchPoolRows = 500_000

// benchMutableOp runs op b.N times, each time on a fresh table from f.
// Tables are prepared in batches outside the timer: StopTimer/StartTimer read
// runtime memstats, so toggling them per iteration dominated wall-clock time.
// Only op itself is timed and counted towards B/op and allocs/op.
func benchMutableOp(b *testing.B, f benchFixture, op func(m *MutableTable)) {
	b.ReportAllocs()
	batch := max(1, min(64, benchPoolRows/max(1, len(f.src.rows))))
	pool := make([]*MutableTable, batch)
	b.ResetTimer()
	for done := 0; done < b.N; done += batch {
		n := min(batch, b.N-done)
		b.StopTimer()
		clear(pool)
		for i := range pool[:n] {
			pool[i] = f.fresh()
		}
		b.StartTimer()
		for _, m := range pool[:n] {
			op(m)
		}
	}
}

// TestBenchFixture_FreshIsIsolated guards the benchmark setup: ops on a fresh
// table must never leak into the fixture or into later fresh tables.
func TestBenchFixture_FreshIsIsolated(t *testing.T) {
	headers, records := benchmarkMutableRecords(10)
	shared := benchFixture{src: NewMutable(headers, records)}
	shared.fresh().Sort("id", false).Bin("revenue", "tier", benchBinDefs()).Rename("city", "town")
	m := shared.fresh()
	assertEqual(t, len(m.Headers()), 4)
	assertEqual(t, m.ColIndex("city"), 1)
	assertEqual(t, m.Col("id")[0], "0")

	cells := benchFixture{src: NewMutable(headers, records), copyCells: true}
	cells.fresh().Map("city", func(string) string { return "x" })
	assertEqual(t, cells.fresh().Col("city")[0], "Berlin")
}

// benchJoinTable returns a pre-generated join Table with n rows.
func benchJoinTable(n int) Table {
	if e, ok := benchCache[n]; ok {
		return e.join
	}
	return benchmarkMutableJoinTable(n)
}

func benchLookupTable() Table {
	return New([]string{"city", "category"}, [][]string{
		{"Berlin", "DE"},
		{"Munich", "DE"},
		{"Hamburg", "DE"},
	})
}

// benchBinDefs returns a set of BinDef ranges over a 0–1100 revenue domain.
// BinDef only has Max and Label; a row falls into the first bin where value < Max.
func benchBinDefs() []BinDef {
	return []BinDef{
		{Max: 400, Label: "low"},
		{Max: 800, Label: "mid"},
		{Max: 1200, Label: "high"},
	}
}

// ── Select ──────────────────────────────────────────────────────────────────

func BenchmarkSelect(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Select("city", "revenue")
			}
		})
	}
}

// ── Where ────────────────────────────────────────────────────────────────────

func BenchmarkWhere(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Where(func(r Row) bool {
					return r.Get("city").UnwrapOr("") == "Berlin"
				})
			}
		})
	}
}

// ── Map ──────────────────────────────────────────────────────────────────────

func BenchmarkMap(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Map("city", func(v string) string { return "[" + v + "]" })
			}
		})
	}
}

// ── MapParallel ──────────────────────────────────────────────────────────────

func BenchmarkMapParallel(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.MapParallel("city", func(v string) string { return "[" + v + "]" })
			}
		})
	}
}

// ── AddCol ───────────────────────────────────────────────────────────────────

func BenchmarkAddCol(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddCol("upper", func(r Row) string {
					return r.Get("city").UnwrapOr("") + "_x"
				})
			}
		})
	}
}

// ── AddColConstValue ─────────────────────────────────────────────────────────

func BenchmarkAddColConstValue(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColConstValue("status", "active")
			}
		})
	}
}

// ── AddColFloat ──────────────────────────────────────────────────────────────

func BenchmarkAddColFloat(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColFloat("rev_f", func(r Row) float64 { return 1.5 })
			}
		})
	}
}

// ── AddColInt ────────────────────────────────────────────────────────────────

func BenchmarkAddColInt(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColInt("idx", func(r Row) int64 { return 42 })
			}
		})
	}
}

// ── AddColSwitch ─────────────────────────────────────────────────────────────

func BenchmarkAddColSwitch(b *testing.B) {
	cases := []Case{
		{
			When: func(r Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" },
			Then: func(r Row) string { return "A" },
		},
		{
			When: func(r Row) bool { return r.Get("city").UnwrapOr("") == "Munich" },
			Then: func(r Row) string { return "B" },
		},
	}
	els := func(r Row) string { return "C" }
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddColSwitch("grade", cases, els)
			}
		})
	}
}

// ── Transform ────────────────────────────────────────────────────────────────

func BenchmarkTransform(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Transform(func(r Row) map[string]string {
					return map[string]string{
						"city":    r.Get("city").UnwrapOr(""),
						"revenue": r.Get("revenue").UnwrapOr("0"),
					}
				})
			}
		})
	}
}

// ── TransformParallel ────────────────────────────────────────────────────────

func BenchmarkTransformParallel(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.TransformParallel(func(r Row) map[string]string {
					return map[string]string{
						"city":    r.Get("city").UnwrapOr(""),
						"revenue": r.Get("revenue").UnwrapOr("0"),
					}
				})
			}
		})
	}
}

// ── Sort ─────────────────────────────────────────────────────────────────────

func BenchmarkSort(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Sort("revenue", true)
			}
		})
	}
}

// ── SortMulti ────────────────────────────────────────────────────────────────

func BenchmarkSortMulti(b *testing.B) {
	keys := []SortKey{Desc("revenue"), Asc("city")}
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.SortMulti(keys...)
			}
		})
	}
}

// ── Join ─────────────────────────────────────────────────────────────────────

func BenchmarkJoin(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchJoinTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Join(other, "id", "id")
			}
		})
	}
}

// ── LeftJoin ─────────────────────────────────────────────────────────────────

func BenchmarkLeftJoin(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchJoinTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.LeftJoin(other, "id", "id")
			}
		})
	}
}

// ── RightJoin ────────────────────────────────────────────────────────────────

func BenchmarkRightJoin(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchJoinTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.RightJoin(other, "id", "id")
			}
		})
	}
}

// ── OuterJoin ────────────────────────────────────────────────────────────────

func BenchmarkOuterJoin(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchJoinTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.OuterJoin(other, "id", "id")
			}
		})
	}
}

// ── AntiJoin ─────────────────────────────────────────────────────────────────

func BenchmarkAntiJoin(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchJoinTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AntiJoin(other, "id", "id")
			}
		})
	}
}

// ── Append / Concat ──────────────────────────────────────────────────────────

func BenchmarkAppend(b *testing.B) {
	for _, sz := range benchSizes {
		a := benchTable(sz.n)
		c := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = a.Append(c)
			}
		})
	}
}

func BenchmarkConcat(b *testing.B) {
	for _, sz := range benchSizes {
		parts := []Table{benchTable(sz.n), benchTable(sz.n), benchTable(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Concat(parts...)
			}
		})
	}
}

// ── Head / Tail ───────────────────────────────────────────────────────────────

func BenchmarkHead(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		n := sz.n / 10
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Head(n)
			}
		})
	}
}

func BenchmarkTail(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		n := sz.n / 10
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Tail(n)
			}
		})
	}
}

// ── Drop / DropEmpty ─────────────────────────────────────────────────────────

func BenchmarkDrop(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Drop("name")
			}
		})
	}
}

func BenchmarkDropEmpty(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.DropEmpty("city", "revenue")
			}
		})
	}
}

// ── FillEmpty / FillForward / FillBackward ───────────────────────────────────

func BenchmarkFillEmpty(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.FillEmpty("city", "unknown")
			}
		})
	}
}

func BenchmarkFillForward(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.FillForward("city")
			}
		})
	}
}

func BenchmarkFillBackward(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.FillBackward("city")
			}
		})
	}
}

// ── Distinct ─────────────────────────────────────────────────────────────────

func BenchmarkDistinct(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Distinct("city")
			}
		})
	}
}

// ── ValueCounts ──────────────────────────────────────────────────────────────

func BenchmarkValueCounts(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.ValueCounts("city")
			}
		})
	}
}

// ── GroupBy ──────────────────────────────────────────────────────────────────

func BenchmarkGroupBy(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.GroupBy("city")
			}
		})
	}
}

// ── RollingAgg ───────────────────────────────────────────────────────────────

func BenchmarkRollingAgg(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.RollingAgg("roll", 10, Sum("revenue"))
			}
		})
	}
}

// ── Intersect ────────────────────────────────────────────────────────────────

func BenchmarkIntersect(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		other := benchTable(sz.n / 2)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Intersect(other, "city")
			}
		})
	}
}

// ── Union ────────────────────────────────────────────────────────────────────

func BenchmarkUnion(b *testing.B) {
	for _, sz := range benchSizes {
		a := benchTable(sz.n)
		c := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Union(a, c, "city")
			}
		})
	}
}

// ── Partition ────────────────────────────────────────────────────────────────

func BenchmarkPartition(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = tb.Partition(func(r Row) bool {
					return r.Get("city").UnwrapOr("") == "Berlin"
				})
			}
		})
	}
}

// ── Chunk ────────────────────────────────────────────────────────────────────

func BenchmarkChunk(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Chunk(1_000)
			}
		})
	}
}

// ── Sample / SampleFrac ──────────────────────────────────────────────────────

func BenchmarkSample(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Sample(sz.n / 10)
			}
		})
	}
}

func BenchmarkSampleFrac(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.SampleFrac(0.1)
			}
		})
	}
}

// ── Coalesce ─────────────────────────────────────────────────────────────────

func BenchmarkCoalesce(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Coalesce("result", "city", "name")
			}
		})
	}
}

// ── Lookup ───────────────────────────────────────────────────────────────────

func BenchmarkLookup(b *testing.B) {
	lut := benchLookupTable()
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Lookup("city", "category", lut, "city", "category")
			}
		})
	}
}

// ── FormatCol ────────────────────────────────────────────────────────────────

func BenchmarkFormatCol(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.FormatCol("revenue", 2)
			}
		})
	}
}

// ── AddRowIndex ──────────────────────────────────────────────────────────────

func BenchmarkAddRowIndex(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.AddRowIndex("idx")
			}
		})
	}
}

// ── Rename / RenameMany ──────────────────────────────────────────────────────

func BenchmarkRename(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Rename("city", "town")
			}
		})
	}
}

func BenchmarkRenameMany(b *testing.B) {
	renames := map[string]string{"city": "town", "revenue": "rev"}
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.RenameMany(renames)
			}
		})
	}
}

// ── Bin ──────────────────────────────────────────────────────────────────────

func BenchmarkBin(b *testing.B) {
	bins := benchBinDefs()
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Bin("revenue", "tier", bins)
			}
		})
	}
}

// ── Explode ──────────────────────────────────────────────────────────────────

func BenchmarkExplode(b *testing.B) {
	// Build a table where the "name" column contains pipe-separated values.
	for _, sz := range benchSizes {
		records := make([][]string, sz.n)
		for i := 0; i < sz.n; i++ {
			records[i] = []string{
				strconv.Itoa(i),
				"a|b|c",
			}
		}
		tb := New([]string{"id", "tags"}, records)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Explode("tags", "|")
			}
		})
	}
}

// ── FillForward / FillBackward (sparse) ─────────────────────────────────────

func BenchmarkFillForwardSparse(b *testing.B) {
	// Every 5th row has a non-empty value; rest are empty – stresses forward-fill.
	for _, sz := range benchSizes {
		records := make([][]string, sz.n)
		for i := 0; i < sz.n; i++ {
			v := ""
			if i%5 == 0 {
				v = "city_" + strconv.Itoa(i)
			}
			records[i] = []string{strconv.Itoa(i), v}
		}
		tb := New([]string{"id", "city"}, records)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.FillForward("city")
			}
		})
	}
}

// ── Transpose ────────────────────────────────────────────────────────────────
// Transpose swaps rows and columns; test with a wide-but-short and a tall table.

func BenchmarkTranspose(b *testing.B) {
	sizes := []struct {
		name string
		rows int
		cols int
	}{
		{"100r_50c", 100, 50},
		{"500r_50c", 500, 50},
		{"1000r_50c", 1_000, 50},
	}
	for _, sz := range sizes {
		headers := make([]string, sz.cols)
		for i := range headers {
			headers[i] = "c" + strconv.Itoa(i)
		}
		records := make([][]string, sz.rows)
		for i := range records {
			row := make([]string, sz.cols)
			for j := range row {
				row[j] = strconv.Itoa(i*sz.cols + j)
			}
			records[i] = row
		}
		tb := New(headers, records)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Transpose()
			}
		})
	}
}

// ── Melt / Pivot ─────────────────────────────────────────────────────────────

func BenchmarkMelt(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Melt([]string{"id"}, "var", "val")
			}
		})
	}
}

func BenchmarkPivot(b *testing.B) {
	// Pivot needs (id, var, val) shape; use melt output as input.
	// Keep sizes smaller because pivot is O(rows × unique-var).
	sizes := []struct {
		name string
		n    int
	}{
		{"1k", 1_000},
		{"5k", 5_000},
		{"10k", 10_000},
	}
	for _, sz := range sizes {
		tb := benchTable(sz.n).Melt([]string{"id"}, "var", "val")
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = tb.Pivot("id", "var", "val")
			}
		})
	}
}

// ── CartesianProduct ─────────────────────────────────────────────────────────
// CartesianProduct is O(n×m); keep sizes small.

func BenchmarkCartesianProduct(b *testing.B) {
	sizes := []struct {
		name string
		a, c int
	}{
		{"100x100", 100, 100},
		{"500x500", 500, 500},
		{"1kx1k", 1_000, 1_000},
	}
	for _, sz := range sizes {
		a := benchTable(sz.a)
		c := benchTable(sz.c)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = CartesianProduct(a, c)
			}
		})
	}
}

// ── MutableTable operations not yet benchmarked ───────────────────────────────
//
// Pattern: prepare a benchFixture ONCE outside b.N; benchMutableOp stamps fresh
// tables from it in batches outside the timer, so every timed iteration starts
// from an unmodified table without paying for setup per iteration.

func BenchmarkMutableSelect(b *testing.B) {
	for _, sz := range benchSizes {
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Select("city", "revenue")
			})
		})
	}
}

func BenchmarkMutableWhere(b *testing.B) {
	for _, sz := range benchSizes {
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Where(func(r Row) bool { return r.Get("city").UnwrapOr("") == "Berlin" })
			})
		})
	}
}

func BenchmarkMutableSort(b *testing.B) {
	for _, sz := range benchSizes {
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Sort("revenue", true)
			})
		})
	}
}

func BenchmarkMutableSortMulti(b *testing.B) {
	keys := []SortKey{Desc("revenue"), Asc("city")}
	for _, sz := range benchSizes {
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.SortMulti(keys...)
			})
		})
	}
}

func BenchmarkMutableJoin(b *testing.B) {
	for _, sz := range benchSizes {
		other := benchJoinTable(sz.n)
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Join(other, "id", "id")
			})
		})
	}
}

func BenchmarkMutableLeftJoin(b *testing.B) {
	for _, sz := range benchSizes {
		other := benchJoinTable(sz.n / 2)
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.LeftJoin(other, "id", "id")
			})
		})
	}
}

func BenchmarkMutableRightJoin(b *testing.B) {
	for _, sz := range benchSizes {
		other := benchJoinTable(sz.n / 2)
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.RightJoin(other, "id", "id")
			})
		})
	}
}

func BenchmarkMutableAntiJoin(b *testing.B) {
	for _, sz := range benchSizes {
		other := benchJoinTable(sz.n / 2)
		f := benchFixture{src: benchMutableView(sz.n)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.AntiJoin(other, "id", "id")
			})
		})
	}
}

func BenchmarkMutableAppend(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		other := benchmarkMutableBaseTable(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Append(other)
			})
		})
	}
}

func BenchmarkMutableFillForward(b *testing.B) {
	for _, sz := range benchSizes {
		// Test data has no empty city values, so FillForward neither writes cells
		// nor changes the structure — one prepared table can be reused.
		m := benchMutableView(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.FillForward("city")
			}
		})
	}
}

func BenchmarkMutableLag(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Lag("revenue", "prev", 7)
			})
		})
	}
}

func BenchmarkMutableLead(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Lead("revenue", "next", 7)
			})
		})
	}
}

func BenchmarkMutableTransform(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records), copyCells: true}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Transform(func(r Row) map[string]string {
					return map[string]string{"city": r.Get("city").UnwrapOr("")}
				})
			})
		})
	}
}

func BenchmarkMutableTransformParallel(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records), copyCells: true}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.TransformParallel(func(r Row) map[string]string {
					return map[string]string{"city": r.Get("city").UnwrapOr("")}
				})
			})
		})
	}
}

func BenchmarkMutableBin(b *testing.B) {
	bins := benchBinDefs()
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Bin("revenue", "tier", bins)
			})
		})
	}
}

func BenchmarkMutableMelt(b *testing.B) {
	for _, sz := range benchSizes {
		headers, records := benchRecords(sz.n)
		f := benchFixture{src: NewMutable(headers, records)}
		b.Run(sz.name, func(b *testing.B) {
			benchMutableOp(b, f, func(m *MutableTable) {
				m.Melt([]string{"id"}, "var", "val")
			})
		})
	}
}
