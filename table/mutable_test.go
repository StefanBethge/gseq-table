package table

import (
	"fmt"
	"strconv"
	"testing"
)

func TestNewMutable_Freeze(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}})

	assertEqual(t, m.Len(), 1)
	rows, cols := m.Shape()
	assertEqual(t, rows, 1)
	assertEqual(t, cols, 2)

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("id").UnwrapOr(""), "1")
	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_MutableCopiesSource(t *testing.T) {
	original := makeTable()
	m := original.Mutable()

	m.Set(0, "name", "Ann")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	assertEqual(t, original.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, m.Freeze().Rows[0].Get("name").UnwrapOr(""), "Ann")
}

func TestMutableTable_FreezeIsolatedFromFutureMutations(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	frozen := m.Freeze()

	m.Set(0, "name", "Ann")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_MutableViewSharesSource(t *testing.T) {
	original := makeTable()
	m := original.MutableView()

	m.Set(0, "name", "Ann")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	assertEqual(t, original.Rows[0].Get("name").UnwrapOr(""), "Ann")
}

func TestMutableTable_FreezeViewSharesFutureMutations(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	view := m.FreezeView()

	m.Set(0, "name", "Ann")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	assertEqual(t, view.Rows[0].Get("name").UnwrapOr(""), "Ann")
}

func TestMutableTable_SetPadsShortRows(t *testing.T) {
	m := NewMutable([]string{"a", "b"}, [][]string{{"1"}})

	m.Set(0, "b", "2")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("a").UnwrapOr(""), "1")
	assertEqual(t, frozen.Rows[0].Get("b").UnwrapOr(""), "2")
}

func TestMutableTable_AppendRowCopiesInput(t *testing.T) {
	m := NewMutable([]string{"id"}, nil)
	input := []string{"1"}

	m.AppendRow(input)
	input[0] = "99"

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("id").UnwrapOr(""), "1")
}

func TestMutableTable_RenameMapFillEmptyAddColDrop(t *testing.T) {
	m := NewMutable([]string{"id", "city"}, [][]string{
		{"1", ""},
		{"2", "Berlin"},
	})

	m.FillEmpty("city", "unknown")
	m.Map("id", func(v string) string { return "ID-" + v })
	m.AddCol("label", func(r Row) string {
		return r.Get("id").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
	})
	m.Rename("id", "key")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}
	m.Drop("city")

	frozen := m.Freeze()
	assertEqual(t, frozen.Headers[0], "key")
	assertEqual(t, frozen.Headers[1], "label")
	assertEqual(t, frozen.Rows[0].Get("key").UnwrapOr(""), "ID-1")
	assertEqual(t, frozen.Rows[0].Get("label").UnwrapOr(""), "ID-1@unknown")
	assertEqual(t, frozen.Rows[1].Get("city").IsNone(), true)
}

func TestMutableTable_AddColKeepsShortRowAlignment(t *testing.T) {
	m := NewMutable([]string{"a", "b"}, [][]string{{"1"}})

	m.AddCol("c", func(r Row) string { return r.Get("a").UnwrapOr("") })

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("a").UnwrapOr(""), "1")
	assertEqual(t, frozen.Rows[0].Get("b").UnwrapOr("x"), "")
	assertEqual(t, frozen.Rows[0].Get("c").UnwrapOr(""), "1")
}

func TestMutableTable_ClampsWideRowsAndDeduplicatesHeaders(t *testing.T) {
	m := NewMutable([]string{"name"}, [][]string{{"Alice", "ignored"}})
	m.AddCol("name", func(r Row) string { return "derived-" + r.Get("name").UnwrapOr("") })

	frozen := m.Freeze()
	assertEqual(t, frozen.Headers[0], "name")
	assertEqual(t, frozen.Headers[1], "name_2")
	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, frozen.Rows[0].Get("name_2").UnwrapOr(""), "derived-Alice")
	assertEqual(t, len(frozen.Rows[0].Values()), 2)
}

func TestMutableTable_SetErrors(t *testing.T) {
	m := NewMutable([]string{"id"}, [][]string{{"1"}})

	m.Set(5, "id", "x")
	if !m.HasErrs() {
		t.Fatal("expected row error")
	}
	m.ResetErrs()
	m.Set(0, "missing", "x")
	if !m.HasErrs() {
		t.Fatal("expected column error")
	}
}

func TestMutableTable_WrapperOps(t *testing.T) {
	m := NewMutable([]string{"name", "city", "age"}, [][]string{
		{"Bob", "Munich", "25"},
		{"Alice", "Berlin", "30"},
		{"Carol", "Berlin", ""},
	})

	m.Where(m.Eq("city", "Berlin"))
	m.Sort("name", true)
	m.FillForward("age")
	m.RenameMany(map[string]string{"age": "years"})
	m.Select("name", "years")
	m.AddRowIndex("idx")

	frozen := m.Freeze()
	assertEqual(t, frozen.Headers[0], "idx")
	assertEqual(t, frozen.Headers[1], "name")
	assertEqual(t, frozen.Headers[2], "years")
	assertEqual(t, len(frozen.Rows), 2)
	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, frozen.Rows[1].Get("years").UnwrapOr(""), "30")
}

func TestMutableTable_AppendJoinAndValueCounts(t *testing.T) {
	m := NewMutable([]string{"id", "city"}, [][]string{{"1", "Berlin"}})
	m.Append(New([]string{"id", "city"}, [][]string{{"2", "Munich"}}))

	counts := m.GroupBy("city")
	assertEqual(t, len(counts), 2)

	m.Join(
		New([]string{"city", "country"}, [][]string{
			{"Berlin", "DE"},
			{"Munich", "DE"},
		}),
		"city", "city",
	)
	assertEqual(t, m.Freeze().Rows[0].Get("country").UnwrapOr(""), "DE")

	m.ValueCounts("country")
	frozen := m.Freeze()
	assertEqual(t, frozen.Headers[0], "value")
	assertEqual(t, frozen.Rows[0].Get("value").UnwrapOr(""), "DE")
	assertEqual(t, frozen.Rows[0].Get("count").UnwrapOr(""), "2")
}

func TestMutableTable_ValueCounts_SortsNumerically(t *testing.T) {
	records := make([][]string, 0, 14)
	for i := 0; i < 12; i++ {
		records = append(records, []string{"Berlin"})
	}
	for i := 0; i < 2; i++ {
		records = append(records, []string{"Munich"})
	}
	m := NewMutable([]string{"city"}, records)
	m.ValueCounts("city")
	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("value").UnwrapOr(""), "Berlin")
	assertEqual(t, frozen.Rows[0].Get("count").UnwrapOr(""), "12")
	assertEqual(t, frozen.Rows[1].Get("value").UnwrapOr(""), "Munich")
}

func TestMutableTable_TimeSeriesAndAggWrappers(t *testing.T) {
	m := NewMutable([]string{"day", "revenue"}, [][]string{
		{"1", "10"},
		{"2", "20"},
		{"3", "30"},
	})

	m.CumSum("revenue", "cum")
	m.Lag("revenue", "prev", 1)
	m.RollingAgg("mean2", 2, Mean("revenue"))

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[1].Get("cum").UnwrapOr(""), "30")
	assertEqual(t, frozen.Rows[1].Get("prev").UnwrapOr(""), "10")
	assertEqual(t, frozen.Rows[2].Get("mean2").UnwrapOr(""), "25")
}

func TestMutableTable_TryMapRollback(t *testing.T) {
	m := NewMutable([]string{"amount"}, [][]string{{"10"}, {"20"}})

	m.TryMap("amount", func(v string) (string, error) {
		if v == "20" {
			return "", fmt.Errorf("boom")
		}
		return v + " EUR", nil
	})
	if !m.HasErrs() {
		t.Fatal("expected error")
	}

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("amount").UnwrapOr(""), "10")
	assertEqual(t, frozen.Rows[1].Get("amount").UnwrapOr(""), "20")
}

func TestMutableTable_TryTransformRollback(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}, {"2", "Bob"}})

	m.TryTransform(func(r Row) (map[string]string, error) {
		if r.Get("id").UnwrapOr("") == "2" {
			return nil, fmt.Errorf("boom")
		}
		return map[string]string{"name": "Dr. " + r.Get("name").UnwrapOr("")}, nil
	})
	if !m.HasErrs() {
		t.Fatal("expected error")
	}

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, frozen.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

func TestMutableTable_MapParallelAndPartition(t *testing.T) {
	m := NewMutable([]string{"name", "city"}, [][]string{
		{"alice", "Berlin"},
		{"bob", "Munich"},
		{"carol", "Berlin"},
	})

	m.MapParallel("name", func(v string) string { return "@" + v })
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}
	m.TransformParallel(func(r Row) map[string]string {
		return map[string]string{"city": r.Get("city").UnwrapOr("") + "!"}
	})

	matches, rest := m.Partition(m.Contains("city", "Berlin"))
	assertEqual(t, len(matches.Rows), 2)
	assertEqual(t, len(rest.Rows), 1)
	assertEqual(t, m.Freeze().Rows[0].Get("name").UnwrapOr(""), "@alice")
}

func TestMutableTable_GroupByPartitionChunkSnapshotsAreIsolated(t *testing.T) {
	m := NewMutable([]string{"id", "city"}, [][]string{
		{"1", "Berlin"},
		{"2", "Munich"},
		{"3", "Berlin"},
	})

	grouped := m.GroupBy("city")
	matched, rest := m.Partition(m.Eq("city", "Berlin"))
	chunks := m.Chunk(2)

	m.Set(0, "city", "Hamburg")
	if m.HasErrs() {
		t.Fatal(m.Errs())
	}

	assertEqual(t, grouped["Berlin"].Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, matched.Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, rest.Rows[0].Get("city").UnwrapOr(""), "Munich")
	assertEqual(t, chunks[0].Rows[0].Get("city").UnwrapOr(""), "Berlin")
}

func BenchmarkTableMutable(b *testing.B) {
	base := benchmarkMutableBaseTable(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = base.Mutable()
	}
}

func BenchmarkTableMutableView(b *testing.B) {
	base := benchmarkMutableBaseTable(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = base.MutableView()
	}
}

func BenchmarkMutableFreeze(b *testing.B) {
	m := benchmarkMutableBaseTable(50_000).Mutable()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.Freeze()
	}
}

func BenchmarkMutableFreezeView(b *testing.B) {
	m := benchmarkMutableBaseTable(50_000).Mutable()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.FreezeView()
	}
}

func BenchmarkMutableDistinct(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.Distinct("city", "name")
	}
}

func BenchmarkMutableIntersect(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	other := benchmarkMutableBaseTable(50_000).Select("city", "name").Distinct("city", "name")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.Intersect(other, "city", "name")
	}
}

func BenchmarkMutableValueCounts(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.ValueCounts("city")
	}
}

func BenchmarkMutableGroupByAgg(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	aggs := []AggDef{
		{Col: "total", Agg: Sum("revenue")},
		{Col: "count", Agg: Count("revenue")},
		{Col: "names", Agg: StringJoin("name", ",")},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.GroupByAgg([]string{"city"}, aggs)
	}
}

func BenchmarkMutableOuterJoin(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	other := benchmarkMutableJoinTable(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.OuterJoin(other, "id", "id")
	}
}

func BenchmarkMutableCumSum(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.CumSum("revenue", "cum")
	}
}

func BenchmarkMutableRank(b *testing.B) {
	headers, records := benchmarkMutableRecords(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := NewMutable(headers, records)
		b.StartTimer()
		m.Rank("revenue", "rank", true)
	}
}

func benchmarkMutableBaseTable(n int) Table {
	headers, records := benchmarkMutableRecords(n)
	return New(headers, records)
}

func benchmarkMutableRecords(n int) ([]string, [][]string) {
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
			"name_" + strconv.Itoa(n-i),
		}
	}
	return headers, records
}

func benchmarkMutableJoinTable(n int) Table {
	records := make([][]string, n)
	for i := 0; i < n; i++ {
		records[i] = []string{
			strconv.Itoa(i),
			"group_" + strconv.Itoa(i%100),
		}
	}
	return New([]string{"id", "group"}, records)
}
