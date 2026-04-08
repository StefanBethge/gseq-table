package table

import (
	"testing"
)

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func makeTable() Table {
	headers := []string{"name", "city", "age"}
	records := [][]string{
		{"Alice", "Berlin", "30"},
		{"Bob", "Munich", "25"},
		{"Carol", "Berlin", "35"},
	}
	return New(headers, records)
}

// Row tests

func TestRow_Get(t *testing.T) {
	row := NewRow([]string{"a", "b"}, []string{"1", "2"})
	assertEqual(t, row.Get("a").UnwrapOr(""), "1")
	assertEqual(t, row.Get("b").UnwrapOr(""), "2")
	assertEqual(t, row.Get("x").IsSome(), false)
}

func TestRow_At(t *testing.T) {
	row := NewRow([]string{"a", "b"}, []string{"1", "2"})
	assertEqual(t, row.At(0).UnwrapOr(""), "1")
	assertEqual(t, row.At(1).UnwrapOr(""), "2")
	assertEqual(t, row.At(5).IsNone(), true)
}

func TestRow_ToMap(t *testing.T) {
	row := NewRow([]string{"a", "b"}, []string{"x", "y"})
	m := row.ToMap()
	assertEqual(t, m["a"], "x")
	assertEqual(t, m["b"], "y")
}

// Table tests

func TestTable_New(t *testing.T) {
	tb := makeTable()
	assertEqual(t, len(tb.Headers), 3)
	assertEqual(t, len(tb.Rows), 3)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_Select(t *testing.T) {
	tb := makeTable().Select("name", "city")
	assertEqual(t, len(tb.Headers), 2)
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Bob")
	assertEqual(t, tb.Rows[1].Get("age").IsNone(), true)
}

func TestTable_Where(t *testing.T) {
	tb := makeTable().Where(func(r Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Carol")
}

func TestTable_Where_NoMatch(t *testing.T) {
	tb := makeTable().Where(func(r Row) bool { return false })
	assertEqual(t, len(tb.Rows), 0)
}

func TestTable_Col(t *testing.T) {
	names := makeTable().Col("name")
	assertEqual(t, len(names), 3)
	assertEqual(t, names[0], "Alice")
	assertEqual(t, names[2], "Carol")
}

func TestTable_Col_Missing(t *testing.T) {
	col := makeTable().Col("unknown")
	assertEqual(t, len(col), 0)
}

func TestTable_Rename(t *testing.T) {
	tb := makeTable().Rename("city", "location")
	assertEqual(t, tb.Headers[1], "location")
	assertEqual(t, tb.Rows[0].Get("location").UnwrapOr(""), "Berlin")
	assertEqual(t, tb.Rows[0].Get("city").IsNone(), true)
}

func TestTable_Append(t *testing.T) {
	a := New([]string{"name", "city"}, [][]string{{"Alice", "Berlin"}})
	b := New([]string{"name", "city"}, [][]string{{"Bob", "Munich"}})
	tb := a.Append(b)
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

func TestTable_Map(t *testing.T) {
	tb := makeTable().Map("city", func(v string) string { return "Stadt:" + v })
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "Stadt:Berlin")
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice") // unchanged
}

func TestTable_Map_UnknownCol(t *testing.T) {
	tb := makeTable()
	result := tb.Map("unknown", func(v string) string { return "x" })
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice") // unchanged
}

func TestTable_AddCol(t *testing.T) {
	tb := makeTable().AddCol("label", func(r Row) string {
		return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
	})
	assertEqual(t, len(tb.Headers), 4)
	assertEqual(t, tb.Rows[0].Get("label").UnwrapOr(""), "Alice@Berlin")
	assertEqual(t, tb.Rows[1].Get("label").UnwrapOr(""), "Bob@Munich")
}

func TestTable_GroupBy(t *testing.T) {
	groups := makeTable().GroupBy("city")
	assertEqual(t, len(groups), 2)
	assertEqual(t, len(groups["Berlin"].Rows), 2)
	assertEqual(t, len(groups["Munich"].Rows), 1)
}

func TestTable_Sort_Asc(t *testing.T) {
	tb := makeTable().Sort("name", true)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[1].Get("name").UnwrapOr(""), "Bob")
	assertEqual(t, tb.Rows[2].Get("name").UnwrapOr(""), "Carol")
}

func TestTable_Sort_Desc(t *testing.T) {
	tb := makeTable().Sort("name", false)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Carol")
	assertEqual(t, tb.Rows[2].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_Join(t *testing.T) {
	left := New([]string{"name", "city"}, [][]string{
		{"Alice", "Berlin"},
		{"Bob", "Munich"},
		{"Carol", "Hamburg"}, // no match
	})
	right := New([]string{"city", "country"}, [][]string{
		{"Berlin", "DE"},
		{"Munich", "DE"},
	})
	tb := left.Join(right, "city", "city")
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, len(tb.Headers), 3) // name, city, country
	assertEqual(t, tb.Rows[0].Get("country").UnwrapOr(""), "DE")
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_Join_MultiMatch(t *testing.T) {
	left := New([]string{"id", "val"}, [][]string{{"1", "a"}})
	right := New([]string{"id", "extra"}, [][]string{{"1", "x"}, {"1", "y"}})
	tb := left.Join(right, "id", "id")
	assertEqual(t, len(tb.Rows), 2)
}

func TestTable_Len(t *testing.T) {
	assertEqual(t, makeTable().Len(), 3)
}

func TestTable_Shape(t *testing.T) {
	r, c := makeTable().Shape()
	assertEqual(t, r, 3)
	assertEqual(t, c, 3)
}

func TestTable_Head(t *testing.T) {
	tb := makeTable().Head(2)
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestTable_Head_Overflow(t *testing.T) {
	tb := makeTable().Head(99)
	assertEqual(t, len(tb.Rows), 3)
}

func TestTable_Tail(t *testing.T) {
	tb := makeTable().Tail(1)
	assertEqual(t, len(tb.Rows), 1)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestTable_Drop(t *testing.T) {
	tb := makeTable().Drop("city", "age")
	assertEqual(t, len(tb.Headers), 1)
	assertEqual(t, tb.Headers[0], "name")
}

func TestTable_DropEmpty(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{
		{"1", "x"},
		{"", "y"},
		{"3", ""},
	})
	assertEqual(t, len(tb.DropEmpty().Rows), 1)
	assertEqual(t, len(tb.DropEmpty("a").Rows), 2) // only check "a"
}

func TestTable_FillEmpty(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"x"}, {""}, {"z"}})
	result := tb.FillEmpty("a", "n/a")
	assertEqual(t, result.Rows[1].Get("a").UnwrapOr(""), "n/a")
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "x")
}

func TestTable_Distinct(t *testing.T) {
	tb := New([]string{"city", "val"}, [][]string{
		{"Berlin", "1"},
		{"Munich", "2"},
		{"Berlin", "3"},
	})
	assertEqual(t, len(tb.Distinct("city").Rows), 2)
	assertEqual(t, len(tb.Distinct().Rows), 3) // all unique when considering both cols
}

func TestTable_Transform(t *testing.T) {
	tb := makeTable().Transform(func(r Row) map[string]string {
		return map[string]string{
			"name": "Dr. " + r.Get("name").UnwrapOr(""),
			"age":  r.Get("age").UnwrapOr("") + "y",
		}
	})
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Dr. Alice")
	assertEqual(t, tb.Rows[0].Get("age").UnwrapOr(""), "30y")
	assertEqual(t, tb.Rows[0].Get("city").UnwrapOr(""), "Berlin") // unchanged
}

func TestTable_SortMulti(t *testing.T) {
	tb := New([]string{"city", "name"}, [][]string{
		{"Berlin", "Carol"},
		{"Munich", "Bob"},
		{"Berlin", "Alice"},
	})
	sorted := tb.SortMulti(Asc("city"), Asc("name"))
	assertEqual(t, sorted.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, sorted.Rows[1].Get("name").UnwrapOr(""), "Carol")
	assertEqual(t, sorted.Rows[2].Get("name").UnwrapOr(""), "Bob")
}

func TestTable_LeftJoin(t *testing.T) {
	left := New([]string{"name", "city"}, [][]string{
		{"Alice", "Berlin"},
		{"Bob", "Hamburg"}, // no match
	})
	right := New([]string{"city", "country"}, [][]string{
		{"Berlin", "DE"},
	})
	tb := left.LeftJoin(right, "city", "city")
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("country").UnwrapOr(""), "DE")
	assertEqual(t, tb.Rows[1].Get("country").UnwrapOr(""), "") // unmatched → empty
}

func TestTable_ValueCounts(t *testing.T) {
	tb := makeTable()
	vc := tb.ValueCounts("city")
	assertEqual(t, vc.Rows[0].Get("value").UnwrapOr(""), "Berlin") // highest count first
	assertEqual(t, vc.Rows[0].Get("count").UnwrapOr(""), "2")
	assertEqual(t, vc.Rows[1].Get("count").UnwrapOr(""), "1")
}

func TestTable_Melt(t *testing.T) {
	tb := New([]string{"name", "q1", "q2"}, [][]string{
		{"Alice", "100", "200"},
		{"Bob", "150", "250"},
	})
	melted := tb.Melt([]string{"name"}, "quarter", "revenue")
	assertEqual(t, len(melted.Rows), 4) // 2 people × 2 quarters
	assertEqual(t, melted.Headers[1], "quarter")
	assertEqual(t, melted.Headers[2], "revenue")
	assertEqual(t, melted.Rows[0].Get("quarter").UnwrapOr(""), "q1")
	assertEqual(t, melted.Rows[0].Get("revenue").UnwrapOr(""), "100")
}

func TestTable_Pivot(t *testing.T) {
	tb := New([]string{"name", "quarter", "revenue"}, [][]string{
		{"Alice", "q1", "100"},
		{"Alice", "q2", "200"},
		{"Bob", "q1", "150"},
		{"Bob", "q2", "250"},
	})
	pivoted := tb.Pivot("name", "quarter", "revenue")
	assertEqual(t, len(pivoted.Rows), 2)
	assertEqual(t, pivoted.Rows[0].Get("q1").UnwrapOr(""), "100")
	assertEqual(t, pivoted.Rows[0].Get("q2").UnwrapOr(""), "200")
	assertEqual(t, pivoted.Rows[1].Get("q1").UnwrapOr(""), "150")
}

func TestTable_Melt_Pivot_Roundtrip(t *testing.T) {
	original := New([]string{"name", "q1", "q2"}, [][]string{
		{"Alice", "100", "200"},
	})
	melted := original.Melt([]string{"name"}, "quarter", "value")
	pivoted := melted.Pivot("name", "quarter", "value")
	assertEqual(t, pivoted.Rows[0].Get("q1").UnwrapOr(""), "100")
	assertEqual(t, pivoted.Rows[0].Get("q2").UnwrapOr(""), "200")
}
