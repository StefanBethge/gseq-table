package table

import (
	"testing"
)

// --- RenameMany ---

func TestRenameMany(t *testing.T) {
	tb := New([]string{"a", "b", "c"}, [][]string{{"1", "2", "3"}})
	result := tb.RenameMany(map[string]string{"a": "x", "c": "z"})
	assertEqual(t, result.Headers[0], "x")
	assertEqual(t, result.Headers[1], "b") // unchanged
	assertEqual(t, result.Headers[2], "z")
	assertEqual(t, result.Rows[0].Get("x").UnwrapOr(""), "1")
}

func TestRenameMany_UnknownIgnored(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}})
	result := tb.RenameMany(map[string]string{"unknown": "x"})
	assertEqual(t, result.Headers[0], "a") // unchanged
}

// --- Concat ---

func TestConcat(t *testing.T) {
	a := New([]string{"name"}, [][]string{{"Alice"}})
	b := New([]string{"name"}, [][]string{{"Bob"}})
	c := New([]string{"name"}, [][]string{{"Carol"}})
	result := Concat(a, b, c)
	assertEqual(t, len(result.Rows), 3)
	assertEqual(t, result.Rows[2].Get("name").UnwrapOr(""), "Carol")
}

func TestConcat_Empty(t *testing.T) {
	result := Concat()
	assertEqual(t, len(result.Rows), 0)
}

// --- AddRowIndex ---

func TestAddRowIndex(t *testing.T) {
	tb := New([]string{"name"}, [][]string{{"Alice"}, {"Bob"}, {"Carol"}})
	result := tb.AddRowIndex("idx")
	assertEqual(t, result.Headers[0], "idx")
	assertEqual(t, result.Headers[1], "name")
	assertEqual(t, result.Rows[0].Get("idx").UnwrapOr(""), "0")
	assertEqual(t, result.Rows[2].Get("idx").UnwrapOr(""), "2")
	assertEqual(t, result.Rows[1].Get("name").UnwrapOr(""), "Bob")
}

// --- Explode ---

func TestExplode(t *testing.T) {
	tb := New([]string{"name", "tags"}, [][]string{
		{"Alice", "go,etl,data"},
		{"Bob", "go"},
	})
	result := tb.Explode("tags", ",")
	assertEqual(t, len(result.Rows), 4) // Alice: 3 tags, Bob: 1 tag
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, result.Rows[0].Get("tags").UnwrapOr(""), "go")
	assertEqual(t, result.Rows[2].Get("tags").UnwrapOr(""), "data")
	assertEqual(t, result.Rows[3].Get("name").UnwrapOr(""), "Bob")
}

func TestExplode_TrimsSpaces(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"a, b, c"}})
	result := tb.Explode("v", ",")
	assertEqual(t, result.Rows[0].Get("v").UnwrapOr(""), "a")
	assertEqual(t, result.Rows[1].Get("v").UnwrapOr(""), "b")
	assertEqual(t, result.Rows[2].Get("v").UnwrapOr(""), "c")
}

func TestExplode_UnknownCol(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"x"}})
	result := tb.Explode("unknown", ",")
	assertEqual(t, len(result.Rows), 1) // unchanged
}

// --- Transpose ---

func TestTranspose(t *testing.T) {
	tb := New([]string{"name", "age"}, [][]string{
		{"Alice", "30"},
		{"Bob", "25"},
	})
	result := tb.Transpose()
	assertEqual(t, result.Headers[0], "column")
	assertEqual(t, result.Headers[1], "0")
	assertEqual(t, result.Headers[2], "1")
	assertEqual(t, len(result.Rows), 2) // one row per original column
	assertEqual(t, result.Rows[0].Get("column").UnwrapOr(""), "name")
	assertEqual(t, result.Rows[0].Get("0").UnwrapOr(""), "Alice")
	assertEqual(t, result.Rows[0].Get("1").UnwrapOr(""), "Bob")
	assertEqual(t, result.Rows[1].Get("column").UnwrapOr(""), "age")
	assertEqual(t, result.Rows[1].Get("0").UnwrapOr(""), "30")
}

// --- FillForward ---

func TestFillForward(t *testing.T) {
	tb := New([]string{"region"}, [][]string{{"EU"}, {""}, {""}, {"US"}, {""}})
	result := tb.FillForward("region")
	assertEqual(t, result.Rows[1].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[2].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[3].Get("region").UnwrapOr(""), "US")
	assertEqual(t, result.Rows[4].Get("region").UnwrapOr(""), "US")
}

func TestFillForward_LeadingEmpty(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{""}, {""}, {"x"}})
	result := tb.FillForward("v")
	// rows before first non-empty stay empty
	assertEqual(t, result.Rows[0].Get("v").UnwrapOr(""), "")
	assertEqual(t, result.Rows[1].Get("v").UnwrapOr(""), "")
	assertEqual(t, result.Rows[2].Get("v").UnwrapOr(""), "x")
}

// --- FillBackward ---

func TestFillBackward(t *testing.T) {
	tb := New([]string{"region"}, [][]string{{""}, {""}, {"EU"}, {""}, {"US"}})
	result := tb.FillBackward("region")
	assertEqual(t, result.Rows[0].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[1].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[2].Get("region").UnwrapOr(""), "EU")
	assertEqual(t, result.Rows[3].Get("region").UnwrapOr(""), "US")
	assertEqual(t, result.Rows[4].Get("region").UnwrapOr(""), "US")
}

func TestFillBackward_TrailingEmpty(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"x"}, {""}, {""}})
	result := tb.FillBackward("v")
	// rows after last non-empty stay empty
	assertEqual(t, result.Rows[0].Get("v").UnwrapOr(""), "x")
	assertEqual(t, result.Rows[1].Get("v").UnwrapOr(""), "")
	assertEqual(t, result.Rows[2].Get("v").UnwrapOr(""), "")
}

// --- Sample ---

func TestSample_LessThanLen(t *testing.T) {
	tb := makeTable()
	result := tb.Sample(2)
	assertEqual(t, len(result.Rows), 2)
	assertEqual(t, len(result.Headers), len(tb.Headers))
}

func TestSample_MoreThanLen(t *testing.T) {
	tb := makeTable()
	result := tb.Sample(100)
	assertEqual(t, len(result.Rows), tb.Len()) // capped at table size
}

func TestSampleFrac(t *testing.T) {
	tb := New([]string{"v"}, func() [][]string {
		var r [][]string
		for i := range 100 {
			r = append(r, []string{string(rune('0' + i%10))})
		}
		return r
	}())
	result := tb.SampleFrac(0.1)
	assertEqual(t, len(result.Rows), 10)
}

// --- Partition ---

func TestPartition(t *testing.T) {
	tb := makeTable()
	active, rest := tb.Partition(func(r Row) bool {
		return r.Get("city").UnwrapOr("") == "Berlin"
	})
	assertEqual(t, len(active.Rows), 2)
	assertEqual(t, len(rest.Rows), 1)
	assertEqual(t, rest.Rows[0].Get("city").UnwrapOr(""), "Munich")
}

func TestPartition_HeadersPreserved(t *testing.T) {
	m, r := makeTable().Partition(func(_ Row) bool { return true })
	assertEqual(t, len(m.Headers), 3)
	assertEqual(t, len(r.Rows), 0)
}

// --- Chunk ---

func TestChunk(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}})
	chunks := tb.Chunk(2)
	assertEqual(t, len(chunks), 3)      // [1,2] [3,4] [5]
	assertEqual(t, len(chunks[0].Rows), 2)
	assertEqual(t, len(chunks[1].Rows), 2)
	assertEqual(t, len(chunks[2].Rows), 1)
}

func TestChunk_ExactFit(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"1"}, {"2"}, {"3"}, {"4"}})
	chunks := tb.Chunk(2)
	assertEqual(t, len(chunks), 2)
}

// --- ForEach ---

func TestForEach(t *testing.T) {
	tb := makeTable()
	count := 0
	tb.ForEach(func(i int, _ Row) { count++ })
	assertEqual(t, count, 3)
}

// --- Coalesce ---

func TestCoalesce(t *testing.T) {
	tb := New([]string{"a", "b", "c"}, [][]string{
		{"", "", "fallback"},
		{"first", "second", "third"},
		{"", "middle", "last"},
	})
	result := tb.Coalesce("result", "a", "b", "c")
	assertEqual(t, result.Rows[0].Get("result").UnwrapOr(""), "fallback")
	assertEqual(t, result.Rows[1].Get("result").UnwrapOr(""), "first")
	assertEqual(t, result.Rows[2].Get("result").UnwrapOr(""), "middle")
}

// --- Lookup ---

func TestLookup(t *testing.T) {
	orders := New([]string{"id", "cust_id"}, [][]string{{"1", "C1"}, {"2", "C2"}, {"3", "C9"}})
	customers := New([]string{"id", "name"}, [][]string{{"C1", "Alice"}, {"C2", "Bob"}})
	result := orders.Lookup("cust_id", "cust_name", customers, "id", "name")
	assertEqual(t, result.Rows[0].Get("cust_name").UnwrapOr(""), "Alice")
	assertEqual(t, result.Rows[1].Get("cust_name").UnwrapOr(""), "Bob")
	assertEqual(t, result.Rows[2].Get("cust_name").UnwrapOr("x"), "") // no match
}

// --- FormatCol ---

func TestFormatCol(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"3.14159"}, {"2.71828"}, {"abc"}})
	result := tb.FormatCol("v", 2)
	assertEqual(t, result.Rows[0].Get("v").UnwrapOr(""), "3.14")
	assertEqual(t, result.Rows[1].Get("v").UnwrapOr(""), "2.72")
	assertEqual(t, result.Rows[2].Get("v").UnwrapOr(""), "abc") // non-numeric unchanged
}

// --- Union ---

func TestUnion(t *testing.T) {
	a := New([]string{"id"}, [][]string{{"1"}, {"2"}, {"3"}})
	b := New([]string{"id"}, [][]string{{"2"}, {"3"}, {"4"}})
	result := Union(a, b, "id")
	assertEqual(t, len(result.Rows), 4) // 1,2,3,4 distinct
}

// --- Intersect ---

func TestIntersect(t *testing.T) {
	a := New([]string{"id"}, [][]string{{"1"}, {"2"}, {"3"}})
	b := New([]string{"id"}, [][]string{{"2"}, {"3"}, {"4"}})
	result := a.Intersect(b, "id")
	assertEqual(t, len(result.Rows), 2) // 2 and 3 are in both
}

func TestIntersect_AllCols(t *testing.T) {
	a := New([]string{"a", "b"}, [][]string{{"1", "x"}, {"2", "y"}})
	b := New([]string{"a", "b"}, [][]string{{"1", "x"}, {"3", "z"}})
	result := a.Intersect(b) // compare all cols
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
}

// --- Bin ---

func TestBin(t *testing.T) {
	tb := New([]string{"age"}, [][]string{{"15"}, {"25"}, {"70"}, {"abc"}})
	result := tb.Bin("age", "group", []BinDef{
		{Max: 18, Label: "minor"},
		{Max: 65, Label: "adult"},
		{Max: 999, Label: "senior"},
	})
	assertEqual(t, result.Rows[0].Get("group").UnwrapOr(""), "minor")
	assertEqual(t, result.Rows[1].Get("group").UnwrapOr(""), "adult")
	assertEqual(t, result.Rows[2].Get("group").UnwrapOr(""), "senior")
	assertEqual(t, result.Rows[3].Get("group").UnwrapOr("x"), "") // unparseable
}

// --- Missing column edge cases ---

func TestExplode_ShortRow(t *testing.T) {
	// Row has fewer values than headers — should not panic
	headers := []string{"name", "tags"}
	rows := []Row{
		NewRow(headers, []string{"Alice"}), // short: no "tags" value
	}
	tb := NewFromRows(headers, rows)
	result := tb.Explode("tags", ",")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestLookup_MissingCol(t *testing.T) {
	orders := New([]string{"id"}, [][]string{{"1"}})
	customers := New([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	// lookup col doesn't exist in orders
	result := orders.Lookup("nonexistent", "cust_name", customers, "id", "name")
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("cust_name").IsNone(), true) // unchanged, no new col
}

func TestLookup_MissingLookupKeyCol(t *testing.T) {
	orders := New([]string{"id"}, [][]string{{"1"}})
	customers := New([]string{"name"}, [][]string{{"Alice"}})
	result := orders.Lookup("id", "cust_name", customers, "nonexistent", "name")
	assertEqual(t, len(result.Rows), 1)
}

func TestBin_MissingCol(t *testing.T) {
	tb := New([]string{"age"}, [][]string{{"25"}})
	result := tb.Bin("nonexistent", "group", []BinDef{{Max: 65, Label: "adult"}})
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("group").IsNone(), true)
}

func TestIntersect_MissingCol(t *testing.T) {
	a := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	b := New([]string{"id"}, [][]string{{"1"}})
	result := a.Intersect(b, "nonexistent")
	assertEqual(t, len(result.Rows), 2) // returns a unchanged
}

func TestFillForward_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}, {""}, {"3"}})
	result := tb.FillForward("nonexistent")
	assertEqual(t, len(result.Rows), 3)
	assertEqual(t, result.Rows[1].Get("a").UnwrapOr(""), "") // unchanged
}

func TestFillBackward_MissingCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{""}, {"2"}, {"3"}})
	result := tb.FillBackward("nonexistent")
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "") // unchanged
}
