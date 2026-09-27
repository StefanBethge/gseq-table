package table

import (
	"iter"
	"slices"
	"strconv"
	"testing"
	"time"
)

func iterTable() Table {
	return New(
		[]string{"name", "qty", "city"},
		[][]string{
			{"apple", "3", "Berlin"},
			{"pear", "", "Munich"},
			{"plum", "abc"},
			{"kiwi", " 7 ", "Hamburg"},
		},
	)
}

func collect2[K, V any](seq iter.Seq2[K, V]) ([]K, []V) {
	var ks []K
	var vs []V
	for k, v := range seq {
		ks = append(ks, k)
		vs = append(vs, v)
	}
	return ks, vs
}

func assertSliceEqual[T comparable](t *testing.T, got, want []T) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func rowNames(rows []Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Get("name").UnwrapOr("")
	}
	return out
}

func TestTableAll(t *testing.T) {
	tb := iterTable()
	idxs, rows := collect2(tb.All())
	assertSliceEqual(t, idxs, []int{0, 1, 2, 3})
	assertSliceEqual(t, rowNames(rows), []string{"apple", "pear", "plum", "kiwi"})
	assertEqual(t, rows[2].Get("city").IsNone(), true) // short row is yielded as is
}

func TestTableAll_ZeroCopy(t *testing.T) {
	tb := iterTable()
	for i, r := range tb.All() {
		if &r.values[0] != &tb.Rows[i].values[0] {
			t.Fatalf("row %d: All copied the row values", i)
		}
	}
	for r := range tb.RowsSeq() {
		if &r.headers[0] != &tb.Headers[0] {
			t.Fatal("RowsSeq copied the headers")
		}
	}
}

func TestTableRowsSeq(t *testing.T) {
	tb := iterTable()
	rows := slices.Collect(tb.RowsSeq())
	assertSliceEqual(t, rowNames(rows), []string{"apple", "pear", "plum", "kiwi"})
}

func TestTableColSeq(t *testing.T) {
	tb := iterTable()
	assertSliceEqual(t, slices.Collect(tb.ColSeq("city")), []string{"Berlin", "Munich", "", "Hamburg"})
	assertSliceEqual(t, slices.Collect(tb.ColSeq("qty")), []string{"3", "", "abc", " 7 "})
	assertEqual(t, len(slices.Collect(tb.ColSeq("missing"))), 0)
	assertEqual(t, tb.HasErrs(), false)
}

func TestTableColSeqAs(t *testing.T) {
	tb := iterTable()
	idxs, vals := collect2(tb.ColSeqAs[int]("qty"))
	assertSliceEqual(t, idxs, []int{0, 3}) // empty and unparseable cells skipped
	assertSliceEqual(t, vals, []int{3, 7})

	idxs, names := collect2(tb.ColSeqAs[string]("city"))
	assertSliceEqual(t, idxs, []int{0, 1, 2, 3}) // strings are never skipped
	assertSliceEqual(t, names, []string{"Berlin", "Munich", "", "Hamburg"})

	idxs, _ = collect2(tb.ColSeqAs[int]("missing"))
	assertEqual(t, len(idxs), 0)
	assertEqual(t, tb.HasErrs(), false)
}

func TestTableColSeqAs_Types(t *testing.T) {
	tb := typedTable()
	_, prices := collect2(tb.ColSeqAs[float64]("price"))
	assertSliceEqual(t, prices, []float64{1.5, 0.25, 2})
	_, active := collect2(tb.ColSeqAs[bool]("active"))
	assertSliceEqual(t, active, []bool{true, false, true})
	idxs, dates := collect2(tb.ColSeqAs[time.Time]("date"))
	assertSliceEqual(t, idxs, []int{0, 1, 3})
	assertEqual(t, dates[1], time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC))

	// matches ReduceAs
	sum := 0
	for _, n := range tb.ColSeqAs[int]("qty") {
		sum += n
	}
	assertEqual(t, sum, tb.SumAs[int]("qty"))
}

func TestTableIter_EarlyBreak(t *testing.T) {
	tb := iterTable()
	n := 0
	for i := range tb.All() {
		n++
		if i == 1 {
			break
		}
	}
	assertEqual(t, n, 2)

	n = 0
	for range tb.RowsSeq() {
		n++
		break
	}
	assertEqual(t, n, 1)

	n = 0
	for v := range tb.ColSeq("name") {
		n++
		if v == "pear" {
			break
		}
	}
	assertEqual(t, n, 2)

	n = 0
	for range tb.ColSeqAs[int]("qty") {
		n++
		break
	}
	assertEqual(t, n, 1)
}

func TestTableIter_Empty(t *testing.T) {
	tb := New([]string{"a"}, nil)
	assertEqual(t, len(slices.Collect(tb.RowsSeq())), 0)
	assertEqual(t, len(slices.Collect(tb.ColSeq("a"))), 0)
	idxs, _ := collect2(tb.All())
	assertEqual(t, len(idxs), 0)
	idxs, _ = collect2(tb.ColSeqAs[int]("a"))
	assertEqual(t, len(idxs), 0)
	assertEqual(t, len(slices.Collect(Table{}.RowsSeq())), 0)
}

func TestTableIter_Reusable(t *testing.T) {
	tb := iterTable()
	seq := tb.ColSeq("name")
	assertSliceEqual(t, slices.Collect(seq), slices.Collect(seq))
}

func TestMutableAll(t *testing.T) {
	m := iterTable().Mutable()
	idxs, rows := collect2(m.All())
	assertSliceEqual(t, idxs, []int{0, 1, 2, 3})
	assertSliceEqual(t, rowNames(rows), []string{"apple", "pear", "plum", "kiwi"})
	assertSliceEqual(t, rowNames(slices.Collect(m.RowsSeq())), []string{"apple", "pear", "plum", "kiwi"})
}

func TestMutableAll_RowsAreViews(t *testing.T) {
	m := iterTable().Mutable()
	for i, r := range m.All() {
		if &r.values[0] != &m.rows[i][0] {
			t.Fatalf("row %d: All copied the row values", i)
		}
	}
	rows := slices.Collect(m.RowsSeq())
	m.Set(0, "name", "APPLE")
	assertEqual(t, rows[0].Get("name").UnwrapOr(""), "APPLE")
}

func TestMutableColSeq(t *testing.T) {
	m := iterTable().Mutable()
	assertSliceEqual(t, slices.Collect(m.ColSeq("city")), []string{"Berlin", "Munich", "", "Hamburg"})
	assertEqual(t, len(slices.Collect(m.ColSeq("missing"))), 0)

	idxs, vals := collect2(m.ColSeqAs[int]("qty"))
	assertSliceEqual(t, idxs, []int{0, 3})
	assertSliceEqual(t, vals, []int{3, 7})
	idxs, _ = collect2(m.ColSeqAs[int]("missing"))
	assertEqual(t, len(idxs), 0)
	assertEqual(t, m.HasErrs(), false)
}

func TestMutableIter_EarlyBreak(t *testing.T) {
	m := iterTable().Mutable()
	n := 0
	for range m.All() {
		n++
		break
	}
	for range m.RowsSeq() {
		n++
		break
	}
	for range m.ColSeq("name") {
		n++
		break
	}
	for range m.ColSeqAs[string]("name") {
		n++
		break
	}
	assertEqual(t, n, 4)
}

func TestMutableIter_SetDuringIteration(t *testing.T) {
	m := iterTable().Mutable()
	var seen []string
	for i, r := range m.All() {
		seen = append(seen, r.Get("qty").UnwrapOr(""))
		if i+1 < m.Len() {
			m.Set(i+1, "qty", "x"+strconv.Itoa(i+1)) // later row: observed
		}
		m.Set(i, "name", "done")
	}
	assertSliceEqual(t, seen, []string{"3", "x1", "x2", "x3"})
	assertSliceEqual(t, slices.Collect(m.ColSeq("name")), []string{"done", "done", "done", "done"})

	// Set that fills a missing cell of a short row is visible too.
	m = iterTable().Mutable()
	var cities []string
	for v := range m.ColSeq("city") {
		cities = append(cities, v)
		if v == "Munich" {
			m.Set(2, "city", "Bonn")
		}
	}
	assertSliceEqual(t, cities, []string{"Berlin", "Munich", "Bonn", "Hamburg"})
}

func TestMutableIter_AppendDuringIteration(t *testing.T) {
	m := iterTable().Mutable()
	var names []string
	for r := range m.RowsSeq() {
		name := r.Get("name").UnwrapOr("")
		names = append(names, name)
		if name == "kiwi" {
			m.AppendRow([]string{"fig", "1", "Bonn"})
		}
	}
	assertSliceEqual(t, names, []string{"apple", "pear", "plum", "kiwi", "fig"})

	var qtys []int
	for _, n := range m.ColSeqAs[int]("qty") {
		qtys = append(qtys, n)
		if n == 7 {
			m.AppendRow([]string{"lime", "9", "Bonn"})
		}
	}
	assertSliceEqual(t, qtys, []int{3, 7, 1, 9})
}

func TestMutableIter_StructuralChangeDoesNotPanic(t *testing.T) {
	m := iterTable().Mutable()
	n := 0
	for range m.ColSeq("city") {
		n++
		if n == 1 {
			m.Drop("city", "qty").Head(2) // column gone, rows shrink
		}
	}
	assertEqual(t, n, 2)

	m = iterTable().Mutable()
	n = 0
	for range m.All() {
		n++
		m.Where(func(Row) bool { return false })
	}
	assertEqual(t, n, 1)
}

func TestMutableIter_FreezeSnapshot(t *testing.T) {
	m := iterTable().Mutable()
	n := 0
	for range m.Freeze().All() {
		n++
		m.AppendRow([]string{"x"})
	}
	assertEqual(t, n, 4)
	assertEqual(t, m.Len(), 8)
}
