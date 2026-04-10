package table

import (
	"fmt"
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

	if err := m.Set(0, "name", "Ann"); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, original.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, m.Freeze().Rows[0].Get("name").UnwrapOr(""), "Ann")
}

func TestMutableTable_FreezeIsolatedFromFutureMutations(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}})
	frozen := m.Freeze()

	if err := m.Set(0, "name", "Ann"); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, frozen.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestMutableTable_SetPadsShortRows(t *testing.T) {
	m := NewMutable([]string{"a", "b"}, [][]string{{"1"}})

	if err := m.Set(0, "b", "2"); err != nil {
		t.Fatal(err)
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

	if err := m.FillEmpty("city", "unknown"); err != nil {
		t.Fatal(err)
	}
	if err := m.Map("id", func(v string) string { return "ID-" + v }); err != nil {
		t.Fatal(err)
	}
	m.AddCol("label", func(r Row) string {
		return r.Get("id").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
	})
	if err := m.Rename("id", "key"); err != nil {
		t.Fatal(err)
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

func TestMutableTable_SetErrors(t *testing.T) {
	m := NewMutable([]string{"id"}, [][]string{{"1"}})

	if err := m.Set(5, "id", "x"); err == nil {
		t.Fatal("expected row error")
	}
	if err := m.Set(0, "missing", "x"); err == nil {
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

	err := m.TryMap("amount", func(v string) (string, error) {
		if v == "20" {
			return "", fmt.Errorf("boom")
		}
		return v + " EUR", nil
	})
	if err == nil {
		t.Fatal("expected error")
	}

	frozen := m.Freeze()
	assertEqual(t, frozen.Rows[0].Get("amount").UnwrapOr(""), "10")
	assertEqual(t, frozen.Rows[1].Get("amount").UnwrapOr(""), "20")
}

func TestMutableTable_TryTransformRollback(t *testing.T) {
	m := NewMutable([]string{"id", "name"}, [][]string{{"1", "Alice"}, {"2", "Bob"}})

	err := m.TryTransform(func(r Row) (map[string]string, error) {
		if r.Get("id").UnwrapOr("") == "2" {
			return nil, fmt.Errorf("boom")
		}
		return map[string]string{"name": "Dr. " + r.Get("name").UnwrapOr("")}, nil
	})
	if err == nil {
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

	if err := m.MapParallel("name", func(v string) string { return "@" + v }); err != nil {
		t.Fatal(err)
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

	if err := m.Set(0, "city", "Hamburg"); err != nil {
		t.Fatal(err)
	}

	assertEqual(t, grouped["Berlin"].Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, matched.Rows[0].Get("city").UnwrapOr(""), "Berlin")
	assertEqual(t, rest.Rows[0].Get("city").UnwrapOr(""), "Munich")
	assertEqual(t, chunks[0].Rows[0].Get("city").UnwrapOr(""), "Berlin")
}
