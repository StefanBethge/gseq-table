package table

import "testing"

func TestBuilder_Add(t *testing.T) {
	b := NewBuilder("name", "city")
	b.Add("Alice", "Berlin").Add("Bob", "Munich")
	tb := b.Build()

	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, tb.Rows[1].Get("city").UnwrapOr(""), "Munich")
}

func TestBuilder_AddMap(t *testing.T) {
	b := NewBuilder("name", "score")
	b.AddMap(map[string]string{"name": "Carol", "score": "92"})
	b.AddMap(map[string]string{"name": "Dave"}) // score missing → ""
	tb := b.Build()

	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("score").UnwrapOr(""), "92")
	assertEqual(t, tb.Rows[1].Get("score").UnwrapOr(""), "")
}

func TestBuilder_Set(t *testing.T) {
	b := NewBuilder("name", "score")
	b.Add("Alice", "0")
	b.Set(0, "score", "99")
	tb := b.Build()

	assertEqual(t, tb.Rows[0].Get("score").UnwrapOr(""), "99")
}

func TestBuilder_BuildIsolation(t *testing.T) {
	// mutations after Build must not affect the sealed Table
	b := NewBuilder("x")
	b.Add("before")
	t1 := b.Build()
	b.Set(0, "x", "after")
	t2 := b.Build()

	assertEqual(t, t1.Rows[0].Get("x").UnwrapOr(""), "before")
	assertEqual(t, t2.Rows[0].Get("x").UnwrapOr(""), "after")
}

func TestBuilder_Len(t *testing.T) {
	b := NewBuilder("v")
	assertEqual(t, b.Len(), 0)
	b.Add("a").Add("b")
	assertEqual(t, b.Len(), 2)
}

func TestBuilder_ExtraValsTruncated(t *testing.T) {
	b := NewBuilder("a", "b")
	b.Add("1", "2", "3") // "3" is extra
	tb := b.Build()
	assertEqual(t, len(tb.Rows[0].Values()), 2)
}
