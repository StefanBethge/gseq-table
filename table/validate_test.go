package table

import "testing"

func TestAssertColumns_Ok(t *testing.T) {
	tb := New([]string{"id", "name", "email"}, nil)
	if err := tb.AssertColumns("id", "name"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAssertColumns_Missing(t *testing.T) {
	tb := New([]string{"id", "name"}, nil)
	if err := tb.AssertColumns("id", "email"); err == nil {
		t.Error("expected error for missing column")
	}
}

func TestAssertNoEmpty_Ok(t *testing.T) {
	tb := New([]string{"id", "name"}, [][]string{{"1", "Alice"}, {"2", "Bob"}})
	if err := tb.AssertNoEmpty("id", "name"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAssertNoEmpty_EmptyCell(t *testing.T) {
	tb := New([]string{"id", "name"}, [][]string{{"1", "Alice"}, {"2", ""}})
	if err := tb.AssertNoEmpty("id", "name"); err == nil {
		t.Error("expected error for empty cell")
	}
}

func TestAssertNoEmpty_MissingCol(t *testing.T) {
	tb := New([]string{"id"}, [][]string{{"1"}})
	if err := tb.AssertNoEmpty("missing"); err == nil {
		t.Error("expected error for missing column")
	}
}

func TestAssertNoEmpty_AllCols(t *testing.T) {
	// no cols given → check all
	tb := New([]string{"a", "b"}, [][]string{{"x", ""}})
	if err := tb.AssertNoEmpty(); err == nil {
		t.Error("expected error for empty cell")
	}
}
