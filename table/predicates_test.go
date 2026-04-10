package table

import "testing"

func predTable() Table {
	return New([]string{"name", "email", "score"}, [][]string{
		{"Alice", "alice@gmail.com", "85"},
		{"Bob", "bob@work.com", "70"},
		{"Carol", "", "92"},
		{"Dave", "dave@gmail.com", ""},
	})
}

func TestEq(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Eq("name", "Alice"))
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestNe(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Ne("name", "Alice"))
	assertEqual(t, len(result.Rows), 3)
}

func TestContains(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Contains("email", "gmail"))
	assertEqual(t, len(result.Rows), 2)
}

func TestPrefix(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Prefix("email", "alice"))
	assertEqual(t, len(result.Rows), 1)
}

func TestSuffix(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Suffix("email", ".com"))
	assertEqual(t, len(result.Rows), 3)
}

func TestMatches(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Matches("email", `^[^@]+@gmail\.com$`))
	assertEqual(t, len(result.Rows), 2)
}

func TestEmpty(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Empty("email"))
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestNotEmpty(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.NotEmpty("email"))
	assertEqual(t, len(result.Rows), 3)
}

func TestAnd(t *testing.T) {
	tb := predTable()
	result := tb.Where(And(tb.Contains("email", "gmail"), tb.NotEmpty("score")))
	// Alice (gmail + score=85), Dave has gmail but no score
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestOr(t *testing.T) {
	tb := predTable()
	result := tb.Where(Or(tb.Eq("name", "Alice"), tb.Eq("name", "Bob")))
	assertEqual(t, len(result.Rows), 2)
}

func TestNot(t *testing.T) {
	tb := predTable()
	result := tb.Where(Not(tb.Empty("email")))
	assertEqual(t, len(result.Rows), 3) // same as NotEmpty
}

// --- Predicates with missing columns ---

func TestEq_MissingCol(t *testing.T) {
	tb := predTable()
	// missing column treated as "" → Eq("nonexistent", "") matches all
	result := tb.Where(tb.Eq("nonexistent", ""))
	assertEqual(t, len(result.Rows), 4)
	// Eq("nonexistent", "x") matches none
	result2 := tb.Where(tb.Eq("nonexistent", "x"))
	assertEqual(t, len(result2.Rows), 0)
}

func TestNe_MissingCol(t *testing.T) {
	tb := predTable()
	// missing column treated as "" → Ne("nonexistent", "") matches none
	result := tb.Where(tb.Ne("nonexistent", ""))
	assertEqual(t, len(result.Rows), 0)
}

func TestContains_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Contains("nonexistent", "x"))
	assertEqual(t, len(result.Rows), 0) // "" doesn't contain "x"
}

func TestPrefix_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Prefix("nonexistent", "x"))
	assertEqual(t, len(result.Rows), 0)
}

func TestSuffix_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Suffix("nonexistent", "x"))
	assertEqual(t, len(result.Rows), 0)
}

func TestMatches_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Matches("nonexistent", `^$`))
	assertEqual(t, len(result.Rows), 4) // "" matches ^$
}

func TestEmpty_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.Empty("nonexistent"))
	assertEqual(t, len(result.Rows), 4) // missing → always empty
}

func TestNotEmpty_MissingCol(t *testing.T) {
	tb := predTable()
	result := tb.Where(tb.NotEmpty("nonexistent"))
	assertEqual(t, len(result.Rows), 0) // missing → never not-empty
}
