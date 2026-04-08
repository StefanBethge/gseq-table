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
	result := predTable().Where(Eq("name", "Alice"))
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestNe(t *testing.T) {
	result := predTable().Where(Ne("name", "Alice"))
	assertEqual(t, len(result.Rows), 3)
}

func TestContains(t *testing.T) {
	result := predTable().Where(Contains("email", "gmail"))
	assertEqual(t, len(result.Rows), 2)
}

func TestPrefix(t *testing.T) {
	result := predTable().Where(Prefix("email", "alice"))
	assertEqual(t, len(result.Rows), 1)
}

func TestSuffix(t *testing.T) {
	result := predTable().Where(Suffix("email", ".com"))
	assertEqual(t, len(result.Rows), 3)
}

func TestMatches(t *testing.T) {
	result := predTable().Where(Matches("email", `^[^@]+@gmail\.com$`))
	assertEqual(t, len(result.Rows), 2)
}

func TestEmpty(t *testing.T) {
	result := predTable().Where(Empty("email"))
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestNotEmpty(t *testing.T) {
	result := predTable().Where(NotEmpty("email"))
	assertEqual(t, len(result.Rows), 3)
}

func TestAnd(t *testing.T) {
	result := predTable().Where(And(Contains("email", "gmail"), NotEmpty("score")))
	// Alice (gmail + score=85), Dave has gmail but no score
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
}

func TestOr(t *testing.T) {
	result := predTable().Where(Or(Eq("name", "Alice"), Eq("name", "Bob")))
	assertEqual(t, len(result.Rows), 2)
}

func TestNot(t *testing.T) {
	result := predTable().Where(Not(Empty("email")))
	assertEqual(t, len(result.Rows), 3) // same as NotEmpty
}
