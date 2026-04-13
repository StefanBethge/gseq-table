package table

import "testing"

func joinTables() (left, right Table) {
	left = New([]string{"name", "dept_id"}, [][]string{
		{"Alice", "10"},
		{"Bob", "20"},
		{"Carol", "30"}, // no matching dept
	})
	right = New([]string{"dept_id", "dept_name"}, [][]string{
		{"10", "Engineering"},
		{"20", "Marketing"},
		{"40", "Finance"}, // no matching employee
	})
	return
}

// --- RightJoin ---

func TestRightJoin_Basic(t *testing.T) {
	left, right := joinTables()
	result := left.RightJoin(right, "dept_id", "dept_id")
	assertEqual(t, len(result.Rows), 3) // 2 matched + Finance (unmatched from right)
}

func TestRightJoin_UnmatchedRight(t *testing.T) {
	left, right := joinTables()
	result := left.RightJoin(right, "dept_id", "dept_id")
	finance := result.Where(func(r Row) bool {
		return r.Get("dept_name").UnwrapOr("") == "Finance"
	})
	assertEqual(t, len(finance.Rows), 1)
	assertEqual(t, finance.Rows[0].Get("name").UnwrapOr(""), "") // no left match → empty
	assertEqual(t, finance.Rows[0].Get("dept_id").UnwrapOr(""), "40")
}

func TestRightJoin_NoLeftMatch_Excluded(t *testing.T) {
	left, right := joinTables()
	result := left.RightJoin(right, "dept_id", "dept_id")
	// Carol (dept 30) has no right match → not in RightJoin result
	carol := result.Where(func(r Row) bool { return r.Get("name").UnwrapOr("") == "Carol" })
	assertEqual(t, len(carol.Rows), 0)
}

// --- OuterJoin ---

func TestOuterJoin_AllRows(t *testing.T) {
	left, right := joinTables()
	result := left.OuterJoin(right, "dept_id", "dept_id")
	// Alice(10)+Eng, Bob(20)+Mkt, Carol(30)+empty, empty+Finance(40)
	assertEqual(t, len(result.Rows), 4)
}

func TestOuterJoin_BothUnmatched(t *testing.T) {
	left, right := joinTables()
	result := left.OuterJoin(right, "dept_id", "dept_id")

	carol := result.Where(func(r Row) bool { return r.Get("name").UnwrapOr("") == "Carol" })
	assertEqual(t, len(carol.Rows), 1)
	assertEqual(t, carol.Rows[0].Get("dept_name").UnwrapOr("x"), "") // no right match

	finance := result.Where(func(r Row) bool {
		return r.Get("dept_name").UnwrapOr("") == "Finance"
	})
	assertEqual(t, len(finance.Rows), 1)
	assertEqual(t, finance.Rows[0].Get("name").UnwrapOr("x"), "") // no left match
}

func TestOuterJoin_DeduplicatesCollidingColumns(t *testing.T) {
	left := New([]string{"dept_id", "name"}, [][]string{{"10", "Alice"}})
	right := New([]string{"dept_id", "name"}, [][]string{{"10", "Engineering"}})
	result := left.OuterJoin(right, "dept_id", "dept_id")
	assertEqual(t, result.Headers[1], "name")
	assertEqual(t, result.Headers[2], "name_2")
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Alice")
	assertEqual(t, result.Rows[0].Get("name_2").UnwrapOr(""), "Engineering")
}

// --- AntiJoin ---

func TestAntiJoin_Basic(t *testing.T) {
	left, right := joinTables()
	result := left.AntiJoin(right, "dept_id", "dept_id")
	// Only Carol (dept 30) has no match in right
	assertEqual(t, len(result.Rows), 1)
	assertEqual(t, result.Rows[0].Get("name").UnwrapOr(""), "Carol")
}

func TestAntiJoin_NoUnmatched(t *testing.T) {
	left := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	right := New([]string{"id"}, [][]string{{"1"}, {"2"}, {"3"}})
	result := left.AntiJoin(right, "id", "id")
	assertEqual(t, len(result.Rows), 0)
}

func TestAntiJoin_AllUnmatched(t *testing.T) {
	left := New([]string{"id"}, [][]string{{"1"}, {"2"}})
	right := New([]string{"id"}, [][]string{{"9"}, {"8"}})
	result := left.AntiJoin(right, "id", "id")
	assertEqual(t, len(result.Rows), 2)
}

