package table

import "testing"

func tsTable() Table {
	return New([]string{"month", "revenue"}, [][]string{
		{"Jan", "100"},
		{"Feb", "150"},
		{"Mar", "120"},
		{"Apr", "200"},
		{"May", "180"},
	})
}

// --- Lag ---

func TestLag_Basic(t *testing.T) {
	result := tsTable().Lag("revenue", "prev", 1)
	assertEqual(t, result.Rows[0].Get("prev").UnwrapOr("x"), "") // first row has no lag
	assertEqual(t, result.Rows[1].Get("prev").UnwrapOr(""), "100")
	assertEqual(t, result.Rows[2].Get("prev").UnwrapOr(""), "150")
}

func TestLag_N2(t *testing.T) {
	result := tsTable().Lag("revenue", "prev2", 2)
	assertEqual(t, result.Rows[0].Get("prev2").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[1].Get("prev2").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[2].Get("prev2").UnwrapOr(""), "100")
}

// --- Lead ---

func TestLead_Basic(t *testing.T) {
	result := tsTable().Lead("revenue", "next", 1)
	assertEqual(t, result.Rows[0].Get("next").UnwrapOr(""), "150")
	assertEqual(t, result.Rows[3].Get("next").UnwrapOr(""), "180")
	assertEqual(t, result.Rows[4].Get("next").UnwrapOr("x"), "") // last row has no lead
}

// --- CumSum ---

func TestCumSum(t *testing.T) {
	result := tsTable().CumSum("revenue", "cum")
	assertEqual(t, result.Rows[0].Get("cum").UnwrapOr(""), "100")
	assertEqual(t, result.Rows[1].Get("cum").UnwrapOr(""), "250")
	assertEqual(t, result.Rows[2].Get("cum").UnwrapOr(""), "370")
	assertEqual(t, result.Rows[4].Get("cum").UnwrapOr(""), "750")
}

func TestCumSum_SkipsInvalid(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"1"}, {"abc"}, {"3"}})
	result := tb.CumSum("v", "c")
	assertEqual(t, result.Rows[0].Get("c").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[1].Get("c").UnwrapOr(""), "1") // "abc" skipped
	assertEqual(t, result.Rows[2].Get("c").UnwrapOr(""), "4")
}

// --- Rank ---

func TestRank_Asc(t *testing.T) {
	tb := New([]string{"score"}, [][]string{{"70"}, {"100"}, {"85"}, {"70"}})
	result := tb.Rank("score", "rank", true)
	assertEqual(t, result.Rows[0].Get("rank").UnwrapOr(""), "1") // 70 → rank 1
	assertEqual(t, result.Rows[1].Get("rank").UnwrapOr(""), "3") // 100 → rank 3
	assertEqual(t, result.Rows[2].Get("rank").UnwrapOr(""), "2") // 85 → rank 2
	assertEqual(t, result.Rows[3].Get("rank").UnwrapOr(""), "1") // 70 → rank 1 (tie)
}

func TestRank_Desc(t *testing.T) {
	tb := New([]string{"score"}, [][]string{{"70"}, {"100"}, {"85"}})
	result := tb.Rank("score", "rank", false)
	assertEqual(t, result.Rows[0].Get("rank").UnwrapOr(""), "3") // 70 → rank 3 (lowest)
	assertEqual(t, result.Rows[1].Get("rank").UnwrapOr(""), "1") // 100 → rank 1 (highest)
}

func TestRank_InvalidEmpty(t *testing.T) {
	tb := New([]string{"v"}, [][]string{{"10"}, {""}, {"20"}})
	result := tb.Rank("v", "r", true)
	assertEqual(t, result.Rows[1].Get("r").UnwrapOr("x"), "") // unparseable → empty rank
}
