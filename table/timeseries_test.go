package table

import (
	"strconv"
	"testing"
)

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

func TestLag_NegativeClampedToZero(t *testing.T) {
	result := tsTable().Lag("revenue", "prev", -1)
	assertEqual(t, result.Rows[0].Get("prev").UnwrapOr(""), "100")
	assertEqual(t, result.Rows[4].Get("prev").UnwrapOr(""), "180")
}

func TestLag_ShortRowKeepsColumnAlignment(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"1"}})
	result := tb.Lag("a", "prev", 0)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[0].Get("b").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[0].Get("prev").UnwrapOr(""), "1")
}

// --- Lead ---

func TestLead_Basic(t *testing.T) {
	result := tsTable().Lead("revenue", "next", 1)
	assertEqual(t, result.Rows[0].Get("next").UnwrapOr(""), "150")
	assertEqual(t, result.Rows[3].Get("next").UnwrapOr(""), "180")
	assertEqual(t, result.Rows[4].Get("next").UnwrapOr("x"), "") // last row has no lead
}

func TestLead_NegativeClampedToZero(t *testing.T) {
	result := tsTable().Lead("revenue", "next", -1)
	assertEqual(t, result.Rows[0].Get("next").UnwrapOr(""), "100")
	assertEqual(t, result.Rows[4].Get("next").UnwrapOr(""), "180")
}

func TestLead_ShortRowKeepsColumnAlignment(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"1"}})
	result := tb.Lead("a", "next", 0)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[0].Get("b").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[0].Get("next").UnwrapOr(""), "1")
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

func TestCumSum_ShortRowKeepsColumnAlignment(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"1"}})
	result := tb.CumSum("a", "cum")
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[0].Get("b").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[0].Get("cum").UnwrapOr(""), "1")
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

func TestRank_ShortRowKeepsColumnAlignment(t *testing.T) {
	tb := New([]string{"a", "b"}, [][]string{{"10"}})
	result := tb.Rank("a", "rank", true)
	assertEqual(t, result.Rows[0].Get("a").UnwrapOr(""), "10")
	assertEqual(t, result.Rows[0].Get("b").UnwrapOr("x"), "")
	assertEqual(t, result.Rows[0].Get("rank").UnwrapOr(""), "1")
}

// --- Missing column edge cases ---

func TestLag_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Lag("nonexistent", "prev", 1)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2) // no new col added
}

func TestLead_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Lead("nonexistent", "next", 1)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func TestCumSum_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.CumSum("nonexistent", "cum")
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func TestRank_MissingCol(t *testing.T) {
	tb := tsTable()
	result := tb.Rank("nonexistent", "rank", true)
	assertEqual(t, len(result.Rows), 5)
	assertEqual(t, len(result.Headers), 2)
}

func BenchmarkLag(b *testing.B) {
	records := make([][]string, 50_000)
	for i := range records {
		records[i] = []string{
			"2024-01-" + strconv.Itoa(i%28+1),
			strconv.Itoa(100 + i%1_000),
		}
	}
	tb := New([]string{"day", "revenue"}, records)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.Lag("revenue", "prev", 7)
	}
}

func BenchmarkLead(b *testing.B) {
	records := make([][]string, 50_000)
	for i := range records {
		records[i] = []string{
			"2024-01-" + strconv.Itoa(i%28+1),
			strconv.Itoa(100 + i%1_000),
		}
	}
	tb := New([]string{"day", "revenue"}, records)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.Lead("revenue", "next", 7)
	}
}

func BenchmarkCumSum(b *testing.B) {
	records := make([][]string, 50_000)
	for i := range records {
		records[i] = []string{
			"2024-01-" + strconv.Itoa(i%28+1),
			strconv.Itoa(100 + i%1_000),
		}
	}
	tb := New([]string{"day", "revenue"}, records)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.CumSum("revenue", "cum")
	}
}

func BenchmarkRank(b *testing.B) {
	records := make([][]string, 50_000)
	for i := range records {
		records[i] = []string{
			strconv.Itoa(100 + i%1_000),
		}
	}
	tb := New([]string{"score"}, records)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.Rank("score", "rank", true)
	}
}
