package schema

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func makeTable() table.Table {
	return table.New(
		[]string{"name", "age", "price", "active", "created_at"},
		[][]string{
			{"Alice", "30", "19.99", "true", "2024-03-15"},
			{"Bob", "25", "4.50", "false", "2024-07-01"},
		},
	)
}

// --- Infer ---

func TestInfer_Types(t *testing.T) {
	s := Infer(makeTable())
	assertEqual(t, s.Col("name"), TypeString)
	assertEqual(t, s.Col("age"), TypeInt)
	assertEqual(t, s.Col("price"), TypeFloat)
	assertEqual(t, s.Col("active"), TypeBool)
	assertEqual(t, s.Col("created_at"), TypeDate)
}

func TestInfer_UnknownCol(t *testing.T) {
	s := Infer(makeTable())
	assertEqual(t, s.Col("missing"), TypeString)
}

func TestInfer_EmptyCellsIgnored(t *testing.T) {
	tb := table.New(
		[]string{"amount"},
		[][]string{{"42"}, {""}, {"17"}},
	)
	assertEqual(t, Infer(tb).Col("amount"), TypeInt)
}

func TestInfer_FallsBackToFloat(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"1"}, {"2.5"}})
	assertEqual(t, Infer(tb).Col("v"), TypeFloat)
}

func TestInfer_FallsBackToString(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"1"}, {"hello"}})
	assertEqual(t, Infer(tb).Col("v"), TypeString)
}

func TestInfer_Bool(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"yes"}, {"no"}})
	assertEqual(t, Infer(tb).Col("v"), TypeBool)
}

func TestInfer_Date_ISO(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2024-01-15"}, {"2023-12-31"}})
	assertEqual(t, Infer(tb).Col("d"), TypeDate)
}

func TestInfer_Date_German(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"15.01.2024"}})
	assertEqual(t, Infer(tb).Col("d"), TypeDate)
}

// --- Cast ---

func TestCast_Override(t *testing.T) {
	s := Infer(makeTable()).Cast("age", TypeString)
	assertEqual(t, s.Col("age"), TypeString)
	assertEqual(t, s.Col("price"), TypeFloat) // others unchanged
}

func TestCast_Immutable(t *testing.T) {
	original := Infer(makeTable())
	_ = original.Cast("age", TypeString)
	assertEqual(t, original.Col("age"), TypeInt) // original unchanged
}

// --- Apply (lenient) ---

func TestApply_Normalises(t *testing.T) {
	tb := table.New(
		[]string{"n", "f", "b", "d"},
		[][]string{{"42", "3.14", "yes", "15.01.2024"}},
	)
	s := Schema{types: map[string]ColType{
		"n": TypeInt, "f": TypeFloat, "b": TypeBool, "d": TypeDate,
	}}
	res := s.Apply(tb)
	assertEqual(t, res.IsOk(), true)
	row := res.Unwrap().Rows[0]
	assertEqual(t, row.Get("n").UnwrapOr(""), "42")
	assertEqual(t, row.Get("f").UnwrapOr(""), "3.14")
	assertEqual(t, row.Get("b").UnwrapOr(""), "true") // "yes" → "true"
	assertEqual(t, row.Get("d").UnwrapOr(""), "2024-01-15")
}

func TestApply_EmptyCell_Lenient(t *testing.T) {
	tb := table.New([]string{"age"}, [][]string{{"30"}, {""}})
	s := Schema{types: map[string]ColType{"age": TypeInt}}
	res := s.Apply(tb)
	assertEqual(t, res.IsOk(), true)
	assertEqual(t, res.Unwrap().Rows[1].Get("age").UnwrapOr("x"), "") // empty preserved
}

func TestApply_InvalidValue_Error(t *testing.T) {
	tb := table.New([]string{"age"}, [][]string{{"not-a-number"}})
	s := Schema{types: map[string]ColType{"age": TypeInt}}
	res := s.Apply(tb)
	assertEqual(t, res.IsErr(), true)
}

func TestApply_StringColSkipped(t *testing.T) {
	tb := makeTable()
	s := Schema{types: map[string]ColType{"name": TypeString}}
	res := s.Apply(tb)
	assertEqual(t, res.IsOk(), true)
	assertEqual(t, res.Unwrap().Rows[0].Get("name").UnwrapOr(""), "Alice")
}

// --- ApplyStrict ---

func TestApplyStrict_EmptyCell_Error(t *testing.T) {
	tb := table.New([]string{"age"}, [][]string{{"30"}, {""}})
	s := Schema{types: map[string]ColType{"age": TypeInt}}
	res := s.ApplyStrict(tb)
	assertEqual(t, res.IsErr(), true)
}

func TestApplyStrict_NoEmptyCells_Ok(t *testing.T) {
	tb := table.New([]string{"age"}, [][]string{{"30"}, {"25"}})
	s := Schema{types: map[string]ColType{"age": TypeInt}}
	res := s.ApplyStrict(tb)
	assertEqual(t, res.IsOk(), true)
}

func TestApplyStrict_EmptyStringCol_Ignored(t *testing.T) {
	// TypeString columns are never validated, even in strict mode
	tb := table.New([]string{"name"}, [][]string{{"Alice"}, {""}})
	s := Schema{types: map[string]ColType{"name": TypeString}}
	res := s.ApplyStrict(tb)
	assertEqual(t, res.IsOk(), true)
}

// --- Typed accessors ---

func TestInt_Ok(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"42"})
	v, ok := Int(row, "v").Get()
	assertEqual(t, ok, true)
	assertEqual(t, v, int64(42))
}

func TestInt_Invalid(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"abc"})
	assertEqual(t, Int(row, "v").IsNone(), true)
}

func TestFloat_Ok(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"3.14"})
	v, ok := Float(row, "v").Get()
	assertEqual(t, ok, true)
	assertEqual(t, v, 3.14)
}

func TestBool_TrueVariants(t *testing.T) {
	for _, s := range []string{"true", "1", "yes", "True", "YES"} {
		row := table.NewRow([]string{"v"}, []string{s})
		v, ok := Bool(row, "v").Get()
		assertEqual(t, ok, true)
		assertEqual(t, v, true)
	}
}

func TestBool_FalseVariants(t *testing.T) {
	for _, s := range []string{"false", "0", "no", "False"} {
		row := table.NewRow([]string{"v"}, []string{s})
		v, ok := Bool(row, "v").Get()
		assertEqual(t, ok, true)
		assertEqual(t, v, false)
	}
}

func TestTime_AutoLayout(t *testing.T) {
	row := table.NewRow([]string{"d"}, []string{"2024-03-15"})
	v, ok := Time(row, "d", "").Get()
	assertEqual(t, ok, true)
	assertEqual(t, v.Format("2006-01-02"), "2024-03-15")
}

func TestTime_ExplicitLayout(t *testing.T) {
	row := table.NewRow([]string{"d"}, []string{"15.03.2024"})
	v, ok := Time(row, "d", "02.01.2006").Get()
	assertEqual(t, ok, true)
	assertEqual(t, v.Format("2006-01-02"), "2024-03-15")
}

func TestTime_Invalid(t *testing.T) {
	row := table.NewRow([]string{"d"}, []string{"not-a-date"})
	assertEqual(t, Time(row, "d", "").IsNone(), true)
}

func TestTime_MissingCol(t *testing.T) {
	row := table.NewRow([]string{"x"}, []string{"2024-01-01"})
	assertEqual(t, Time(row, "missing", "").IsNone(), true)
}

// --- Numeric column aggregators ---

func numTable() table.Table {
	return table.New(
		[]string{"name", "score", "status"},
		[][]string{
			{"Alice", "80", "active"},
			{"Bob", "90", "active"},
			{"Carol", "70", "inactive"},
			{"Dave", "", "active"}, // empty score
		},
	)
}

func TestSumCol(t *testing.T) {
	assertEqual(t, SumCol(numTable(), "score"), 240.0)
}

func TestMeanCol(t *testing.T) {
	// mean of 80, 90, 70 (empty skipped) = 240/3 = 80
	assertEqual(t, MeanCol(numTable(), "score"), 80.0)
}

func TestMeanCol_EmptyTable(t *testing.T) {
	tb := table.New([]string{"v"}, nil)
	assertEqual(t, MeanCol(tb, "v"), 0.0)
}

func TestMinCol(t *testing.T) {
	assertEqual(t, MinCol(numTable(), "score"), 70.0)
}

func TestMaxCol(t *testing.T) {
	assertEqual(t, MaxCol(numTable(), "score"), 90.0)
}

func TestCountCol(t *testing.T) {
	// 3 non-empty scores (Dave's is empty)
	assertEqual(t, CountCol(numTable(), "score"), 3)
}

func TestCountWhere(t *testing.T) {
	assertEqual(t, CountWhere(numTable(), "status", "active"), 3)
	assertEqual(t, CountWhere(numTable(), "status", "inactive"), 1)
	assertEqual(t, CountWhere(numTable(), "status", "unknown"), 0)
}

// --- StdDevCol / MedianCol / Describe ---

func TestStdDevCol(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"2"}, {"4"}, {"4"}, {"4"}, {"5"}, {"5"}, {"7"}, {"9"}})
	// population std dev of [2,4,4,4,5,5,7,9] = 2.0
	std := StdDevCol(tb, "v")
	if std < 1.99 || std > 2.01 {
		t.Errorf("expected ~2.0, got %f", std)
	}
}

func TestStdDevCol_LessThanTwo(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"5"}})
	assertEqual(t, StdDevCol(tb, "v"), 0.0)
}

func TestMedianCol_Odd(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"3"}, {"1"}, {"2"}})
	assertEqual(t, MedianCol(tb, "v"), 2.0) // sorted: [1,2,3] → median=2
}

func TestMedianCol_Even(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"1"}, {"3"}, {"5"}, {"7"}})
	assertEqual(t, MedianCol(tb, "v"), 4.0) // sorted: [1,3,5,7] → (3+5)/2=4
}

func TestMedianCol_Empty(t *testing.T) {
	tb := table.New([]string{"v"}, nil)
	assertEqual(t, MedianCol(tb, "v"), 0.0)
}

func TestDescribe_NumericCol(t *testing.T) {
	tb := table.New([]string{"score"}, [][]string{{"80"}, {"90"}, {"70"}})
	result := Describe(tb)
	assertEqual(t, len(result.Rows), 1)
	row := result.Rows[0]
	assertEqual(t, row.Get("column").UnwrapOr(""), "score")
	assertEqual(t, row.Get("count").UnwrapOr(""), "3")
	assertEqual(t, row.Get("min").UnwrapOr(""), "70")
	assertEqual(t, row.Get("max").UnwrapOr(""), "90")
	assertEqual(t, row.Get("mean").UnwrapOr(""), "80")
}

func TestDescribe_NonNumericCol(t *testing.T) {
	tb := table.New([]string{"name"}, [][]string{{"Alice"}, {"Bob"}})
	result := Describe(tb)
	row := result.Rows[0]
	assertEqual(t, row.Get("column").UnwrapOr(""), "name")
	assertEqual(t, row.Get("count").UnwrapOr(""), "0") // non-numeric → count=0
	assertEqual(t, row.Get("min").UnwrapOr("x"), "")
}

// --- FreqMap ---

func TestFreqMap(t *testing.T) {
	tb := table.New([]string{"status"}, [][]string{
		{"active"}, {"active"}, {"inactive"}, {"active"}, {""},
	})
	m := FreqMap(tb, "status")
	assertEqual(t, m["active"], 3)
	assertEqual(t, m["inactive"], 1)
	assertEqual(t, m[""], 1)
}

// --- MinMaxNorm ---

func TestMinMaxNorm(t *testing.T) {
	tb := table.New([]string{"score"}, [][]string{{"0"}, {"50"}, {"100"}})
	result := MinMaxNorm(tb, "score")
	assertEqual(t, result.Rows[0].Get("score").UnwrapOr(""), "0")
	assertEqual(t, result.Rows[1].Get("score").UnwrapOr(""), "0.5")
	assertEqual(t, result.Rows[2].Get("score").UnwrapOr(""), "1")
}

func TestMinMaxNorm_AllEqual(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"5"}, {"5"}, {"5"}})
	result := MinMaxNorm(tb, "v")
	// all equal → return unchanged
	assertEqual(t, result.Rows[0].Get("v").UnwrapOr(""), "5")
}

func TestMinMaxNorm_NonNumericUnchanged(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"1"}, {"abc"}, {"3"}})
	result := MinMaxNorm(tb, "v")
	// "abc" is left as-is
	assertEqual(t, result.Rows[1].Get("v").UnwrapOr(""), "abc")
}
