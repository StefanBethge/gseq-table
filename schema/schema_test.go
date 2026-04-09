package schema

import (
	"testing"
	"time"

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

// --- Numeric column operations ---

func arithTable() table.Table {
	return table.New(
		[]string{"a", "b"},
		[][]string{
			{"10", "3"},
			{"20", "5"},
			{"", "7"},   // a missing
			{"15", "0"}, // b zero (div by zero)
		},
	)
}

func TestAdd(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("sum", Add("a", "b"))
	assertEqual(t, result.Rows[0].Get("sum").UnwrapOr(""), "13")
	assertEqual(t, result.Rows[1].Get("sum").UnwrapOr(""), "25")
	assertEqual(t, result.Rows[2].Get("sum").UnwrapOr(""), "7") // 0 + 7
}

func TestSub(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("diff", Sub("a", "b"))
	assertEqual(t, result.Rows[0].Get("diff").UnwrapOr(""), "7")
	assertEqual(t, result.Rows[1].Get("diff").UnwrapOr(""), "15")
}

func TestMul(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("product", Mul("a", "b"))
	assertEqual(t, result.Rows[0].Get("product").UnwrapOr(""), "30")
	assertEqual(t, result.Rows[1].Get("product").UnwrapOr(""), "100")
}

func TestDiv(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("ratio", Div("a", "b"))
	// 10/3 = 3.3333...
	v := result.Rows[0].Get("ratio").UnwrapOr("")
	f, _ := Float(result.Rows[0], "ratio").Get()
	if f < 3.33 || f > 3.34 {
		t.Errorf("expected ~3.33, got %s", v)
	}
	// 20/5 = 4
	assertEqual(t, result.Rows[1].Get("ratio").UnwrapOr(""), "4")
}

func TestDiv_ByZero(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("ratio", Div("a", "b"))
	// b=0 → result 0
	assertEqual(t, result.Rows[3].Get("ratio").UnwrapOr(""), "0")
}

func TestAdd_ThreeCols(t *testing.T) {
	tb := table.New([]string{"a", "b", "c"}, [][]string{{"10", "20", "30"}, {"1", "2", "3"}})
	result := tb.AddColFloat("sum", Add("a", "b", "c"))
	assertEqual(t, result.Rows[0].Get("sum").UnwrapOr(""), "60")
	assertEqual(t, result.Rows[1].Get("sum").UnwrapOr(""), "6")
}

func TestAdd_SingleCol(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("same", Add("a"))
	assertEqual(t, result.Rows[0].Get("same").UnwrapOr(""), "10")
}

func TestAdd_Empty(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("zero", Add())
	assertEqual(t, result.Rows[0].Get("zero").UnwrapOr(""), "0")
}

func TestAdd_MissingCol(t *testing.T) {
	tb := arithTable()
	result := tb.AddColFloat("sum", Add("a", "nonexistent"))
	// nonexistent → 0, so sum = a + 0
	assertEqual(t, result.Rows[0].Get("sum").UnwrapOr(""), "10")
}

func TestSub_ThreeCols(t *testing.T) {
	tb := table.New([]string{"a", "b", "c"}, [][]string{{"100", "30", "20"}})
	result := tb.AddColFloat("net", Sub("a", "b", "c"))
	assertEqual(t, result.Rows[0].Get("net").UnwrapOr(""), "50") // 100 - 30 - 20
}

func TestMul_ThreeCols(t *testing.T) {
	tb := table.New([]string{"l", "w", "h"}, [][]string{{"2", "3", "4"}})
	result := tb.AddColFloat("vol", Mul("l", "w", "h"))
	assertEqual(t, result.Rows[0].Get("vol").UnwrapOr(""), "24") // 2*3*4
}

func TestMin2_ThreeCols(t *testing.T) {
	tb := table.New([]string{"a", "b", "c"}, [][]string{{"10", "3", "7"}})
	result := tb.AddColFloat("min", Min2("a", "b", "c"))
	assertEqual(t, result.Rows[0].Get("min").UnwrapOr(""), "3")
}

func TestMax2_ThreeCols(t *testing.T) {
	tb := table.New([]string{"a", "b", "c"}, [][]string{{"10", "3", "7"}})
	result := tb.AddColFloat("max", Max2("a", "b", "c"))
	assertEqual(t, result.Rows[0].Get("max").UnwrapOr(""), "10")
}

// --- Date column operations ---

func dateTable() table.Table {
	return table.New(
		[]string{"start", "end", "invalid"},
		[][]string{
			{"2024-01-15", "2024-03-15", "not-a-date"},
			{"2024-06-01", "2024-06-30", ""},
			{"15.03.2024", "2024-12-31", "abc"},
		},
	)
}

func TestDateDiffDays(t *testing.T) {
	tb := dateTable()
	result := tb.AddColFloat("days", DateDiffDays("end", "start"))
	assertEqual(t, result.Rows[0].Get("days").UnwrapOr(""), "60") // Jan 15 → Mar 15
	assertEqual(t, result.Rows[1].Get("days").UnwrapOr(""), "29") // Jun 1 → Jun 30
}

func TestDateDiffDays_InvalidDate(t *testing.T) {
	tb := dateTable()
	result := tb.AddColFloat("days", DateDiffDays("invalid", "start"))
	assertEqual(t, result.Rows[0].Get("days").UnwrapOr(""), "0") // invalid → 0
}

func TestDateAddDays(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("future", DateAddDays("start", 30))
	assertEqual(t, result.Rows[0].Get("future").UnwrapOr(""), "2024-02-14")
	assertEqual(t, result.Rows[1].Get("future").UnwrapOr(""), "2024-07-01")
}

func TestDateAddDays_Negative(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("past", DateAddDays("start", -10))
	assertEqual(t, result.Rows[0].Get("past").UnwrapOr(""), "2024-01-05")
}

func TestDateAddDays_InvalidDate(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("future", DateAddDays("invalid", 30))
	assertEqual(t, result.Rows[0].Get("future").UnwrapOr("x"), "") // invalid → empty
}

func TestDateYear(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("year", DateYear("start"))
	assertEqual(t, result.Rows[0].Get("year").UnwrapOr(""), "2024")
}

func TestDateMonth(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("month", DateMonth("start"))
	assertEqual(t, result.Rows[0].Get("month").UnwrapOr(""), "1")  // January
	assertEqual(t, result.Rows[1].Get("month").UnwrapOr(""), "6")  // June
}

func TestDateDay(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("day", DateDay("start"))
	assertEqual(t, result.Rows[0].Get("day").UnwrapOr(""), "15")
	assertEqual(t, result.Rows[1].Get("day").UnwrapOr(""), "1")
}

func TestDateFormat(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("display", DateFormat("start", "02.01.2006"))
	assertEqual(t, result.Rows[0].Get("display").UnwrapOr(""), "15.01.2024")
	assertEqual(t, result.Rows[1].Get("display").UnwrapOr(""), "01.06.2024")
}

func TestDateFormat_InvalidDate(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("display", DateFormat("invalid", "02.01.2006"))
	assertEqual(t, result.Rows[0].Get("display").UnwrapOr("x"), "")
}

func TestDateWeekday(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("weekday", DateWeekday("start"))
	assertEqual(t, result.Rows[0].Get("weekday").UnwrapOr(""), "Monday") // 2024-01-15 was Monday
}

func TestDateQuarter(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("q", DateQuarter("start"))
	assertEqual(t, result.Rows[0].Get("q").UnwrapOr(""), "1") // January → Q1
	assertEqual(t, result.Rows[1].Get("q").UnwrapOr(""), "2") // June → Q2
}

func TestDateYear_InvalidDate(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("year", DateYear("invalid"))
	assertEqual(t, result.Rows[0].Get("year").UnwrapOr("x"), "")
}

func TestDateGermanFormat(t *testing.T) {
	// Row 2 has "15.03.2024" as start — German date format
	tb := dateTable()
	result := tb.AddCol("year", DateYear("start"))
	assertEqual(t, result.Rows[2].Get("year").UnwrapOr(""), "2024")

	result2 := tb.AddCol("month", DateMonth("start"))
	assertEqual(t, result2.Rows[2].Get("month").UnwrapOr(""), "3") // March
}

// --- Additional numeric operations ---

func TestAbs(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"-5"}, {"3"}, {"0"}})
	result := tb.AddColFloat("abs", Abs("v"))
	assertEqual(t, result.Rows[0].Get("abs").UnwrapOr(""), "5")
	assertEqual(t, result.Rows[1].Get("abs").UnwrapOr(""), "3")
	assertEqual(t, result.Rows[2].Get("abs").UnwrapOr(""), "0")
}

func TestNeg(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"10"}, {"-3"}})
	result := tb.AddColFloat("neg", Neg("v"))
	assertEqual(t, result.Rows[0].Get("neg").UnwrapOr(""), "-10")
	assertEqual(t, result.Rows[1].Get("neg").UnwrapOr(""), "3")
}

func TestAddConst(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"100"}, {"200"}})
	result := tb.AddColFloat("plus", AddConst("v", 19))
	assertEqual(t, result.Rows[0].Get("plus").UnwrapOr(""), "119")
	assertEqual(t, result.Rows[1].Get("plus").UnwrapOr(""), "219")
}

func TestMulConst(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"100"}, {"50"}})
	result := tb.AddColFloat("scaled", MulConst("v", 0.5))
	assertEqual(t, result.Rows[0].Get("scaled").UnwrapOr(""), "50")
	assertEqual(t, result.Rows[1].Get("scaled").UnwrapOr(""), "25")
}

func TestMod(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"10", "3"}, {"7", "0"}})
	result := tb.AddColFloat("mod", Mod("a", "b"))
	assertEqual(t, result.Rows[0].Get("mod").UnwrapOr(""), "1")
	assertEqual(t, result.Rows[1].Get("mod").UnwrapOr(""), "0") // b=0
}

func TestMin2(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"10", "3"}, {"5", "8"}})
	result := tb.AddColFloat("min", Min2("a", "b"))
	assertEqual(t, result.Rows[0].Get("min").UnwrapOr(""), "3")
	assertEqual(t, result.Rows[1].Get("min").UnwrapOr(""), "5")
}

func TestMax2(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"10", "3"}, {"5", "8"}})
	result := tb.AddColFloat("max", Max2("a", "b"))
	assertEqual(t, result.Rows[0].Get("max").UnwrapOr(""), "10")
	assertEqual(t, result.Rows[1].Get("max").UnwrapOr(""), "8")
}

func TestRound(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"3.14159"}, {"2.71828"}})
	result := tb.AddColFloat("r", Round("v", 2))
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "3.14")
	assertEqual(t, result.Rows[1].Get("r").UnwrapOr(""), "2.72")
}

func TestClamp(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"-5"}, {"50"}, {"150"}})
	result := tb.AddColFloat("c", Clamp("v", 0, 100))
	assertEqual(t, result.Rows[0].Get("c").UnwrapOr(""), "0")
	assertEqual(t, result.Rows[1].Get("c").UnwrapOr(""), "50")
	assertEqual(t, result.Rows[2].Get("c").UnwrapOr(""), "100")
}

func TestPct(t *testing.T) {
	tb := table.New([]string{"part", "total"}, [][]string{{"25", "100"}, {"1", "3"}})
	result := tb.AddColFloat("pct", Pct("part", "total"))
	assertEqual(t, result.Rows[0].Get("pct").UnwrapOr(""), "25")
	f, _ := Float(result.Rows[1], "pct").Get()
	if f < 33.3 || f > 33.4 {
		t.Errorf("expected ~33.33, got %f", f)
	}
}

func TestPct_ZeroDenom(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"10", "0"}})
	result := tb.AddColFloat("pct", Pct("a", "b"))
	assertEqual(t, result.Rows[0].Get("pct").UnwrapOr(""), "0")
}

// --- Additional date operations ---

func TestDateDiffMonths(t *testing.T) {
	tb := dateTable()
	result := tb.AddColFloat("months", DateDiffMonths("end", "start"))
	f, _ := Float(result.Rows[0], "months").Get()
	// Jan 15 → Mar 15 = ~2 months
	if f < 1.9 || f > 2.1 {
		t.Errorf("expected ~2 months, got %f", f)
	}
}

func TestDateDiffYears(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{
		{"2024-06-15", "2020-06-15"},
	})
	result := tb.AddColFloat("years", DateDiffYears("a", "b"))
	f, _ := Float(result.Rows[0], "years").Get()
	if f < 3.9 || f > 4.1 {
		t.Errorf("expected ~4 years, got %f", f)
	}
}

func TestDateAddMonths(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("future", DateAddMonths("start", 3))
	assertEqual(t, result.Rows[0].Get("future").UnwrapOr(""), "2024-04-15")
	assertEqual(t, result.Rows[1].Get("future").UnwrapOr(""), "2024-09-01")
}

func TestDateWeek(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("week", DateWeek("start"))
	assertEqual(t, result.Rows[0].Get("week").UnwrapOr(""), "3") // 2024-01-15 = week 3
}

func TestDateStartOfMonth(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("start_m", DateStartOfMonth("start"))
	assertEqual(t, result.Rows[0].Get("start_m").UnwrapOr(""), "2024-01-01")
	assertEqual(t, result.Rows[1].Get("start_m").UnwrapOr(""), "2024-06-01")
}

func TestDateEndOfMonth(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("end_m", DateEndOfMonth("start"))
	assertEqual(t, result.Rows[0].Get("end_m").UnwrapOr(""), "2024-01-31")
	assertEqual(t, result.Rows[1].Get("end_m").UnwrapOr(""), "2024-06-30")
}

func TestDateEndOfMonth_February(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2024-02-15"}}) // leap year
	result := tb.AddCol("end_m", DateEndOfMonth("d"))
	assertEqual(t, result.Rows[0].Get("end_m").UnwrapOr(""), "2024-02-29")
}

func TestDateAge(t *testing.T) {
	// Use a fixed reference date
	tb := table.New([]string{"birthday"}, [][]string{{"1990-06-15"}, {"2000-01-01"}})
	ref, _ := time.Parse("2006-01-02", "2024-06-15")
	result := tb.AddCol("age", DateAge("birthday", ref))
	assertEqual(t, result.Rows[0].Get("age").UnwrapOr(""), "34")
	assertEqual(t, result.Rows[1].Get("age").UnwrapOr(""), "24")
}

func TestDateAge_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	ref, _ := time.Parse("2006-01-02", "2024-01-01")
	result := tb.AddCol("age", DateAge("d", ref))
	assertEqual(t, result.Rows[0].Get("age").UnwrapOr("x"), "")
}

func TestDateBetween(t *testing.T) {
	tb := table.New([]string{"event", "from", "to"}, [][]string{
		{"2024-03-15", "2024-01-01", "2024-06-30"}, // in range
		{"2024-09-01", "2024-01-01", "2024-06-30"}, // out of range
		{"2024-01-01", "2024-01-01", "2024-06-30"}, // boundary
	})
	result := tb.Where(DateBetween("event", "from", "to"))
	assertEqual(t, len(result.Rows), 2) // first and third
}

func TestDateTrunc_Month(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("trunc", DateTrunc("start", "month"))
	assertEqual(t, result.Rows[0].Get("trunc").UnwrapOr(""), "2024-01-01")
	assertEqual(t, result.Rows[1].Get("trunc").UnwrapOr(""), "2024-06-01")
}

func TestDateTrunc_Year(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("trunc", DateTrunc("start", "year"))
	assertEqual(t, result.Rows[0].Get("trunc").UnwrapOr(""), "2024-01-01")
}

func TestDateTrunc_Day(t *testing.T) {
	tb := dateTable()
	result := tb.AddCol("trunc", DateTrunc("start", "day"))
	assertEqual(t, result.Rows[0].Get("trunc").UnwrapOr(""), "2024-01-15")
}
