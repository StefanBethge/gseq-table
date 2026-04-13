package schema

import (
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/slice"
)

// --- Cols() ---

func TestCols_ReturnsAllMappings(t *testing.T) {
	s := Infer(makeTable())
	cols := s.Cols()
	assertEqual(t, cols["name"], TypeString)
	assertEqual(t, cols["age"], TypeInt)
	assertEqual(t, cols["price"], TypeFloat)
	assertEqual(t, cols["active"], TypeBool)
	assertEqual(t, cols["created_at"], TypeDate)
}

func TestCols_Empty(t *testing.T) {
	s := Schema{}
	cols := s.Cols()
	assertEqual(t, len(cols), 0)
}

func TestCols_IsACopy(t *testing.T) {
	s := Infer(makeTable())
	cols := s.Cols()
	cols["name"] = TypeInt // mutate copy
	// original should be unchanged
	assertEqual(t, s.Col("name"), TypeString)
}

// --- inferCol (internal) ---

func TestInferCol_Int(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"1", "42", "100"}), TypeInt)
}

func TestInferCol_Float(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"1", "2.5"}), TypeFloat)
}

func TestInferCol_Bool(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"yes", "no"}), TypeBool)
}

func TestInferCol_Date(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"2024-01-15"}), TypeDate)
}

func TestInferCol_String(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"hello", "world"}), TypeString)
}

func TestInferCol_AllEmpty(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"", ""}), TypeString)
}

func TestInferCol_SkipsEmpty(t *testing.T) {
	assertEqual(t, inferCol(slice.Slice[string]{"", "42", ""}), TypeInt)
}

// --- normalize (internal) ---

func TestNormalize_TypeString_ReturnsAsIs(t *testing.T) {
	got, err := normalize("hello world", TypeString)
	assertEqual(t, err, nil)
	assertEqual(t, got, "hello world")
}

func TestNormalize_TypeFloat_InvalidValue(t *testing.T) {
	_, err := normalize("not-a-float", TypeFloat)
	assertEqual(t, err != nil, true)
}

func TestNormalize_TypeBool_InvalidValue(t *testing.T) {
	_, err := normalize("maybe", TypeBool)
	assertEqual(t, err != nil, true)
}

func TestNormalize_TypeDate_InvalidValue(t *testing.T) {
	_, err := normalize("not-a-date", TypeDate)
	assertEqual(t, err != nil, true)
}

// --- timeVal (internal) ---

func TestTimeVal_MissingColumn(t *testing.T) {
	row := table.NewRow([]string{"other"}, []string{"2024-01-01"})
	got := timeVal(row, "missing")
	assertEqual(t, got.IsZero(), true)
}

// --- colVals (internal, tested via exported functions) ---

func TestColVals_MissingColumn(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"1"}, {"2"}})
	// SumCol on nonexistent column hits the idx < 0 branch in colVals
	assertEqual(t, SumCol(tb, "nonexistent"), 0.0)
}

// --- computeColStats with missing column ---

func TestComputeColStats_MissingColumn(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"1"}, {"2"}})
	s := computeColStats(tb, "nonexistent")
	assertEqual(t, s.count, 0)
}

// --- Float accessor edge cases ---

func TestFloat_MissingColumn(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"3.14"})
	assertEqual(t, Float(row, "missing").IsNone(), true)
}

func TestFloat_NonParseable(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"not-a-float"})
	assertEqual(t, Float(row, "v").IsNone(), true)
}

func TestFloat_EmptyString(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{""})
	// "" cannot be parsed as float
	assertEqual(t, Float(row, "v").IsNone(), true)
}

// --- Bool accessor edge cases ---

func TestBool_MissingColumn(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"true"})
	assertEqual(t, Bool(row, "missing").IsNone(), true)
}

func TestBool_Unrecognised(t *testing.T) {
	// "on", "off", "y", "n" are not accepted
	for _, s := range []string{"on", "off", "y", "n", "maybe", ""} {
		row := table.NewRow([]string{"v"}, []string{s})
		assertEqual(t, Bool(row, "v").IsNone(), true)
	}
}

// --- Int accessor edge cases ---

func TestInt_NegativeNumber(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"-42"})
	v, ok := Int(row, "v").Get()
	assertEqual(t, ok, true)
	assertEqual(t, v, int64(-42))
}

func TestInt_MissingColumn(t *testing.T) {
	row := table.NewRow([]string{"v"}, []string{"10"})
	assertEqual(t, Int(row, "missing").IsNone(), true)
}

// --- floatMedian with single element ---

func TestMedianCol_Single(t *testing.T) {
	tb := table.New([]string{"v"}, [][]string{{"42"}})
	assertEqual(t, MedianCol(tb, "v"), 42.0)
}

// --- Sub edge cases ---

func TestSub_Empty(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"10"}})
	result := tb.AddColFloat("r", Sub())
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

func TestSub_ZeroResult(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"5", "5"}})
	result := tb.AddColFloat("r", Sub("a", "b"))
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

// --- Mul edge cases ---

func TestMul_Empty(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"10"}})
	result := tb.AddColFloat("r", Mul())
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

func TestMul_ZeroResult(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"5", "0"}})
	result := tb.AddColFloat("r", Mul("a", "b"))
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

// --- Max2 / Min2 edge cases ---

func TestMax2_Empty(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"10"}})
	result := tb.AddColFloat("r", Max2())
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

func TestMax2_SingleCol(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"7"}})
	result := tb.AddColFloat("r", Max2("a"))
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "7")
}

func TestMin2_Empty(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"10"}})
	result := tb.AddColFloat("r", Min2())
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "0")
}

func TestMin2_SingleCol(t *testing.T) {
	tb := table.New([]string{"a"}, [][]string{{"3"}})
	result := tb.AddColFloat("r", Min2("a"))
	assertEqual(t, result.Rows[0].Get("r").UnwrapOr(""), "3")
}

// --- DateAge edge cases ---

func TestDateAge_ZeroRef_UsesNow(t *testing.T) {
	// With zero ref, DateAge uses time.Now(). Just verify we get a non-empty result.
	tb := table.New([]string{"birth"}, [][]string{{"1990-01-01"}})
	result := tb.AddCol("age", DateAge("birth", time.Time{}))
	age := result.Rows[0].Get("age").UnwrapOr("")
	if age == "" {
		t.Errorf("expected non-empty age, got empty string")
	}
}

func TestDateAge_FutureBirth(t *testing.T) {
	// Birth is after ref → negative years
	ref, _ := time.Parse("2006-01-02", "2020-01-01")
	tb := table.New([]string{"birth"}, [][]string{{"2025-06-15"}})
	result := tb.AddCol("age", DateAge("birth", ref))
	// 2020 - 2025 = -5 (or -6 depending on YearDay)
	age := result.Rows[0].Get("age").UnwrapOr("")
	if age == "" {
		t.Errorf("expected negative age string, got empty")
	}
}

// --- DateEndOfMonth edge cases ---

func TestDateEndOfMonth_NonLeapFeb(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2023-02-10"}}) // 2023 is not a leap year
	result := tb.AddCol("end_m", DateEndOfMonth("d"))
	assertEqual(t, result.Rows[0].Get("end_m").UnwrapOr(""), "2023-02-28")
}

func TestDateEndOfMonth_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("end_m", DateEndOfMonth("d"))
	assertEqual(t, result.Rows[0].Get("end_m").UnwrapOr("x"), "")
}

// --- DateStartOfMonth edge cases ---

func TestDateStartOfMonth_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("start_m", DateStartOfMonth("d"))
	assertEqual(t, result.Rows[0].Get("start_m").UnwrapOr("x"), "")
}

// --- DateWeek edge cases ---

func TestDateWeek_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("week", DateWeek("d"))
	assertEqual(t, result.Rows[0].Get("week").UnwrapOr("x"), "")
}

// --- DateMonth edge cases ---

func TestDateMonth_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("month", DateMonth("d"))
	assertEqual(t, result.Rows[0].Get("month").UnwrapOr("x"), "")
}

// --- DateDay edge cases ---

func TestDateDay_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("day", DateDay("d"))
	assertEqual(t, result.Rows[0].Get("day").UnwrapOr("x"), "")
}

// --- DateWeekday edge cases ---

func TestDateWeekday_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("weekday", DateWeekday("d"))
	assertEqual(t, result.Rows[0].Get("weekday").UnwrapOr("x"), "")
}

// --- DateQuarter additional cases ---

func TestDateQuarter_Q3(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2024-08-15"}}) // August → Q3
	result := tb.AddCol("q", DateQuarter("d"))
	assertEqual(t, result.Rows[0].Get("q").UnwrapOr(""), "3")
}

func TestDateQuarter_Q4(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2024-11-01"}}) // November → Q4
	result := tb.AddCol("q", DateQuarter("d"))
	assertEqual(t, result.Rows[0].Get("q").UnwrapOr(""), "4")
}

func TestDateQuarter_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("q", DateQuarter("d"))
	assertEqual(t, result.Rows[0].Get("q").UnwrapOr("x"), "")
}

// --- DateDiffMonths edge cases ---

func TestDateDiffMonths_InvalidDate(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"not-a-date", "2024-01-01"}})
	result := tb.AddColFloat("months", DateDiffMonths("a", "b"))
	assertEqual(t, result.Rows[0].Get("months").UnwrapOr(""), "0")
}

// --- DateDiffYears edge cases ---

func TestDateDiffYears_InvalidDate(t *testing.T) {
	tb := table.New([]string{"a", "b"}, [][]string{{"not-a-date", "2024-01-01"}})
	result := tb.AddColFloat("years", DateDiffYears("a", "b"))
	assertEqual(t, result.Rows[0].Get("years").UnwrapOr(""), "0")
}

// --- DateAddMonths edge cases ---

func TestDateAddMonths_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("future", DateAddMonths("d", 3))
	assertEqual(t, result.Rows[0].Get("future").UnwrapOr("x"), "")
}

// --- DateBetween edge cases ---

func TestDateBetween_InvalidDateCol(t *testing.T) {
	// event date is invalid → predicate returns false
	tb := table.New(
		[]string{"event", "from", "to"},
		[][]string{{"not-a-date", "2024-01-01", "2024-06-30"}},
	)
	result := tb.Where(DateBetween("event", "from", "to"))
	assertEqual(t, len(result.Rows), 0)
}

func TestDateBetween_InvalidRangeCol(t *testing.T) {
	// start/end cols are invalid → predicate returns false
	tb := table.New(
		[]string{"event", "from", "to"},
		[][]string{{"2024-03-15", "not-a-date", "2024-06-30"}},
	)
	result := tb.Where(DateBetween("event", "from", "to"))
	assertEqual(t, len(result.Rows), 0)
}

// --- DateTrunc edge cases ---

func TestDateTrunc_InvalidDate(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"not-a-date"}})
	result := tb.AddCol("trunc", DateTrunc("d", "month"))
	assertEqual(t, result.Rows[0].Get("trunc").UnwrapOr("x"), "")
}

func TestDateTrunc_UnknownPrecision(t *testing.T) {
	// unrecognised precision falls through to "day" (default case)
	tb := table.New([]string{"d"}, [][]string{{"2024-03-15"}})
	result := tb.AddCol("trunc", DateTrunc("d", "week"))
	assertEqual(t, result.Rows[0].Get("trunc").UnwrapOr(""), "2024-03-15")
}
