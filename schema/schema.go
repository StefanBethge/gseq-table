// Package schema provides type inference and type conversion for table.Table.
//
// All cell values in a Table are strings. This package adds a Schema layer
// that maps column names to detected or declared types, normalises cell values
// into canonical string form, and provides typed accessors for reading values
// out of a Row at runtime.
//
// # Inferring a schema
//
//	s := schema.Infer(t)
//	s.Col("age")    // schema.TypeInt
//	s.Col("price")  // schema.TypeFloat
//	s.Col("name")   // schema.TypeString
//
// # Overriding a column type
//
//	s = s.Cast("created_at", schema.TypeDate)
//
// # Applying the schema
//
//	// Lenient: empty cells are left unchanged, invalid values return an error.
//	res := s.Apply(t)
//
//	// Strict: empty cells also return an error.
//	res := s.ApplyStrict(t)
//
// # Typed row access
//
//	schema.Int(row, "age")          // option.Option[int64]
//	schema.Float(row, "price")      // option.Option[float64]
//	schema.Bool(row, "active")      // option.Option[bool]
//	schema.Time(row, "date", "")    // option.Option[time.Time]
package schema

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq/result"
	"github.com/stefanbethge/gseq/slice"
)

// ColType is the declared or inferred type of a column.
type ColType string

const (
	TypeString ColType = "string"
	TypeInt    ColType = "int"
	TypeFloat  ColType = "float"
	TypeBool   ColType = "bool"
	TypeDate   ColType = "date"
)

// dateLayouts are tried in order during inference and Time parsing.
var dateLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02",
	"02.01.2006",
	"01/02/2006",
	"02 Jan 2006",
	"Jan 02, 2006",
}

// Schema maps column names to their ColType. It is immutable; Cast returns a
// new Schema.
type Schema struct {
	types map[string]ColType
}

// Col returns the ColType for name, or TypeString if the column is not in the
// schema.
func (s Schema) Col(name string) ColType {
	if t, ok := s.types[name]; ok {
		return t
	}
	return TypeString
}

// Cast returns a new Schema with col set to typ. All other mappings are
// preserved.
//
//	s = s.Cast("price", schema.TypeFloat).Cast("created_at", schema.TypeDate)
func (s Schema) Cast(col string, typ ColType) Schema {
	next := Schema{types: make(map[string]ColType, len(s.types)+1)}
	for k, v := range s.types {
		next.types[k] = v
	}
	next.types[col] = typ
	return next
}

// Cols returns a copy of all column→type pairs in the schema.
func (s Schema) Cols() map[string]ColType {
	out := make(map[string]ColType, len(s.types))
	for k, v := range s.types {
		out[k] = v
	}
	return out
}

// Infer scans every column in t and returns a Schema with the most specific
// type that fits all non-empty values. Type priority: Int > Float > Bool >
// Date > String.
func Infer(t table.Table) Schema {
	types := make(map[string]ColType, len(t.Headers))
	for _, col := range t.Headers {
		types[col] = inferCol(t.Col(col))
	}
	return Schema{types: types}
}

// Apply normalises every non-empty cell in t according to the schema and
// returns a new Table. Empty cells are left unchanged.
// Returns Err if a non-empty value cannot be parsed as its declared type.
func (s Schema) Apply(t table.Table) result.Result[table.Table, error] {
	return s.apply(t, false)
}

// ApplyStrict behaves like Apply but also returns Err when a cell that is
// declared as a non-string type contains an empty string.
func (s Schema) ApplyStrict(t table.Table) result.Result[table.Table, error] {
	return s.apply(t, true)
}

// apply is the shared implementation for Apply and ApplyStrict.
func (s Schema) apply(t table.Table, strict bool) result.Result[table.Table, error] {
	out := t
	for _, col := range t.Headers {
		typ := s.Col(col)
		if typ == TypeString {
			continue
		}
		idx := colIndex(out, col)
		if idx < 0 {
			continue
		}
		rows := make(slice.Slice[table.Row], len(out.Rows))
		for ri, row := range out.Rows {
			vals := row.Values()
			raw := ""
			if idx < len(vals) {
				raw = vals[idx]
			}
			if raw == "" {
				if strict {
					return result.Err[table.Table, error](
						fmt.Errorf("column %q row %d: empty value not allowed in strict mode", col, ri),
					)
				}
				rows[ri] = row
				continue
			}
			normalized, err := normalize(raw, typ)
			if err != nil {
				return result.Err[table.Table, error](
					fmt.Errorf("column %q row %d: cannot parse %q as %s: %w", col, ri, raw, typ, err),
				)
			}
			newVals := make(slice.Slice[string], len(vals))
			copy(newVals, vals)
			newVals[idx] = normalized
			rows[ri] = table.NewRow(row.Headers(), newVals)
		}
		out = table.NewFromRows(out.Headers, rows)
	}
	return result.Ok[table.Table, error](out)
}

// --- Typed row accessors ---

// Int parses the value of col in r as int64. Returns None if the column is
// missing or the value cannot be parsed.
func Int(r table.Row, col string) option.Option[int64] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[int64]()
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return option.None[int64]()
	}
	return option.Some(n)
}

// Float parses the value of col in r as float64. Returns None if the column
// is missing or the value cannot be parsed.
func Float(r table.Row, col string) option.Option[float64] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[float64]()
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return option.None[float64]()
	}
	return option.Some(f)
}

// Bool parses the value of col in r as bool.
// Accepted values (case-insensitive): true/false, 1/0, yes/no.
// Returns None if the column is missing or the value is not recognised.
func Bool(r table.Row, col string) option.Option[bool] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[bool]()
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes":
		return option.Some(true)
	case "false", "0", "no":
		return option.Some(false)
	}
	return option.None[bool]()
}

// Time parses the value of col in r as time.Time using layout.
// If layout is empty, all common formats are tried automatically.
// Returns None if the column is missing or no layout matches.
func Time(r table.Row, col string, layout string) option.Option[time.Time] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[time.Time]()
	}
	raw := strings.TrimSpace(v)
	if layout != "" {
		t, err := time.Parse(layout, raw)
		if err != nil {
			return option.None[time.Time]()
		}
		return option.Some(t)
	}
	t := tryParseDate(raw)
	if t.IsZero() {
		return option.None[time.Time]()
	}
	return option.Some(t)
}

// --- internal helpers ---

func inferCol(vals slice.Slice[string]) ColType {
	current := TypeInt
	for _, v := range vals {
		if v == "" {
			continue
		}
		current = narrow(current, v)
		if current == TypeString {
			return TypeString
		}
	}
	return current
}

// narrow returns the most specific type that still accepts v, starting from
// current and falling back toward TypeString.
func narrow(current ColType, v string) ColType {
	switch current {
	case TypeInt:
		if _, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return TypeInt
		}
		return narrow(TypeFloat, v)
	case TypeFloat:
		clean := strings.ReplaceAll(strings.TrimSpace(v), ",", "")
		if _, err := strconv.ParseFloat(clean, 64); err == nil {
			return TypeFloat
		}
		return narrow(TypeBool, v)
	case TypeBool:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "false", "1", "0", "yes", "no":
			return TypeBool
		}
		return narrow(TypeDate, v)
	case TypeDate:
		if !tryParseDate(v).IsZero() {
			return TypeDate
		}
		return TypeString
	}
	return TypeString
}

func tryParseDate(v string) time.Time {
	v = strings.TrimSpace(v)
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t
		}
	}
	return time.Time{}
}

// normalize parses v as typ and returns it in canonical string form.
func normalize(v string, typ ColType) (string, error) {
	v = strings.TrimSpace(v)
	switch typ {
	case TypeInt:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(n, 10), nil
	case TypeFloat:
		f, err := strconv.ParseFloat(strings.ReplaceAll(v, ",", ""), 64)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(f, 'f', -1, 64), nil
	case TypeBool:
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			return "true", nil
		case "false", "0", "no":
			return "false", nil
		}
		return "", fmt.Errorf("unrecognised bool value %q", v)
	case TypeDate:
		t := tryParseDate(v)
		if t.IsZero() {
			return "", fmt.Errorf("unrecognised date value %q", v)
		}
		return t.Format("2006-01-02"), nil
	}
	return v, nil
}

func colIndex(t table.Table, col string) int {
	return t.ColIndex(col)
}

// --- Numeric column aggregators ---
//
// These functions operate on the string values of a column, parsing each cell
// as float64. Unparseable values (including empty strings) are silently
// skipped unless otherwise noted.
//
// All functions iterate rows directly using the O(1) column index rather than
// extracting a full column slice.

// colVals is an internal helper that iterates the raw string values of col
// without allocating an intermediate slice.
func colVals(t table.Table, col string, fn func(string)) {
	idx := t.ColIndex(col)
	if idx < 0 {
		return
	}
	for _, row := range t.Rows {
		if idx < len(row.Values()) {
			fn(row.Values()[idx])
		}
	}
}

// SumCol returns the sum of all parseable float values in col.
//
//	schema.SumCol(t, "revenue")
func SumCol(t table.Table, col string) float64 {
	var sum float64
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			sum += f
		}
	})
	return sum
}

// MeanCol returns the arithmetic mean of all parseable float values in col.
// Returns 0 if no parseable values are found.
//
//	schema.MeanCol(t, "age")
func MeanCol(t table.Table, col string) float64 {
	var sum float64
	var n int
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			sum += f
			n++
		}
	})
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

// MinCol returns the minimum parseable float value in col.
// Returns 0 if no parseable values are found.
//
//	schema.MinCol(t, "price")
func MinCol(t table.Table, col string) float64 {
	var min float64
	first := true
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			if first || f < min {
				min = f
				first = false
			}
		}
	})
	return min
}

// MaxCol returns the maximum parseable float value in col.
// Returns 0 if no parseable values are found.
//
//	schema.MaxCol(t, "price")
func MaxCol(t table.Table, col string) float64 {
	var max float64
	first := true
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			if first || f > max {
				max = f
				first = false
			}
		}
	})
	return max
}

// CountCol counts non-empty values in col.
//
//	schema.CountCol(t, "email")
func CountCol(t table.Table, col string) int {
	var n int
	colVals(t, col, func(v string) {
		if v != "" {
			n++
		}
	})
	return n
}

// CountWhere counts rows where col == val (case-sensitive).
//
//	schema.CountWhere(t, "status", "active")
func CountWhere(t table.Table, col, val string) int {
	var n int
	colVals(t, col, func(v string) {
		if v == val {
			n++
		}
	})
	return n
}

// StdDevCol returns the population standard deviation of the parseable float
// values in col. Returns 0 if fewer than two values are found.
//
//	schema.StdDevCol(t, "revenue")
func StdDevCol(t table.Table, col string) float64 {
	mean := MeanCol(t, col)
	var sumSq float64
	var n int
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			d := f - mean
			sumSq += d * d
			n++
		}
	})
	if n < 2 {
		return 0
	}
	return math.Sqrt(sumSq / float64(n))
}

// MedianCol returns the median of the parseable float values in col.
// Returns 0 if no parseable values are found.
//
//	schema.MedianCol(t, "age")
func MedianCol(t table.Table, col string) float64 {
	var vals []float64
	colVals(t, col, func(v string) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			vals = append(vals, f)
		}
	})
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	n := len(vals)
	if n%2 == 1 {
		return vals[n/2]
	}
	return (vals[n/2-1] + vals[n/2]) / 2
}

// colStats computes count, sum, sumSq, min, max and all numeric values in a
// single pass over the rows. Used by Describe to avoid repeated column scans.
type colStats struct {
	count          int
	sum, sumSq     float64
	min, max       float64
	numericVals    []float64
}

func computeColStats(t table.Table, col string) colStats {
	idx := t.ColIndex(col)
	var s colStats
	first := true
	for _, row := range t.Rows {
		if idx < 0 || idx >= len(row.Values()) {
			continue
		}
		v := row.Values()[idx]
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			continue
		}
		s.count++
		s.sum += f
		s.numericVals = append(s.numericVals, f)
		if first || f < s.min {
			s.min = f
		}
		if first || f > s.max {
			s.max = f
		}
		first = false
	}
	if s.count > 0 {
		mean := s.sum / float64(s.count)
		for _, f := range s.numericVals {
			d := f - mean
			s.sumSq += d * d
		}
	}
	return s
}

// Describe returns a summary statistics table with one row per column of t.
// Columns: "column", "count", "min", "max", "mean", "std", "median".
// count reflects only parseable numeric values; non-numeric columns show
// count=0 and empty strings for all stat fields.
//
//	schema.Describe(t)
//	// column   count  min   max    mean   std    median
//	// revenue  10     50    500    227.3  134.1  215
//	// name     0
func Describe(t table.Table) table.Table {
	headers := []string{"column", "count", "min", "max", "mean", "std", "median"}
	records := make([][]string, len(t.Headers))
	for i, col := range t.Headers {
		s := computeColStats(t, col)
		if s.count == 0 {
			records[i] = []string{col, "0", "", "", "", "", ""}
			continue
		}
		mean := s.sum / float64(s.count)
		var std float64
		if s.count >= 2 {
			std = math.Sqrt(s.sumSq / float64(s.count))
		}
		sort.Float64s(s.numericVals)
		n := len(s.numericVals)
		var median float64
		if n%2 == 1 {
			median = s.numericVals[n/2]
		} else {
			median = (s.numericVals[n/2-1] + s.numericVals[n/2]) / 2
		}
		records[i] = []string{
			col,
			strconv.Itoa(s.count),
			strconv.FormatFloat(s.min, 'f', -1, 64),
			strconv.FormatFloat(s.max, 'f', -1, 64),
			strconv.FormatFloat(mean, 'f', -1, 64),
			strconv.FormatFloat(std, 'f', -1, 64),
			strconv.FormatFloat(median, 'f', -1, 64),
		}
	}
	return table.New(headers, records)
}

// FreqMap returns a map[string]int counting how often each distinct value
// appears in col (including empty strings).
//
//	schema.FreqMap(t, "status") // → map["active":42 "inactive":8]
func FreqMap(t table.Table, col string) map[string]int {
	result := make(map[string]int)
	colVals(t, col, func(v string) {
		result[v]++
	})
	return result
}

// MinMaxNorm normalises the numeric values in col to the [0, 1] range using
// min-max scaling: (x - min) / (max - min). Non-parseable values are left
// unchanged. If all values are equal, the column is returned unchanged.
//
//	schema.MinMaxNorm(t, "score")
func MinMaxNorm(t table.Table, col string) table.Table {
	min := MinCol(t, col)
	max := MaxCol(t, col)
	if max == min {
		return t
	}
	return t.Map(col, func(v string) string {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return v
		}
		return strconv.FormatFloat((f-min)/(max-min), 'f', -1, 64)
	})
}

// --- Numeric column operations ---
//
// These functions return func(table.Row) float64 for direct use with
// table.Table.AddColFloat. Unparseable values are treated as 0.
//
//	t.AddColFloat("total", schema.Add("price", "tax"))
//	t.AddColFloat("margin", schema.Div("profit", "revenue"))

// floatVal parses col from r as float64, returning 0 on failure.
func floatVal(r table.Row, col string) float64 {
	v, ok := r.Get(col).Get()
	if !ok {
		return 0
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0
	}
	return f
}

// Add returns a function that sums the float values of all named columns.
//
//	t.AddColFloat("total", schema.Add("price", "tax"))
//	t.AddColFloat("grand", schema.Add("price", "tax", "shipping", "handling"))
func Add(cols ...string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		var sum float64
		for _, col := range cols {
			sum += floatVal(r, col)
		}
		return sum
	}
}

// Sub returns a function that subtracts all subsequent columns from the first.
//
//	t.AddColFloat("profit", schema.Sub("revenue", "cost"))
//	t.AddColFloat("net", schema.Sub("revenue", "cost", "tax", "fees"))
func Sub(cols ...string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		if len(cols) == 0 {
			return 0
		}
		result := floatVal(r, cols[0])
		for _, col := range cols[1:] {
			result -= floatVal(r, col)
		}
		return result
	}
}

// Mul returns a function that multiplies the float values of all named columns.
//
//	t.AddColFloat("line_total", schema.Mul("qty", "unit_price"))
//	t.AddColFloat("volume", schema.Mul("length", "width", "height"))
func Mul(cols ...string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		if len(cols) == 0 {
			return 0
		}
		result := floatVal(r, cols[0])
		for _, col := range cols[1:] {
			result *= floatVal(r, col)
		}
		return result
	}
}

// Div returns a function that divides colA by colB. Returns 0 when colB is 0.
//
//	t.AddColFloat("avg_price", schema.Div("total", "count"))
func Div(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := floatVal(r, colA)
		b := floatVal(r, colB)
		if b == 0 {
			return 0
		}
		return a / b
	}
}

// --- Date column operations ---
//
// These functions return closures for use with AddCol or AddColFloat.
// Dates are parsed using the same dateLayouts as Infer/Time. Unparseable
// values produce an empty string (for string functions) or 0 (for float
// functions).
//
//	t.AddColFloat("days", schema.DateDiffDays("end", "start"))
//	t.AddCol("year", schema.DateYear("created_at"))
//	t.AddCol("next", schema.DateAddDays("review_date", 90))

// timeVal parses col from r as time.Time. Returns zero time on failure.
func timeVal(r table.Row, col string) time.Time {
	v, ok := r.Get(col).Get()
	if !ok {
		return time.Time{}
	}
	return tryParseDate(v)
}

// DateDiffDays returns a function that computes the difference in days
// between two date columns (colA - colB). Fractional days are included.
//
//	t.AddColFloat("laufzeit", schema.DateDiffDays("end_date", "start_date"))
func DateDiffDays(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := timeVal(r, colA)
		b := timeVal(r, colB)
		if a.IsZero() || b.IsZero() {
			return 0
		}
		return a.Sub(b).Hours() / 24
	}
}

// DateAddDays returns a function that adds days to a date column and returns
// the result as an ISO date string (2006-01-02). Negative days subtract.
//
//	t.AddCol("due_date", schema.DateAddDays("created_at", 30))
func DateAddDays(col string, days int) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return t.AddDate(0, 0, days).Format("2006-01-02")
	}
}

// DateYear returns a function that extracts the year from a date column.
//
//	t.AddCol("year", schema.DateYear("created_at"))
func DateYear(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return strconv.Itoa(t.Year())
	}
}

// DateMonth returns a function that extracts the month (1–12) from a date column.
//
//	t.AddCol("month", schema.DateMonth("created_at"))
func DateMonth(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return strconv.Itoa(int(t.Month()))
	}
}

// DateDay returns a function that extracts the day of month (1–31) from a date column.
//
//	t.AddCol("day", schema.DateDay("created_at"))
func DateDay(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return strconv.Itoa(t.Day())
	}
}

// DateFormat returns a function that formats a date column using a Go time
// layout string.
//
//	t.AddCol("display", schema.DateFormat("created_at", "02.01.2006"))
func DateFormat(col string, layout string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return t.Format(layout)
	}
}

// --- Additional numeric operations ---

// Abs returns a function that computes the absolute value of a column.
//
//	t.AddColFloat("abs_pnl", schema.Abs("pnl"))
func Abs(col string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		return math.Abs(floatVal(r, col))
	}
}

// Neg returns a function that negates a column's value.
//
//	t.AddColFloat("loss", schema.Neg("profit"))
func Neg(col string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		return -floatVal(r, col)
	}
}

// AddConst returns a function that adds a constant to a column.
//
//	t.AddColFloat("price_vat", schema.AddConst("price", 19.0))
func AddConst(col string, c float64) func(table.Row) float64 {
	return func(r table.Row) float64 {
		return floatVal(r, col) + c
	}
}

// MulConst returns a function that multiplies a column by a constant.
//
//	t.AddColFloat("price_eur", schema.MulConst("price_usd", 0.92))
func MulConst(col string, c float64) func(table.Row) float64 {
	return func(r table.Row) float64 {
		return floatVal(r, col) * c
	}
}

// Mod returns a function that computes colA mod colB. Returns 0 if colB is 0.
//
//	t.AddColFloat("remainder", schema.Mod("total", "batch_size"))
func Mod(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := floatVal(r, colA)
		b := floatVal(r, colB)
		if b == 0 {
			return 0
		}
		return math.Mod(a, b)
	}
}

// Min2 returns a function that returns the smallest value across all named columns.
//
//	t.AddColFloat("lower", schema.Min2("bid", "ask"))
//	t.AddColFloat("cheapest", schema.Min2("price_a", "price_b", "price_c"))
func Min2(cols ...string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		if len(cols) == 0 {
			return 0
		}
		min := floatVal(r, cols[0])
		for _, col := range cols[1:] {
			if v := floatVal(r, col); v < min {
				min = v
			}
		}
		return min
	}
}

// Max2 returns a function that returns the largest value across all named columns.
//
//	t.AddColFloat("upper", schema.Max2("bid", "ask"))
//	t.AddColFloat("highest", schema.Max2("price_a", "price_b", "price_c"))
func Max2(cols ...string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		if len(cols) == 0 {
			return 0
		}
		max := floatVal(r, cols[0])
		for _, col := range cols[1:] {
			if v := floatVal(r, col); v > max {
				max = v
			}
		}
		return max
	}
}

// Round returns a function that rounds a column to n decimal places.
//
//	t.AddColFloat("rounded", schema.Round("ratio", 2))
func Round(col string, decimals int) func(table.Row) float64 {
	pow := math.Pow(10, float64(decimals))
	return func(r table.Row) float64 {
		return math.Round(floatVal(r, col)*pow) / pow
	}
}

// Clamp returns a function that clamps a column's value to [min, max].
//
//	t.AddColFloat("score", schema.Clamp("raw_score", 0, 100))
func Clamp(col string, min, max float64) func(table.Row) float64 {
	return func(r table.Row) float64 {
		v := floatVal(r, col)
		if v < min {
			return min
		}
		if v > max {
			return max
		}
		return v
	}
}

// Pct returns a function that computes (colA / colB) * 100. Returns 0 if colB is 0.
//
//	t.AddColFloat("margin_pct", schema.Pct("profit", "revenue"))
func Pct(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := floatVal(r, colA)
		b := floatVal(r, colB)
		if b == 0 {
			return 0
		}
		return (a / b) * 100
	}
}

// --- Additional date operations ---

// DateDiffMonths returns a function that computes the approximate difference
// in months between two date columns (colA - colB). Uses 30.44 days/month.
//
//	t.AddColFloat("months", schema.DateDiffMonths("end", "start"))
func DateDiffMonths(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := timeVal(r, colA)
		b := timeVal(r, colB)
		if a.IsZero() || b.IsZero() {
			return 0
		}
		return a.Sub(b).Hours() / (24 * 30.44)
	}
}

// DateDiffYears returns a function that computes the approximate difference
// in years between two date columns (colA - colB). Uses 365.25 days/year.
//
//	t.AddColFloat("years", schema.DateDiffYears("end", "start"))
func DateDiffYears(colA, colB string) func(table.Row) float64 {
	return func(r table.Row) float64 {
		a := timeVal(r, colA)
		b := timeVal(r, colB)
		if a.IsZero() || b.IsZero() {
			return 0
		}
		return a.Sub(b).Hours() / (24 * 365.25)
	}
}

// DateAddMonths returns a function that adds months to a date column.
//
//	t.AddCol("review", schema.DateAddMonths("created_at", 6))
func DateAddMonths(col string, months int) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return t.AddDate(0, months, 0).Format("2006-01-02")
	}
}

// DateWeek returns a function that extracts the ISO week number (1–53).
//
//	t.AddCol("week", schema.DateWeek("date"))
func DateWeek(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		_, week := t.ISOWeek()
		return strconv.Itoa(week)
	}
}

// DateStartOfMonth returns a function that truncates a date to the first day
// of its month.
//
//	t.AddCol("month_start", schema.DateStartOfMonth("date"))
func DateStartOfMonth(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
	}
}

// DateEndOfMonth returns a function that returns the last day of a date's month.
//
//	t.AddCol("month_end", schema.DateEndOfMonth("date"))
func DateEndOfMonth(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		// first day of next month, minus one day
		first := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
		return first.AddDate(0, 0, -1).Format("2006-01-02")
	}
}

// DateAge returns a function that computes the age in full years at a reference
// date. If ref is zero, time.Now() is used.
//
//	t.AddCol("age", schema.DateAge("birthday", time.Time{}))  // age today
//	t.AddCol("age", schema.DateAge("birthday", cutoff))       // age at cutoff
func DateAge(col string, ref time.Time) func(table.Row) string {
	return func(r table.Row) string {
		birth := timeVal(r, col)
		if birth.IsZero() {
			return ""
		}
		now := ref
		if now.IsZero() {
			now = time.Now()
		}
		years := now.Year() - birth.Year()
		if now.YearDay() < birth.YearDay() {
			years--
		}
		return strconv.Itoa(years)
	}
}

// DateBetween returns a predicate: startCol <= row date <= endCol.
// All three dates are parsed from their respective columns.
//
//	t.Where(schema.DateBetween("event_date", "period_start", "period_end"))
func DateBetween(dateCol, startCol, endCol string) func(table.Row) bool {
	return func(r table.Row) bool {
		d := timeVal(r, dateCol)
		s := timeVal(r, startCol)
		e := timeVal(r, endCol)
		if d.IsZero() || s.IsZero() || e.IsZero() {
			return false
		}
		return !d.Before(s) && !d.After(e)
	}
}

// DateTrunc truncates a date to the given precision: "day", "month", or "year".
//
//	t.AddCol("period", schema.DateTrunc("date", "month"))
//	// 2024-03-15 → 2024-03-01
//	// 2024-07-22 → 2024-07-01
func DateTrunc(col string, precision string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		switch precision {
		case "year":
			return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
		case "month":
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
		default: // "day"
			return t.Format("2006-01-02")
		}
	}
}

// DateWeekday returns a function that extracts the weekday name (Monday, …)
// from a date column.
//
//	t.AddCol("weekday", schema.DateWeekday("date"))
func DateWeekday(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return t.Weekday().String()
	}
}

// DateQuarter returns a function that extracts the quarter (1–4) from a date column.
//
//	t.AddCol("quarter", schema.DateQuarter("date"))
func DateQuarter(col string) func(table.Row) string {
	return func(r table.Row) string {
		t := timeVal(r, col)
		if t.IsZero() {
			return ""
		}
		return strconv.Itoa((int(t.Month())-1)/3 + 1)
	}
}
