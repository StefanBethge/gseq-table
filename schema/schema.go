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
		out = table.Table{Headers: out.Headers, Rows: rows}
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
	for i, h := range t.Headers {
		if h == col {
			return i
		}
	}
	return -1
}

// --- Numeric column aggregators ---
//
// These functions operate on the string values of a column, parsing each cell
// as float64. Unparseable values (including empty strings) are silently
// skipped unless otherwise noted.

// SumCol returns the sum of all parseable float values in col.
//
//	schema.SumCol(t, "revenue")
func SumCol(t table.Table, col string) float64 {
	var sum float64
	for _, v := range t.Col(col) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			sum += f
		}
	}
	return sum
}

// MeanCol returns the arithmetic mean of all parseable float values in col.
// Returns 0 if no parseable values are found.
//
//	schema.MeanCol(t, "age")
func MeanCol(t table.Table, col string) float64 {
	var sum float64
	var n int
	for _, v := range t.Col(col) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			sum += f
			n++
		}
	}
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
	for _, v := range t.Col(col) {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			continue
		}
		if first || f < min {
			min = f
			first = false
		}
	}
	return min
}

// MaxCol returns the maximum parseable float value in col.
// Returns 0 if no parseable values are found.
//
//	schema.MaxCol(t, "price")
func MaxCol(t table.Table, col string) float64 {
	var max float64
	first := true
	for _, v := range t.Col(col) {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			continue
		}
		if first || f > max {
			max = f
			first = false
		}
	}
	return max
}

// CountCol counts non-empty values in col.
//
//	schema.CountCol(t, "email")
func CountCol(t table.Table, col string) int {
	var n int
	for _, v := range t.Col(col) {
		if v != "" {
			n++
		}
	}
	return n
}

// CountWhere counts rows where col == val (case-sensitive).
//
//	schema.CountWhere(t, "status", "active")
func CountWhere(t table.Table, col, val string) int {
	var n int
	for _, v := range t.Col(col) {
		if v == val {
			n++
		}
	}
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
	for _, v := range t.Col(col) {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			continue
		}
		d := f - mean
		sumSq += d * d
		n++
	}
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
	for _, v := range t.Col(col) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			vals = append(vals, f)
		}
	}
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

// numericCount counts the parseable float64 values in col.
func numericCount(t table.Table, col string) int {
	var n int
	for _, v := range t.Col(col) {
		if _, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			n++
		}
	}
	return n
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
		count := numericCount(t, col)
		if count == 0 {
			records[i] = []string{col, "0", "", "", "", "", ""}
			continue
		}
		records[i] = []string{
			col,
			strconv.Itoa(count),
			strconv.FormatFloat(MinCol(t, col), 'f', -1, 64),
			strconv.FormatFloat(MaxCol(t, col), 'f', -1, 64),
			strconv.FormatFloat(MeanCol(t, col), 'f', -1, 64),
			strconv.FormatFloat(StdDevCol(t, col), 'f', -1, 64),
			strconv.FormatFloat(MedianCol(t, col), 'f', -1, 64),
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
	for _, v := range t.Col(col) {
		result[v]++
	}
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
