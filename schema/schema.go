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
