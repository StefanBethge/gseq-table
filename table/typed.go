//go:build go1.27

package table

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stefanbethge/gseq/option"
)

// This file and the other typed*.go files use Go 1.27 generic methods. They
// are guarded by the go1.27 build constraint so the module keeps building on
// older toolchains; on those toolchains the typed methods are simply absent.
//
// Parsing follows the conventions of the schema package's row accessors
// (schema.Int, schema.Float, schema.Bool, schema.Time): surrounding whitespace
// is trimmed, booleans accept true/false, 1/0 and yes/no (case-insensitive),
// and dates are tried against the same list of common layouts. Empty cells are
// treated as missing for every type except string.

// Integer is the set of built-in integer types supported by the typed methods.
type Integer interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64
}

// Float is the set of built-in floating-point types supported by the typed
// methods.
type Float interface {
	float32 | float64
}

// Number is the set of numeric types usable with the typed aggregation methods
// (SumAs, MinAs, MaxAs).
type Number interface {
	Integer | Float
}

// Ordered is the set of cell types that can be compared with < and >.
type Ordered interface {
	Number | string
}

// Value is the set of cell types the typed methods (GetAs, ColAs, MapAs,
// AddColAs, ...) can parse from and format to a cell string.
//
// For other types use the *With variants, which take an explicit parse
// function.
type Value interface {
	Number | string | bool | time.Time
}

// typedDateLayouts mirrors the layouts the schema package tries when parsing
// dates, in the same order.
var typedDateLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02",
	"02.01.2006",
	"01/02/2006",
	"02 Jan 2006",
	"Jan 02, 2006",
}

// parseValue parses a raw cell string as T. It reports false if raw is empty
// (for non-string types) or cannot be parsed.
func parseValue[T Value](raw string) (T, bool) {
	var out T
	if p, ok := any(&out).(*string); ok {
		*p = raw
		return out, true
	}
	s := strings.TrimSpace(raw)
	if s == "" {
		return out, false
	}
	var err error
	switch p := any(&out).(type) {
	case *int:
		var n int64
		n, err = strconv.ParseInt(s, 10, strconv.IntSize)
		*p = int(n)
	case *int8:
		var n int64
		n, err = strconv.ParseInt(s, 10, 8)
		*p = int8(n)
	case *int16:
		var n int64
		n, err = strconv.ParseInt(s, 10, 16)
		*p = int16(n)
	case *int32:
		var n int64
		n, err = strconv.ParseInt(s, 10, 32)
		*p = int32(n)
	case *int64:
		*p, err = strconv.ParseInt(s, 10, 64)
	case *uint:
		var n uint64
		n, err = strconv.ParseUint(s, 10, strconv.IntSize)
		*p = uint(n)
	case *uint8:
		var n uint64
		n, err = strconv.ParseUint(s, 10, 8)
		*p = uint8(n)
	case *uint16:
		var n uint64
		n, err = strconv.ParseUint(s, 10, 16)
		*p = uint16(n)
	case *uint32:
		var n uint64
		n, err = strconv.ParseUint(s, 10, 32)
		*p = uint32(n)
	case *uint64:
		*p, err = strconv.ParseUint(s, 10, 64)
	case *float32:
		var f float64
		f, err = strconv.ParseFloat(s, 32)
		*p = float32(f)
	case *float64:
		*p, err = strconv.ParseFloat(s, 64)
	case *bool:
		switch strings.ToLower(s) {
		case "true", "1", "yes":
			*p = true
		case "false", "0", "no":
			*p = false
		default:
			return out, false
		}
	case *time.Time:
		for _, layout := range typedDateLayouts {
			if t, perr := time.Parse(layout, s); perr == nil {
				*p = t
				return out, true
			}
		}
		return out, false
	}
	if err != nil {
		var zero T
		return zero, false
	}
	return out, true
}

// parseValueErr is parseValue with a descriptive error for strict callers.
func parseValueErr[T Value](raw string) (T, error) {
	v, ok := parseValue[T](raw)
	if !ok {
		return v, fmt.Errorf("cannot parse %q as %T", raw, v)
	}
	return v, nil
}

// formatValue renders v in the canonical cell form: base-10 integers, the
// shortest round-tripping float representation, "true"/"false", and dates as
// "2006-01-02" when v is midnight UTC (RFC 3339 otherwise).
func formatValue[T Value](v T) string {
	switch x := any(v).(type) {
	case string:
		return x
	case int:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case time.Time:
		if x.Location() == time.UTC && x.Equal(x.Truncate(24*time.Hour)) {
			return x.Format("2006-01-02")
		}
		return x.Format(time.RFC3339Nano)
	}
	return fmt.Sprint(v)
}

// --- Row ---

// GetAs parses the value of col as T. Returns None if the column does not
// exist, the cell is empty (for non-string T), or the value cannot be parsed.
//
//	age := row.GetAs[int]("age").UnwrapOr(0)
//	price := row.GetAs[float64]("price")   // option.Option[float64]
//	born := row.GetAs[time.Time]("birthday")
func (r Row) GetAs[T Value](col string) option.Option[T] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[T]()
	}
	return optionOf(parseValue[T](v))
}

// AtAs parses the value at position i (zero-based) as T. Returns None if i is
// out of range or the value cannot be parsed.
//
//	first := row.AtAs[int64](0)
func (r Row) AtAs[T Value](i int) option.Option[T] {
	v, ok := r.At(i).Get()
	if !ok {
		return option.None[T]()
	}
	return optionOf(parseValue[T](v))
}

// GetWith parses the value of col with parse. Returns None if the column does
// not exist or parse returns an error. T is inferred from parse, so it works
// for any type:
//
//	n := row.GetWith("n", strconv.Atoi)              // option.Option[int]
//	d := row.GetWith("ttl", time.ParseDuration)      // option.Option[time.Duration]
func (r Row) GetWith[T any](col string, parse func(string) (T, error)) option.Option[T] {
	v, ok := r.Get(col).Get()
	if !ok {
		return option.None[T]()
	}
	parsed, err := parse(v)
	if err != nil {
		return option.None[T]()
	}
	return option.Some(parsed)
}

func optionOf[T any](v T, ok bool) option.Option[T] {
	if !ok {
		return option.None[T]()
	}
	return option.Some(v)
}
