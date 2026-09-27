package table

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stefanbethge/gseq-table/internal/cell"
	"github.com/stefanbethge/gseq/option"
)

// This file and the other typed*.go files use Go 1.27 generic methods.
//
// Parsing and formatting use the internal cell package, the same rules the
// schema package uses: surrounding whitespace is trimmed, booleans accept
// true/false, 1/0 and yes/no (case-insensitive), and dates are tried against
// cell.DateLayouts. Empty cells are treated as missing for every type except
// string.

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

// parseValue parses a raw cell string as T using the shared cell rules. It
// reports false if raw is empty (for non-string types) or cannot be parsed.
func parseValue[T Value](raw string) (T, bool) {
	var out T
	if p, ok := any(&out).(*string); ok {
		*p = raw
		return out, true
	}
	if strings.TrimSpace(raw) == "" {
		return out, false
	}
	var err error
	switch p := any(&out).(type) {
	case *int:
		*p, err = parseInt[int](raw, strconv.IntSize)
	case *int8:
		*p, err = parseInt[int8](raw, 8)
	case *int16:
		*p, err = parseInt[int16](raw, 16)
	case *int32:
		*p, err = parseInt[int32](raw, 32)
	case *int64:
		*p, err = cell.ParseInt(raw, 64)
	case *uint:
		*p, err = parseUint[uint](raw, strconv.IntSize)
	case *uint8:
		*p, err = parseUint[uint8](raw, 8)
	case *uint16:
		*p, err = parseUint[uint16](raw, 16)
	case *uint32:
		*p, err = parseUint[uint32](raw, 32)
	case *uint64:
		*p, err = cell.ParseUint(raw, 64)
	case *float32:
		var f float64
		f, err = cell.ParseFloat(raw, 32)
		*p = float32(f)
	case *float64:
		*p, err = cell.ParseFloat(raw, 64)
	case *bool:
		var ok bool
		if *p, ok = cell.ParseBool(raw); !ok {
			return out, false
		}
	case *time.Time:
		*p, err = cell.ParseDate(raw)
	}
	if err != nil {
		var zero T
		return zero, false
	}
	return out, true
}

func parseInt[T Integer](raw string, bitSize int) (T, error) {
	n, err := cell.ParseInt(raw, bitSize)
	return T(n), err
}

func parseUint[T Integer](raw string, bitSize int) (T, error) {
	n, err := cell.ParseUint(raw, bitSize)
	return T(n), err
}

// parseValueErr is parseValue with a descriptive error for strict callers.
func parseValueErr[T Value](raw string) (T, error) {
	v, ok := parseValue[T](raw)
	if !ok {
		return v, fmt.Errorf("cannot parse %q as %T", raw, v)
	}
	return v, nil
}

// formatValue renders v in the canonical cell form defined by the cell
// package.
func formatValue[T Value](v T) string {
	switch x := any(v).(type) {
	case string:
		return x
	case int:
		return cell.FormatInt(int64(x))
	case int8:
		return cell.FormatInt(int64(x))
	case int16:
		return cell.FormatInt(int64(x))
	case int32:
		return cell.FormatInt(int64(x))
	case int64:
		return cell.FormatInt(x)
	case uint:
		return cell.FormatUint(uint64(x))
	case uint8:
		return cell.FormatUint(uint64(x))
	case uint16:
		return cell.FormatUint(uint64(x))
	case uint32:
		return cell.FormatUint(uint64(x))
	case uint64:
		return cell.FormatUint(x)
	case float32:
		return cell.FormatFloat(float64(x), 32)
	case float64:
		return cell.FormatFloat(x, 64)
	case bool:
		return cell.FormatBool(x)
	case time.Time:
		return cell.FormatTime(x)
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
