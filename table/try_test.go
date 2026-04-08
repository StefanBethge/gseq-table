package table

import (
	"fmt"
	"strconv"
	"testing"
)

func TestTryTransform_Ok(t *testing.T) {
	tb := New([]string{"price"}, [][]string{{"10"}, {"20"}})
	res := tb.TryTransform(func(r Row) (map[string]string, error) {
		f, err := strconv.ParseFloat(r.Get("price").UnwrapOr(""), 64)
		if err != nil {
			return nil, fmt.Errorf("bad price: %s", r.Get("price").UnwrapOr(""))
		}
		return map[string]string{"price": strconv.FormatFloat(f*1.19, 'f', 2, 64)}, nil
	})
	assertEqual(t, res.IsOk(), true)
	assertEqual(t, res.Unwrap().Rows[0].Get("price").UnwrapOr(""), "11.90")
}

func TestTryTransform_Error(t *testing.T) {
	tb := New([]string{"price"}, [][]string{{"10"}, {"bad"}, {"30"}})
	res := tb.TryTransform(func(r Row) (map[string]string, error) {
		_, err := strconv.ParseFloat(r.Get("price").UnwrapOr(""), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid: %s", r.Get("price").UnwrapOr(""))
		}
		return nil, nil
	})
	assertEqual(t, res.IsErr(), true)
}

func TestTryMap_Ok(t *testing.T) {
	tb := New([]string{"n"}, [][]string{{"1"}, {"2"}, {"3"}})
	res := tb.TryMap("n", func(v string) (string, error) {
		n, err := strconv.Atoi(v)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(n * n), nil
	})
	assertEqual(t, res.IsOk(), true)
	assertEqual(t, res.Unwrap().Rows[2].Get("n").UnwrapOr(""), "9")
}

func TestTryMap_Error(t *testing.T) {
	tb := New([]string{"n"}, [][]string{{"1"}, {"abc"}})
	res := tb.TryMap("n", func(v string) (string, error) {
		_, err := strconv.Atoi(v)
		if err != nil {
			return "", fmt.Errorf("not int: %s", v)
		}
		return v, nil
	})
	assertEqual(t, res.IsErr(), true)
}

func TestTryMap_UnknownCol(t *testing.T) {
	tb := New([]string{"a"}, [][]string{{"1"}})
	res := tb.TryMap("unknown", func(v string) (string, error) { return v, nil })
	assertEqual(t, res.IsOk(), true) // unknown col is a no-op
}
