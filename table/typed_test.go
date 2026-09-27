package table

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func typedTable() Table {
	return New(
		[]string{"name", "qty", "price", "active", "date"},
		[][]string{
			{"apple", "3", "1.5", "yes", "2024-01-15"},
			{"pear", " 10 ", "0.25", "false", "15.02.2024"},
			{"plum", "", "abc", "maybe", ""},
			{"kiwi", "-2", "2", "1", "2024-03-01T10:30:00Z"},
		},
	)
}

func TestRowGetAs(t *testing.T) {
	tb := typedTable()
	r0, r1, r2 := tb.Rows[0], tb.Rows[1], tb.Rows[2]

	assertEqual(t, r0.GetAs[int]("qty").UnwrapOr(-1), 3)
	assertEqual(t, r1.GetAs[int64]("qty").UnwrapOr(-1), int64(10)) // whitespace trimmed
	assertEqual(t, r0.GetAs[float64]("price").UnwrapOr(0), 1.5)
	assertEqual(t, r0.GetAs[float32]("price").UnwrapOr(0), float32(1.5))
	assertEqual(t, r0.GetAs[bool]("active").UnwrapOr(false), true)
	assertEqual(t, r1.GetAs[bool]("active").UnwrapOr(true), false)
	assertEqual(t, r0.GetAs[string]("name").UnwrapOr(""), "apple")
	assertEqual(t, r0.GetAs[time.Time]("date").UnwrapOr(time.Time{}), time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC))
	assertEqual(t, r1.GetAs[time.Time]("date").UnwrapOr(time.Time{}), time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC))

	// empty, unparseable and missing → None
	assertEqual(t, r2.GetAs[int]("qty").IsNone(), true)
	assertEqual(t, r2.GetAs[float64]("price").IsNone(), true)
	assertEqual(t, r2.GetAs[bool]("active").IsNone(), true)
	assertEqual(t, r2.GetAs[time.Time]("date").IsNone(), true)
	assertEqual(t, r0.GetAs[int]("missing").IsNone(), true)
	assertEqual(t, r0.GetAs[int]("price").IsNone(), true) // 1.5 is not an int

	// empty string is a valid string
	assertEqual(t, r2.GetAs[string]("qty").UnwrapOr("x"), "")
}

func TestRowGetAs_IntRanges(t *testing.T) {
	r := NewRow([]string{"a", "b"}, []string{"300", "-1"})
	assertEqual(t, r.GetAs[int8]("a").IsNone(), true)
	assertEqual(t, r.GetAs[int16]("a").UnwrapOr(0), int16(300))
	assertEqual(t, r.GetAs[int32]("a").UnwrapOr(0), int32(300))
	assertEqual(t, r.GetAs[uint8]("a").IsNone(), true)
	assertEqual(t, r.GetAs[uint16]("a").UnwrapOr(0), uint16(300))
	assertEqual(t, r.GetAs[uint32]("a").UnwrapOr(0), uint32(300))
	assertEqual(t, r.GetAs[uint64]("a").UnwrapOr(0), uint64(300))
	assertEqual(t, r.GetAs[uint]("a").UnwrapOr(0), uint(300))
	assertEqual(t, r.GetAs[uint]("b").IsNone(), true)
	assertEqual(t, r.GetAs[int8]("b").UnwrapOr(0), int8(-1))
}

func TestRowAtAs(t *testing.T) {
	r := typedTable().Rows[0]
	assertEqual(t, r.AtAs[int](1).UnwrapOr(0), 3)
	assertEqual(t, r.AtAs[int](0).IsNone(), true)
	assertEqual(t, r.AtAs[int](99).IsNone(), true)
	assertEqual(t, r.AtAs[int](-1).IsNone(), true)
}

func TestRowGetWith(t *testing.T) {
	r := NewRow([]string{"n", "ttl"}, []string{"42", "1m30s"})
	assertEqual(t, r.GetWith("n", strconv.Atoi).UnwrapOr(0), 42)
	assertEqual(t, r.GetWith("ttl", time.ParseDuration).UnwrapOr(0), 90*time.Second)
	assertEqual(t, r.GetWith("ttl", strconv.Atoi).IsNone(), true)
	assertEqual(t, r.GetWith("missing", strconv.Atoi).IsNone(), true)
}

func TestTableColAs(t *testing.T) {
	tb := typedTable()
	names, err := tb.ColAs[string]("name")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, strings.Join(names, ","), "apple,pear,plum,kiwi")

	_, err = tb.ColAs[int]("qty")
	if err == nil || !strings.Contains(err.Error(), `ColAs: column "qty" row 2`) {
		t.Fatalf("expected parse error for row 2, got %v", err)
	}

	_, err = tb.ColAs[int]("missing")
	if err == nil || !strings.Contains(err.Error(), `unknown column "missing"`) {
		t.Fatalf("expected unknown column error, got %v", err)
	}

	ok := tb.Where(func(r Row) bool { return r.GetAs[int]("qty").IsSome() })
	qty, err := ok.ColAs[int]("qty")
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, len(qty), 3)
	assertEqual(t, qty[0]+qty[1]+qty[2], 11)
}

func TestTableColAs_SourcePrefix(t *testing.T) {
	_, err := typedTable().WithSource("in.csv").ColAs[int]("nope")
	if err == nil || !strings.HasPrefix(err.Error(), "[in.csv] ColAs:") {
		t.Fatalf("expected source prefix, got %v", err)
	}
}

func TestTableColWith(t *testing.T) {
	tb := New([]string{"d"}, [][]string{{"1s"}, {"2m"}})
	ds, err := tb.ColWith("d", time.ParseDuration)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, ds[1], 2*time.Minute)

	sentinel := errors.New("boom")
	_, err = tb.ColWith("d", func(string) (int, error) { return 0, sentinel })
	if err == nil || !strings.Contains(err.Error(), `ColWith: column "d" row 0: boom`) {
		t.Fatalf("unexpected error %v", err)
	}
	_, err = tb.ColWith("x", time.ParseDuration)
	if err == nil {
		t.Fatal("expected unknown column error")
	}
}

func TestTableColOptAs(t *testing.T) {
	prices := typedTable().ColOptAs[float64]("price")
	assertEqual(t, len(prices), 4)
	assertEqual(t, prices[1].UnwrapOr(0), 0.25)
	assertEqual(t, prices[2].IsNone(), true)
	assertEqual(t, prices[3].UnwrapOr(0), 2.0)
	if typedTable().ColOptAs[float64]("missing") != nil {
		t.Fatal("expected nil for missing column")
	}
}

func TestTableMapAs(t *testing.T) {
	tb := typedTable()
	out := tb.MapAs("qty", func(n int) int { return n * 2 })
	assertEqual(t, strings.Join(out.Col("qty"), ","), "6,20,,-4")
	// original unchanged
	assertEqual(t, strings.Join(tb.Col("qty"), ","), "3, 10 ,,-2")

	// type-changing map; unparseable cells are left as-is
	out = tb.MapAs("price", func(p float64) bool { return p >= 1 })
	assertEqual(t, strings.Join(out.Col("price"), ","), "true,false,abc,true")

	// explicit type arguments work too
	out = tb.MapAs[float64, float64]("price", func(p float64) float64 { return p * 10 })
	assertEqual(t, strings.Join(out.Col("price"), ","), "15,2.5,abc,20")
	assertEqual(t, out.HasErrs(), false)
}

func TestTableAddColAs(t *testing.T) {
	tb := typedTable()
	out := tb.AddColAs("total", func(r Row) float64 {
		return r.GetAs[float64]("price").UnwrapOr(0) * float64(r.GetAs[int]("qty").UnwrapOr(0))
	})
	assertEqual(t, strings.Join(out.Col("total"), ","), "4.5,2.5,0,-4")

	out = tb.AddColAs("in_stock", func(r Row) bool { return r.GetAs[int]("qty").UnwrapOr(0) > 0 })
	assertEqual(t, strings.Join(out.Col("in_stock"), ","), "true,true,false,false")

	out = tb.AddColAs("next", func(r Row) time.Time {
		return r.GetAs[time.Time]("date").UnwrapOr(time.Time{}).AddDate(0, 0, 1)
	})
	assertEqual(t, out.Rows[0].Get("next").UnwrapOr(""), "2024-01-16")
	assertEqual(t, out.Rows[3].Get("next").UnwrapOr(""), "2024-03-02T10:30:00Z")
}

func TestTableAggregations(t *testing.T) {
	tb := typedTable()
	assertEqual(t, tb.SumAs[int]("qty"), 11)
	assertEqual(t, tb.SumAs[float64]("price"), 3.75)
	assertEqual(t, tb.SumAs[int]("missing"), 0)

	assertEqual(t, tb.MinAs[int]("qty").UnwrapOr(0), -2)
	assertEqual(t, tb.MaxAs[int]("qty").UnwrapOr(0), 10)
	assertEqual(t, tb.MinAs[float64]("price").UnwrapOr(0), 0.25)
	assertEqual(t, tb.MaxAs[string]("name").UnwrapOr(""), "plum")
	assertEqual(t, tb.MinAs[string]("name").UnwrapOr(""), "apple")
	assertEqual(t, tb.MinAs[int]("name").IsNone(), true)
	assertEqual(t, tb.MaxAs[int]("missing").IsNone(), true)

	actives := tb.ReduceAs("active", 0, func(n int, b bool) int {
		if b {
			n++
		}
		return n
	})
	assertEqual(t, actives, 2)

	latest := tb.ReduceAs("date", time.Time{}, func(acc, d time.Time) time.Time {
		if d.After(acc) {
			return d
		}
		return acc
	})
	assertEqual(t, latest, time.Date(2024, 3, 1, 10, 30, 0, 0, time.UTC))
	assertEqual(t, tb.ReduceAs("missing", 7, func(acc, v int) int { return acc + v }), 7)
}

func TestMutableTyped(t *testing.T) {
	m := typedTable().Mutable()

	qty, err := m.ColAs[int]("qty")
	if err == nil {
		t.Fatalf("expected parse error, got %v", qty)
	}
	if _, err := m.ColAs[string]("name"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ColWith("qty", strconv.Atoi); err == nil {
		t.Fatal("expected parse error from ColWith")
	}
	if _, err := m.ColAs[int]("missing"); err == nil {
		t.Fatal("expected unknown column error")
	}
	assertEqual(t, m.ColOptAs[int]("qty")[1].UnwrapOr(0), 10)
	assertEqual(t, m.ColOptAs[int]("missing") == nil, true)

	m.MapAs("qty", func(n int) int { return n + 1 }).
		AddColAs("double", func(r Row) float64 { return 2 * r.GetAs[float64]("price").UnwrapOr(0) }).
		SetAs(2, "qty", 7).
		SetAs(2, "active", true)

	assertEqual(t, strings.Join(m.Col("qty"), ","), "4,11,7,-1")
	assertEqual(t, strings.Join(m.Col("double"), ","), "3,0.5,0,4")
	assertEqual(t, strings.Join(m.Col("active"), ","), "yes,false,true,1")

	assertEqual(t, m.SumAs[int]("qty"), 21)
	assertEqual(t, m.MinAs[int]("qty").UnwrapOr(0), -1)
	assertEqual(t, m.MaxAs[float64]("double").UnwrapOr(0), 4.0)
	assertEqual(t, m.ReduceAs("active", 0, func(n int, b bool) int {
		if b {
			n++
		}
		return n
	}), 3)
	assertEqual(t, m.ReduceAs("missing", 1, func(acc, v int) int { return acc + v }), 1)
	assertEqual(t, m.HasErrs(), false)
}

func TestFormatValue(t *testing.T) {
	assertEqual(t, formatValue(int8(-5)), "-5")
	assertEqual(t, formatValue(int16(5)), "5")
	assertEqual(t, formatValue(int32(5)), "5")
	assertEqual(t, formatValue(uint(5)), "5")
	assertEqual(t, formatValue(uint8(5)), "5")
	assertEqual(t, formatValue(uint16(5)), "5")
	assertEqual(t, formatValue(uint32(5)), "5")
	assertEqual(t, formatValue(uint64(5)), "5")
	assertEqual(t, formatValue(float32(0.1)), "0.1")
	assertEqual(t, formatValue(math.Pi), "3.141592653589793")
	assertEqual(t, formatValue(false), "false")
	assertEqual(t, formatValue("x"), "x")
	berlin := time.FixedZone("CET", 3600)
	assertEqual(t, formatValue(time.Date(2024, 1, 1, 0, 0, 0, 0, berlin)), "2024-01-01T00:00:00+01:00")
}

func TestParseFormatRoundTrip(t *testing.T) {
	for _, raw := range []string{"0", "-17", "42"} {
		v, ok := parseValue[int64](raw)
		assertEqual(t, ok, true)
		assertEqual(t, formatValue(v), raw)
	}
	for _, raw := range []string{"2024-02-29", "2024-02-29T13:14:15Z"} {
		v, ok := parseValue[time.Time](raw)
		assertEqual(t, ok, true)
		assertEqual(t, formatValue(v), raw)
	}
}
