//go:build go1.27

package table

import (
	"strings"
	"testing"
	"time"
)

// The zero time ("0001-01-01") counts as "not parsed" on every typed time
// path, matching schema.Time (the rule lives in internal/cell.ParseDate).
func zeroDateTable() Table {
	return New([]string{"d"}, [][]string{
		{"2024-01-15"},
		{"0001-01-01"},
		{"0001-01-01T00:00:00Z"},
		{"01.01.0001"},
	})
}

func TestTypedZeroDate_Row(t *testing.T) {
	tb := zeroDateTable()
	assertEqual(t, tb.Rows[0].GetAs[time.Time]("d").IsSome(), true)
	for i := 1; i < len(tb.Rows); i++ {
		r := tb.Rows[i]
		assertEqual(t, r.GetAs[time.Time]("d").IsNone(), true)
		assertEqual(t, r.AtAs[time.Time](0).IsNone(), true)
	}
}

func TestTypedZeroDate_Table(t *testing.T) {
	tb := zeroDateTable()

	_, err := tb.ColAs[time.Time]("d")
	if err == nil || !strings.Contains(err.Error(), `ColAs: column "d" row 1`) {
		t.Fatalf("expected ColAs error at row 1, got %v", err)
	}

	opts := tb.ColOptAs[time.Time]("d")
	assertEqual(t, opts[0].IsSome(), true)
	assertEqual(t, opts[1].IsNone(), true)
	assertEqual(t, opts[2].IsNone(), true)
	assertEqual(t, opts[3].IsNone(), true)

	// MapAs skips zero dates and leaves those cells unchanged
	out := tb.MapAs("d", func(d time.Time) time.Time { return d.AddDate(0, 0, 1) })
	assertEqual(t, strings.Join(out.Col("d"), ","), "2024-01-16,0001-01-01,0001-01-01T00:00:00Z,01.01.0001")

	// aggregations skip zero dates
	earliest := tb.ReduceAs("d", time.Time{}, func(acc, d time.Time) time.Time {
		if acc.IsZero() || d.Before(acc) {
			return d
		}
		return acc
	})
	assertEqual(t, earliest, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC))
	assertEqual(t, tb.ReduceAs("d", 0, func(n int, _ time.Time) int { return n + 1 }), 1)
}

func TestTypedZeroDate_Mutable(t *testing.T) {
	m := zeroDateTable().Mutable()
	if _, err := m.ColAs[time.Time]("d"); err == nil {
		t.Fatal("expected ColAs error for zero date")
	}
	assertEqual(t, m.ColOptAs[time.Time]("d")[1].IsNone(), true)
	assertEqual(t, m.ReduceAs("d", 0, func(n int, _ time.Time) int { return n + 1 }), 1)
	m.MapAs("d", func(d time.Time) time.Time { return d.AddDate(1, 0, 0) })
	assertEqual(t, strings.Join(m.Col("d"), ","), "2025-01-15,0001-01-01,0001-01-01T00:00:00Z,01.01.0001")
}
