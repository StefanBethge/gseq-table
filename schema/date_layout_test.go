package schema

import (
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/table"
)

// --- ParseDate ---

func TestParseDate_CustomLayouts(t *testing.T) {
	cases := []struct {
		layout, in string
		want       time.Time
	}{
		{"2.1.2006", "5.3.2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"2.1.2006", " 15.11.2024 ", time.Date(2024, 11, 15, 0, 0, 0, 0, time.UTC)},
		{"02.01.2006 15:04", "05.03.2024 14:30", time.Date(2024, 3, 5, 14, 30, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		got, err := ParseDate(c.in, c.layout)
		if err != nil || !got.Equal(c.want) {
			t.Errorf("ParseDate(%q, %q) = %v, %v; want %v", c.in, c.layout, got, err, c.want)
		}
	}
}

func TestParseDate_EmptyLayoutUsesBuiltIn(t *testing.T) {
	got, err := ParseDate("15.03.2024", "")
	assertEqual(t, err, nil)
	assertEqual(t, got.Format("2006-01-02"), "2024-03-15")
}

func TestParseDate_Invalid(t *testing.T) {
	cases := []struct{ layout, in string }{
		{"2.1.2006", ""},
		{"2.1.2006", "not-a-date"},
		{"2.1.2006", "2024-03-05"},
		{"2.1.2006", "32.1.2024"},
		{"02.01.2006 15:04", "05.03.2024"},
		{"02.01.2006 15:04", "05.03.2024 25:00"},
		{"", "not-a-date"},
	}
	for _, c := range cases {
		if _, err := ParseDate(c.in, c.layout); err == nil {
			t.Errorf("ParseDate(%q, %q): expected error", c.in, c.layout)
		}
	}
}

// The zero time counts as not parsed with a custom layout too (#11).
func TestParseDate_ZeroDateRejected(t *testing.T) {
	for _, c := range []struct{ layout, in string }{
		{"2.1.2006", "1.1.0001"},
		{"02.01.2006 15:04", "01.01.0001 00:00"},
		{"", "0001-01-01"},
	} {
		if _, err := ParseDate(c.in, c.layout); err == nil {
			t.Errorf("ParseDate(%q, %q): expected zero date error", c.in, c.layout)
		}
	}
	row := table.NewRow([]string{"d"}, []string{"1.1.0001"})
	assertEqual(t, Time(row, "d", "2.1.2006").IsNone(), true)
}

// --- CastDate ---

func TestCastDate_SetsTypeAndLayout(t *testing.T) {
	s := Schema{}.CastDate("d", "2.1.2006")
	assertEqual(t, s.Col("d"), TypeDate)
	assertEqual(t, s.DateLayout("d"), "2.1.2006")
	assertEqual(t, s.DateLayout("other"), "")
}

func TestCastDate_Immutable(t *testing.T) {
	base := Schema{}.Cast("n", TypeInt)
	next := base.CastDate("d", "2.1.2006")
	assertEqual(t, base.Col("d"), TypeString)
	assertEqual(t, base.DateLayout("d"), "")
	assertEqual(t, next.Col("n"), TypeInt)

	again := next.CastDate("e", "02.01.2006 15:04")
	assertEqual(t, next.DateLayout("e"), "")
	assertEqual(t, again.DateLayout("d"), "2.1.2006")
	assertEqual(t, again.DateLayout("e"), "02.01.2006 15:04")
}

func TestCastDate_CastDropsLayout(t *testing.T) {
	s := Schema{}.CastDate("d", "2.1.2006").CastDate("e", "2.1.2006")
	s = s.Cast("d", TypeDate)
	assertEqual(t, s.Col("d"), TypeDate)
	assertEqual(t, s.DateLayout("d"), "")
	assertEqual(t, s.DateLayout("e"), "2.1.2006")

	s = s.CastDate("e", "")
	assertEqual(t, s.DateLayout("e"), "")
}

func TestCastDate_Apply(t *testing.T) {
	tb := table.New([]string{"d", "ts"}, [][]string{
		{"5.3.2024", "05.03.2024 14:30"},
		{"", ""},
		{"15.11.2024", "15.11.2024 00:00"},
	})
	s := Schema{}.CastDate("d", "2.1.2006").CastDate("ts", "02.01.2006 15:04")
	res := s.Apply(tb)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	out := res.Unwrap()
	assertEqual(t, out.Rows[0].Get("d").UnwrapOr(""), "2024-03-05")
	assertEqual(t, out.Rows[0].Get("ts").UnwrapOr(""), "2024-03-05")
	assertEqual(t, out.Rows[1].Get("d").UnwrapOr("x"), "")
	assertEqual(t, out.Rows[2].Get("d").UnwrapOr(""), "2024-11-15")
	assertEqual(t, out.Rows[2].Get("ts").UnwrapOr(""), "2024-11-15")

	if s.ApplyStrict(tb).IsOk() {
		t.Error("ApplyStrict: expected error for empty cell")
	}
}

// A custom layout replaces the built-in layouts for that column.
func TestCastDate_Apply_Invalid(t *testing.T) {
	for _, in := range []string{"not-a-date", "2024-03-05", "1.1.0001", "31.2.2024"} {
		tb := table.New([]string{"d"}, [][]string{{in}})
		if (Schema{}).CastDate("d", "2.1.2006").Apply(tb).IsOk() {
			t.Errorf("%q: expected error", in)
		}
	}
}
