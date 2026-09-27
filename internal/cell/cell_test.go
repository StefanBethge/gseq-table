package cell

import (
	"math"
	"testing"
	"time"
)

func TestParseInt(t *testing.T) {
	cases := []struct {
		in      string
		bitSize int
		want    int64
		ok      bool
	}{
		{"42", 64, 42, true},
		{" -7 ", 64, -7, true},
		{"+3", 64, 3, true},
		{"127", 8, 127, true},
		{"128", 8, 0, false},
		{"1.5", 64, 0, false},
		{"1,000", 64, 0, false},
		{"", 64, 0, false},
		{"abc", 64, 0, false},
	}
	for _, c := range cases {
		got, err := ParseInt(c.in, c.bitSize)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("ParseInt(%q, %d) = %d, %v; want %d, ok=%v", c.in, c.bitSize, got, err, c.want, c.ok)
		}
	}
}

func TestParseUint(t *testing.T) {
	if n, err := ParseUint(" 255 ", 8); err != nil || n != 255 {
		t.Errorf("got %d, %v", n, err)
	}
	for _, in := range []string{"-1", "256x", ""} {
		if _, err := ParseUint(in, 8); err == nil {
			t.Errorf("ParseUint(%q) should fail", in)
		}
	}
}

func TestParseFloat(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"1.5", 1.5, true},
		{" -0.25 ", -0.25, true},
		{"1e3", 1000, true},
		{"42", 42, true},
		{"1,000", 0, false}, // thousands separators are a schema normalization concern
		{"", 0, false},
		{"abc", 0, false},
	}
	for _, c := range cases {
		got, err := ParseFloat(c.in, 64)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("ParseFloat(%q) = %v, %v; want %v, ok=%v", c.in, got, err, c.want, c.ok)
		}
	}
}

func TestParseBool(t *testing.T) {
	for in, want := range map[string]bool{
		"true": true, "TRUE": true, " yes ": true, "1": true, "Yes": true,
		"false": false, "False": false, "no": false, "NO": false, "0": false,
	} {
		got, ok := ParseBool(in)
		if !ok || got != want {
			t.Errorf("ParseBool(%q) = %v, %v; want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "maybe", "2", "y", "t"} {
		if _, ok := ParseBool(in); ok {
			t.Errorf("ParseBool(%q) should fail", in)
		}
	}
}

func TestParseDate(t *testing.T) {
	jan15 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"2024-01-15T10:00:00Z": time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		"2024-01-15T10:00:00":  time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		"2024-01-15":           jan15,
		" 2024-01-15 ":         jan15,
		"15.01.2024":           jan15,
		"01/15/2024":           jan15,
		"15 Jan 2024":          jan15,
		"Jan 15, 2024":         jan15,
	}
	for in, want := range cases {
		got, err := ParseDate(in)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseDate(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "not a date", "2024-13-01"} {
		if got, err := ParseDate(in); err == nil || !got.IsZero() {
			t.Errorf("ParseDate(%q) should fail with zero time, got %v, %v", in, got, err)
		}
	}
}

func TestParseDateLayout(t *testing.T) {
	got, err := ParseDateLayout("2006/01/02", " 2024/01/15 ")
	if err != nil || !got.Equal(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("got %v, %v", got, err)
	}
	if _, err := ParseDateLayout("2006/01/02", "2024-01-15"); err == nil {
		t.Error("expected layout mismatch error")
	}
	if _, err := ParseDateLayout("2.1.2006", "1.1.0001"); err == nil {
		t.Error("expected zero date error")
	}
}

func TestFormat(t *testing.T) {
	check := func(got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	check(FormatInt(-17), "-17")
	check(FormatUint(18446744073709551615), "18446744073709551615")
	check(FormatFloat(0.1, 64), "0.1")
	check(FormatFloat(float64(float32(0.1)), 32), "0.1")
	check(FormatFloat(math.Pi, 64), "3.141592653589793")
	check(FormatFloat(1000, 64), "1000")
	check(FormatBool(true), "true")
	check(FormatDate(time.Date(2024, 2, 29, 13, 0, 0, 0, time.UTC)), "2024-02-29")
	check(FormatTime(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)), "2024-02-29")
	check(FormatTime(time.Date(2024, 2, 29, 13, 14, 15, 0, time.UTC)), "2024-02-29T13:14:15Z")
	check(FormatTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("CET", 3600))), "2024-01-01T00:00:00+01:00")
}

func TestRoundTrip(t *testing.T) {
	for _, in := range []string{"2024-02-29", "2024-02-29T13:14:15Z", "2024-02-29T13:14:15.5+02:00"} {
		d, err := ParseDate(in)
		if err != nil {
			t.Fatal(err)
		}
		if got := FormatTime(d); got != in {
			t.Errorf("FormatTime(ParseDate(%q)) = %q", in, got)
		}
	}
	for _, in := range []string{"0", "-3.25", "1e-07", "123456.789"} {
		f, err := ParseFloat(in, 64)
		if err != nil {
			t.Fatal(err)
		}
		g, _ := ParseFloat(FormatFloat(f, 64), 64)
		if g != f {
			t.Errorf("float round trip %q: %v != %v", in, g, f)
		}
	}
}

func TestParseDate_ZeroTimeIsNotParsed(t *testing.T) {
	for _, in := range []string{
		"0001-01-01",
		" 0001-01-01 ",
		"0001-01-01T00:00:00Z",
		"0001-01-01T00:00:00",
		"01.01.0001",
		"01/01/0001",
		"01 Jan 0001",
		"Jan 01, 0001",
		"0001-01-01T01:00:00+01:00", // same instant as the zero time
	} {
		got, err := ParseDate(in)
		if err == nil {
			t.Errorf("ParseDate(%q) = %v, want error", in, got)
		}
		if !got.IsZero() {
			t.Errorf("ParseDate(%q) returned non-zero time %v on failure", in, got)
		}
	}
	// the day after the zero date is a normal date
	got, err := ParseDate("0001-01-02")
	if err != nil || !got.Equal(time.Date(1, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("ParseDate(0001-01-02) = %v, %v", got, err)
	}
}
