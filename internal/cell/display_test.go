package cell

import "testing"

func TestWidth(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"abc", 3},
		{"München", 7},    // 8 bytes, 7 runes
		{"日本語", 6},        // wide
		{"ｈｉ", 4},         // fullwidth
		{"e\u0301", 1},    // combining acute accent
		{"a\u200db", 2},   // zero-width joiner
		{"👍", 2},          // emoji
		{"한국어", 6},        // Hangul
		{"\xff", 1},       // invalid UTF-8 counts as one column
		{"ab\u3000cd", 6}, // ideographic space is wide
		{"\u303f", 1},     // ideographic half fill space is narrow
		{"\U00020000", 2}, // CJK Extension B
	}
	for _, c := range cases {
		if got := Width(c.in); got != c.want {
			t.Errorf("Width(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		in    string
		width int
		want  string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello", 4, "hel…"},
		{"hello", 1, "…"},
		{"hello", 0, "hello"},
		{"hello", -1, "hello"},
		{"Müller", 3, "Mü…"},
		{"日本語", 4, "日…"}, // a wide rune that does not fit is dropped whole
		{"日本語", 5, "日本…"},
		{"e\u0301e\u0301e\u0301", 2, "e\u0301…"}, // combining mark stays with its base
	}
	for _, c := range cases {
		got := Truncate(c.in, c.width)
		if got != c.want {
			t.Errorf("Truncate(%q, %d) = %q, want %q", c.in, c.width, got, c.want)
		}
		if c.width > 0 && Width(got) > c.width {
			t.Errorf("Truncate(%q, %d) width %d exceeds limit", c.in, c.width, Width(got))
		}
	}
}

func TestPad(t *testing.T) {
	cases := []struct {
		in    string
		width int
		right bool
		want  string
	}{
		{"ab", 4, false, "ab  "},
		{"ab", 4, true, "  ab"},
		{"日本", 5, false, "日本 "},
		{"abcd", 2, false, "abcd"},
	}
	for _, c := range cases {
		if got := Pad(c.in, c.width, c.right); got != c.want {
			t.Errorf("Pad(%q, %d, %v) = %q, want %q", c.in, c.width, c.right, got, c.want)
		}
	}
}

func TestEscapeControl(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a\nb", `a\nb`},
		{"a\r\nb", `a\r\nb`},
		{"a\tb", `a\tb`},
		{"a\x00b", "a\uFFFDb"},
		{"x\u0085y", "x\uFFFDy"}, // C1 control
		{"日本\n", `日本\n`},
	}
	for _, c := range cases {
		if got := EscapeControl(c.in); got != c.want {
			t.Errorf("EscapeControl(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIsNumeric(t *testing.T) {
	yes := []string{"0", "-1", "3.14", " 42 ", "1e3", "+7", ".5"}
	no := []string{"", "abc", "1,5", "NaN", "Inf", "-inf", "1e400", "12a"}
	for _, s := range yes {
		if !IsNumeric(s) {
			t.Errorf("IsNumeric(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if IsNumeric(s) {
			t.Errorf("IsNumeric(%q) = true, want false", s)
		}
	}
}

func BenchmarkWidth(b *testing.B) {
	for _, s := range []struct{ name, v string }{
		{"ascii", "customer_12345"},
		{"unicode", "München 日本語 👍"},
	} {
		b.Run(s.name, func(b *testing.B) {
			for b.Loop() {
				_ = Width(s.v)
			}
		})
	}
}
