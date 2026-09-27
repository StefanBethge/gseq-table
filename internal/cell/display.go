package cell

import (
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Ellipsis marks a value that was cut short by Truncate.
const Ellipsis = "…"

// wideRanges lists the code points rendered two terminal columns wide:
// East Asian Wide and Fullwidth characters plus emoji presentation symbols.
// It is a compact approximation of Unicode's EastAsianWidth.txt, sorted by
// range start.
var wideRanges = [][2]rune{
	{0x1100, 0x115F}, {0x231A, 0x231B}, {0x2329, 0x232A}, {0x23E9, 0x23EC},
	{0x23F0, 0x23F0}, {0x23F3, 0x23F3}, {0x25FD, 0x25FE}, {0x2614, 0x2615},
	{0x2648, 0x2653}, {0x267F, 0x267F}, {0x2693, 0x2693}, {0x26A1, 0x26A1},
	{0x26AA, 0x26AB}, {0x26BD, 0x26BE}, {0x26C4, 0x26C5}, {0x26CE, 0x26CE},
	{0x26D4, 0x26D4}, {0x26EA, 0x26EA}, {0x26F2, 0x26F3}, {0x26F5, 0x26F5},
	{0x26FA, 0x26FA}, {0x26FD, 0x26FD}, {0x2705, 0x2705}, {0x270A, 0x270B},
	{0x2728, 0x2728}, {0x274C, 0x274C}, {0x274E, 0x274E}, {0x2753, 0x2755},
	{0x2757, 0x2757}, {0x2795, 0x2797}, {0x27B0, 0x27B0}, {0x27BF, 0x27BF},
	{0x2B1B, 0x2B1C}, {0x2B50, 0x2B50}, {0x2B55, 0x2B55}, {0x2E80, 0x303E},
	{0x3041, 0x33FF}, {0x3400, 0x4DBF}, {0x4E00, 0x9FFF}, {0xA000, 0xA4CF},
	{0xA960, 0xA97F}, {0xAC00, 0xD7A3}, {0xF900, 0xFAFF}, {0xFE10, 0xFE19},
	{0xFE30, 0xFE6F}, {0xFF00, 0xFF60}, {0xFFE0, 0xFFE6}, {0x16FE0, 0x16FE4},
	{0x17000, 0x18CFF}, {0x1B000, 0x1B2FF}, {0x1F004, 0x1F004}, {0x1F0CF, 0x1F0CF},
	{0x1F18E, 0x1F18E}, {0x1F191, 0x1F19A}, {0x1F200, 0x1F251}, {0x1F300, 0x1F64F},
	{0x1F680, 0x1F6FF}, {0x1F7E0, 0x1F7EB}, {0x1F90C, 0x1F9FF}, {0x1FA70, 0x1FAFF},
	{0x20000, 0x2FFFD}, {0x30000, 0x3FFFD},
}

// RuneWidth returns the number of terminal columns r occupies: 0 for
// combining marks and format characters (e.g. zero-width joiner), 2 for wide
// East Asian characters and emoji, 1 otherwise.
func RuneWidth(r rune) int {
	if r < 0x1100 {
		if r >= 0x300 && unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf) {
			return 0
		}
		return 1
	}
	if unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf) {
		return 0
	}
	i := sort.Search(len(wideRanges), func(i int) bool { return wideRanges[i][1] >= r })
	if i < len(wideRanges) && wideRanges[i][0] <= r {
		return 2
	}
	return 1
}

// Width returns the display width of s in terminal columns. It counts runes,
// not bytes, and accounts for wide and zero-width characters (see RuneWidth).
func Width(s string) int {
	w := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			w++
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		w += RuneWidth(r)
		i += size
	}
	return w
}

// Truncate shortens s to at most width display columns. A value that does not
// fit is cut on a rune boundary and ends in Ellipsis. A width of 0 or less
// returns s unchanged.
func Truncate(s string, width int) string {
	if width <= 0 || Width(s) <= width {
		return s
	}
	limit := width - 1 // reserve one column for the ellipsis
	w := 0
	end := 0
	for i, r := range s {
		rw := RuneWidth(r)
		if w+rw > limit {
			break
		}
		w += rw
		end = i + utf8.RuneLen(r)
	}
	return s[:end] + Ellipsis
}

// Pad fills s with spaces up to width display columns. Right-aligned values
// get their padding on the left. Values already at least width wide are
// returned unchanged.
func Pad(s string, width int, right bool) string {
	n := width - Width(s)
	if n <= 0 {
		return s
	}
	if right {
		return strings.Repeat(" ", n) + s
	}
	return s + strings.Repeat(" ", n)
}

// EscapeControl replaces control characters that would break a line-oriented
// layout: newline, carriage return and tab become the escapes \n, \r and \t,
// and any other control character becomes U+FFFD.
func EscapeControl(s string) string {
	clean := true
	for _, r := range s {
		if unicode.IsControl(r) {
			clean = false
			break
		}
	}
	if clean {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 2)
	for _, r := range s {
		switch {
		case r == '\n':
			sb.WriteString(`\n`)
		case r == '\r':
			sb.WriteString(`\r`)
		case r == '\t':
			sb.WriteString(`\t`)
		case unicode.IsControl(r):
			sb.WriteRune(utf8.RuneError)
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// IsNumeric reports whether s parses as a finite number under the same rules
// as ParseFloat ("NaN" and "Inf" do not count). Renderers use it to
// right-align numeric columns.
func IsNumeric(s string) bool {
	f, err := ParseFloat(s, 64)
	return err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
}
