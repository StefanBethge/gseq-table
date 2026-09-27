package schema

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/stefanbethge/gseq-table/table"
)

// --- String column operations ---
//
// These functions return func(table.Row) string for direct use with
// table.Table.AddCol. A missing column produces an empty string, like the date
// helpers do for unparseable values. Positions, widths and lengths count runes,
// not bytes. Regular expressions are compiled once when the helper is created;
// an invalid pattern panics, like table.Table.Matches.
//
//	t.AddCol("email", schema.Lower("email"))
//	t.AddCol("zip", schema.PadLeft("zip", 5, '0'))
//	t.AddCol("domain", schema.RegexExtract("email", `@(.+)$`, 1))

// strVal returns the value of col in r, or "" and false if col is missing.
func strVal(r table.Row, col string) (string, bool) {
	return r.Get(col).Get()
}

// mapStr returns a helper that applies fn to the value of col. Missing
// columns produce "".
func mapStr(col string, fn func(string) string) func(table.Row) string {
	return func(r table.Row) string {
		v, ok := strVal(r, col)
		if !ok {
			return ""
		}
		return fn(v)
	}
}

// Trim returns a function that removes leading and trailing whitespace.
//
//	t.AddCol("name", schema.Trim("name"))
func Trim(col string) func(table.Row) string {
	return mapStr(col, strings.TrimSpace)
}

// TrimPrefix returns a function that removes prefix from the start of the
// value, if present.
//
//	t.AddCol("id", schema.TrimPrefix("id", "INV-"))
func TrimPrefix(col, prefix string) func(table.Row) string {
	return mapStr(col, func(v string) string { return strings.TrimPrefix(v, prefix) })
}

// TrimSuffix returns a function that removes suffix from the end of the
// value, if present.
//
//	t.AddCol("file", schema.TrimSuffix("file", ".csv"))
func TrimSuffix(col, suffix string) func(table.Row) string {
	return mapStr(col, func(v string) string { return strings.TrimSuffix(v, suffix) })
}

// Lower returns a function that converts the value to lower case.
//
//	t.AddCol("email", schema.Lower("email"))
func Lower(col string) func(table.Row) string {
	return mapStr(col, strings.ToLower)
}

// Upper returns a function that converts the value to upper case.
//
//	t.AddCol("country", schema.Upper("country"))
func Upper(col string) func(table.Row) string {
	return mapStr(col, strings.ToUpper)
}

// Title returns a function that upper-cases the first letter of every word
// and lower-cases the rest. A word is a run of letters and digits; any other
// rune (space, hyphen, apostrophe, …) starts a new word.
//
//	t.AddCol("name", schema.Title("name"))
//	// "aNNA-lena MÜLLER" → "Anna-Lena Müller"
func Title(col string) func(table.Row) string {
	return mapStr(col, titleCase)
}

func titleCase(v string) string {
	var b strings.Builder
	b.Grow(len(v))
	start := true
	for _, c := range v {
		if unicode.IsLetter(c) || unicode.IsDigit(c) {
			if start {
				b.WriteRune(unicode.ToTitle(c))
			} else {
				b.WriteRune(unicode.ToLower(c))
			}
			start = false
			continue
		}
		b.WriteRune(c)
		start = true
	}
	return b.String()
}

// Replace returns a function that replaces every occurrence of old with new.
//
//	t.AddCol("amount", schema.Replace("amount", ",", "."))
func Replace(col, old, new string) func(table.Row) string {
	return mapStr(col, func(v string) string { return strings.ReplaceAll(v, old, new) })
}

// RegexReplace returns a function that replaces every match of pattern with
// repl. Inside repl, $1 or ${name} refer to capture groups, as in
// regexp.Regexp.ReplaceAllString. Panics if pattern is not a valid regular
// expression.
//
//	t.AddCol("phone", schema.RegexReplace("phone", `[^0-9+]`, ""))
func RegexReplace(col, pattern, repl string) func(table.Row) string {
	re := regexp.MustCompile(pattern)
	return mapStr(col, func(v string) string { return re.ReplaceAllString(v, repl) })
}

// RegexExtract returns a function that extracts capture group group of the
// first match of pattern. Group 0 is the whole match. Produces "" when the
// value does not match. Panics if pattern is not a valid regular expression
// or group is not a group of pattern.
//
//	t.AddCol("domain", schema.RegexExtract("email", `@(.+)$`, 1))
func RegexExtract(col, pattern string, group int) func(table.Row) string {
	re := regexp.MustCompile(pattern)
	if group < 0 || group > re.NumSubexp() {
		panic("schema.RegexExtract: group " + strconv.Itoa(group) + " out of range for pattern " + strconv.Quote(pattern))
	}
	return mapStr(col, func(v string) string {
		m := re.FindStringSubmatchIndex(v)
		if m == nil || m[2*group] < 0 {
			return ""
		}
		return v[m[2*group]:m[2*group+1]]
	})
}

// SplitPart returns a function that splits the value on sep and returns the
// part at idx (0-based). A negative idx counts from the end, so -1 is the last
// part. Produces "" when idx is out of range.
//
//	t.AddCol("first", schema.SplitPart("name", " ", 0))
//	t.AddCol("ext", schema.SplitPart("file", ".", -1))
func SplitPart(col, sep string, idx int) func(table.Row) string {
	return mapStr(col, func(v string) string {
		parts := strings.Split(v, sep)
		i := idx
		if i < 0 {
			i += len(parts)
		}
		if i < 0 || i >= len(parts) {
			return ""
		}
		return parts[i]
	})
}

// PadLeft returns a function that left-pads the value with pad up to width
// runes. Values that are already at least width runes long are unchanged.
//
//	t.AddCol("zip", schema.PadLeft("zip", 5, '0')) // "123" → "00123"
func PadLeft(col string, width int, pad rune) func(table.Row) string {
	return mapStr(col, func(v string) string {
		n := width - utf8.RuneCountInString(v)
		if n <= 0 {
			return v
		}
		return strings.Repeat(string(pad), n) + v
	})
}

// PadRight returns a function that right-pads the value with pad up to width
// runes. Values that are already at least width runes long are unchanged.
//
//	t.AddCol("code", schema.PadRight("code", 8, '_')) // "AB" → "AB______"
func PadRight(col string, width int, pad rune) func(table.Row) string {
	return mapStr(col, func(v string) string {
		n := width - utf8.RuneCountInString(v)
		if n <= 0 {
			return v
		}
		return v + strings.Repeat(string(pad), n)
	})
}

// Substr returns a function that returns up to length runes starting at rune
// offset start (0-based). A negative start counts from the end; a negative
// length takes everything up to the end. The range is clamped to the value,
// so out-of-range arguments produce a shorter or empty string.
//
//	t.AddCol("year", schema.Substr("period", 0, 4))  // "2024-Q1" → "2024"
//	t.AddCol("q", schema.Substr("period", -2, -1))   // "2024-Q1" → "Q1"
func Substr(col string, start, length int) func(table.Row) string {
	return mapStr(col, func(v string) string {
		n := utf8.RuneCountInString(v)
		from := start
		if from < 0 {
			from = max(n+from, 0)
		}
		if from >= n {
			return ""
		}
		to := n
		if length >= 0 {
			to = min(from+length, n)
		}
		return v[runeOffset(v, from):runeOffset(v, to)]
	})
}

// runeOffset returns the byte offset of the i-th rune in v, or len(v) if v
// has i runes or fewer.
func runeOffset(v string, i int) int {
	for off := range v {
		if i == 0 {
			return off
		}
		i--
	}
	return len(v)
}

// Len returns a function that returns the length of the value in runes as a
// decimal string. A missing column produces "", an empty value "0".
//
//	t.AddCol("name_len", schema.Len("name"))
func Len(col string) func(table.Row) string {
	return mapStr(col, func(v string) string { return strconv.Itoa(utf8.RuneCountInString(v)) })
}

// Concat returns a function that joins the values of cols with sep. Missing
// columns contribute an empty string, like Add treats them as 0.
//
//	t.AddCol("full_name", schema.Concat(" ", "first", "last"))
func Concat(sep string, cols ...string) func(table.Row) string {
	return func(r table.Row) string {
		var b strings.Builder
		for i, col := range cols {
			if i > 0 {
				b.WriteString(sep)
			}
			v, _ := strVal(r, col)
			b.WriteString(v)
		}
		return b.String()
	}
}
