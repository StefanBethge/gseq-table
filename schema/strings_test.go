package schema

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// strRow returns a single row with a text column "s" (and "a"/"b" for Concat).
func strRow(s string) table.Row {
	return table.New([]string{"s", "a", "b"}, [][]string{{s, "x", "y"}}).Rows[0]
}

func TestStringHelpers(t *testing.T) {
	tests := []struct {
		name string
		fn   func(table.Row) string
		in   string
		want string
	}{
		{"Trim", Trim("s"), " \t hi there \n", "hi there"},
		{"Trim/empty", Trim("s"), "   ", ""},
		{"TrimPrefix", TrimPrefix("s", "INV-"), "INV-42", "42"},
		{"TrimPrefix/absent", TrimPrefix("s", "INV-"), "42-INV-", "42-INV-"},
		{"TrimPrefix/once", TrimPrefix("s", "a"), "aab", "ab"},
		{"TrimSuffix", TrimSuffix("s", ".csv"), "data.csv", "data"},
		{"TrimSuffix/absent", TrimSuffix("s", ".csv"), "data.tsv", "data.tsv"},
		{"Lower", Lower("s"), "HeLLo ÄÖÜ", "hello äöü"},
		{"Upper", Upper("s"), "grüße", "GRÜßE"},
		{"Title", Title("s"), "aNNA-lena MÜLLER", "Anna-Lena Müller"},
		{"Title/apostrophe", Title("s"), "o'neil", "O'Neil"},
		{"Title/digits", Title("s"), "3rd  street", "3rd  Street"},
		{"Title/empty", Title("s"), "", ""},
		{"Replace", Replace("s", ",", "."), "1,234,5", "1.234.5"},
		{"Replace/none", Replace("s", ";", "."), "1,2", "1,2"},
		{"RegexReplace", RegexReplace("s", `[^0-9+]`, ""), "+49 (30) 123-45", "+493012345"},
		{"RegexReplace/groups", RegexReplace("s", `(\w+)@(\w+)`, "$2:$1"), "ann@corp", "corp:ann"},
		{"RegexReplace/named", RegexReplace("s", `(?P<y>\d{4})-(?P<m>\d{2})`, "${m}/${y}"), "2024-03", "03/2024"},
		{"RegexExtract/group", RegexExtract("s", `@(.+)$`, 1), "ann@example.com", "example.com"},
		{"RegexExtract/whole", RegexExtract("s", `\d+`, 0), "abc 123 def 456", "123"},
		{"RegexExtract/nomatch", RegexExtract("s", `@(.+)$`, 1), "no-email", ""},
		{"RegexExtract/unmatched group", RegexExtract("s", `(a)|(b)`, 2), "a", ""},
		{"RegexExtract/empty group", RegexExtract("s", `x(\d*)y`, 1), "xy", ""},
		{"SplitPart/first", SplitPart("s", " ", 0), "Anna Lena Müller", "Anna"},
		{"SplitPart/middle", SplitPart("s", " ", 1), "Anna Lena Müller", "Lena"},
		{"SplitPart/last", SplitPart("s", ".", -1), "archive.tar.gz", "gz"},
		{"SplitPart/neg", SplitPart("s", ".", -3), "archive.tar.gz", "archive"},
		{"SplitPart/out of range", SplitPart("s", ".", 3), "archive.tar.gz", ""},
		{"SplitPart/neg out of range", SplitPart("s", ".", -4), "archive.tar.gz", ""},
		{"SplitPart/no sep", SplitPart("s", ";", 0), "abc", "abc"},
		{"SplitPart/empty parts", SplitPart("s", ",", 1), "a,,c", ""},
		{"PadLeft", PadLeft("s", 5, '0'), "123", "00123"},
		{"PadLeft/long", PadLeft("s", 2, '0'), "123", "123"},
		{"PadLeft/unicode", PadLeft("s", 4, '·'), "äö", "··äö"},
		{"PadLeft/empty", PadLeft("s", 3, '0'), "", "000"},
		{"PadRight", PadRight("s", 5, '_'), "AB", "AB___"},
		{"PadRight/long", PadRight("s", 1, '_'), "AB", "AB"},
		{"PadRight/unicode", PadRight("s", 3, '_'), "ü", "ü__"},
		{"Substr", Substr("s", 0, 4), "2024-Q1", "2024"},
		{"Substr/middle", Substr("s", 5, 2), "2024-Q1", "Q1"},
		{"Substr/neg start", Substr("s", -2, -1), "2024-Q1", "Q1"},
		{"Substr/rest", Substr("s", 2, -1), "abcdef", "cdef"},
		{"Substr/clamp length", Substr("s", 4, 10), "abcdef", "ef"},
		{"Substr/start past end", Substr("s", 6, 2), "abcdef", ""},
		{"Substr/neg start clamp", Substr("s", -10, 2), "abcdef", "ab"},
		{"Substr/zero length", Substr("s", 1, 0), "abcdef", ""},
		{"Substr/unicode", Substr("s", 1, 2), "äöüß", "öü"},
		{"Substr/unicode to end", Substr("s", -1, -1), "äöüß", "ß"},
		{"Len", Len("s"), "hello", "5"},
		{"Len/unicode", Len("s"), "Müller", "6"},
		{"Len/empty", Len("s"), "", "0"},
		{"Concat", Concat(" ", "s", "a", "b"), "w", "w x y"},
		{"Concat/single", Concat("-", "a"), "w", "x"},
		{"Concat/none", Concat("-"), "w", ""},
		{"Concat/missing", Concat("-", "a", "nope", "b"), "w", "x--y"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertEqual(t, tc.fn(strRow(tc.in)), tc.want)
		})
	}
}

func TestStringHelpers_MissingColumn(t *testing.T) {
	r := strRow("value")
	fns := map[string]func(table.Row) string{
		"Trim":         Trim("nope"),
		"TrimPrefix":   TrimPrefix("nope", "x"),
		"TrimSuffix":   TrimSuffix("nope", "x"),
		"Lower":        Lower("nope"),
		"Upper":        Upper("nope"),
		"Title":        Title("nope"),
		"Replace":      Replace("nope", "", "x"),
		"RegexReplace": RegexReplace("nope", `^`, "x"),
		"RegexExtract": RegexExtract("nope", `.*`, 0),
		"SplitPart":    SplitPart("nope", ",", 0),
		"PadLeft":      PadLeft("nope", 3, '0'),
		"PadRight":     PadRight("nope", 3, '0'),
		"Substr":       Substr("nope", 0, 2),
		"Len":          Len("nope"),
	}
	for name, fn := range fns {
		t.Run(name, func(t *testing.T) {
			assertEqual(t, fn(r), "")
		})
	}
}

func TestStringHelpers_AddCol(t *testing.T) {
	tb := table.New(
		[]string{"first", "last", "email", "zip"},
		[][]string{
			{" anna ", "MÜLLER", "Anna@Example.com", "123"},
			{"bob", "smith", "bob@corp.io", "98765"},
		},
	)
	res := tb.
		AddCol("first_clean", Title("first")).
		AddCol("full", Concat(" ", "first_clean", "last")).
		AddCol("domain", RegexExtract("email", `@(.+)$`, 1)).
		AddCol("zip5", PadLeft("zip", 5, '0'))
	if len(res.Errs()) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errs())
	}
	assertEqual(t, res.Rows[0].Get("first_clean").UnwrapOr(""), " Anna ")
	assertEqual(t, res.Rows[0].Get("full").UnwrapOr(""), " Anna  MÜLLER")
	assertEqual(t, res.Rows[0].Get("domain").UnwrapOr(""), "Example.com")
	assertEqual(t, res.Rows[0].Get("zip5").UnwrapOr(""), "00123")
	assertEqual(t, res.Rows[1].Get("domain").UnwrapOr(""), "corp.io")
	assertEqual(t, res.Rows[1].Get("zip5").UnwrapOr(""), "98765")
}

// TestSplitPart_NegativeIdxReused guards against the closure mutating its
// captured index, which would make later rows resolve a different part.
func TestSplitPart_NegativeIdxReused(t *testing.T) {
	fn := SplitPart("s", ".", -1)
	for range 3 {
		assertEqual(t, fn(strRow("a.b.c")), "c")
	}
	assertEqual(t, fn(strRow("x.y")), "y")
}

func TestRegexHelpers_Panics(t *testing.T) {
	tests := map[string]func(){
		"RegexReplace/invalid": func() { RegexReplace("s", `(`, "") },
		"RegexExtract/invalid": func() { RegexExtract("s", `[`, 0) },
		"RegexExtract/group":   func() { RegexExtract("s", `(a)`, 2) },
		"RegexExtract/neg":     func() { RegexExtract("s", `(a)`, -1) },
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected panic, got none")
				}
			}()
			fn()
		})
	}
}
