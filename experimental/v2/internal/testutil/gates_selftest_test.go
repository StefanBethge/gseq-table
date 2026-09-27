package testutil

// Self-tests of the docs gates: a small design set in a temporary directory,
// mutated to trigger each rule. They guard against a gate that stays green
// because it has stopped checking.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a minimal design set plus a module with one proving test.
var fixture = map[string]string{
	"docs/index.md": `# Set

| Dokument | IDs |
|---|---|
| UC | [UC1](05-use-cases.md#uc1-erster-fall) – [UC2](05-use-cases.md#uc2-zweiter-fall) |
| D | [D1](10-design-decisions.md#d1-erste-entscheidung) – [D2](10-design-decisions.md#d2-uber-das-set) |
| T | [T1](30-test-plan.md#t1-erster-test) – [T2](30-test-plan.md#t2-zweiter-test) |
`,
	"docs/05-use-cases.md": `# Use Cases

### UC1 — Erster Fall

Siehe [D1](10-design-decisions.md#d1-erste-entscheidung), Code ` + "`D9`" + ` ist erlaubt.

### UC2 — Zweiter Fall

Folgt auf [UC1](#uc1-erster-fall).

` + "```" + `
UC7 in einem Codeblock ist erlaubt
` + "```" + `
`,
	"docs/10-design-decisions.md": `# Decisions

### D1 — Erste Entscheidung

**Entscheidung:** Etwas.
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-erster-fall), [UC2](05-use-cases.md#uc2-zweiter-fall)

### D2 — Über das Set

**Entscheidung:** Etwas über das Set.
**Betroffene Use Cases:** keine.
`,
	"docs/30-test-plan.md": `# Test-Plan

### T1 — Erster Test

**Beweist:** [D1](10-design-decisions.md#d1-erste-entscheidung)

### T2 — Zweiter Test

**Beweist:** [D1](10-design-decisions.md#d1-erste-entscheidung)

## Welcher Test beweist welchen Fall

| T-Fall | Tests |
|---|---|
| [T1](#t1-erster-test) | ` + "`TestOne`" + ` |
| [T2](#t2-zweiter-test) | ausstehend |
`,
	"mod/go.mod": "module example.com/mod\n\ngo 1.27\n",
	"mod/one_test.go": `package mod

import "testing"

func Proves(testing.TB, ...string) {}

func TestOne(t *testing.T) {
	t.Run("sub", func(t *testing.T) { Proves(t, "T1") })
}
`,
}

func writeFixture(t *testing.T, edits map[string]func(string) string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range fixture {
		if edit := edits[name]; edit != nil {
			content = edit(content)
		}
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name, edit := range edits {
		if _, ok := fixture[name]; !ok {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, []byte(edit("")), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir
}

func fixtureConfig(dir string) docsConfig {
	return docsConfig{
		designDir:          filepath.Join(dir, "docs"),
		floors:             map[string]int{"UC": 2, "D": 2, "T": 2},
		noUseCaseDecisions: []string{"D2"},
	}
}

func replace(old, new string) func(string) string {
	return func(s string) string { return strings.Replace(s, old, new, 1) }
}

func appendText(extra string) func(string) string {
	return func(s string) string { return s + extra }
}

func TestDocsGateFixtureIsClean(t *testing.T) {
	dir := writeFixture(t, nil)
	cfg := fixtureConfig(dir)
	// The fixture has no P, F and G families; only those it has are checked
	// for index ranges.
	for _, p := range checkDocs(cfg) {
		if !strings.Contains(p, "no range statement for family") {
			t.Error(p)
		}
	}
	for _, p := range checkPlanMapping(filepath.Join(dir, "docs"), filepath.Join(dir, "mod")) {
		t.Error(p)
	}
}

func TestDocsGateDetects(t *testing.T) {
	tests := []struct {
		name  string
		edits map[string]func(string) string
		cfg   func(*docsConfig)
		want  string
	}{
		{
			name:  "duplicate ID",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\n### UC2 — Zweiter Fall nochmal\n")},
			want:  "UC2 defined again",
		},
		{
			name:  "hole in a family",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\n### UC4 — Vierter Fall\n")},
			want:  "family UC is not contiguous: UC3 missing",
		},
		{
			name: "floor not raised",
			cfg:  func(c *docsConfig) { c.floors["D"] = 1 },
			want: "raise the floor",
		},
		{
			name: "parser gone blind",
			cfg:  func(c *docsConfig) { c.floors["D"] = 3 },
			want: "the parser may have gone blind",
		},
		{
			name:  "malformed ID heading",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\n### UC3 - Falscher Strich\n")},
			want:  `is not of the form "### <ID> — <Titel>"`,
		},
		{
			name:  "bare ID in prose",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\nWie D1 sagt.\n")},
			want:  "bare ID D1",
		},
		{
			name:  "wrong anchor",
			edits: map[string]func(string) string{"docs/05-use-cases.md": replace("#d1-erste-entscheidung", "#d1-erste")},
			want:  "anchor should be #d1-erste-entscheidung",
		},
		{
			name:  "ID link to the wrong file",
			edits: map[string]func(string) string{"docs/05-use-cases.md": replace("10-design-decisions.md#d1", "30-test-plan.md#d1")},
			want:  "D1 is defined in",
		},
		{
			name:  "undefined ID",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\nSiehe [D9](10-design-decisions.md#d9-x).\n")},
			want:  "D9 is not defined",
		},
		{
			name:  "missing target file",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\nSiehe [hier](99-fehlt.md).\n")},
			want:  "target file does not exist",
		},
		{
			name:  "missing anchor in a non-ID link",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\nSiehe [Abschnitt](30-test-plan.md#gibt-es-nicht).\n")},
			want:  "no heading with anchor #gibt-es-nicht",
		},
		{
			name:  "ID mixed into link text",
			edits: map[string]func(string) string{"docs/05-use-cases.md": appendText("\nSiehe [D1 und mehr](10-design-decisions.md#d1-erste-entscheidung).\n")},
			want:  "link text mixes an ID",
		},
		{
			name:  "stale index range",
			edits: map[string]func(string) string{"docs/index.md": replace("[D2](10-design-decisions.md#d2-uber-das-set)", "[D1](10-design-decisions.md#d1-erste-entscheidung)")},
			want:  "range D1–D1 is stale; current range is D1–D2",
		},
		{
			name:  "use case named by no decision",
			edits: map[string]func(string) string{"docs/10-design-decisions.md": replace(", [UC2](05-use-cases.md#uc2-zweiter-fall)", "")},
			want:  "UC2 is not named by any decision",
		},
		{
			name: "decision without use case and not exempt",
			cfg:  func(c *docsConfig) { c.noUseCaseDecisions = nil },
			want: "D2 names no use case",
		},
		{
			name:  "exempt decision names a use case",
			edits: map[string]func(string) string{"docs/10-design-decisions.md": replace("**Betroffene Use Cases:** keine.", "**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-erster-fall)")},
			want:  "remove it there",
		},
		{
			name:  "decision without use case field",
			edits: map[string]func(string) string{"docs/10-design-decisions.md": replace("**Betroffene Use Cases:** keine.", "")},
			want:  `D2 has no line "**Betroffene Use Cases:**"`,
		},
		{
			name: "rejected use case is exempt from coverage",
			edits: map[string]func(string) string{
				"docs/05-use-cases.md":        appendText("\n### UC3 — Verworfener Fall\n\n!!! failure \"Verworfen (2026-09-27)\"\n"),
				"docs/10-design-decisions.md": appendText("\nUnd noch UC3 ohne Link.\n"),
			},
			cfg:  func(c *docsConfig) { c.floors["UC"] = 3 },
			want: "bare ID UC3", // and nothing about UC3 coverage, checked below
		},
		{
			name:  "bare ID in an extra file",
			edits: map[string]func(string) string{"CLAUDE.md": appendText("Siehe D1.\n")},
			cfg:   func(c *docsConfig) { c.extraFiles = []string{filepath.Join(filepath.Dir(c.designDir), "CLAUDE.md")} },
			want:  "bare ID D1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeFixture(t, tt.edits)
			cfg := fixtureConfig(dir)
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			probs := checkDocs(cfg)
			if !anyContains(probs, tt.want) {
				t.Errorf("want a problem containing %q, got:\n%s", tt.want, strings.Join(probs, "\n"))
			}
			if anyContains(probs, "UC3 is not named") {
				t.Errorf("rejected use case must be exempt, got:\n%s", strings.Join(probs, "\n"))
			}
		})
	}
}

func TestPlanMappingGateDetects(t *testing.T) {
	tests := []struct {
		name  string
		edits map[string]func(string) string
		want  string
	}{
		{
			name:  "pending row although a test proves the case",
			edits: map[string]func(string) string{"mod/one_test.go": appendText("\nfunc TestTwo(t *testing.T) { Proves(t, \"T2\") }\n")},
			want:  "the row for T2 in",
		},
		{
			name:  "listed test does not prove the case",
			edits: map[string]func(string) string{"docs/30-test-plan.md": replace("| ausstehend |", "| `TestMissing` |")},
			want:  "T2 lists TestMissing, but no test function",
		},
		{
			name:  "proving test not listed",
			edits: map[string]func(string) string{"mod/one_test.go": appendText("\nfunc TestOther(t *testing.T) { Proves(t, \"T1\") }\n")},
			want:  "TestOther (",
		},
		{
			name:  "missing row",
			edits: map[string]func(string) string{"docs/30-test-plan.md": replace("| [T2](#t2-zweiter-test) | ausstehend |\n", "")},
			want:  "T2 has no row",
		},
		{
			name:  "malformed cell",
			edits: map[string]func(string) string{"docs/30-test-plan.md": replace("| ausstehend |", "| offen |")},
			want:  `cell "offen"`,
		},
		{
			name:  "unknown T case in Proves",
			edits: map[string]func(string) string{"mod/one_test.go": appendText("\nfunc TestNine(t *testing.T) { Proves(t, \"T9\") }\n")},
			want:  "T9 is not a T case",
		},
		{
			name:  "Proves outside a test function",
			edits: map[string]func(string) string{"mod/one_test.go": appendText("\nfunc helper(t *testing.T) { Proves(t, \"T2\") }\n")},
			want:  "must be called directly in a top-level Test function",
		},
		{
			name:  "non-literal argument",
			edits: map[string]func(string) string{"mod/one_test.go": appendText("\nvar id = \"T2\"\n\nfunc TestVar(t *testing.T) { Proves(t, id) }\n")},
			want:  "arguments must be string literals",
		},
		{
			name:  "missing mapping section",
			edits: map[string]func(string) string{"docs/30-test-plan.md": replace("## Welcher Test beweist welchen Fall", "## Etwas anderes")},
			want:  "no section",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writeFixture(t, tt.edits)
			probs := checkPlanMapping(filepath.Join(dir, "docs"), filepath.Join(dir, "mod"))
			if !anyContains(probs, tt.want) {
				t.Errorf("want a problem containing %q, got:\n%s", tt.want, strings.Join(probs, "\n"))
			}
		})
	}
}

func TestSlugify(t *testing.T) {
	tests := []struct{ in, want string }{
		{"D1 — Aussortierte Zeilen sind eine Tabelle", "d1-aussortierte-zeilen-sind-eine-tabelle"},
		{"D34 — Der Prototyp liegt unter experimental/v2 ohne Zusage, v2.0.0 ist", "d34-der-prototyp-liegt-unter-experimentalv2-ohne-zusage-v200-ist"},
		{"D47 — Ergebniszeilen tragen einen row_key, bei 1:n-Joins", "d47-ergebniszeilen-tragen-einen-row_key-bei-1n-joins"},
		{"T28 — sortiert die Zeile mit code=custom aus", "t28-sortiert-die-zeile-mit-codecustom-aus"},
		{"G41 — Umfang und Maß für auffällige Abweichungen", "g41-umfang-und-ma-fur-auffallige-abweichungen"},
		{"G8 — \"Immer ändern\" gegen Zweige", "g8-immer-andern-gegen-zweige"},
		{"Äußere Größe", "auere-groe"},
		{"  Leer  --  zeichen und ﬁ ", "leer-zeichen-und-fi"},
	}
	for _, tt := range tests {
		got, err := slugify(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("slugify(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
	if _, err := slugify("Łódź"); err == nil {
		t.Error("slugify: want an error for a letter without NFKD folding")
	}
}

func TestProvesRejectsInvalidIDs(t *testing.T) {
	for _, ids := range [][]string{nil, {"X1"}, {"T0"}, {"t1"}, {"T1", "D1"}} {
		ft := &fatalRecorder{TB: t}
		func() {
			defer func() { _ = recover() }()
			Proves(ft, ids...)
		}()
		if !ft.failed {
			t.Errorf("Proves(%q) did not fail", ids)
		}
	}
	ft := &fatalRecorder{TB: t}
	Proves(ft, "T1", "T43")
	if ft.failed {
		t.Error("Proves(T1, T43) failed")
	}
}

// fatalRecorder records Fatal calls instead of stopping the test and ignores
// attributes.
type fatalRecorder struct {
	testing.TB
	failed bool
}

func (f *fatalRecorder) Helper()               {}
func (f *fatalRecorder) Attr(string, string)   {}
func (f *fatalRecorder) Fatal(...any)          { f.failed = true; panic("fatal") }
func (f *fatalRecorder) Fatalf(string, ...any) { f.failed = true; panic("fatal") }

func anyContains(probs []string, want string) bool {
	for _, p := range probs {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}
