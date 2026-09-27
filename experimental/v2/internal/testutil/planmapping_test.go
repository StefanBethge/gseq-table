package testutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	testPlanFile    = "30-test-plan.md"
	mappingHeading  = "Welcher Test beweist welchen Fall"
	pendingMark     = "ausstehend"
	provesFuncName  = "Proves"
	testutilPkgPath = "internal/testutil" // relative to the module root
)

func TestPlanMappingConsistency(t *testing.T) {
	root := repoRoot(t)
	modRoot := moduleRoot(t)
	for _, p := range checkPlanMapping(filepath.Join(root, designDir), modRoot) {
		t.Error(p)
	}
}

// checkPlanMapping checks the table "Welcher Test beweist welchen Fall" in the
// test plan against the Proves calls in the Go sources below modRoot, in both
// directions.
func checkPlanMapping(designDir, modRoot string) []string {
	plan, err := parseMarkdown(filepath.Join(designDir, testPlanFile))
	if err != nil {
		return []string{err.Error()}
	}
	defs, probs := collectDefs(map[string]*mdFile{plan.path: plan})
	table, tprobs := parseMappingTable(plan, defs)
	probs = append(probs, tprobs...)
	proofs, pprobs := collectProofs(modRoot, defs)
	probs = append(probs, pprobs...)
	if table == nil {
		return probs
	}

	for _, id := range sortedIDs(defs) {
		row, ok := table[id]
		if !ok {
			probs = append(probs, fmt.Sprintf("%s: %s has no row in the table %q", rel(plan.path), id, mappingHeading))
			continue
		}
		proven := proofs[id]
		for _, name := range row.tests {
			if _, ok := proven[name]; !ok {
				probs = append(probs, fmt.Sprintf("%s:%d: %s lists %s, but no test function %s calls %s(t, %q)", rel(plan.path), row.line, id, name, name, provesFuncName, id))
			}
		}
		for _, name := range sortedKeys(proven) {
			if !slices.Contains(row.tests, name) {
				what := "does not list it"
				if row.pending {
					what = "is still marked " + pendingMark
				}
				probs = append(probs, fmt.Sprintf("%s proves %s, but the row for %s in %s %s", proven[name], id, id, rel(plan.path), what))
			}
		}
	}
	return probs
}

type mappingRow struct {
	line    int
	pending bool
	tests   []string
}

var (
	mappingRowRe = regexp.MustCompile(`^\|\s*\[(T[0-9]+)\]\([^)]*\)\s*\|\s*(.*?)\s*\|\s*$`)
	testNameRe   = regexp.MustCompile("^`(Test[A-Za-z0-9_]*)`$")
)

func parseMappingTable(plan *mdFile, defs map[string]idDef) (map[string]mappingRow, []string) {
	var probs []string
	report := func(l mdLine, format string, args ...any) {
		probs = append(probs, fmt.Sprintf("%s:%d: %s", rel(plan.path), l.num, fmt.Sprintf(format, args...)))
	}
	start := -1
	for i, l := range plan.lines {
		if l.heading == 2 && l.text == mappingHeading {
			start = i + 1
			break
		}
	}
	if start < 0 {
		return nil, []string{fmt.Sprintf("%s: no section \"## %s\"", rel(plan.path), mappingHeading)}
	}
	rows := map[string]mappingRow{}
	for _, l := range plan.lines[start:] {
		if l.heading > 0 && l.heading <= 2 {
			break
		}
		if !strings.HasPrefix(l.raw, "| [") {
			continue
		}
		m := mappingRowRe.FindStringSubmatch(l.raw)
		if m == nil {
			report(l, "table row is not of the form \"| [T<n>](#anchor) | %s or `TestName`, ... |\"", pendingMark)
			continue
		}
		id, cell := m[1], m[2]
		if _, ok := defs[id]; !ok {
			report(l, "%s is not defined", id)
			continue
		}
		if prev, dup := rows[id]; dup {
			report(l, "%s has a second row (first at line %d)", id, prev.line)
			continue
		}
		row := mappingRow{line: l.num}
		if cell == pendingMark {
			row.pending = true
		} else {
			for part := range strings.SplitSeq(cell, ",") {
				tm := testNameRe.FindStringSubmatch(strings.TrimSpace(part))
				if tm == nil {
					report(l, "cell %q: want %s or test names in backticks, separated by commas", cell, pendingMark)
					row.tests = nil
					break
				}
				row.tests = append(row.tests, tm[1])
			}
		}
		rows[id] = row
	}
	return rows, probs
}

// collectProofs returns, per T case, the test functions that call Proves for
// it, mapped to their position. Proves must be called with string literals
// inside a top-level Test function. The testutil package itself is skipped:
// it defines Proves and tests it with invalid ids.
func collectProofs(modRoot string, defs map[string]idDef) (map[string]map[string]string, []string) {
	var probs []string
	proofs := map[string]map[string]string{}
	testFuncs := map[string]string{} // test name -> position, for proving tests
	skip := filepath.Join(modRoot, testutilPkgPath)
	fset := token.NewFileSet()

	err := filepath.WalkDir(modRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path == skip || name == "testdata" || name == "vendor" || (path != modRoot && strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			if path != modRoot {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir // nested module
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			probs = append(probs, err.Error())
			return nil
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			isTest := fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test")
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || !isProvesCall(call) {
					return true
				}
				pos := rel(fset.Position(call.Pos()).String())
				if !isTest {
					probs = append(probs, fmt.Sprintf("%s: %s must be called directly in a top-level Test function, not in %s", pos, provesFuncName, fn.Name.Name))
					return true
				}
				if prev, ok := testFuncs[fn.Name.Name]; ok && !strings.HasPrefix(prev, rel(path)+":") {
					probs = append(probs, fmt.Sprintf("%s: test name %s also proves T cases at %s; names in the table must be unique", pos, fn.Name.Name, prev))
				}
				testFuncs[fn.Name.Name] = pos
				for _, arg := range call.Args[1:] {
					lit, ok := arg.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						probs = append(probs, fmt.Sprintf("%s: %s arguments must be string literals", pos, provesFuncName))
						continue
					}
					id, _ := strconv.Unquote(lit.Value)
					if _, ok := defs[id]; !ok {
						probs = append(probs, fmt.Sprintf("%s: %s(t, %q): %s is not a T case of the test plan", pos, provesFuncName, id, id))
						continue
					}
					if proofs[id] == nil {
						proofs[id] = map[string]string{}
					}
					proofs[id][fn.Name.Name] = fmt.Sprintf("%s (%s)", fn.Name.Name, pos)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		probs = append(probs, err.Error())
	}
	return proofs, probs
}

func isProvesCall(call *ast.CallExpr) bool {
	if len(call.Args) < 2 {
		return false
	}
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name == provesFuncName
	case *ast.SelectorExpr:
		return fun.Sel.Name == provesFuncName
	}
	return false
}

// moduleRoot walks up from the working directory to the nearest go.mod.
func moduleRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}
		dir = parent
	}
}

func sortedIDs(defs map[string]idDef) []string {
	ids := sortedKeys(defs)
	sort.Slice(ids, func(i, j int) bool { return defs[ids[i]].num < defs[ids[j]].num })
	return ids
}
