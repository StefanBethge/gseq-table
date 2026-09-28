package apiguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

func TestPublicAPIHasNoGseqTypes(t *testing.T) {
	testutil.Proves(t, "T31")
	root := moduleRoot(t)
	probs, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range probs {
		t.Error(p)
	}

	// Guard against a check that stays green because it sees nothing.
	pkgs, err := PublicPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{".", "csv", "excel"} {
		found := false
		for _, p := range pkgs {
			found = found || p == want
		}
		if !found {
			t.Errorf("public package %q not checked; checked %q", want, pkgs)
		}
	}
}

const gseqImports = `import (
	"github.com/stefanbethge/gseq/slice"
	opt "github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq-table/table"
)
`

// Self-tests: each source leaks a gseq type through the exported API, or
// must not be reported.
func TestCheckFindsLeaks(t *testing.T) {
	leaks := map[string]string{
		"param":            "func F(s slice.Slice[int]) {}",
		"result":           "func F() opt.Option[int] { var o opt.Option[int]; return o }",
		"type param bound": "func F[T slice.Slice[int]]() {}",
		"method":           "type T struct{}\nfunc (T) M() slice.Slice[int] { return nil }",
		"pointer method":   "type T[X any] struct{}\nfunc (*T[X]) M(o opt.Option[X]) {}",
		"field":            "type T struct{ S slice.Slice[int] }",
		"embedded":         "type T struct{ slice.Slice[int] }",
		"interface":        "type I interface{ M() opt.Option[int] }",
		"alias":            "type A = slice.Slice[int]",
		"defined":          "type A slice.Slice[int]",
		"var type":         "var V slice.Slice[int]",
		"var value":        "var V = slice.From(1)",
		"func type":        "type F func(slice.Slice[int])",
		"map chan":         "var V map[string]chan []*opt.Option[int]",
		"via unexported":   "type hidden struct{ S slice.Slice[int] }\nfunc F() hidden { return hidden{} }",
		"unexported method": "type hidden struct{}\nfunc (hidden) M() slice.Slice[int] { return nil }\n" +
			"func F() *hidden { return nil }",
	}
	for name, src := range leaks {
		t.Run(name, func(t *testing.T) {
			root := writeModule(t, map[string]string{"api.go": "package api\n" + gseqImports + src + "\n"})
			probs, err := Check(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(probs) == 0 {
				t.Fatalf("leak not found in:\n%s", src)
			}
			if !strings.Contains(probs[0], "api.go:") || !strings.Contains(probs[0], "github.com/stefanbethge/gseq/") {
				t.Errorf("problem lacks position or package: %s", probs[0])
			}
		})
	}
}

func TestCheckAllowsInternalUse(t *testing.T) {
	allowed := map[string]string{
		"unexported func":    "func f(s slice.Slice[int]) {}",
		"unexported field":   "type T struct{ s slice.Slice[int] }",
		"unexported method":  "type T struct{}\nfunc (T) m() slice.Slice[int] { return nil }",
		"unreachable type":   "type hidden struct{ S slice.Slice[int] }",
		"method of hidden":   "type hidden struct{}\nfunc (hidden) M() slice.Slice[int] { return nil }",
		"body":               "func F() int { return len(slice.From(1)) }",
		"unexported var":     "var v = slice.From(1)",
		"gseq-table package": "func F() table.Table { return table.Table{} }",
		"local name clash":   "type slicer struct{}\nfunc F(slice slicer) {}",
	}
	for name, src := range allowed {
		t.Run(name, func(t *testing.T) {
			root := writeModule(t, map[string]string{"api.go": "package api\n" + gseqImports + src + "\n"})
			probs, err := Check(root)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range probs {
				t.Error(p)
			}
		})
	}
}

func TestCheckSkipsNonPublicPackages(t *testing.T) {
	leak := "package x\n" + gseqImports + "func F(s slice.Slice[int]) {}\n"
	root := writeModule(t, map[string]string{
		"api.go":                "package api\n",
		"internal/x/x.go":       leak,
		"sub/internal/x/x.go":   leak,
		"testdata/x/x.go":       leak,
		".hidden/x.go":          leak,
		"api_test.go":           strings.Replace(leak, "package x", "package api", 1),
		"cmd/tool/main.go":      strings.Replace(leak, "package x", "package main", 1),
		"nested/go.mod":         "module nested\n",
		"nested/x.go":           leak,
		"leaky/leaky.go":        strings.Replace(leak, "package x", "package leaky", 1),
		"leaky/leaky_extra.txt": "not go",
	})
	probs, err := Check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(probs) != 1 || !strings.Contains(probs[0], filepath.Join("leaky", "leaky.go")) {
		t.Errorf("want exactly the leak in leaky/leaky.go, got %q", probs)
	}
	pkgs, err := PublicPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(pkgs, ",") != ".,leaky" {
		t.Errorf("PublicPackages = %q", pkgs)
	}
}

func TestCheckReportsParseErrors(t *testing.T) {
	root := writeModule(t, map[string]string{"api.go": "package api\nfunc {"})
	if _, err := Check(root); err == nil {
		t.Error("syntax error not reported")
	}
}

func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files["go.mod"] = "module example.com/api\n"
	for name, content := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func moduleRoot(t *testing.T) string {
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
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
