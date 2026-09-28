// Package apiguard checks that the exported API of the v2 module contains no
// gseq types (design decision D33, test case T31).
//
// The check works on the syntax tree, without type checking. It walks every
// exported declaration of every public package: function and method
// signatures, exported struct fields and embedded fields, interface methods,
// type definitions and aliases, and the types and initial values of exported
// variables and constants. An unexported type that such a declaration
// mentions is part of the API as well: its exported fields and methods are
// reachable, so they are checked the same way.
package apiguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// gseqModule is the module whose types must not appear in the API. Its
// sibling github.com/stefanbethge/gseq-table is a different module.
const gseqModule = "github.com/stefanbethge/gseq"

func isGseq(importPath string) bool {
	return importPath == gseqModule || strings.HasPrefix(importPath, gseqModule+"/")
}

// PublicPackages returns the directories below root, relative to it and
// sorted, whose package belongs to the public API: not under internal/ or
// testdata/, not a nested module, not hidden and not a main package.
func PublicPackages(root string) ([]string, error) {
	pkgs, err := parsePackages(root)
	if err != nil {
		return nil, err
	}
	dirs := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		dirs = append(dirs, p.dir)
	}
	return dirs, nil
}

// Check returns one problem per gseq type found in the exported API of the
// public packages below root, as "file:line: ..." with paths relative to
// root.
func Check(root string) ([]string, error) {
	pkgs, err := parsePackages(root)
	if err != nil {
		return nil, err
	}
	var probs []string
	for _, p := range pkgs {
		probs = append(probs, p.check(root)...)
	}
	sort.Strings(probs)
	return probs, nil
}

type pkg struct {
	dir   string
	fset  *token.FileSet
	files []*ast.File
}

func parsePackages(root string) ([]*pkg, error) {
	byDir := map[string]*pkg{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == root {
				return nil
			}
			name := d.Name()
			if name == "internal" || name == "testdata" || name == "vendor" ||
				strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
				return filepath.SkipDir // nested module
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		if f.Name.Name == "main" {
			return nil
		}
		dir, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		dir = filepath.ToSlash(dir)
		if byDir[dir] == nil {
			byDir[dir] = &pkg{dir: dir, fset: fset}
		}
		byDir[dir].files = append(byDir[dir].files, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	pkgs := make([]*pkg, 0, len(byDir))
	for _, p := range byDir {
		pkgs = append(pkgs, p)
	}
	sort.Slice(pkgs, func(i, j int) bool { return pkgs[i].dir < pkgs[j].dir })
	return pkgs, nil
}

type inFile[T any] struct {
	node T
	file *ast.File
}

// checker walks the API of one package.
type checker struct {
	root    string
	fset    *token.FileSet
	types   map[string]inFile[*ast.TypeSpec]
	funcs   map[string]inFile[*ast.FuncDecl]   // package-level functions
	methods map[string][]inFile[*ast.FuncDecl] // by receiver type name
	reached map[string]bool
	probs   []string
}

func (p *pkg) check(root string) []string {
	c := &checker{
		root:    root,
		fset:    p.fset,
		types:   map[string]inFile[*ast.TypeSpec]{},
		funcs:   map[string]inFile[*ast.FuncDecl]{},
		methods: map[string][]inFile[*ast.FuncDecl]{},
		reached: map[string]bool{},
	}
	for _, f := range p.files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					c.funcs[d.Name.Name] = inFile[*ast.FuncDecl]{d, f}
				} else if len(d.Recv.List) == 1 {
					recv := baseTypeName(d.Recv.List[0].Type)
					c.methods[recv] = append(c.methods[recv], inFile[*ast.FuncDecl]{d, f})
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					if ts, ok := s.(*ast.TypeSpec); ok {
						c.types[ts.Name.Name] = inFile[*ast.TypeSpec]{ts, f}
					}
				}
			}
		}
	}

	for _, f := range p.files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					c.walk(d.Type, f, d.Name.Name, false)
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							c.reach(s.Name.Name)
						}
					case *ast.ValueSpec:
						c.valueSpec(s, f)
					}
				}
			}
		}
	}
	return c.probs
}

func (c *checker) valueSpec(s *ast.ValueSpec, f *ast.File) {
	for _, name := range s.Names {
		if !name.IsExported() {
			continue
		}
		if s.Type != nil {
			c.walk(s.Type, f, name.Name, false)
		}
		for _, v := range s.Values {
			c.walk(v, f, name.Name, true)
		}
		return
	}
}

// reach marks a type of the package as part of the API and walks its
// definition and exported methods.
func (c *checker) reach(name string) {
	if c.reached[name] {
		return
	}
	c.reached[name] = true
	ts := c.types[name]
	if ts.node.TypeParams != nil {
		c.walk(ts.node.TypeParams, ts.file, name, false)
	}
	c.walk(ts.node.Type, ts.file, name, false)
	for _, m := range c.methods[name] {
		if m.node.Name.IsExported() {
			c.walk(m.node.Type, m.file, name+"."+m.node.Name.Name, false)
		}
	}
}

// walk reports gseq types in the type expression n. In a value expression
// (value is true), calls of package functions count with their results.
func (c *checker) walk(n ast.Node, f *ast.File, what string, value bool) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			if id, ok := n.X.(*ast.Ident); ok {
				if p := importPath(f, id.Name); isGseq(p) {
					pos := c.fset.Position(n.Pos())
					file, _ := filepath.Rel(c.root, pos.Filename)
					c.probs = append(c.probs, fmt.Sprintf("%s:%d: exported %s uses gseq type %s.%s (D33)", file, pos.Line, what, p, n.Sel.Name))
				}
			}
			return false
		case *ast.Ident:
			if _, ok := c.types[n.Name]; ok && !n.IsExported() {
				c.reach(n.Name)
			}
			if fn, ok := c.funcs[n.Name]; ok && value && fn.node.Type.Results != nil {
				c.walk(fn.node.Type.Results, fn.file, what, false)
			}
		case *ast.StructType:
			for _, field := range n.Fields.List {
				if exportedField(field) {
					c.walk(field.Type, f, what, value)
				}
			}
			return false
		case *ast.Field:
			c.walk(n.Type, f, what, value)
			return false
		case *ast.FuncLit:
			c.walk(n.Type, f, what, false)
			return false
		}
		return true
	})
}

// exportedField reports whether a struct field is visible outside the
// package: an exported name, or an embedded type (its promoted fields and
// methods may be).
func exportedField(field *ast.Field) bool {
	if len(field.Names) == 0 {
		return true
	}
	for _, n := range field.Names {
		if n.IsExported() {
			return true
		}
	}
	return false
}

// baseTypeName returns the type name of a method receiver such as T, *T or
// *T[X].
func baseTypeName(e ast.Expr) string {
	for {
		switch t := e.(type) {
		case *ast.StarExpr:
			e = t.X
		case *ast.IndexExpr:
			e = t.X
		case *ast.IndexListExpr:
			e = t.X
		case *ast.ParenExpr:
			e = t.X
		case *ast.Ident:
			return t.Name
		default:
			return ""
		}
	}
}

// importPath returns the path of the package that name refers to in f, or ""
// if name is not an import of f.
func importPath(f *ast.File, name string) string {
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		local := path.Base(p)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		if local == name {
			return p
		}
	}
	return ""
}
