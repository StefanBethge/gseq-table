package gtable

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// Expr is a typed expression over the columns of a row, for derived columns
// and filters (D32), for example Col("netto").Mul(Lit(1.19)). The plan checks
// an expression against the columns before the run: an unknown column or a
// type conflict is a plan error (D19). The engine evaluates it column by
// column, block by block.
//
// Nulls behave as in SQL (D30): arithmetic and functions with a null operand
// yield null, comparisons with null are null and therefore not true, And and
// Or use three-valued logic.
//
// If an expression fails for a row at run time (division by zero, overflow),
// the row is rejected with code "expr" (D54), unless the expression is
// wrapped in OrNull.
type Expr struct {
	n node
}

type node interface {
	check(s schema) (block.Kind, error)
	eval(c *evalCtx) *vec
}

// evalCtx is the evaluation of an expression over one block. reasons holds
// the reason of the first run-time failure of each row.
type evalCtx struct {
	blk     block.Block
	s       schema
	n       int
	reasons reasons
}

func newEvalCtx(blk block.Block, s schema) *evalCtx {
	return &evalCtx{blk: blk, s: s, n: blk.Len()}
}

func (c *evalCtx) fail(i int, reason string) {
	if c.reasons.at(i) == "" {
		c.reasons.set(i, reason, c.n)
	}
}

// reasons holds a reason per row, "" for none. It stays nil until a row
// fails, so that a block without failures costs nothing (D113).
type reasons []string

// at returns the reason of row i.
func (r reasons) at(i int) string {
	if r == nil {
		return ""
	}
	return r[i]
}

// set sets the reason of row i of n rows.
func (r *reasons) set(i int, reason string, n int) {
	if reason == "" && *r == nil {
		return
	}
	if *r == nil {
		*r = make(reasons, n)
	}
	(*r)[i] = reason
}

func (e Expr) check(s schema) (block.Kind, error) {
	if e.n == nil {
		return 0, errors.New("empty expression")
	}
	return e.n.check(s)
}

// Col refers to the column with the given name.
func Col(name string) Expr { return Expr{colNode{name}} }

// Lit is a constant: a string, int, int64, float64, bool or time.Time. Any
// other type is a plan error.
func Lit(v any) Expr { return Expr{litNode{v}} }

type colNode struct{ name string }

func (n colNode) check(s schema) (block.Kind, error) {
	i, err := s.lookup(n.name)
	if err != nil {
		return 0, err
	}
	return s[i].kind, nil
}

func (n colNode) eval(c *evalCtx) *vec { return vecOf(c.blk.Column(c.s.index(n.name))) }

type litNode struct{ v any }

func (n litNode) check(schema) (block.Kind, error) {
	switch n.v.(type) {
	case string:
		return block.Text, nil
	case int, int64:
		return block.Int, nil
	case float64:
		return block.Float, nil
	case bool:
		return block.Bool, nil
	case time.Time:
		return block.Timestamp, nil
	}
	return 0, fmt.Errorf("literal of unsupported type %T", n.v)
}

func (n litNode) eval(c *evalCtx) *vec {
	k, _ := n.check(nil)
	v := newVec(k, c.n)
	for i := range c.n {
		switch x := n.v.(type) {
		case string:
			v.texts[i] = x
		case int:
			v.ints[i] = int64(x)
		case int64:
			v.ints[i] = x
		case float64:
			v.flts[i] = x
		case bool:
			v.bools[i] = x
		case time.Time:
			v.times[i] = x
		}
	}
	return v
}

// fnNode is a function of its arguments, evaluated cell by cell. typ gives
// the result type for the argument types or a plan error. row computes cell
// i and returns a non-empty reason if it fails at run time. Unless nullAware
// is set, a null argument makes the result null without calling row (D30).
type fnNode struct {
	name      string
	args      []node
	typ       func(ks []block.Kind) (block.Kind, error)
	row       func(out *vec, i int, args []*vec) string
	nullAware bool
}

func (n fnNode) check(s schema) (block.Kind, error) {
	ks := make([]block.Kind, len(n.args))
	for i, a := range n.args {
		if a == nil {
			return 0, fmt.Errorf("%s: empty expression", n.name)
		}
		k, err := a.check(s)
		if err != nil {
			return 0, err
		}
		ks[i] = k
	}
	k, err := n.typ(ks)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", n.name, err)
	}
	return k, nil
}

func (n fnNode) eval(c *evalCtx) *vec {
	args := make([]*vec, len(n.args))
	ks := make([]block.Kind, len(n.args))
	for i, a := range n.args {
		args[i] = a.eval(c)
		ks[i] = args[i].kind
	}
	k, _ := n.typ(ks)
	out := newVec(k, c.n)
rows:
	for i := range c.n {
		if !n.nullAware {
			for _, a := range args {
				if a.null[i] {
					out.null[i] = true
					continue rows
				}
			}
		}
		if reason := n.row(out, i, args); reason != "" {
			out.null[i] = true
			c.fail(i, reason)
		}
	}
	return out
}

func fn(name string, typ func([]block.Kind) (block.Kind, error), row func(*vec, int, []*vec) string, args ...Expr) Expr {
	ns := make([]node, len(args))
	for i, a := range args {
		ns[i] = a.n
	}
	return Expr{fnNode{name: name, args: ns, typ: typ, row: row}}
}

func nullAwareFn(name string, typ func([]block.Kind) (block.Kind, error), row func(*vec, int, []*vec) string, args ...Expr) Expr {
	e := fn(name, typ, row, args...)
	n := e.n.(fnNode)
	n.nullAware = true
	return Expr{n}
}

// OrNull makes a run-time failure of e yield null for that row instead of
// rejecting it (D54).
func (e Expr) OrNull() Expr { return Expr{orNullNode{e.n}} }

type orNullNode struct{ x node }

func (n orNullNode) check(s schema) (block.Kind, error) {
	if n.x == nil {
		return 0, errors.New("OrNull: empty expression")
	}
	return n.x.check(s)
}

func (n orNullNode) eval(c *evalCtx) *vec {
	sub := &evalCtx{blk: c.blk, s: c.s, n: c.n}
	v := n.x.eval(sub)
	for i, r := range sub.reasons {
		if r != "" {
			v.null[i] = true
		}
	}
	return v
}

// ---------------------------------------------------------------------------
// Type rules

func isNumeric(k block.Kind) bool { return k == block.Int || k == block.Float }

// numericResult is the type of arithmetic on a and b: Int for two integers,
// otherwise Float, as an integer is widened (D71).
func numericResult(ks []block.Kind) (block.Kind, error) {
	for _, k := range ks {
		if !isNumeric(k) {
			return 0, typeConflict(ks)
		}
	}
	if slices.Contains(ks, block.Float) {
		return block.Float, nil
	}
	return block.Int, nil
}

func typeConflict(ks []block.Kind) error {
	names := make([]string, len(ks))
	for i, k := range ks {
		names[i] = k.String()
	}
	return fmt.Errorf("type conflict: %s", strings.Join(names, ", "))
}

// sameKind accepts arguments of one type, or numbers that widen to a common
// numeric type (D71).
func sameKind(ks []block.Kind) (block.Kind, error) {
	if len(ks) == 0 {
		return 0, errors.New("no arguments")
	}
	if isNumeric(ks[0]) {
		return numericResult(ks)
	}
	for _, k := range ks[1:] {
		if k != ks[0] {
			return 0, typeConflict(ks)
		}
	}
	return ks[0], nil
}

func want(res block.Kind, args ...block.Kind) func([]block.Kind) (block.Kind, error) {
	return func(ks []block.Kind) (block.Kind, error) {
		for i, k := range ks {
			if k != args[i] {
				return 0, fmt.Errorf("argument %d is %s, want %s", i+1, k, args[i])
			}
		}
		return res, nil
	}
}

func wantNumeric(ks []block.Kind) (block.Kind, error) { return numericResult(ks) }

// ---------------------------------------------------------------------------
// Arithmetic (D71: integers widen to floats, division yields a float,
// division by zero and overflow fail the row with code expr, D54)

// Add returns e + o.
func (e Expr) Add(o Expr) Expr { return arith("Add", "+", e, o) }

// Sub returns e - o.
func (e Expr) Sub(o Expr) Expr { return arith("Sub", "-", e, o) }

// Mul returns e * o.
func (e Expr) Mul(o Expr) Expr { return arith("Mul", "*", e, o) }

// Div returns e / o as a float, also for two integers (D71).
func (e Expr) Div(o Expr) Expr {
	return fn("Div", func(ks []block.Kind) (block.Kind, error) {
		if _, err := numericResult(ks); err != nil {
			return 0, err
		}
		return block.Float, nil
	}, func(out *vec, i int, a []*vec) string {
		d := a[1].float(i)
		if d == 0 {
			return fmt.Sprintf("division by zero: %s / %s", formatCell(a[0].kind, a[0], i), formatCell(a[1].kind, a[1], i))
		}
		out.flts[i] = a[0].float(i) / d
		return finite(out.flts[i])
	}, e, o)
}

// Mod returns the remainder of e / o: an integer for two integers, otherwise
// a float.
func (e Expr) Mod(o Expr) Expr {
	return fn("Mod", numericResult, func(out *vec, i int, a []*vec) string {
		if a[1].float(i) == 0 {
			return fmt.Sprintf("division by zero: %s %% %s", formatCell(a[0].kind, a[0], i), formatCell(a[1].kind, a[1], i))
		}
		if out.kind == block.Int {
			out.ints[i] = a[0].ints[i] % a[1].ints[i]
			return ""
		}
		out.flts[i] = math.Mod(a[0].float(i), a[1].float(i))
		return finite(out.flts[i])
	}, e, o)
}

func arith(name, sym string, l, r Expr) Expr {
	return fn(name, numericResult, func(out *vec, i int, a []*vec) string {
		if out.kind == block.Float {
			x, y := a[0].float(i), a[1].float(i)
			switch sym {
			case "+":
				out.flts[i] = x + y
			case "-":
				out.flts[i] = x - y
			case "*":
				out.flts[i] = x * y
			}
			return finite(out.flts[i])
		}
		x, y := a[0].ints[i], a[1].ints[i]
		v, ok := intOp(sym, x, y)
		if !ok {
			return fmt.Sprintf("integer overflow: %d %s %d", x, sym, y)
		}
		out.ints[i] = v
		return ""
	}, l, r)
}

func intOp(sym string, x, y int64) (int64, bool) {
	switch sym {
	case "+":
		s := x + y
		return s, !((x > 0 && y > 0 && s < 0) || (x < 0 && y < 0 && s >= 0))
	case "-":
		s := x - y
		return s, !((x >= 0 && y < 0 && s < 0) || (x < 0 && y > 0 && s >= 0))
	case "*":
		if x == 0 || y == 0 {
			return 0, true
		}
		p := x * y
		if p/y != x || (x == -1 && y == math.MinInt64) || (y == -1 && x == math.MinInt64) {
			return 0, false
		}
		return p, true
	}
	panic("gtable: unknown integer operator " + sym)
}

func finite(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "floating-point overflow"
	}
	return ""
}

// Neg returns -e.
func (e Expr) Neg() Expr {
	return fn("Neg", wantNumeric, func(out *vec, i int, a []*vec) string {
		if out.kind == block.Float {
			out.flts[i] = -a[0].flts[i]
			return ""
		}
		if a[0].ints[i] == math.MinInt64 {
			return fmt.Sprintf("integer overflow: -(%d)", a[0].ints[i])
		}
		out.ints[i] = -a[0].ints[i]
		return ""
	}, e)
}

// Abs returns the absolute value of e.
func (e Expr) Abs() Expr {
	return fn("Abs", wantNumeric, func(out *vec, i int, a []*vec) string {
		if out.kind == block.Float {
			out.flts[i] = math.Abs(a[0].flts[i])
			return ""
		}
		x := a[0].ints[i]
		if x == math.MinInt64 {
			return fmt.Sprintf("integer overflow: abs(%d)", x)
		}
		if x < 0 {
			x = -x
		}
		out.ints[i] = x
		return ""
	}, e)
}

// Round rounds e to the given number of decimals, half away from zero. An
// integer stays unchanged. A negative number of decimals is a plan error.
func (e Expr) Round(decimals int) Expr {
	return fn("Round", func(ks []block.Kind) (block.Kind, error) {
		if decimals < 0 {
			return 0, fmt.Errorf("negative decimals %d", decimals)
		}
		return wantNumeric(ks)
	}, func(out *vec, i int, a []*vec) string {
		if out.kind == block.Int {
			out.ints[i] = a[0].ints[i]
			return ""
		}
		x, p := a[0].flts[i], math.Pow10(decimals)
		if r := math.Round(x*p) / p; !math.IsInf(r, 0) && !math.IsNaN(r) {
			x = r // a float too large to scale has no fraction to round
		}
		out.flts[i] = x
		return ""
	}, e)
}

// ---------------------------------------------------------------------------
// Comparisons and logic

// Eq reports e == o. Numbers of both types compare by value (D71).
func (e Expr) Eq(o Expr) Expr { return compare("Eq", e, o, func(c int) bool { return c == 0 }) }

// Ne reports e != o.
func (e Expr) Ne(o Expr) Expr { return compare("Ne", e, o, func(c int) bool { return c != 0 }) }

// Lt reports e < o.
func (e Expr) Lt(o Expr) Expr { return compare("Lt", e, o, func(c int) bool { return c < 0 }) }

// Le reports e <= o.
func (e Expr) Le(o Expr) Expr { return compare("Le", e, o, func(c int) bool { return c <= 0 }) }

// Gt reports e > o.
func (e Expr) Gt(o Expr) Expr { return compare("Gt", e, o, func(c int) bool { return c > 0 }) }

// Ge reports e >= o.
func (e Expr) Ge(o Expr) Expr { return compare("Ge", e, o, func(c int) bool { return c >= 0 }) }

func compare(name string, l, r Expr, ok func(int) bool) Expr {
	return fn(name, func(ks []block.Kind) (block.Kind, error) {
		if _, err := sameKind(ks); err != nil {
			return 0, err
		}
		return block.Bool, nil
	}, func(out *vec, i int, a []*vec) string {
		out.bools[i] = ok(compareCells(a[0], i, a[1], i))
		return ""
	}, l, r)
}

// compareCells compares two non-null cells of the same or numeric kinds.
func compareCells(a *vec, i int, b *vec, j int) int {
	switch {
	case a.kind == block.Int && b.kind == block.Int:
		return cmp3(a.ints[i], b.ints[j])
	case isNumeric(a.kind):
		return cmp3(a.float(i), b.float(j))
	case a.kind == block.Text:
		return strings.Compare(a.text(i), b.text(j))
	case a.kind == block.Bool:
		x, y := a.bools[i], b.bools[j]
		switch {
		case x == y:
			return 0
		case !x:
			return -1
		}
		return 1
	case a.kind == block.Timestamp:
		return a.times[i].Compare(b.times[j])
	}
	panic("gtable: compare of " + a.kind.String())
}

func cmp3[T int64 | float64](x, y T) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

var wantBool2 = want(block.Bool, block.Bool, block.Bool)

// And is true if both are true, false if either is false, and null otherwise
// (SQL three-valued logic, D30).
func (e Expr) And(o Expr) Expr {
	return nullAwareFn("And", wantBool2, func(out *vec, i int, a []*vec) string {
		x, y := a[0], a[1]
		switch {
		case (!x.null[i] && !x.bools[i]) || (!y.null[i] && !y.bools[i]):
			out.bools[i] = false
		case x.null[i] || y.null[i]:
			out.null[i] = true
		default:
			out.bools[i] = true
		}
		return ""
	}, e, o)
}

// Or is true if either is true, false if both are false, and null otherwise
// (SQL three-valued logic, D30).
func (e Expr) Or(o Expr) Expr {
	return nullAwareFn("Or", wantBool2, func(out *vec, i int, a []*vec) string {
		x, y := a[0], a[1]
		switch {
		case (!x.null[i] && x.bools[i]) || (!y.null[i] && y.bools[i]):
			out.bools[i] = true
		case x.null[i] || y.null[i]:
			out.null[i] = true
		default:
			out.bools[i] = false
		}
		return ""
	}, e, o)
}

// Not negates a boolean; null stays null.
func (e Expr) Not() Expr {
	return fn("Not", want(block.Bool, block.Bool), func(out *vec, i int, a []*vec) string {
		out.bools[i] = !a[0].bools[i]
		return ""
	}, e)
}

// IsNull reports whether e is null. It is never null itself.
func (e Expr) IsNull() Expr { return nullTest("IsNull", e, true) }

// IsNotNull reports whether e has a value. It is never null itself.
func (e Expr) IsNotNull() Expr { return nullTest("IsNotNull", e, false) }

func nullTest(name string, e Expr, want bool) Expr {
	return nullAwareFn(name, func([]block.Kind) (block.Kind, error) { return block.Bool, nil },
		func(out *vec, i int, a []*vec) string {
			out.bools[i] = a[0].null[i] == want
			return ""
		}, e)
}

// Coalesce returns the first argument that is not null, or null. All
// arguments have one type, or are numbers that widen (D71).
func Coalesce(args ...Expr) Expr {
	return nullAwareFn("Coalesce", sameKind, func(out *vec, i int, a []*vec) string {
		for _, x := range a {
			if !x.null[i] {
				out.set(i, x, i)
				return ""
			}
		}
		out.null[i] = true
		return ""
	}, args...)
}

// ---------------------------------------------------------------------------
// Text functions (from v1 schema, D32)

var wantText = want(block.Text, block.Text)

func textFn(name string, e Expr, f func(string) string) Expr {
	return fn(name, wantText, func(out *vec, i int, a []*vec) string {
		out.texts[i] = f(a[0].text(i))
		return ""
	}, e)
}

// Trim removes leading and trailing white space.
func (e Expr) Trim() Expr { return textFn("Trim", e, strings.TrimSpace) }

// Lower converts text to lower case.
func (e Expr) Lower() Expr { return textFn("Lower", e, strings.ToLower) }

// Upper converts text to upper case.
func (e Expr) Upper() Expr { return textFn("Upper", e, strings.ToUpper) }

// Replace replaces every occurrence of old with repl.
func (e Expr) Replace(old, repl string) Expr {
	return textFn("Replace", e, func(s string) string { return strings.ReplaceAll(s, old, repl) })
}

// Len returns the number of characters of a text.
func (e Expr) Len() Expr {
	return fn("Len", want(block.Int, block.Text), func(out *vec, i int, a []*vec) string {
		out.ints[i] = int64(utf8.RuneCountInString(a[0].text(i)))
		return ""
	}, e)
}

func textTest(name string, e Expr, f func(string) bool) Expr {
	return fn(name, want(block.Bool, block.Text), func(out *vec, i int, a []*vec) string {
		out.bools[i] = f(a[0].text(i))
		return ""
	}, e)
}

// Contains reports whether the text contains sub.
func (e Expr) Contains(sub string) Expr {
	return textTest("Contains", e, func(s string) bool { return strings.Contains(s, sub) })
}

// HasPrefix reports whether the text starts with prefix.
func (e Expr) HasPrefix(prefix string) Expr {
	return textTest("HasPrefix", e, func(s string) bool { return strings.HasPrefix(s, prefix) })
}

// HasSuffix reports whether the text ends with suffix.
func (e Expr) HasSuffix(suffix string) Expr {
	return textTest("HasSuffix", e, func(s string) bool { return strings.HasSuffix(s, suffix) })
}

// Concat joins texts; a null argument makes the result null (D30).
func Concat(args ...Expr) Expr {
	return fn("Concat", func(ks []block.Kind) (block.Kind, error) {
		for i, k := range ks {
			if k != block.Text {
				return 0, fmt.Errorf("argument %d is %s, want text", i+1, k)
			}
		}
		return block.Text, nil
	}, func(out *vec, i int, a []*vec) string {
		out.appendText(i, a, i)
		return ""
	}, args...)
}

// ---------------------------------------------------------------------------
// Date functions (from v1 schema, D32)

func datePart(name string, e Expr, f func(time.Time) int) Expr {
	return fn(name, want(block.Int, block.Timestamp), func(out *vec, i int, a []*vec) string {
		out.ints[i] = int64(f(a[0].times[i]))
		return ""
	}, e)
}

// Year returns the year of a timestamp.
func (e Expr) Year() Expr { return datePart("Year", e, time.Time.Year) }

// Month returns the month of a timestamp, 1 to 12.
func (e Expr) Month() Expr {
	return datePart("Month", e, func(t time.Time) int { return int(t.Month()) })
}

// Day returns the day of the month of a timestamp.
func (e Expr) Day() Expr { return datePart("Day", e, time.Time.Day) }

// AddDays adds n calendar days to a timestamp.
func (e Expr) AddDays(n int) Expr {
	return fn("AddDays", want(block.Timestamp, block.Timestamp), func(out *vec, i int, a []*vec) string {
		out.times[i] = a[0].times[i].AddDate(0, 0, n)
		return ""
	}, e)
}

// DiffDays returns the days from o to e as a float, like v1 DateDiffDays.
func (e Expr) DiffDays(o Expr) Expr {
	return fn("DiffDays", want(block.Float, block.Timestamp, block.Timestamp), func(out *vec, i int, a []*vec) string {
		out.flts[i] = a[0].times[i].Sub(a[1].times[i]).Hours() / 24
		return ""
	}, e, o)
}

// Format formats a timestamp with a Go time layout.
func (e Expr) Format(layout string) Expr {
	return fn("Format", want(block.Text, block.Timestamp), func(out *vec, i int, a []*vec) string {
		out.texts[i] = a[0].times[i].Format(layout)
		return ""
	}, e)
}

// String returns a short description, for plan errors and debugging.
func (e Expr) String() string {
	switch n := e.n.(type) {
	case colNode:
		return "Col(" + strconv.Quote(n.name) + ")"
	case litNode:
		return fmt.Sprintf("Lit(%v)", n.v)
	case fnNode:
		return n.name + "(…)"
	case orNullNode:
		return Expr{n.x}.String() + ".OrNull()"
	}
	return "Expr{}"
}
