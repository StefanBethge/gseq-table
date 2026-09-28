package gtable

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// info returns the column of info column name under the default prefix.
func info(t *testing.T, tbl Table, name string) []string {
	t.Helper()
	return cells(t, tbl, DefaultInfoPrefix+name)
}

func sourceRows(t *testing.T, rr RejectedRows, name string) Table {
	t.Helper()
	tbl, ok := rr.Source(name)
	if !ok {
		t.Fatalf("no rejected rows for source %q", name)
	}
	if tbl.Err() != nil {
		t.Fatalf("source %q: %v", name, tbl.Err())
	}
	return tbl
}

func TestRowFailingInOneStepIsRejectedOnceWithAnEntryPerColumn(t *testing.T) {
	testutil.Proves(t, "T6")

	src := NewTable(
		Texts("id", "1", "2", "3"),
		Texts("a", "1", "x", "3"),
		Texts("b", "1.5", "y", "2"),
		Texts("c", "true", "z", "false"),
	)
	later := 0
	for _, run := range []struct {
		name string
		f    func(ops ...Op) Table
	}{
		{"table", func(ops ...Op) Table {
			tbl := src
			for _, op := range ops {
				tbl = tbl.Apply(op)
			}
			return tbl
		}},
		{"pipeline", func(ops ...Op) Table {
			p := From(src, 2)
			for _, op := range ops {
				p.Then(op)
			}
			tbl, err := p.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			return tbl
		}},
	} {
		later = 0
		got := run.f(
			CastAll(Cast("a", TypeInt), Cast("b", TypeFloat), Cast("c", TypeBool)),
			WhereFunc(func(Row) (bool, error) { later++; return true, nil }),
		)
		// A later step does not see the rejected row (D15).
		if later != 2 {
			t.Errorf("%s: the later step counted %d rows, want 2", run.name, later)
		}
		wantCells(t, got, "id", "1", "3")

		rr := got.RejectedRows()
		rows := sourceRows(t, rr, "code")
		if rows.Len() != 1 {
			t.Fatalf("%s: rejected rows = %d, want the row once (D44)", run.name, rows.Len())
		}
		if got := info(t, rows, "error_count"); !slices.Equal(got, []string{"3"}) {
			t.Errorf("%s: error_count = %v, want 3", run.name, got)
		}
		// The info columns describe the first error.
		if got := info(t, rows, "column"); !slices.Equal(got, []string{"a"}) {
			t.Errorf("%s: column = %v, want the first error", run.name, got)
		}

		ov := rr.Overview()
		ids := info(t, ov, "reject_id")
		if len(ids) != 3 || ids[0] != ids[1] || ids[1] != ids[2] {
			t.Errorf("%s: overview reject_ids = %v, want three equal ones", run.name, ids)
		}
		if got := info(t, ov, "column"); !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Errorf("%s: overview columns = %v", run.name, got)
		}
		if got := info(t, ov, "value"); !slices.Equal(got, []string{"x", "y", "z"}) {
			t.Errorf("%s: overview values = %v", run.name, got)
		}
		if info(t, rows, "reject_id")[0] != ids[0] {
			t.Errorf("%s: the source table and the overview differ in reject_id", run.name)
		}
	}
}

func TestRejectsPerSourceAndOverviewAgree(t *testing.T) {
	testutil.Proves(t, "T7")

	orders := NewTable(
		Texts("order", "o1", "o2", "o3", "o4"),
		Texts("customer", "1", "2", "x", "1"),
		Texts("amount", "10", "abc", "5", "-"),
		Texts("_gseq_step", "raw1", "raw2", "raw3", "raw4"), // a data column with a default-prefix name
	).AsSource("orders")
	customers := NewTable(
		Texts("id", "1", "2", "?"),
		Texts("name", "Ada", "Bo", "Cy"),
	).AsSource("customers")

	// With the default prefix, the data column collides with the reserved
	// info columns: a plan error before the run.
	wantPlanError(t, From(orders, 2).Check())

	right := customers.Cast("id", TypeInt) // "?" is rejected
	p := From(orders, 2).InfoPrefix("x_").
		Then(CastAll(Cast("customer", TypeInt), Cast("amount", TypeInt))).
		Then(InnerJoin(right, OnPair("customer", "id")))
	got, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantCells(t, got, "order", "o1")

	rr := got.RejectedRows()
	var names []string
	for _, s := range rr.Sources() {
		names = append(names, s.Source)
	}
	// The rejects of the left side come first, then those of the right
	// (D69).
	if !slices.Equal(names, []string{"orders", "customers"}) {
		t.Fatalf("sources = %v", names)
	}

	infoNames := []string{"reject_id", "run_id", "record_key", "row_key", "error_count", "source", "sheet",
		"line", "offset", "cell", "step", "column", "value", "reason", "prev_reason", "code", "raw_line"}
	prefixed := func(p string) []string {
		out := make([]string, len(infoNames))
		for i, n := range infoNames {
			out[i] = p + n
		}
		return out
	}
	// Each table has the raw columns of its source, then the info columns
	// with the changed prefix (D13, D14).
	ot, _ := rr.Source("orders")
	if want := append([]string{"order", "customer", "amount", "_gseq_step"}, prefixed("x_")...); !slices.Equal(ot.Columns(), want) {
		t.Errorf("orders columns = %v, want %v", ot.Columns(), want)
	}
	ct, _ := rr.Source("customers")
	if want := append([]string{"id", "name"}, prefixed("x_")...); !slices.Equal(ct.Columns(), want) {
		t.Errorf("customers columns = %v, want %v", ct.Columns(), want)
	}
	// The data column keeps its raw value, and the raw state is the value
	// from the source, not the cast one.
	wantCells(t, ot, "_gseq_step", "raw2", "raw3", "raw4")
	wantCells(t, ot, "amount", "abc", "5", "-")
	wantCells(t, ot, "x_source", "orders", "orders", "orders")
	wantCells(t, ot, "x_line", "1", "2", "3")
	wantCells(t, ot, "x_step", "cast_all", "cast_all", "cast_all")
	wantCells(t, ot, "x_error_count", "1", "1", "1")
	wantCells(t, ct, "id", "?")

	// The overview has exactly the info columns, one entry per error of the
	// rows in the source tables (D44).
	ov := rr.Overview()
	if !slices.Equal(ov.Columns(), prefixed("x_")) {
		t.Errorf("overview columns = %v", ov.Columns())
	}
	perKey := map[string]int{}
	for _, k := range cells(t, ov, "x_record_key") {
		perKey[k]++
	}
	total := 0
	for _, s := range rr.Sources() {
		keys := cells(t, s.Rows, "x_record_key")
		counts := cells(t, s.Rows, "x_error_count")
		for i, k := range keys {
			if want := counts[i]; want != strconv.Itoa(perKey[k]) {
				t.Errorf("%s: record %s has error_count %s, overview has %d entries", s.Source, k, want, perKey[k])
			}
			total += perKey[k]
		}
	}
	if total != ov.Len() {
		t.Errorf("overview has %d entries, the source tables account for %d", ov.Len(), total)
	}
}

func TestFailedJoinRowRejectsItsSourceRowsTogether(t *testing.T) {
	testutil.Proves(t, "T8")

	// A result row of a join fails: both source rows are rejected under one
	// reject_id (D11).
	left := NewTable(Texts("cust", "a", "b"), Ints("qty", 2, 0)).AsSource("orders")
	right := NewTable(Texts("cust", "a", "b"), Ints("price", 10, 20)).AsSource("customers")
	got := left.InnerJoin(right, On("cust")).With("unit", Col("price").Div(Col("qty")))
	wantCells(t, got, "cust", "a")
	rr := got.RejectedRows()
	ov := rr.Overview()
	if got := info(t, ov, "source"); !slices.Equal(got, []string{"orders", "customers"}) {
		t.Fatalf("overview sources = %v", got)
	}
	if ids := info(t, ov, "reject_id"); ids[0] != ids[1] {
		t.Errorf("reject_ids = %v, want one for both source rows", ids)
	}
	ok := sourceRows(t, rr, "orders")
	ck := sourceRows(t, rr, "customers")
	wantCells(t, ok, "cust", "b")
	wantCells(t, ck, "price", "20")
	rowKey := info(t, ok, "record_key")[0] + "+" + info(t, ck, "record_key")[0]
	if got := info(t, ok, "row_key"); got[0] != rowKey {
		t.Errorf("row_key = %v, want %s (D47)", got, rowKey)
	}

	// 1:n: one of five result rows of a left row fails. The other four
	// arrive, and the left row counts as passed through (D47).
	order := NewTable(Texts("cust", "m"), Ints("total", 100)).AsSource("orders")
	lines := NewTable(Texts("cust", "m", "m", "m", "m", "m"), Ints("share", 1, 2, 0, 4, 5)).AsSource("lines")
	res := order.InnerJoin(lines, On("cust")).With("part", Col("total").Div(Col("share")))
	if res.Len() != 4 {
		t.Fatalf("result rows = %d, want 4", res.Len())
	}
	if passed := passedSourceRows(res); !passed[recordOf(t, order, 0)] {
		t.Error("the left row does not count as passed through")
	}
	rr = res.RejectedRows()
	wantCells(t, sourceRows(t, rr, "lines"), "share", "0")
	wantCells(t, sourceRows(t, rr, "orders"), "total", "100")

	// A master row on the right with many failing partners stands once in
	// its table, and error_count counts its errors.
	many := NewTable(Texts("cust", "m", "m", "m", "m"), Ints("share", 0, 1, 0, 0)).AsSource("lines")
	master := NewTable(Texts("cust", "m"), Ints("total", 100)).AsSource("master")
	res = many.InnerJoin(master, On("cust")).With("part", Col("total").Div(Col("share")))
	rr = res.RejectedRows()
	mt := sourceRows(t, rr, "master")
	if mt.Len() != 1 {
		t.Fatalf("master rows = %d, want 1", mt.Len())
	}
	if got := info(t, mt, "error_count"); got[0] != "3" {
		t.Errorf("error_count = %v, want 3", got)
	}
	if n := sourceRows(t, rr, "lines").Len(); n != 3 {
		t.Errorf("lines rejected = %d, want 3", n)
	}
	if n := rr.Overview().Len(); n != 6 {
		t.Errorf("overview entries = %d, want 2 per failed result row", n)
	}
}

// passedSourceRows returns the record_keys of the source rows that reach the
// result in at least one row: they count as passed through (D43, D47).
func passedSourceRows(t Table) map[string]bool {
	out := map[string]bool{}
	for _, orig := range t.orig {
		for _, o := range orig {
			for _, r := range o.refs {
				out[r.src.recordKey(r.row)] = true
			}
		}
	}
	return out
}

func recordOf(t *testing.T, tbl Table, row int) string {
	t.Helper()
	if len(tbl.srcs) != 1 {
		t.Fatalf("table has %d sources", len(tbl.srcs))
	}
	return tbl.srcs[0].recordKey(row)
}

func TestCodeTableIsANamedSource(t *testing.T) {
	testutil.Proves(t, "T48")

	vals := Texts("n", "1", "x", "3")
	plain := NewTable(vals).Cast("n", TypeInt)
	rows := sourceRows(t, plain.RejectedRows(), "code")
	// The raw state is the values at creation, and the location is the
	// row index (D50).
	wantCells(t, rows, "n", "x")
	if got := info(t, rows, "line"); !slices.Equal(got, []string{"1"}) {
		t.Errorf("line = %v, want the row index", got)
	}
	wantCells(t, rows, DefaultInfoPrefix+"source", "code")

	named := NewTable(vals).AsSource("lieferung").Cast("n", TypeInt)
	rr := named.RejectedRows()
	if _, ok := rr.Source("code"); ok {
		t.Error("the named source is still called code")
	}
	wantCells(t, sourceRows(t, rr, "lieferung"), DefaultInfoPrefix+"source", "lieferung")

	// AsSource after an operation is a plan error.
	pe := wantPlanError(t, NewTable(vals).Select("n").AsSource("x").Err())
	if pe.Step != "as_source" {
		t.Errorf("step = %q", pe.Step)
	}
	wantPlanError(t, NewTable(vals).AsSource("").Err())

	// Two different sources of the same name in a join are a plan error,
	// at once and in a pipeline.
	a := NewTable(Texts("k", "1"), Texts("x", "a"))
	b := NewTable(Texts("k", "1"), Texts("y", "b"))
	pe = wantPlanError(t, a.InnerJoin(b, On("k")).Err())
	if !strings.Contains(pe.Error(), `"code"`) {
		t.Errorf("plan error = %v", pe)
	}
	wantPlanError(t, From(a, 1).Then(InnerJoin(b, On("k"))).Check())
	if j := a.AsSource("a").InnerJoin(b, On("k")); j.Err() != nil {
		t.Errorf("join of differently named sources: %v", j.Err())
	}
	// A self join joins one source, not two.
	if j := a.InnerJoin(a.Rename("x", "x2"), On("k")); j.Err() != nil {
		t.Errorf("self join: %v", j.Err())
	}
}

var hex16 = regexp.MustCompile(`^[0-9a-f]{16}$`)

func TestIdentifiersHaveFixedForms(t *testing.T) {
	testutil.Proves(t, "T49")

	left := NewTable(Texts("k", "1", "2"), Texts("a", "x", "1")).AsSource("left").Cast("a", TypeInt)
	right := NewTable(Texts("k", "1", "2"), Texts("b", "y", "2")).AsSource("right").Cast("b", TypeInt)
	j := left.InnerJoin(right, On("k"))
	ov := j.RejectedRows().Overview()
	runs := info(t, ov, "run_id")
	for _, r := range runs {
		if !hex16.MatchString(r) {
			t.Errorf("run_id %q is not 16 hex characters", r)
		}
	}
	if runs[0] == runs[1] {
		t.Error("two tables built with NewTable share a run_id")
	}
	ids := info(t, ov, "reject_id")
	if ids[0] == ids[1] || ids[0] != runs[0]+"-1" {
		t.Errorf("reject_ids = %v, want <run_id>-<n> and unique", ids)
	}

	// Two branches of a chain count on under one run_id.
	base := NewTable(Texts("n", "x", "y"))
	b1 := base.Cast("n", TypeInt)
	b2 := base.Cast("n", TypeFloat)
	if b1.Rejects()[0].ID == b2.Rejects()[0].ID {
		t.Errorf("two branches have the same reject_id %s", b1.Rejects()[0].ID)
	}
	if !strings.HasPrefix(b2.Rejects()[0].ID, info(t, b1.RejectedRows().Overview(), "run_id")[0]+"-") {
		t.Error("a branch has another run_id")
	}

	// record_key follows the values: equal values, equal keys.
	k1 := recordOf(t, NewTable(Texts("n", "1", "2")), 1)
	k2 := recordOf(t, NewTable(Texts("n", "1", "2")), 1)
	k3 := recordOf(t, NewTable(Texts("n", "1", "3")), 1)
	if k1 != k2 || k1 == k3 {
		t.Errorf("record_keys %s, %s, %s: want the first two equal and the third different", k1, k2, k3)
	}
	if fp, idx, ok := strings.Cut(k1, ":"); !ok || !hex16.MatchString(fp) || idx != "1" {
		t.Errorf("record_key %q is not <fingerprint>:<row index>", k1)
	}

	// The row_key of a failed join row joins the record_keys, left first.
	fail := j.With("q", Col("a").Div(Lit(0)))
	rr := fail.RejectedRows()
	lk := info(t, sourceRows(t, rr, "left"), "record_key")
	rk := info(t, sourceRows(t, rr, "right"), "record_key")
	var rowKeys []string
	for i, s := range info(t, rr.Overview(), "step") {
		if s == "with" {
			rowKeys = append(rowKeys, info(t, rr.Overview(), "row_key")[i])
		}
	}
	if len(rowKeys) != 2 || rowKeys[0] != lk[len(lk)-1]+"+"+rk[len(rk)-1] || rowKeys[0] != rowKeys[1] {
		t.Errorf("row_keys = %v, left keys %v, right keys %v", rowKeys, lk, rk)
	}
}

func TestAggregatedRejectsStandPerStepAndInTheOverview(t *testing.T) {
	testutil.Proves(t, "T50")

	src := NewTable(
		Texts("k", "a", "a", "b", "c", "c", "c"),
		Ints("n", 1, 2, 0, 9223372036854775807, 1, 0),
		Ints("m", 1, 2, 3, 9223372036854775807, 1, 0),
	)
	got := src.
		GroupBy([]string{"k"}, Sum("n"), Sum("m"), Count("n").As("rows")).
		With("inv", Lit(10).Div(Col("n")))
	wantCells(t, got, "k", "a")

	rr := got.RejectedRows()
	if len(rr.Sources()) != 0 {
		t.Errorf("aggregated rows stand in a table per source: %v", rr.Sources())
	}
	agg := rr.Aggregated()
	if len(agg) != 2 || agg[0].Step != "group_by" || agg[1].Step != "with" {
		t.Fatalf("aggregated tables = %v", agg)
	}
	// Group c overflows in both sums: one row, two errors under one id.
	g := agg[0].Rows
	wantCells(t, g, "k", "c")
	wantCells(t, g, "n", "<null>")
	wantCells(t, g, "rows", "3")
	wantCells(t, g, DefaultInfoPrefix+"error_count", "2")
	wantCells(t, g, DefaultInfoPrefix+"source_rows", "3")
	// Group b fails later, with its values as they went into the step.
	w := agg[1].Rows
	want := append([]string{"k", "n", "m", "rows"}, prefixedAgg()...)
	if !slices.Equal(w.Columns(), want) {
		t.Errorf("columns = %v, want %v", w.Columns(), want)
	}
	wantCells(t, w, "k", "b")
	wantCells(t, w, "n", "0")
	wantCells(t, w, DefaultInfoPrefix+"source_rows", "1")
	wantCells(t, w, DefaultInfoPrefix+"code", CodeExpr)

	ov := rr.Overview()
	if ov.Len() != 3 {
		t.Fatalf("overview entries = %d, want 3", ov.Len())
	}
	ids := info(t, ov, "reject_id")
	if ids[0] != ids[1] || ids[1] == ids[2] {
		t.Errorf("reject_ids = %v", ids)
	}
	wantCells(t, ov, DefaultInfoPrefix+"source", "<null>", "<null>", "<null>")
	wantCells(t, ov, DefaultInfoPrefix+"record_key", "<null>", "<null>", "<null>")
	wantCells(t, ov, DefaultInfoPrefix+"column", "n", "m", "inv")
}

func prefixedAgg() []string {
	var out []string
	for _, n := range []string{"reject_id", "run_id", "step", "column", "value", "reason", "code", "error_count", "source_rows"} {
		out = append(out, DefaultInfoPrefix+n)
	}
	return out
}

func TestCastAllAndWithAllAreOneStep(t *testing.T) {
	testutil.Proves(t, "T51")

	// Every part sees the columns before the step.
	src := NewTable(Ints("a", 1, 2), Ints("b", 10, 20))
	got := src.Apply(WithAll(With("a", Col("b")), With("b", Col("a")), With("c", Col("a").Add(Col("b")))))
	wantCells(t, got, "a", "10", "20")
	wantCells(t, got, "b", "1", "2")
	wantCells(t, got, "c", "11", "22")

	raw := NewTable(Texts("x", "1", "a", "3"), Texts("y", "b", "c", "4"))
	cast := raw.Apply(CastAll(Cast("x", TypeInt), Cast("y", TypeInt)))
	wantCells(t, cast, "x", "3")
	rs := cast.Rejects()
	if len(rs) != 3 || rs[1].ID != rs[2].ID || rs[0].ID == rs[1].ID {
		t.Errorf("rejects = %v, want one id per row and two entries for row 1", rs)
	}
	if n := sourceRows(t, cast.RejectedRows(), "code").Len(); n != 2 {
		t.Errorf("rejected rows = %d, want 2", n)
	}

	// With in WithAll: expression errors reject the row once.
	div := src.Apply(WithAll(With("p", Lit(1).Div(Col("a").Sub(Lit(1)))), With("q", Lit(1).Div(Col("a").Sub(Lit(1))))))
	if rs := div.Rejects(); len(rs) != 2 || rs[0].ID != rs[1].ID || div.Len() != 1 {
		t.Errorf("rejects = %v, rows = %d", rs, div.Len())
	}

	for name, op := range map[string]Op{
		"cast twice":      CastAll(Cast("x", TypeInt), Cast("x", TypeFloat)),
		"with twice":      WithAll(With("z", Lit(1)), With("z", Lit(2))),
		"foreign in cast": CastAll(Cast("x", TypeInt), Select("x")),
		"foreign in with": WithAll(With("z", Lit(1)), Cast("x", TypeInt)),
		"empty cast":      CastAll(),
		"empty with":      WithAll(),
		"bad part":        CastAll(Cast("nope", TypeInt)),
	} {
		if err := raw.Apply(op).Err(); err == nil {
			t.Errorf("%s: no plan error", name)
		} else {
			wantPlanError(t, err)
		}
	}
}

func TestStickyErrorKeepsTheDataBeforeTheFailedOperation(t *testing.T) {
	testutil.Proves(t, "T52")

	before := orders().Cast("amount", TypeFloat)
	failed := before.Select("nope")
	wantPlanError(t, failed.Err())
	if !slices.Equal(failed.Columns(), before.Columns()) || failed.Len() != before.Len() {
		t.Errorf("after the error: %v, %d rows; want %v, %d", failed.Columns(), failed.Len(), before.Columns(), before.Len())
	}
	wantCells(t, Table{s: failed.s, blocks: failed.blocks}, "amount", "10.5", "7")
	if len(failed.Rejects()) != 1 {
		t.Errorf("rejects = %v, want those from before", failed.Rejects())
	}
	next := failed.With("x", Lit(1)).Sort(Asc("id"))
	if !slices.Equal(next.Columns(), before.Columns()) || next.Err() != failed.Err() {
		t.Error("an operation after the error changed the table")
	}
}

// The error behavior is chosen per pipeline, per error kind and per code,
// and defaults to reject (D3, D5, D19; prepares T3 and T15).
func TestErrorPolicyPerKindAndCode(t *testing.T) {
	src := NewTable(Texts("a", "1", "x", "0"))
	ops := func(p *Pipeline) *Pipeline {
		return p.Then(Cast("a", TypeInt)).Then(With("b", Lit(1).Div(Col("a"))))
	}
	run := func(p *Pipeline) (Table, error) { return ops(p).Run(context.Background()) }

	got, err := run(From(src, 10))
	if err != nil || got.Len() != 1 || !slices.Equal(codes(got.Rejects()), []string{CodeParse, CodeExpr}) {
		t.Fatalf("default: %v, %v, %v", got.Len(), codes(got.Rejects()), err)
	}

	var de *DataError
	if _, err := run(From(src, 10).OnError(ModeStop)); !errors.As(err, &de) || de.Reject.Code != CodeParse {
		t.Errorf("stop: err = %v", err)
	}
	if _, err := run(From(src, 10).OnErrorKind(KindData, ModeStop)); !errors.As(err, &de) || de.Reject.Code != CodeParse {
		t.Errorf("stop for data errors: err = %v", err)
	}
	// A code setting beats the kind and the pipeline setting.
	if _, err := run(From(src, 10).OnError(ModeStop).OnErrorCode(CodeParse, ModeReject)); !errors.As(err, &de) || de.Reject.Code != CodeExpr {
		t.Errorf("parse rejected, expr stops: err = %v", err)
	}
	got, err = run(From(src, 10).OnErrorKind(KindData, ModeStop).OnErrorCode(CodeParse, ModeReject).OnErrorCode(CodeExpr, ModeReject))
	if err != nil || len(got.Rejects()) != 2 {
		t.Errorf("both codes rejected: %v, %v", got.Rejects(), err)
	}
	// Delivery errors have their own kind (D19, D42).
	if kindOfCode(CodeMissingColumn) != KindDelivery || kindOfCode(CodeParse) != KindData || kindOfCode("custom:x") != KindData {
		t.Error("kindOfCode")
	}
	if KindData.String() != "data" || KindDelivery.String() != "delivery" {
		t.Error("ErrorKind.String")
	}

	// The same for a table.
	tbl := src.WithErrorMode(ModeStop).WithErrorCodeMode(CodeParse, ModeReject).Cast("a", TypeInt)
	if tbl.Err() != nil || len(tbl.Rejects()) != 1 {
		t.Errorf("table, parse rejected: %v, %v", tbl.Rejects(), tbl.Err())
	}
	if e := tbl.With("b", Lit(1).Div(Col("a"))).Err(); !errors.As(e, &de) {
		t.Errorf("table, expr stops: %v", e)
	}
	if e := src.WithErrorKindMode(KindData, ModeStop).Cast("a", TypeInt).Err(); !errors.As(e, &de) {
		t.Errorf("table, data errors stop: %v", e)
	}
	wantPlanError(t, From(src, 1).InfoPrefix("").Check())
}

// The raw state is kept apart from the working data: a changed column shows
// its source value in the rejected rows (D9; prepares T1).
func TestRejectedRowsCarryTheRawState(t *testing.T) {
	src := NewTable(Texts("id", "1", "2"), Texts("v", "a", "b"))
	got := src.
		With("v", Concat(Col("v"), Lit("!"))).
		WhereFunc(func(r Row) (bool, error) {
			v, _ := r.Text("v")
			if v == "b!" {
				return false, errors.New("no b")
			}
			return true, nil
		})
	rows := sourceRows(t, got.RejectedRows(), "code")
	wantCells(t, rows, "v", "b")
	wantCells(t, rows, "id", "2")
	wantCells(t, got, "v", "a!")
	wantCells(t, src, "v", "a", "b")
}

// A reject table of a source whose raw columns use the prefix carries a
// sticky plan error (D14).
func TestRejectTableWithPrefixClash(t *testing.T) {
	src := NewTable(Texts("_gseq_x", "a"), Texts("n", "x")).Cast("n", TypeInt)
	rows, ok := src.RejectedRows().Source("code")
	if !ok {
		t.Fatal("no rejected rows")
	}
	wantPlanError(t, rows.Err())
	if _, ok := src.RejectedRows().Source("nope"); ok {
		t.Error("an unknown source has rejected rows")
	}
}
