package gtable

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// seenRows records the rows a branch sees, for the given columns.
type seenRows struct {
	mu   sync.Mutex
	rows []map[string]string
}

// op returns a step that records every row and sets column name to "seen".
func (s *seenRows) op(name string, cols ...string) Op {
	return WithFunc(name, func(r Row) (string, error) {
		row := map[string]string{}
		for _, c := range cols {
			i := slices.Index(r.Columns(), c)
			if i < 0 {
				row[c] = "<missing>"
				continue
			}
			v := "<null>"
			if !r.IsNull(c) {
				if c == DefaultInfoPrefix+"error_count" {
					n, _ := r.Int(c)
					v = strings.Repeat("|", int(n))
				} else {
					v, _ = r.Text(c)
				}
			}
			row[c] = v
		}
		s.mu.Lock()
		s.rows = append(s.rows, row)
		s.mu.Unlock()
		return "seen", nil
	})
}

func mustRun(t *testing.T, p *Pipeline) Result {
	t.Helper()
	res, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func ic(name string) string { return DefaultInfoPrefix + name }

func TestFailBranchSeesTheStateBeforeTheStep(t *testing.T) {
	testutil.Proves(t, "T19")
	src := NewTable(
		Texts("id", "1", "2", "3", "4"),
		Texts("date", "2026-09-01", "01.09.2026", "2026-09-03", "junk"),
	)
	seen := &seenRows{}
	p := From(src, 3).
		Step("note", With("note", Col("id").Upper())).
		Step("cast", Cast("date", TypeTimestamp)).
		OnFail(NewBranch().
			Step("look", seen.op("seen", "id", "note", "date", ic("reject_id"), ic("run_id"), ic("row_key"),
				ic("error_count"), ic("step"), ic("column"), ic("value"), ic("reason"), ic("prev_reason"),
				ic("code"), ic("source"), ic("line"), ic("record_key"))).
			Step("cast_alt", Cast("date", TypeTimestamp, DateFormat("02.01.2006"))))
	res := mustRun(t, p)

	// The branch sees the failed rows in the state before the step: the
	// result of the step before, the date still as text, and the info
	// columns of the error (D25, D91).
	if len(seen.rows) != 2 {
		t.Fatalf("branch saw %d rows: %v", len(seen.rows), seen.rows)
	}
	r := seen.rows[0]
	want := map[string]string{
		"id": "2", "note": "2", "date": "01.09.2026",
		ic("error_count"): "|", ic("step"): "cast", ic("column"): "date", ic("value"): "01.09.2026",
		ic("reason"): `not a date: "01.09.2026"`, ic("prev_reason"): "<null>", ic("code"): CodeParse,
		// No location in the branch (D91).
		ic("source"): "<missing>", ic("line"): "<missing>", ic("record_key"): "<missing>",
	}
	for c, w := range want {
		if r[c] != w {
			t.Errorf("branch row: %s = %q, want %q", c, r[c], w)
		}
	}
	if r[ic("reject_id")] == "<null>" || r[ic("run_id")] == "<null>" || !strings.HasSuffix(r[ic("row_key")], ":1") {
		t.Errorf("branch row lacks ids: %v", r)
	}
	if seen.rows[1]["id"] != "4" {
		t.Errorf("second branch row = %v", seen.rows[1])
	}

	// Rows the branch processed flow back without the info columns, in
	// the input order (D25, D26, D90).
	tbl := res.Table
	if !slices.Equal(tbl.Columns(), []string{"id", "date", "note", "seen"}) {
		t.Fatalf("columns %v", tbl.Columns())
	}
	wantCells(t, tbl, "id", "1", "2", "3")
	wantCells(t, tbl, "date", "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
	wantCells(t, tbl, "seen", "<null>", "seen", "<null>")

	// What fails in the branch too is rejected.
	rs := tbl.Rejects()
	if len(rs) != 1 || rs[0].Step != "cast › cast_alt" {
		t.Errorf("rejects %v", rs)
	}
}

func TestBranchesMergeByNameAndTypeConflictsArePlanErrors(t *testing.T) {
	testutil.Proves(t, "T20")
	src := NewTable(Texts("kind", "a", "b", "a", "b"), Texts("v", "1", "2", "3", "4"))

	// Columns are matched by name; a column missing in one branch is null
	// there (D26).
	res := mustRun(t, From(src, 3).Split("route", Col("kind").Eq(Lit("a")),
		NewBranch().Step("x", With("x", Lit(int64(7)))),
		NewBranch().Step("y", With("y", Lit("rest"))).Step("v", With("v", Col("v").Upper()))))
	tbl := res.Table
	if !slices.Equal(tbl.Columns(), []string{"kind", "v", "x", "y"}) {
		t.Fatalf("columns %v", tbl.Columns())
	}
	wantCells(t, tbl, "kind", "a", "b", "a", "b")
	wantCells(t, tbl, "x", "7", "<null>", "7", "<null>")
	wantCells(t, tbl, "y", "<null>", "rest", "<null>", "rest")

	// A null condition sends the row to the second branch, like ELSE in
	// SQL (D30).
	withNull := NewTable(Texts("kind", "a", "b").WithNulls(1))
	res = mustRun(t, From(withNull, 2).Split("route", Col("kind").Eq(Lit("a")),
		NewBranch().Step("m", With("m", Lit(true))), nil))
	wantCells(t, res.Table, "m", "true", "<null>")

	// The info columns of a fail branch are gone after the return (D26).
	res = mustRun(t, From(src, 3).Step("cast", Cast("v", TypeInt)).
		OnFail(NewBranch().Step("fix", With("v", Lit(int64(0))))))
	for _, c := range res.Table.Columns() {
		if strings.HasPrefix(c, DefaultInfoPrefix) {
			t.Errorf("info column %s after the return", c)
		}
	}

	// Columns of the same name with different types are a plan error
	// before any row is read (D19, D26).
	for name, p := range map[string]*Pipeline{
		"split": From(src, 3).Split("route", Col("kind").Eq(Lit("a")),
			NewBranch().Step("x", With("x", Lit(int64(1)))),
			NewBranch().Step("x", With("x", Lit("one")))),
		"fail branch": From(src, 3).Step("cast", Cast("v", TypeInt)).
			OnFail(NewBranch().Step("keep", With("note", Lit("text stays text")))),
		"no step before OnFail": From(src, 3).OnFail(NewBranch()),
		"two fail branches":     From(src, 3).Step("cast", Cast("v", TypeInt)).OnFail(NewBranch()).OnFail(NewBranch()),
		"condition not bool":    From(src, 3).Split("route", Col("v"), nil, nil),
	} {
		res, err := p.Run(context.Background())
		wantPlanError(t, err)
		if res.Status != StatusPlanError || res.Counts.Read != 0 {
			t.Errorf("%s: status %v, counts %s", name, res.Status, countsOf(res.Counts))
		}
	}
}

func TestRowFailingAgainInTheBranchKeepsIDPathAndReasonAndCountsOnce(t *testing.T) {
	testutil.Proves(t, "T21")
	src := NewTable(
		Texts("id", "1", "2", "3", "4"),
		Texts("date", "2026-09-01", "02.09.2026", "junk", "04.09.2026"),
		Texts("amount", "1", "x", "3", "4"),
	)
	seen := &seenRows{}
	p := func() *Pipeline {
		return From(src, 2).
			Step("cast", Cast("date", TypeTimestamp)).
			OnFail(NewBranch().
				Step("look", seen.op("seen", "id", ic("reject_id"))).
				Step("cast_alt", Cast("date", TypeTimestamp, DateFormat("02.01.2006")))).
			Step("check", Cast("amount", TypeInt))
	}
	res := mustRun(t, p().Threshold(MaxRejected(2)).StepThreshold("cast", MaxRejected(1)))
	ids := map[string]string{}
	for _, r := range seen.rows {
		ids[r["id"]] = r[ic("reject_id")]
	}

	byStep := map[string]Reject{}
	for _, r := range res.Table.Rejects() {
		byStep[r.Step] = r
	}
	// Failing again in the branch: same reject_id, the whole path, and
	// the reason from the main path (D27, D92).
	again, ok := byStep["cast › cast_alt"]
	if !ok || again.ID != ids["3"] || again.PrevReason != `not a date: "junk"` ||
		again.Reason != `not a date in format "02.01.2006": "junk"` {
		t.Errorf("row failing again: %+v (branch id %s)", again, ids["3"])
	}
	// Rescued, then failing later in the main path: its history stays
	// (D46, D92).
	later, ok := byStep["cast › check"]
	if !ok || later.ID != ids["2"] || later.PrevReason != `not a date: "02.09.2026"` || later.Column != "amount" {
		t.Errorf("rescued row failing later: %+v (branch id %s)", later, ids["2"])
	}
	if len(res.Table.Rejects()) != 2 {
		t.Errorf("rejects %v", res.Table.Rejects())
	}
	ov := res.Table.RejectedRows().Overview()
	wantCells(t, ov, ic("prev_reason"), `not a date: "02.09.2026"`, `not a date: "junk"`)

	// Every source row counts once; rescued rows are passed in the step
	// and shown apart (D27, D46).
	wantBalanced(t, res)
	if countsOf(res.Counts) != "4=2+2+0+0" || res.Counts.ByCode[CodeParse] != 2 || res.Counts.Rescued != 2 {
		t.Errorf("run counts %s %v rescued %d", countsOf(res.Counts), res.Counts.ByCode, res.Counts.Rescued)
	}
	if c := stepCounts(t, res, "cast"); countsOf(c) != "4=3+1+0+0" || c.Rescued != 2 {
		t.Errorf("cast counts %s rescued %d", countsOf(c), c.Rescued)
	}
	if c := stepCounts(t, res, "cast_alt"); countsOf(c) != "3=2+1+0+0" {
		t.Errorf("cast_alt counts %s", countsOf(c))
	}
	if c := stepCounts(t, res, "check"); countsOf(c) != "3=2+1+0+0" {
		t.Errorf("check counts %s", countsOf(c))
	}
	src1, _ := res.Table.RejectedRows().Source("code")
	if src1.Len() != 2 {
		t.Errorf("per source table has %d rows", src1.Len())
	}

	// Rescued rows do not count for the threshold (D46).
	if res.Status != StatusOK {
		t.Errorf("status %v, causes %v", res.Status, res.Causes)
	}
	if res := mustRun(t, p().Threshold(MaxRejected(1))); res.Status != StatusFailedThreshold {
		t.Errorf("over the threshold: status %v", res.Status)
	}
}

// modes are the copy modes (D8).
var modes = map[string]CopyMode{"auto": CopyAuto, "always copy": CopyAlways, "in place": CopyInPlace}

func TestNoBranchSeesTheChangesOfAnotherInAnyMode(t *testing.T) {
	testutil.Proves(t, "T25")
	src := NewTable(Texts("name", "a", "b", "c", "d"), Texts("n", "1", "2", "3", "x"))
	for mode, m := range modes {
		res := mustRun(t, From(src, 4).CopyMode(m).
			Split("route", Col("name").Eq(Lit("a")).Or(Col("name").Eq(Lit("c"))),
				NewBranch().Step("up", With("name", Col("name").Upper())),
				NewBranch().Step("rest", With("name", Lit("rest")))).
			Step("cast", Cast("n", TypeInt)))
		// Each branch sees only its own change (D7, D9).
		wantCells(t, res.Table, "name", "A", "rest", "C")
		// The raw state is unchanged (D9, D64).
		raw, _ := res.Table.RejectedRows().Source("code")
		wantCells(t, raw, "name", "d")
		// In the mode "always in place" the trace notes the copy at the
		// branch (D9).
		atBranch := slices.ContainsFunc(res.Trace, func(e TraceEntry) bool {
			return e.Step == "route" && e.Event == TraceCopyAtBranch && e.Blocks == 1
		})
		if atBranch != (m == CopyInPlace) {
			t.Errorf("%s: trace %v", mode, res.Trace)
		}
	}
	// The source table is unchanged.
	wantCells(t, src, "name", "a", "b", "c", "d")
}

func TestCopyModesGiveTheSameResult(t *testing.T) {
	testutil.Proves(t, "T26")
	src := NewTable(
		Texts("id", "1", "2", "3", "4", "5", "6"),
		Texts("name", " a", "b ", "c", "d", "e", "f"),
		Texts("date", "2026-09-01", "02.09.2026", "junk", "2026-09-04", "05.09.2026", "2026-09-06"),
		Texts("amount", "1", "2", "3", "x", "5", "6"),
	)
	var want, wantRejects string
	for _, mode := range []string{"auto", "always copy", "in place"} {
		res := mustRun(t, From(src, 4).CopyMode(modes[mode]).
			Step("trim", With("name", Col("name").Trim())).
			Step("cast", Cast("date", TypeTimestamp)).
			OnFail(NewBranch().Step("cast_alt", Cast("date", TypeTimestamp, DateFormat("02.01.2006")))).
			Step("amount", Cast("amount", TypeInt)).
			Step("double", With("amount", Col("amount").Mul(Lit(int64(2))))).
			Step("upper", With("name", Col("name").Upper())).
			Step("drop", Where(Col("id").Ne(Lit("6")))))
		got := res.Table.String()
		var rs []string
		for _, r := range res.Table.Rejects() {
			r.ID = ""
			rs = append(rs, r.String()+" prev="+r.PrevReason)
		}
		raw, _ := res.Table.RejectedRows().Source("code")
		gotRejects := strings.Join(rs, "\n") + "\n" + raw.Select("id", "name", "date", "amount").String()
		if want == "" {
			want, wantRejects = got, gotRejects
			continue
		}
		if got != want {
			t.Errorf("%s: result\n%s\nwant\n%s", mode, got, want)
		}
		if gotRejects != wantRejects {
			t.Errorf("%s: rejects\n%s\nwant\n%s", mode, gotRejects, wantRejects)
		}
	}
	if !strings.Contains(wantRejects, "cast › cast_alt") || !strings.Contains(want, "B") {
		t.Errorf("unexpected result\n%s\n%s", want, wantRejects)
	}
}

func TestInPlaceCopiesARawColumnOnce(t *testing.T) {
	testutil.Proves(t, "T38")
	src := func() Source {
		return NewSource(&fakeReader{
			h:    Header{Source: "names.csv", Columns: []string{"name", "n"}, ID: "d1"},
			recs: []Record{rec(2, "a", "1"), rec(3, "b", "2"), rec(4, "c", "3"), rec(5, "d", "x")},
		})
	}
	res := mustRun(t, FromSource(src(), 2).CopyMode(CopyInPlace).
		Step("up", With("name", Col("name").Upper())).
		Step("again", With("name", Col("name").Trim())).
		Step("cast", Cast("n", TypeInt)))
	wantCells(t, res.Table, "name", "A", "B", "C")

	// The first change of the shared raw column copies it once per block;
	// the second changes it in place (D55, D64).
	if len(res.Trace) != 1 {
		t.Fatalf("trace %v", res.Trace)
	}
	if e := res.Trace[0]; e.Step != "up" || e.Column != "name" || e.Event != TraceCopyShared || e.Blocks != 2 {
		t.Errorf("trace %v", e)
	}
	// The raw state is unchanged (D9).
	raw, _ := res.Table.RejectedRows().Source("names.csv")
	wantCells(t, raw, "name", "d")

	// The automatic mode copies too, but notes nothing.
	if res := mustRun(t, FromSource(src(), 2).Step("up", With("name", Col("name").Upper()))); len(res.Trace) != 0 {
		t.Errorf("auto: trace %v", res.Trace)
	}
}

func TestBranchesKeepTheInputOrderAndHoldOnlyBlockSteps(t *testing.T) {
	testutil.Proves(t, "T60")
	src := NewTable(
		Texts("id", "1", "2", "3", "4", "5", "6", "7"),
		Texts("v", "1", "x2", "3", "x4", "x5", "6", "7"),
	)
	var want []string
	for _, n := range []int{1, 2, 3, 100} {
		res := mustRun(t, From(src, n).
			Step("cast", Cast("v", TypeInt)).
			OnFail(NewBranch().Step("strip", With("v", Col("v").Trim())).Step("zero", With("v", Lit(int64(0))))).
			Split("route", Col("v").Gt(Lit(int64(2))),
				NewBranch().Step("big", With("big", Lit(true))).Step("odd", Where(Col("id").Ne(Lit("7")))),
				nil))
		got := cells(t, res.Table, "id")
		if want == nil {
			want = got
			if !slices.Equal(got, []string{"1", "2", "3", "4", "5", "6"}) {
				t.Fatalf("block length %d: ids %v", n, got)
			}
			continue
		}
		if !slices.Equal(got, want) {
			t.Errorf("block length %d: ids %v, want %v", n, got, want)
		}
	}

	// A step over all rows in a branch, or a fail branch on one, is a plan
	// error before any row is read (D90).
	for name, p := range map[string]*Pipeline{
		"sort in a fail branch": From(src, 2).Step("cast", Cast("v", TypeInt)).
			OnFail(NewBranch().Step("sort", Sort(Asc("id")))),
		"group in a split": From(src, 2).Split("route", Col("id").Eq(Lit("1")),
			NewBranch().Step("group", GroupBy([]string{"id"}, Count("v"))), nil),
		"fail branch on a group": From(src, 2).Step("group", GroupBy([]string{"id"}, Count("v"))).
			OnFail(NewBranch()),
	} {
		res, err := p.Run(context.Background())
		pe := wantPlanError(t, err)
		if res.Counts.Read != 0 || !strings.Contains(pe.Error(), "block by block") {
			t.Errorf("%s: %v, counts %s", name, err, countsOf(res.Counts))
		}
	}
}

func TestInPlaceDoesNotChangeASourceTable(t *testing.T) {
	testutil.Proves(t, "T61")
	tbl := NewTable(Texts("n", "1", "2")).Cast("n", TypeInt)
	res := mustRun(t, From(tbl, 10).CopyMode(CopyInPlace).Step("add", With("n", Col("n").Add(Lit(int64(10))))))
	wantCells(t, res.Table, "n", "11", "12")
	wantCells(t, tbl, "n", "1", "2")
	if len(res.Trace) != 1 || res.Trace[0].Event != TraceCopyShared || res.Trace[0].Column != "n" {
		t.Errorf("trace %v", res.Trace)
	}

	// The same holds in the automatic mode and for an operation applied
	// to the table at once (D5, D93).
	res = mustRun(t, From(tbl, 10).Step("add", With("n", Col("n").Add(Lit(int64(10))))))
	wantCells(t, res.Table, "n", "11", "12")
	next := tbl.With("n", Col("n").Mul(Lit(int64(3))))
	wantCells(t, next, "n", "3", "6")
	wantCells(t, tbl, "n", "1", "2")
	// A result table changed again keeps its values too.
	again := res.Table.With("n", Lit(int64(0)))
	wantCells(t, again, "n", "0", "0")
	wantCells(t, res.Table, "n", "11", "12")
}

// A part of WithAll that reads a column another part changes sees the
// value before the step, also when the engine changes columns in place
// (D7, D78).
func TestWithAllSwapsInPlace(t *testing.T) {
	src := NewTable(Texts("a", "1", "2"), Texts("b", "x", "y")).Apply(WithAll(With("a", Col("a").Trim()), With("b", Col("b").Trim())))
	for mode, m := range modes {
		res := mustRun(t, From(src, 10).CopyMode(m).
			Step("own", WithAll(With("a", Col("a").Upper()), With("b", Col("b").Upper()))).
			Step("swap", WithAll(With("a", Col("b")), With("b", Col("a")))))
		if got := cells(t, res.Table, "a"); !slices.Equal(got, []string{"X", "Y"}) {
			t.Errorf("%s: a = %v", mode, got)
		}
		if got := cells(t, res.Table, "b"); !slices.Equal(got, []string{"1", "2"}) {
			t.Errorf("%s: b = %v", mode, got)
		}
	}
}

// After a group by, a fail branch gets aggregated rows without raw state;
// what fails there too stands in the table of its step with the values of
// the row, without the info columns of the branch (D25, D77).
func TestFailBranchTakesAggregatedRows(t *testing.T) {
	src := NewTable(Texts("k", "a", "a", "b"), Texts("v", "1", "2", "x"))
	res := mustRun(t, From(src, 2).Step("group", GroupBy([]string{"k"}, StringJoin("v", ""))).
		Step("cast", Cast("v", TypeInt)).
		OnFail(NewBranch().Step("alt", Cast("v", TypeInt, Lenient()))))
	wantCells(t, res.Table, "k", "a")
	agg := res.Table.RejectedRows().Aggregated()
	if len(agg) != 1 || agg[0].Step != "alt" {
		t.Fatalf("aggregated %v", agg)
	}
	rows := agg[0].Rows
	wantCells(t, rows, "k", "b")
	wantCells(t, rows, ic("step"), "cast › alt")
	wantCells(t, rows, ic("source_rows"), "1")
	if countsOf(res.Counts) != "3=2+1+0+0" {
		t.Errorf("counts %s", countsOf(res.Counts))
	}
}

// A row that goes into a fail branch is no error yet: the mode "stop"
// applies to rows that fail in the branch too.
func TestStopAppliesToRowsTheBranchFails(t *testing.T) {
	src := NewTable(Texts("date", "2026-09-01", "02.09.2026", "junk"))
	p := func(alt string) *Pipeline {
		return From(src, 10).OnErrorCode(CodeParse, ModeStop).
			Step("cast", Cast("date", TypeTimestamp)).
			OnFail(NewBranch().Step("cast_alt", Cast("date", TypeTimestamp, DateFormat(alt))))
	}
	rescue := NewTable(Texts("date", "2026-09-01", "02.09.2026"))
	res := mustRun(t, From(rescue, 10).OnErrorCode(CodeParse, ModeStop).
		Step("cast", Cast("date", TypeTimestamp)).
		OnFail(NewBranch().Step("cast_alt", Cast("date", TypeTimestamp, DateFormat("02.01.2006")))))
	if res.Status != StatusOK || res.Table.Len() != 2 {
		t.Errorf("rescued: status %v, %d rows", res.Status, res.Table.Len())
	}
	res, err := p("02.01.2006").Run(context.Background())
	var de *DataError
	if !errors.As(err, &de) || de.Reject.Step != "cast › cast_alt" || res.Status != StatusAborted {
		t.Errorf("failing again: err %v, status %v", err, res.Status)
	}
}
