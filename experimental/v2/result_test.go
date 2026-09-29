package gtable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// amounts returns a table source of n rows whose amount is bad in the given
// rows.
func amounts(n int, bad ...int) Table {
	ids := make([]string, n)
	vals := make([]string, n)
	for i := range n {
		ids[i] = strconv.Itoa(i)
		vals[i] = strconv.Itoa(i + 1)
		if slices.Contains(bad, i) {
			vals[i] = "x" + vals[i]
		}
	}
	return NewTable(Texts("id", ids...), Texts("amount", vals...))
}

// counts formats c as read=passed+rejected+dropped+unprocessed.
func countsOf(c Counts) string {
	return fmt.Sprintf("%d=%d+%d+%d+%d", c.Read, c.Passed, c.Rejected, c.Dropped, c.Unprocessed)
}

// wantBalanced checks read = passed + rejected + dropped + unprocessed for
// the run and every step (D43, D84).
func wantBalanced(t *testing.T, res Result) {
	t.Helper()
	all := append([]StepCounts{{Step: "run", Counts: res.Counts}}, res.Steps...)
	for _, s := range all {
		c := s.Counts
		if c.Read != c.Passed+c.Rejected+c.Dropped+c.Unprocessed {
			t.Errorf("%s: counts %s do not balance", s.Step, countsOf(c))
		}
	}
}

func stepCounts(t *testing.T, res Result, name string) Counts {
	t.Helper()
	s, ok := res.Step(name)
	if !ok {
		t.Fatalf("no counts for step %q", name)
	}
	return s.Counts
}

func TestThresholdFailsTheRunAndRunsToTheEnd(t *testing.T) {
	testutil.Proves(t, "T4")
	ctx := context.Background()
	src := amounts(10, 2, 5, 8)
	run := func(p *Pipeline) Result {
		t.Helper()
		res, err := p.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	// Over the limit, the run runs to the end and fails; every rejected
	// row is there (D4, D20).
	for name, lim := range map[string]Limit{
		"absolute": MaxRejected(2),
		"share":    MaxRejectedShare(0.2),
	} {
		res := run(From(src, 3).Then(Cast("amount", TypeInt)).Threshold(lim))
		if res.Status != StatusFailedThreshold || res.ExitCode() != 1 {
			t.Errorf("%s: status %v, exit %d", name, res.Status, res.ExitCode())
		}
		if len(res.Table.Rejects()) != 3 || res.Table.Len() != 7 || countsOf(res.Counts) != "10=7+3+0+0" {
			t.Errorf("%s: %d rejects, %d rows, counts %s", name, len(res.Table.Rejects()), res.Table.Len(), countsOf(res.Counts))
		}
		var te *ThresholdError
		if len(res.Causes) != 1 || !errors.As(res.Causes[0].Err, &te) || te.Step != "" || te.Rejected != 3 || te.Read != 10 {
			t.Errorf("%s: causes %v", name, res.Causes)
		}
	}

	// At the limit is not over it (D4).
	for _, lim := range []Limit{MaxRejected(3), MaxRejectedShare(0.3)} {
		if res := run(From(src, 3).Then(Cast("amount", TypeInt)).Threshold(lim)); res.Status != StatusOK {
			t.Errorf("%v: status %v, want ok", lim, res.Status)
		}
	}

	// Per step, the share is over the rows that went into the step: 3 of
	// 4 after the filter, while the run has 3 of 10 (D20, D45).
	keep := Col("id").Eq(Lit("2")).Or(Col("id").Eq(Lit("5"))).Or(Col("id").Eq(Lit("8"))).Or(Col("id").Eq(Lit("0")))
	p := func() *Pipeline { return From(src, 3).Then(Where(keep)).Then(Cast("amount", TypeInt)) }
	if res := run(p().Threshold(MaxRejectedShare(0.5))); res.Status != StatusOK {
		t.Errorf("run share: status %v, want ok", res.Status)
	}
	res := run(p().StepThreshold("cast", MaxRejectedShare(0.5)))
	var te *ThresholdError
	if res.Status != StatusFailedThreshold || len(res.Causes) != 1 || !errors.As(res.Causes[0].Err, &te) || te.Step != "cast" || te.Read != 4 {
		t.Errorf("step share: status %v, causes %v", res.Status, res.Causes)
	}

	// With the abort option, the run ends as soon as the limit is over.
	res, err := From(src, 2).Then(Cast("amount", TypeInt)).Threshold(MaxRejected(1)).AbortOnThreshold(0).Run(ctx)
	if !errors.As(err, &te) || res.Status != StatusFailedThreshold {
		t.Fatalf("abort: err %v, status %v", err, res.Status)
	}
	if len(res.Table.Rejects()) != 2 || res.Counts.Read != 6 || !errors.Is(res.Table.Err(), err) {
		t.Errorf("abort: %d rejects, counts %s, sticky %v", len(res.Table.Rejects()), countsOf(res.Counts), res.Table.Err())
	}

	// A step threshold names a step of the plan; a limit is in range.
	for name, p := range map[string]*Pipeline{
		"unknown step":   From(src, 3).StepThreshold("nope", MaxRejected(1)),
		"negative rows":  From(src, 3).Threshold(MaxRejected(-1)),
		"share over one": From(src, 3).Threshold(MaxRejectedShare(1.5)),
		"negative min":   From(src, 3).AbortOnThreshold(-1),
	} {
		res, err := p.Run(ctx)
		wantPlanError(t, err)
		if res.Status != StatusPlanError {
			t.Errorf("%s: status %v", name, res.Status)
		}
	}
}

func TestThresholdAbortsAfterTheMinimumForAShareAndAtOnceForAnAbsoluteLimit(t *testing.T) {
	testutil.Proves(t, "T43")
	ctx := context.Background()
	src := amounts(10, 0, 1, 2)
	p := func() *Pipeline { return From(src, 1).Then(Cast("amount", TypeInt)) }

	// The share is over at once, but only aborts after 5 rows (D45, D89).
	res, err := p().Threshold(MaxRejectedShare(0.1)).AbortOnThreshold(5).Run(ctx)
	var te *ThresholdError
	if !errors.As(err, &te) || res.Status != StatusFailedThreshold || res.Counts.Read != 5 || te.Read != 5 {
		t.Errorf("share: err %v, status %v, counts %s", err, res.Status, countsOf(res.Counts))
	}
	// Under the minimum until the end: the run ends and fails.
	res, err = p().Threshold(MaxRejectedShare(0.1)).AbortOnThreshold(100).Run(ctx)
	if err != nil || res.Status != StatusFailedThreshold || res.Counts.Read != 10 {
		t.Errorf("minimum not reached: err %v, status %v, counts %s", err, res.Status, countsOf(res.Counts))
	}
	// An absolute limit aborts at once, whatever the minimum.
	res, err = p().Threshold(MaxRejected(1)).AbortOnThreshold(100).Run(ctx)
	if !errors.As(err, &te) || res.Status != StatusFailedThreshold || res.Counts.Read != 2 {
		t.Errorf("absolute: err %v, status %v, counts %s", err, res.Status, countsOf(res.Counts))
	}
	// With both, one is enough.
	res, err = p().Threshold(MaxRejected(100), MaxRejectedShare(0.1)).AbortOnThreshold(5).Run(ctx)
	if !errors.As(err, &te) || res.Counts.Read != 5 {
		t.Errorf("both: err %v, counts %s", err, countsOf(res.Counts))
	}
	wantBalanced(t, res)
}

func TestDirtyDeliveryRunsThroughWithDefaults(t *testing.T) {
	testutil.Proves(t, "T5")
	r := &fakeReader{
		h: Header{Source: "d.csv", Columns: []string{"id", "amount"}, ID: "fp"},
		recs: []Record{
			rec(2, "1", "10"),
			rec(3, "2", "zehn"), // does not parse
			rec(4, "3"),         // cannot be split
			rec(5, "4", "-5"),   // fails the validation
			rec(6, "5", "20"),
		},
	}
	validate := WithFunc("checked", func(row Row) (int64, error) {
		v, _ := row.Int("amount")
		if v < 0 {
			return 0, errors.New("negative amount")
		}
		return v, nil
	})
	res, err := FromSource(NewSource(r), 2).Then(Cast("amount", TypeInt)).Then(validate).Run(context.Background())
	if err != nil || res.Status != StatusOK || res.ExitCode() != 0 || len(res.Causes) != 0 {
		t.Fatalf("err %v, status %v, causes %v", err, res.Status, res.Causes)
	}
	wantCells(t, res.Table, "id", "1", "5")
	if got := codes(res.Table.Rejects()); !slices.Equal(got, []string{CodeParse, CodeUnparseableLine, CodeCustom}) {
		t.Errorf("codes = %q", got)
	}
	if countsOf(res.Counts) != "5=2+3+0+0" {
		t.Errorf("counts %s", countsOf(res.Counts))
	}
}

func TestHeaderCheckFindsMissingNewAndRenamedColumns(t *testing.T) {
	testutil.Proves(t, "T14")
	ctx := context.Background()
	reader := func() *fakeReader {
		return &fakeReader{
			h:    Header{Source: "d.csv", Columns: []string{"id", "kunden_nr", "extra"}, ID: "fp"},
			recs: []Record{rec(2, "1", "K1", "x"), rec(3, "2", "K2", "y")},
		}
	}
	src := func() Source { return NewSource(reader()).Expect("id", "Kunden Nr") }

	// Reject mode: every row is rejected with missing_column, the run runs
	// to the end, and the status is delivery_error (D19, D42).
	res, err := FromSource(src(), 10).Run(ctx)
	if err != nil || res.Status != StatusDeliveryError || res.ExitCode() != 3 {
		t.Fatalf("err %v, status %v", err, res.Status)
	}
	if got := codes(res.Table.Rejects()); !slices.Equal(got, []string{CodeMissingColumn, CodeMissingColumn}) {
		t.Errorf("codes = %q", got)
	}
	var de *DeliveryError
	if len(res.Causes) != 1 || !errors.As(res.Causes[0].Err, &de) || de.Code != CodeMissingColumn {
		t.Errorf("causes %v", res.Causes)
	}
	want := []string{
		"missing_column:Kunden Nr:",
		"new_column:kunden_nr:",
		"new_column:extra:",
		"probably_renamed:Kunden Nr:kunden_nr",
	}
	if got := findingKinds(res.Report); !slices.Equal(got, want) {
		t.Errorf("report = %q, want %q", got, want)
	}
	// The new column is passed on (D22).
	if !slices.Contains(res.Table.Columns(), "extra") {
		t.Errorf("columns %q", res.Table.Columns())
	}

	// Stop mode stops, and the status is still delivery_error (D63).
	res, err = FromSource(src(), 10).OnError(ModeStop).Run(ctx)
	if !errors.As(err, &de) || de.Code != CodeMissingColumn || res.Status != StatusDeliveryError {
		t.Errorf("stop: err %v, status %v", err, res.Status)
	}
	if got := findingKinds(res.Report); !slices.Equal(got, want) {
		t.Errorf("stop: report = %q", got)
	}
}

func TestPlanErrorsStopTheRunAndOtherErrorsFollowTheirCode(t *testing.T) {
	testutil.Proves(t, "T15")
	ctx := context.Background()

	// A plan error: no row is read, in any mode (D19, D32).
	for _, m := range []ErrorMode{ModeReject, ModeStop} {
		for name, op := range map[string]Op{
			"unknown column": With("x", Col("nope").Add(Lit(1))),
			"type conflict":  With("x", Col("amount").Add(Lit(1))),
		} {
			src := &countingSource{tableSource: tableSource{delivery(10)}}
			p := &Pipeline{src: src, blockLen: 2}
			res, err := p.OnError(m).Then(op).Run(ctx)
			wantPlanError(t, err)
			if res.Status != StatusPlanError || res.ExitCode() != 5 || src.read != 0 {
				t.Errorf("%s, %v: status %v, %d rows read", name, m, res.Status, src.read)
			}
			if len(res.Causes) != 1 || res.Causes[0].Status != StatusPlanError {
				t.Errorf("%s: causes %v", name, res.Causes)
			}
		}
	}

	// parse rejects, missing_column stops (D19, D42).
	reader := func(cols ...string) *fakeReader {
		return &fakeReader{
			h:    Header{Source: "d.csv", Columns: cols, ID: "fp"},
			recs: []Record{rec(2, "1", "x"), rec(3, "2", "3")},
		}
	}
	p := func(r Reader) *Pipeline {
		return FromSource(NewSource(r).Expect("id", "amount"), 2).Then(Cast("amount", TypeInt)).
			OnError(ModeStop).OnErrorCode(CodeParse, ModeReject).OnErrorCode(CodeMissingColumn, ModeStop)
	}
	res, err := p(reader("id", "amount")).Run(ctx)
	if err != nil || res.Status != StatusOK || !slices.Equal(codes(res.Table.Rejects()), []string{CodeParse}) {
		t.Errorf("parse: err %v, status %v, rejects %v", err, res.Status, res.Table.Rejects())
	}
	res, err = p(reader("id", "betrag")).Run(ctx)
	var de *DeliveryError
	if !errors.As(err, &de) || res.Status != StatusDeliveryError {
		t.Errorf("missing column: err %v, status %v", err, res.Status)
	}

	// An expression that fails at run time rejects the row with expr (D54).
	res, err = From(NewTable(Ints("a", 1, 0)), 2).Then(With("b", Lit(1).Div(Col("a")))).Run(ctx)
	if err != nil || res.Status != StatusOK || !slices.Equal(codes(res.Table.Rejects()), []string{CodeExpr}) {
		t.Errorf("expr: err %v, status %v, rejects %v", err, res.Status, res.Table.Rejects())
	}
}

func TestStatusAndCountsAreConsistent(t *testing.T) {
	testutil.Proves(t, "T16")
	ctx := context.Background()

	// Filter, inner join without partner and a filter over nulls (D43).
	orders := NewTable(
		Texts("order", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
		Texts("cust", "0", "1", "2", "3", "0", "1", "2", "3", "0", "1"),
		Texts("amount", "", "x", "5", "5", "", "7", "y", "1", "0", "3"),
	).AsSource("orders")
	customers := NewTable(Texts("cust", "0", "1", "2"), Texts("name", "A", "B", "C")).AsSource("customers")
	res, err := From(orders, 3).
		Then(Cast("amount", TypeInt)).
		Then(InnerJoin(customers, On("cust"))).
		Then(Where(Col("amount").Gt(Lit(0)))).
		Run(ctx)
	if err != nil || res.Status != StatusOK {
		t.Fatalf("err %v, status %v", err, res.Status)
	}
	wantCells(t, res.Table, "order", "2", "5", "9")
	for name, want := range map[string]string{
		"run":        "13=5+2+6+0",
		"cast":       "10=8+2+0+0",
		"inner_join": "11=9+0+2+0",
		"where":      "9=5+0+4+0",
	} {
		c := res.Counts
		if name != "run" {
			c = stepCounts(t, res, name)
		}
		if countsOf(c) != want {
			t.Errorf("%s: counts %s, want %s", name, countsOf(c), want)
		}
	}
	if res.Counts.ByCode[CodeParse] != 2 || len(res.Counts.ByCode) != 1 {
		t.Errorf("by code %v", res.Counts.ByCode)
	}
	wantBalanced(t, res)

	// Every status has its own exit code, rising with its precedence (D21,
	// D63, D86).
	for s, want := range map[Status]string{
		StatusOK: "ok", StatusFailedThreshold: "failed_threshold", StatusAborted: "aborted",
		StatusDeliveryError: "delivery_error", StatusSinkError: "sink_error", StatusPlanError: "plan_error",
	} {
		if s.String() != want || s.ExitCode() != int(s) {
			t.Errorf("status %d: %q, exit %d", s, s, s.ExitCode())
		}
	}
	if StatusOK.ExitCode() != 0 || StatusPlanError.ExitCode() != 5 {
		t.Error("exit codes are not 0 to 5")
	}

	// Several statuses: the highest counts, and all are named (D63).
	r := &fakeReader{
		h:    Header{Source: "d.csv", Columns: []string{"id"}, ID: "fp"},
		recs: []Record{rec(2, "1"), rec(3, "2")},
	}
	res, err = FromSource(NewSource(r).Expect("id", "amount"), 2).Threshold(MaxRejected(0)).Run(ctx)
	if err != nil || res.Status != StatusDeliveryError || res.ExitCode() != 3 {
		t.Fatalf("err %v, status %v", err, res.Status)
	}
	var got []Status
	for _, c := range res.Causes {
		got = append(got, c.Status)
	}
	if !slices.Equal(got, []Status{StatusDeliveryError, StatusFailedThreshold}) {
		t.Errorf("causes %v", res.Causes)
	}

	// A canceled run is aborted, and the counts still balance.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	res, err = From(orders, 3).Then(Select("order")).Run(cctx)
	if !errors.Is(err, context.Canceled) || res.Status != StatusAborted || res.ExitCode() != 2 {
		t.Errorf("canceled: err %v, status %v", err, res.Status)
	}
	wantBalanced(t, res)
}

func TestFormatChangesAppearInTheChangeReport(t *testing.T) {
	testutil.Proves(t, "T17")
	dates := []string{"2026-09-01", "01.09.2026", "02.09.2026", "03.09.2026", "03.09.2026",
		"04.09.2026", "05.09.2026", "06.09.2026", "2026-09-10", "07.09.2026"}
	var recs []Record
	for i, d := range dates {
		amount := "1"
		if i == 4 {
			amount = "eins"
		}
		recs = append(recs, rec(i+2, strconv.Itoa(i), d, amount))
	}
	r := &fakeReader{h: Header{Source: "d.csv", Columns: []string{"id", "date", "amount"}, ID: "fp"}, recs: recs}
	res, err := FromSource(NewSource(r), 3).
		Then(CastAll(Cast("date", TypeTimestamp), Cast("amount", TypeInt))).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 8 of 10 dates fail with parse; one amount of 10 is no format change.
	if len(res.Report) != 1 {
		t.Fatalf("report %v", res.Report)
	}
	f := res.Report[0]
	wantEx := []string{"01.09.2026", "02.09.2026", "03.09.2026", "04.09.2026", "05.09.2026"}
	if f.Kind != FindingFormatChange || f.Source != "d.csv" || f.Column != "date" || f.Count != 8 || !slices.Equal(f.Examples, wantEx) {
		t.Errorf("finding %+v", f)
	}
	// The change report is a table with a row per finding (D23).
	rep := res.ChangeReport()
	wantCells(t, rep, "kind", FindingFormatChange)
	wantCells(t, rep, "count", "8")
	wantCells(t, rep, "column", "date")
}

func TestFormatChangeShareCountsTheRowsThatWentIntoTheStep(t *testing.T) {
	testutil.Proves(t, "T59")
	ctx := context.Background()
	// 10 rows, the filter drops 6, and 2 of the remaining 4 fail.
	src := NewTable(
		Texts("keep", "n", "y", "n", "y", "n", "y", "n", "y", "n", "n"),
		Texts("amount", "1", "a", "x", "2", "x", "b", "x", "3", "x", "x"),
	)
	p := func() *Pipeline {
		return From(src, 4).Then(Where(Col("keep").Eq(Lit("y")))).Then(Cast("amount", TypeInt))
	}
	res, err := p().Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Report) != 1 || res.Report[0].Kind != FindingFormatChange || res.Report[0].Count != 2 ||
		!slices.Equal(res.Report[0].Examples, []string{"a", "b"}) {
		t.Errorf("report %+v", res.Report)
	}
	// A higher limit for the column, or for the pipeline, has no finding
	// (D59).
	for _, p := range []*Pipeline{p().FormatChangeLimitFor("amount", 0.6), p().FormatChangeLimit(0.75)} {
		if res, _ := p.Run(ctx); len(res.Report) != 0 {
			t.Errorf("report %+v, want none", res.Report)
		}
	}
	// A column limit wins over the pipeline limit.
	if res, _ := p().FormatChangeLimit(0.9).FormatChangeLimitFor("amount", 0.5).Run(ctx); len(res.Report) != 1 {
		t.Errorf("report %+v, want one", res.Report)
	}
	// A limit is a share above 0.
	for _, p := range []*Pipeline{p().FormatChangeLimit(0), p().FormatChangeLimitFor("amount", 2)} {
		_, err := p.Run(ctx)
		wantPlanError(t, err)
	}

	// missing_column rejects every row but is no format change.
	r := &fakeReader{
		h:    Header{Source: "d.csv", Columns: []string{"id"}, ID: "fp"},
		recs: []Record{rec(2, "1"), rec(3, "2")},
	}
	res, _ = FromSource(NewSource(r).Expect("id", "amount"), 2).Run(ctx)
	if got := findingKinds(res.Report); !slices.Equal(got, []string{"missing_column:amount:"}) {
		t.Errorf("report %q", got)
	}

	// At most five examples, each once.
	vals := []string{"a", "b", "a", "c", "d", "e", "f", "g"}
	res, _ = From(NewTable(Texts("amount", vals...)), 3).Then(Cast("amount", TypeInt)).Run(ctx)
	if len(res.Report) != 1 || !slices.Equal(res.Report[0].Examples, []string{"a", "b", "c", "d", "e"}) || res.Report[0].Count != 8 {
		t.Errorf("report %+v", res.Report)
	}
}

func TestSourceRowCountsOnce(t *testing.T) {
	testutil.Proves(t, "T57")
	ctx := context.Background()

	// One left row with three partners: (L, 0) fails in one step with expr,
	// (L, 5) in another with custom, (L, 1) passes. The left row counts
	// once, as rejected, and under both codes (D84).
	left := NewTable(Texts("k", "a"), Texts("l", "L")).AsSource("left")
	right := NewTable(Texts("k", "a", "a", "a"), Ints("r", 0, 1, 5)).AsSource("right")
	res, err := From(left, 2).
		Then(InnerJoin(right, On("k"))).
		Step("inverse", With("inv", Lit(1).Div(Col("r")))).
		Step("check", WhereFunc(func(row Row) (bool, error) {
			if v, _ := row.Int("r"); v == 5 {
				return false, errors.New("five")
			}
			return true, nil
		})).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countsOf(res.Counts) != "4=1+3+0+0" {
		t.Errorf("run: counts %s, want 4=1+3+0+0", countsOf(res.Counts))
	}
	if res.Counts.ByCode[CodeExpr] != 2 || res.Counts.ByCode[CodeCustom] != 2 {
		t.Errorf("by code %v", res.Counts.ByCode)
	}
	// In the step, the left row and right row 0 are rejected, right rows 1
	// and 5 passed; rejected wins over passed.
	if got := countsOf(stepCounts(t, res, "inverse")); got != "4=2+2+0+0" {
		t.Errorf("inverse: counts %s, want 4=2+2+0+0", got)
	}
	wantBalanced(t, res)

	// A failed aggregated row counts its source rows as rejected (D12).
	src := NewTable(Texts("g", "a", "a", "b"), Ints("v", math.MaxInt64, 1, 1))
	res, err = From(src, 2).Then(GroupBy([]string{"g"}, Sum("v"))).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if countsOf(res.Counts) != "3=1+2+0+0" || countsOf(stepCounts(t, res, "group_by")) != "3=1+2+0+0" {
		t.Errorf("group: counts %s", countsOf(res.Counts))
	}

	// A stop leaves the rows read so far and not done as not processed.
	res, err = From(amounts(6, 3), 2).Then(Cast("amount", TypeInt)).Then(Sort(Asc("amount"))).OnError(ModeStop).Run(ctx)
	var dataErr *DataError
	if !errors.As(err, &dataErr) || res.Status != StatusAborted {
		t.Fatalf("stop: err %v, status %v", err, res.Status)
	}
	if countsOf(res.Counts) != "4=0+0+0+4" {
		t.Errorf("stop: counts %s, want 4=0+0+0+4", countsOf(res.Counts))
	}
	wantBalanced(t, res)
}

func TestDroppedRowsReleaseTheRawState(t *testing.T) {
	testutil.Proves(t, "T58")
	var recs []Record
	for i := range 9 {
		amount := strconv.Itoa(i)
		if i == 1 {
			amount = "eins"
		}
		recs = append(recs, rec(i+2, strconv.Itoa(i), amount))
	}
	r := &fakeReader{h: Header{Source: "d.csv", Columns: []string{"id", "amount"}, ID: "fp"}, recs: recs}
	// Blocks of 3: the filter drops rows 0-5 except row 1, which is
	// rejected. The first two blocks have no row left in the plan.
	res, err := FromSource(NewSource(r), 3).
		Then(WhereFunc(func(row Row) (bool, error) {
			id, _ := row.Text("id")
			return id == "1" || id >= "6", nil
		})).
		Then(Cast("amount", TypeInt)).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw := res.Table.srcs[0]
	var freed []int
	for i, c := range raw.chunks {
		if c == nil {
			freed = append(freed, i)
		}
	}
	if !slices.Equal(freed, []int{0, 1}) {
		t.Errorf("freed chunks %v, want 0, 1", freed)
	}
	// The rejected row still shows its raw state (D87).
	rows := sourceRows(t, res.Table.RejectedRows(), "d.csv")
	wantCells(t, rows, "id", "1")
	wantCells(t, rows, "amount", "eins")

	// Rows of the result table keep their raw state for later rejects
	// (D50, D87).
	later := res.Table.With("x", Lit(1).Div(Col("amount").Sub(Lit(7))))
	rows = sourceRows(t, later.RejectedRows(), "d.csv")
	wantCells(t, rows, "id", "1", "7")
	wantCells(t, rows, "amount", "eins", "7")
}

// An unreadable delivery is a finding of the change report and sets
// delivery_error (D42).
func TestUnreadableDeliveryIsInTheReport(t *testing.T) {
	r := &fakeReader{h: Header{Source: "gone.csv"}, openErr: errors.New("no such file")}
	res, err := FromSource(NewSource(r), 2).Run(context.Background())
	var de *DeliveryError
	if !errors.As(err, &de) || res.Status != StatusDeliveryError || res.ExitCode() != 3 {
		t.Fatalf("err %v, status %v", err, res.Status)
	}
	if got := findingKinds(res.Report); !slices.Equal(got, []string{"unreadable::no such file"}) {
		t.Errorf("report %q", got)
	}
}
