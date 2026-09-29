package gtable

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// memSink is a sink that keeps the blocks it gets. It rejects the failAt-th
// block with err, and Close returns closeErr.
type memSink struct {
	blocks   []Block
	writes   int
	failAt   int
	err      error
	closed   int
	closeErr error
	onWrite  func(Block)
}

func (s *memSink) Write(_ context.Context, b Block) error {
	s.writes++
	if s.onWrite != nil {
		s.onWrite(b)
	}
	if s.writes == s.failAt {
		return s.err
	}
	s.blocks = append(s.blocks, b)
	return nil
}

func (s *memSink) Close() error {
	s.closed++
	return s.closeErr
}

// col returns column name over all blocks, as text.
func (s *memSink) col(t *testing.T, name string) []string {
	t.Helper()
	out := []string{}
	for _, b := range s.blocks {
		out = append(out, cells(t, b.Rows, name)...)
	}
	return out
}

// rows returns the number of rows over all blocks.
func (s *memSink) rows() int {
	n := 0
	for _, b := range s.blocks {
		n += b.Rows.Len()
	}
	return n
}

var errTarget = errors.New("target is full")

func TestStopEndsTheRunAtTheFirstDataErrorAndRejectDoesNot(t *testing.T) {
	testutil.Proves(t, "T3")
	ctx := context.Background()

	// Stop: the run is aborted, the block with the error is not written,
	// not even its row before the error, and the blocks before it stay
	// (D3, D51).
	out := &memSink{}
	res, err := From(amounts(10, 5), 2).Then(Cast("amount", TypeInt)).OnError(ModeStop).To(out).Run(ctx)
	var de *DataError
	if !errors.As(err, &de) || res.Status != StatusAborted {
		t.Fatalf("stop: err %v, status %v", err, res.Status)
	}
	if got := out.col(t, "id"); !slices.Equal(got, []string{"0", "1", "2", "3"}) {
		t.Errorf("stop: target holds %q, want the two blocks before the error", got)
	}
	if out.closed != 1 {
		t.Errorf("stop: target closed %d times", out.closed)
	}

	// Reject: the run is ok with exactly one rejected row.
	out = &memSink{}
	res, err = From(amounts(10, 5), 2).Then(Cast("amount", TypeInt)).To(out).Run(ctx)
	if err != nil || res.Status != StatusOK || len(res.Table.Rejects()) != 1 || res.Counts.Rejected != 1 {
		t.Fatalf("reject: err %v, status %v, rejects %v", err, res.Status, res.Table.Rejects())
	}
	if out.rows() != 9 {
		t.Errorf("reject: target holds %d rows, want 9", out.rows())
	}
}

func TestSinkThatRejectsABlockEndsTheRunWithSinkError(t *testing.T) {
	testutil.Proves(t, "T33")
	ctx := context.Background()

	// The result target rejects the second block: the run ends at once
	// with sink_error and the target's error, and no further block is
	// written (D40).
	out := &memSink{failAt: 2, err: errTarget}
	res, err := From(amounts(10), 2).Then(Cast("amount", TypeInt)).To(out).Run(ctx)
	var se *SinkError
	if !errors.As(err, &se) || !errors.Is(err, errTarget) {
		t.Fatalf("err = %v, want a *SinkError with the target's error", err)
	}
	if res.Status != StatusSinkError || res.ExitCode() != 4 {
		t.Errorf("status %v, exit code %d", res.Status, res.ExitCode())
	}
	if out.writes != 2 || out.rows() != 2 {
		t.Errorf("target got %d blocks and holds %d rows, want 2 and 2", out.writes, out.rows())
	}
	// The rows of the rejected block did not reach the target (D84).
	if res.Counts.Passed != 2 || res.Counts.Unprocessed < 2 {
		t.Errorf("counts %s", countsOf(res.Counts))
	}
	wantBalanced(t, res)

	// The same holds for a writer of rejected rows (D40).
	out = &memSink{}
	rejects := &memSink{failAt: 1, err: errTarget}
	res, err = From(amounts(10, 1, 5), 2).Then(Cast("amount", TypeInt)).
		To(out).RejectsTo("code", rejects).Run(ctx)
	if !errors.As(err, &se) || !errors.Is(err, errTarget) || res.Status != StatusSinkError {
		t.Fatalf("rejects: err %v, status %v", err, res.Status)
	}
	if rejects.writes != 1 || out.writes != 1 {
		t.Errorf("rejects: writer got %d blocks, target %d, want 1 and 1", rejects.writes, out.writes)
	}
	if rejects.closed != 1 || out.closed != 1 {
		t.Errorf("rejects: closed %d and %d times", rejects.closed, out.closed)
	}
	wantBalanced(t, res)
}

func TestHighestStatusAppliesAndTheResultNamesAllCauses(t *testing.T) {
	testutil.Proves(t, "T36")
	ctx := context.Background()
	reader := func() *fakeReader {
		return &fakeReader{
			h:    Header{Source: "d.csv", Columns: []string{"id", "amount"}, ID: "fp"},
			recs: []Record{rec(2, "1", "5"), rec(3, "2", "6")},
		}
	}
	statuses := func(cs []Cause) []Status {
		out := make([]Status, len(cs))
		for i, c := range cs {
			out[i] = c.Status
		}
		return out
	}

	// A delivery error and an exceeded threshold give delivery_error
	// (D63).
	res, err := FromSource(NewSource(reader()).Expect("id", "amount", "country"), 2).
		Threshold(MaxRejected(0)).Run(ctx)
	if err != nil || res.Status != StatusDeliveryError {
		t.Fatalf("threshold: err %v, status %v", err, res.Status)
	}
	if got := statuses(res.Causes); !slices.Equal(got, []Status{StatusDeliveryError, StatusFailedThreshold}) {
		t.Errorf("threshold: causes %v", res.Causes)
	}

	// A delivery error in stop mode gives delivery_error (D63).
	res, err = FromSource(NewSource(reader()).Expect("id", "amount", "country"), 2).
		OnError(ModeStop).Run(ctx)
	var de *DeliveryError
	if !errors.As(err, &de) || res.Status != StatusDeliveryError {
		t.Errorf("stop: err %v, status %v", err, res.Status)
	}

	// A write error after a delivery error gives sink_error, and the
	// result names the delivery error and the threshold too (D40, D63).
	out := &memSink{failAt: 1, err: errTarget}
	res, err = FromSource(NewSource(reader()).Expect("id", "amount", "country"), 2).
		Threshold(MaxRejected(0)).To(out).Run(ctx)
	var se *SinkError
	if !errors.As(err, &se) || res.Status != StatusSinkError {
		t.Fatalf("sink: err %v, status %v", err, res.Status)
	}
	if got := statuses(res.Causes); !slices.Equal(got, []Status{StatusSinkError, StatusDeliveryError, StatusFailedThreshold}) {
		t.Errorf("sink: causes %v", res.Causes)
	}
}

func TestRejectWritersWriteDuringTheRunElseTheResultHoldsTheRejectsUntilClose(t *testing.T) {
	testutil.Proves(t, "T40")
	ctx := context.Background()

	// With a writer in the plan, the rejected rows are in it during the
	// run: when the target gets the third block, the rejects of the first
	// two are written (D49).
	rejects := &memSink{}
	var during []int
	out := &memSink{onWrite: func(Block) { during = append(during, rejects.rows()) }}
	res, err := From(amounts(6, 0, 2), 2).Then(Cast("amount", TypeInt)).
		To(out).RejectsToEach(func(string) Sink { return rejects }).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(during, []int{0, 1, 2}) {
		t.Errorf("rejects written when the target got a block: %v, want [0 1 2]", during)
	}
	if got := rejects.col(t, DefaultInfoPrefix+"line"); !slices.Equal(got, []string{"0", "2"}) {
		t.Errorf("rejects written: lines %q", got)
	}
	// The result holds only the overview and the counts.
	if srcs := res.Table.RejectedRows().Sources(); len(srcs) != 0 {
		t.Errorf("result holds rejects of %d sources, want none", len(srcs))
	}
	if ov := res.Table.RejectedRows().Overview(); ov.Len() != 2 || res.Counts.Rejected != 2 {
		t.Errorf("overview has %d entries, %d rows rejected", ov.Len(), res.Counts.Rejected)
	}

	// Without writers, the result holds the rejected rows until Close.
	res, err = From(amounts(6, 0, 2), 2).Then(Cast("amount", TypeInt)).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := res.Table.RejectedRows().Source("code")
	if !ok || rows.Len() != 2 {
		t.Fatalf("result holds %v rejected rows", rows.Len())
	}
	wantCells(t, rows, "amount", "x1", "x3")
	if err := res.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, _ := res.Table.RejectedRows().Source("code"); !errors.Is(rows.Err(), ErrClosed) {
		t.Errorf("after Close: sticky error %v", rows.Err())
	}
}

func TestResultWithASinkHoldsNoRowsAndEverySinkIsClosed(t *testing.T) {
	testutil.Proves(t, "T62")
	ctx := context.Background()

	// With a target, the result table has the columns but no rows; the
	// rows are in the target (D94).
	out := &memSink{}
	res, err := From(amounts(5, 1), 2).Then(Cast("amount", TypeInt)).To(out).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Table.Len() != 0 || !slices.Equal(res.Table.Columns(), []string{"id", "amount"}) {
		t.Errorf("result table has %d rows and columns %v", res.Table.Len(), res.Table.Columns())
	}
	if got := out.col(t, "amount"); !slices.Equal(got, []string{"1", "3", "4", "5"}) {
		t.Errorf("target holds %q", got)
	}
	if got := out.blocks[0].RowKeys; len(got) != 1 || got[0] != res.Table.srcs[0].recordKey(0) {
		t.Errorf("row keys %q", got)
	}
	if res.Counts.Passed != 4 || len(res.Table.Rejects()) != 1 || out.closed != 1 {
		t.Errorf("counts %s, rejects %v, closed %d", countsOf(res.Counts), res.Table.Rejects(), out.closed)
	}

	// A run without result rows writes one empty block, so a file gets
	// its header (D100).
	out = &memSink{}
	if _, err := From(amounts(5), 2).Then(Where(Col("id").Eq(Lit("x")))).To(out).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if out.writes != 1 || out.rows() != 0 || !slices.Equal(out.blocks[0].Rows.Columns(), []string{"id", "amount"}) {
		t.Errorf("empty run: %d blocks, %d rows", out.writes, out.rows())
	}

	// Every target is closed, also after an early end, and a failing
	// Close is sink_error (D100, D40).
	for name, p := range map[string]func(out, rej *memSink) *Pipeline{
		"stop": func(out, rej *memSink) *Pipeline {
			return From(amounts(5, 3), 2).Then(Cast("amount", TypeInt)).OnError(ModeStop).To(out).RejectsTo("code", rej)
		},
		"sink error": func(out, rej *memSink) *Pipeline {
			out.failAt, out.err = 1, errTarget
			return From(amounts(5, 3), 2).Then(Cast("amount", TypeInt)).To(out).RejectsTo("code", rej)
		},
	} {
		out, rej := &memSink{}, &memSink{}
		if _, err := p(out, rej).Run(ctx); err == nil {
			t.Errorf("%s: no error", name)
		}
		if out.closed != 1 || rej.closed != 1 {
			t.Errorf("%s: closed %d and %d times", name, out.closed, rej.closed)
		}
	}
	out = &memSink{closeErr: errTarget}
	res, err = From(amounts(3), 2).To(out).Run(ctx)
	var se *SinkError
	if !errors.As(err, &se) || !errors.Is(err, errTarget) || res.Status != StatusSinkError {
		t.Errorf("close error: err %v, status %v", err, res.Status)
	}
}

func TestRejectWritersPerSourceAndRejectsAfterAnEarlyEnd(t *testing.T) {
	testutil.Proves(t, "T63")
	ctx := context.Background()

	// A writer for a named source goes before the factory; the factory
	// gets the name of every other source with rejected rows, and a source
	// without writer stays in the result (D95).
	customers := NewTable(Texts("customer", "K1", "K2", "K3"), Texts("since", "2020-01-01T00:00:00Z", "bad", "2021-01-01T00:00:00Z")).
		AsSource("customers").Cast("since", TypeTimestamp)
	orders := NewTable(Texts("order", "1", "2", "3"), Texts("customer", "K1", "K3", "K1"), Texts("amount", "5", "x", "7")).
		AsSource("orders")
	named, overview := &memSink{}, &memSink{}
	var asked []string
	res, err := From(orders, 2).Then(Cast("amount", TypeInt)).
		Then(InnerJoin(customers, On("customer"))).
		RejectsTo("orders", named).
		RejectsToEach(func(src string) Sink { asked = append(asked, src); return nil }).
		OverviewTo(overview).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(asked, []string{"customers"}) {
		t.Errorf("factory asked for %q", asked)
	}
	if got := named.blocks[0].Rows.Columns()[:3]; !slices.Equal(got, []string{"order", "customer", "amount"}) {
		t.Errorf("writer of orders got columns %v", got)
	}
	if got := named.col(t, "order"); !slices.Equal(got, []string{"2"}) {
		t.Errorf("writer of orders got %q", got)
	}
	srcs := res.Table.RejectedRows().Sources()
	if len(srcs) != 1 || srcs[0].Source != "customers" {
		t.Errorf("result holds the rejects of %v", srcs)
	}
	if res.Table.RejectedRows().Overview().Len() != 2 || overview.rows() != 2 {
		t.Errorf("overview: %d in the result, %d written", res.Table.RejectedRows().Overview().Len(), overview.rows())
	}

	// After a stop no target gets a block, and the rejected rows of the
	// failing block stay in the result, although their source has a
	// writer (D96).
	tbl := NewTable(Texts("amount", "x", "1", "y", "2"), Ints("n", 1, 1, 1, 0))
	out, rejects := &memSink{}, &memSink{}
	res, err = From(tbl, 2).Then(Cast("amount", TypeInt)).Then(With("q", Lit(1).Div(Col("n")))).
		OnErrorCode(CodeExpr, ModeStop).To(out).RejectsTo("code", rejects).Run(ctx)
	if res.Status != StatusAborted || err == nil {
		t.Fatalf("stop: err %v, status %v", err, res.Status)
	}
	if out.writes != 1 || rejects.writes != 1 {
		t.Errorf("stop: target got %d blocks, writer %d, want 1 and 1", out.writes, rejects.writes)
	}
	wantCells(t, rejects.blocks[0].Rows, "amount", "x")
	rows, ok := res.Table.RejectedRows().Source("code")
	if !ok {
		t.Fatal("stop: no rejected rows in the result")
	}
	wantCells(t, rows, "amount", "y")
}

func TestCloseReleasesTheRejectedRowsAndKeepsTheCounts(t *testing.T) {
	testutil.Proves(t, "T65")
	ctx := context.Background()
	res, err := From(amounts(4, 1), 2).Then(Cast("amount", TypeInt)).
		Then(GroupBy([]string{"id"}, Sum("amount").As("total"))).
		Then(With("bad", Col("total").Div(Lit(0)))).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rr := res.Table.RejectedRows()
	if _, ok := rr.Source("code"); !ok || rr.Overview().Len() == 0 || len(rr.Aggregated()) != 1 {
		t.Fatalf("before Close: sources %v, aggregated %v", rr.Sources(), rr.Aggregated())
	}
	status, counts, report := res.Status, countsOf(res.Counts), len(res.Report)
	derived := res.Table.Select("id")

	if err := res.Close(); err != nil {
		t.Fatal(err)
	}
	if err := res.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	for _, rr := range []RejectedRows{res.Table.RejectedRows(), derived.RejectedRows()} {
		rows, ok := rr.Source("code")
		if !ok || !errors.Is(rows.Err(), ErrClosed) {
			t.Errorf("source after Close: %v, sticky error %v", ok, rows.Err())
		}
		if err := rr.Overview().Err(); !errors.Is(err, ErrClosed) {
			t.Errorf("overview after Close: %v", err)
		}
		for _, a := range rr.Aggregated() {
			if !errors.Is(a.Rows.Err(), ErrClosed) {
				t.Errorf("aggregated after Close: %v", a.Rows.Err())
			}
		}
	}
	if res.Status != status || countsOf(res.Counts) != counts || len(res.Report) != report {
		t.Errorf("Close changed status, counts or report")
	}
}
