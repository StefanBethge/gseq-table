package gtable

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/memlimit"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// measurements returns a delivery of n rows over k stations: station,
// value and big. Every 97th value is not a number, and the station s7 has
// values of big whose sum overflows.
func measurements(n, k int) *fakeReader {
	recs := make([]Record, n)
	for i := range n {
		st := "s" + strconv.Itoa((i*7+i/k)%k)
		val := strconv.FormatFloat(float64((i*37)%199-99)/10, 'f', 1, 64)
		if i%97 == 5 {
			val = "n/a"
		}
		big := "1"
		if st == "s7" {
			big = "4611686018427387904"
		}
		recs[i] = rec(i+1, st, val, big)
	}
	return &fakeReader{h: Header{Source: "m.csv", Columns: []string{"station", "value", "big"}, ID: "fp"}, recs: recs}
}

// isolated gives p a memory pool of its own with the given budget, so that
// tests running at the same time do not share the process budget, and a
// spill directory of its own.
func isolated(t *testing.T, p *Pipeline, budget int64) *Pipeline {
	t.Helper()
	p.pool = newMemPool(budget)
	return p.SpillDir(t.TempDir())
}

// entries lists what t's rows and rejects hold, rejects sorted, without
// the identifiers of the run and of the rejects, which differ from run to
// run.
func entries(t *testing.T, tbl Table) string {
	t.Helper()
	if tbl.Err() != nil {
		t.Fatalf("sticky error: %v", tbl.Err())
	}
	var sb strings.Builder
	sb.WriteString(rowsOf(t, tbl, false))
	rr := tbl.RejectedRows()
	for _, s := range rr.Sources() {
		fmt.Fprintf(&sb, "source %s:\n%s", s.Source, rowsOf(t, s.Rows, true))
	}
	for _, s := range rr.Aggregated() {
		fmt.Fprintf(&sb, "aggregated %s:\n%s", s.Step, rowsOf(t, s.Rows, true))
	}
	fmt.Fprintf(&sb, "overview:\n%s", rowsOf(t, rr.Overview(), true))
	return sb.String()
}

func rowsOf(t *testing.T, tbl Table, sorted bool) string {
	t.Helper()
	var cols []string
	for _, c := range tbl.Columns() {
		if c != DefaultInfoPrefix+"reject_id" && c != DefaultInfoPrefix+"run_id" {
			cols = append(cols, c)
		}
	}
	vals := make([][]string, len(cols))
	for i, c := range cols {
		vals[i] = cells(t, tbl, c)
	}
	lines := make([]string, tbl.Len())
	for r := range lines {
		parts := make([]string, len(cols))
		for i := range cols {
			parts[i] = cols[i] + "=" + vals[i][r]
		}
		lines[r] = strings.Join(parts, " ")
	}
	if sorted {
		slices.Sort(lines)
	}
	return strings.Join(lines, "\n") + "\n"
}

func regions() Table {
	var st, reg []string
	for i := range 40 {
		st = append(st, "s"+strconv.Itoa(i))
		reg = append(reg, "r"+strconv.Itoa(i%3))
	}
	return NewTable(Texts("station", st...), Texts("region", reg...)).AsSource("regions")
}

func TestSamePipelineGivesTheSameResultInMemoryBlockwiseAndSpilled(t *testing.T) {
	testutil.Proves(t, "T22")
	ctx := context.Background()
	plans := map[string][]Op{
		"sort": {
			Cast("value", TypeFloat),
			Where(Col("value").Gt(Lit(-5.0))),
			Sort(Desc("value"), Asc("station")),
		},
		"group": {
			Cast("value", TypeFloat),
			Cast("big", TypeInt),
			GroupBy([]string{"station"}, Min("value"), Mean("value").As("mean"), Max("value").As("max"),
				Sum("big"), Count("value").As("n"), Median("value").As("median")),
			With("inv", Lit(1.0).Div(Col("value").Add(Lit(9.9)))),
			Sort(Asc("station")),
		},
		"join": {
			Cast("value", TypeFloat),
			LeftJoin(regions(), On("station")),
			Where(Col("region").Ne(Lit("r1"))),
			Sort(Asc("region"), Desc("value")),
		},
	}
	for name, ops := range plans {
		t.Run(name, func(t *testing.T) {
			tbl, err := NewSource(measurements(3000, 45)).Table(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, op := range ops {
				tbl = tbl.Apply(op)
			}
			want := entries(t, tbl)

			for _, mode := range []struct {
				name     string
				blockLen int
				budget   int64
			}{
				{"in memory", 1 << 20, 0},
				{"blockwise", 7, 0},
				{"spilled", 7, 12 << 10},
			} {
				p := FromSource(NewSource(measurements(3000, 45)), mode.blockLen)
				for _, op := range ops {
					p.Then(op)
				}
				isolated(t, p, 1<<40)
				if mode.budget > 0 {
					p.MemoryBudget(mode.budget)
				}
				res, err := p.Run(ctx)
				if err != nil {
					t.Fatalf("%s: %v", mode.name, err)
				}
				if got := entries(t, res.Table); got != want {
					t.Errorf("%s differs from the Table methods:\n%s\nwant:\n%s", mode.name, got, want)
				}
				if spilled := res.mem.spills > 0; spilled != (mode.budget > 0) {
					t.Errorf("%s: spilled %d times", mode.name, res.mem.spills)
				}
				wantBalanced(t, res)
				if err := res.Close(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestGroupByRejectsAggregatedRowsAndStaysInTheBudget(t *testing.T) {
	testutil.Proves(t, "T9")
	const budget = 64 << 10
	// Groups of 100 rows: one group must fit in memory (G67).
	p := FromSource(NewSource(measurements(20000, 200)), 32).
		Then(Cast("value", TypeFloat)).
		Then(Cast("big", TypeInt)).
		Then(GroupBy([]string{"station"}, Min("value"), Mean("value").As("mean"), Max("value").As("max"), Sum("big"))).
		Then(Sort(Asc("station")))
	res, err := isolated(t, p, budget).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.mem.read < 10*budget {
		t.Fatalf("the delivery holds %d bytes, not a multiple of the budget", res.mem.read)
	}
	if res.mem.spills == 0 {
		t.Error("the run did not spill")
	}
	t.Logf("read %d bytes, peak %d bytes, budget %d, %d spills", res.mem.read, res.mem.peak, budget, res.mem.spills)
	if peak := res.mem.peak; peak > budget*11/10 {
		t.Errorf("peak %d bytes, over the budget of %d plus 10 %%", peak, budget)
	}
	if res.Table.Len() != 199 {
		t.Errorf("%d groups, want 199 (s7 is rejected)", res.Table.Len())
	}
	agg := res.Table.RejectedRows().Aggregated()
	if len(agg) != 1 || agg[0].Step != "group_by" {
		t.Fatalf("aggregated rejects = %v", agg)
	}
	rows := agg[0].Rows
	wantCells(t, rows, "station", "s7")
	s7 := 0
	for _, r := range measurements(20000, 200).recs {
		if r.Fields[0] == "s7" && r.Fields[1] != "n/a" {
			s7++
		}
	}
	wantCells(t, rows, DefaultInfoPrefix+"source_rows", strconv.Itoa(s7))
	wantCells(t, rows, DefaultInfoPrefix+"code", CodeExpr)
	wantBalanced(t, res)
}

func TestJoinRightSideOverTheBudgetAbortsTheRun(t *testing.T) {
	p := FromSource(NewSource(measurements(100, 45)), 16).
		Then(InnerJoin(regions(), On("station")))
	res, err := isolated(t, p, 1<<40).MemoryBudget(256).Run(context.Background())
	var me *MemoryError
	if !errors.As(err, &me) || res.Status != StatusAborted {
		t.Fatalf("err %v, status %v, want a *MemoryError and aborted", err, res.Status)
	}
	if !strings.Contains(err.Error(), "right side") || !strings.Contains(err.Error(), "must fit in memory") {
		t.Errorf("reason %q does not name the limitation", err)
	}
	if res.Counts.Read != 0 {
		t.Errorf("read %d rows before the check", res.Counts.Read)
	}
}

// spillingRun returns a pipeline that sorts n rows under a small budget and
// rejects every 97th row.
func spillingRun(n int) *Pipeline {
	return FromSource(NewSource(measurements(n, 45)), 16).
		Then(Cast("value", TypeFloat)).
		Then(Sort(Asc("value"))).
		MemoryBudget(8 << 10)
}

func wantEmpty(t *testing.T, dir string) {
	t.Helper()
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		names := make([]string, len(left))
		for i, e := range left {
			names[i] = e.Name()
		}
		t.Errorf("left in %s: %v", dir, names)
	}
}

func TestNothingIsLeftNextToTheTargets(t *testing.T) {
	testutil.Proves(t, "T24")
	ctx := context.Background()
	// The default spill root is the system's temporary directory.
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	// Without spilled rejected rows nothing is left after the run; with
	// them, nothing after Close (D49).
	wantCleaned := func(res Result) {
		t.Helper()
		if !res.mem.keptSpilled {
			wantEmpty(t, tmp)
		}
		if err := res.Close(); err != nil {
			t.Fatal(err)
		}
		wantEmpty(t, tmp)
	}
	run := func(p *Pipeline) (Result, error) {
		p.pool = newMemPool(1 << 40)
		return p.Run(ctx)
	}

	clean := FromSource(NewSource(measurements(2000, 45)), 16).Then(Sort(Asc("value"))).MemoryBudget(8 << 10)
	res, err := run(clean)
	if err != nil || res.mem.spills == 0 || res.mem.keptSpilled {
		t.Fatalf("err %v, %d spills, rejects spilled %v", err, res.mem.spills, res.mem.keptSpilled)
	}
	wantEmpty(t, tmp)
	wantCleaned(res)

	res, err = run(spillingRun(2000))
	if err != nil || res.mem.spills == 0 {
		t.Fatalf("err %v, %d spills", err, res.mem.spills)
	}
	wantCleaned(res)

	stopped := spillingRun(2000).OnErrorCode(CodeParse, ModeStop)
	res, err = run(stopped)
	if err == nil {
		t.Fatal("the stopped run gave no error")
	}
	wantCleaned(res)

	cctx, cancel := context.WithCancel(ctx)
	seen := 0
	canceled := FromSource(NewSource(measurements(2000, 45)), 16).
		Then(Cast("value", TypeFloat)).
		Then(Sort(Asc("value"))).
		Then(WhereFunc(func(Row) (bool, error) {
			if seen++; seen == 100 {
				cancel()
			}
			return true, nil
		})).
		MemoryBudget(8 << 10)
	canceled.pool = newMemPool(1 << 40)
	res, err = canceled.Run(cctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want the context's", err)
	}
	wantCleaned(res)

	// Rejected rows held in the result stay readable until Close (D49).
	held, err := run(manyRejects(3000))
	if err != nil {
		t.Fatal(err)
	}
	if !held.mem.keptSpilled {
		t.Fatal("the rejected rows were not spilled")
	}
	rows := sourceRows(t, held.Table.RejectedRows(), "m.csv")
	if rows.Len() != 1500 {
		t.Errorf("%d rejected rows, want 1500", rows.Len())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	wantEmpty(t, tmp)
}

// manyRejects returns a pipeline in which every second of n rows is
// rejected, under a budget smaller than the copies of their raw state.
func manyRejects(n int) *Pipeline {
	r := measurements(n, 45)
	for i := range r.recs {
		r.recs[i].Fields[1] = "1.5"
		if i%2 == 1 {
			r.recs[i].Fields[1] = "bad-" + strconv.Itoa(i)
		}
	}
	return FromSource(NewSource(r), 32).Then(Cast("value", TypeFloat)).MemoryBudget(16 << 10)
}

func TestSpilledFilesAreOnlyForTheRunningUser(t *testing.T) {
	testutil.Proves(t, "T35")
	root := filepath.Join(t.TempDir(), "spill")
	res, err := manyRejects(3000).SpillDir(root).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	n := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		want := fs.FileMode(0o600)
		if d.IsDir() {
			want = 0o700
		}
		if got := st.Mode().Perm(); got != want {
			t.Errorf("%s: mode %v, want %v", filepath.Base(path), got, want)
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n < 3 {
		t.Errorf("found %d entries, want the directory, its lock and a spill file", n)
	}
}

// withMemoryLimit sets the Go memory limit for the test and restores it.
func withMemoryLimit(t *testing.T, limit int64) {
	t.Helper()
	prev := debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
}

func withDetectedLimit(t *testing.T, limit int64) {
	t.Helper()
	prev := detectLimit
	detectLimit = func() int64 { return limit }
	t.Cleanup(func() { detectLimit = prev })
}

func TestGOMEMLIMITOnlyOnRequestAndRunsShareOneBudget(t *testing.T) {
	testutil.Proves(t, "T39")
	ctx := context.Background()
	t.Setenv("GOMEMLIMIT", "")
	withDetectedLimit(t, 8<<30)

	withMemoryLimit(t, math.MaxInt64)
	if _, err := isolated(t, spillingRun(200), 1<<40).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
		t.Errorf("without the option GOMEMLIMIT = %d", got)
	}

	withMemoryLimit(t, 3<<30)
	if _, err := isolated(t, spillingRun(200), 1<<40).WithManagedMemory().Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := debug.SetMemoryLimit(-1); got != 3<<30 {
		t.Errorf("a limit set by the program became %d", got)
	}

	withMemoryLimit(t, math.MaxInt64)
	t.Setenv("GOMEMLIMIT", "1GiB")
	if _, err := isolated(t, spillingRun(200), 1<<40).WithManagedMemory().Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
		t.Errorf("with GOMEMLIMIT in the environment the limit became %d", got)
	}

	// Two runs at the same time stay within the one budget together.
	const budget = 64 << 10
	pool := newMemPool(budget)
	var wg sync.WaitGroup
	results := make([]Result, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() {
			p := FromSource(NewSource(measurements(4000, 45)), 8).
				Then(Cast("value", TypeFloat)).
				Then(Sort(Desc("value"))).
				SpillDir(t.TempDir())
			p.pool = pool
			results[i], errs[i] = p.Run(ctx)
		})
	}
	wg.Wait()
	for i, res := range results {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if res.mem.spills == 0 {
			t.Errorf("run %d did not spill", i)
		}
		if res.Table.Len() != 4000-42 {
			t.Errorf("run %d: %d rows", i, res.Table.Len())
		}
		res.Close()
	}
	t.Logf("peak of both runs %d bytes, budget %d", pool.peak.Load(), budget)
	if peak := pool.peak.Load(); peak > budget*11/10 {
		t.Errorf("peak of both runs %d bytes, over the budget of %d plus 10 %%", peak, budget)
	}
	if used := pool.used.Load(); used != 0 {
		t.Errorf("%d bytes still counted after the runs", used)
	}
}

func TestRunSpillsAtItsOwnCapAndTheDefaultFollowsTheCgroup(t *testing.T) {
	testutil.Proves(t, "T62")
	const cap = 48 << 10
	res, err := isolated(t, spillingRun(6000), 1<<40).MemoryBudget(cap).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.mem.spills == 0 {
		t.Error("the run did not spill at its cap")
	}
	t.Logf("read %d bytes, peak %d bytes, cap %d", res.mem.read, res.mem.peak, cap)
	if peak := res.mem.peak; peak > cap*11/10 {
		t.Errorf("peak %d bytes, over the cap of %d plus 10 %%", peak, cap)
	}

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "memory.max"), []byte("2147483648\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	v1 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(v1, "memory"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v1, "memory", "memory.limit_in_bytes"), []byte("4294967296\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		limit int64
		want  int64
	}{
		{"cgroup v2", memlimit.DetectFrom(root, "", 64<<30), 512 << 20},
		{"cgroup v1", memlimit.DetectFrom(v1, "", 64<<30), 1 << 30},
		{"physical", memlimit.DetectFrom(t.TempDir(), "", 64<<30), 16 << 30},
	} {
		withDetectedLimit(t, c.limit)
		if got := newMemPool(0).budget(); got != c.want {
			t.Errorf("%s: default budget %d, want %d", c.name, got, c.want)
		}
	}

	t.Cleanup(func() { SetMemoryBudget(0) })
	SetMemoryBudget(1 << 20)
	if got := procPool.budget(); got != 1<<20 {
		t.Errorf("set budget = %d", got)
	}
	SetMemoryBudget(0)
	if got := procPool.budget(); got != 16<<30 {
		t.Errorf("budget after reset = %d, want the default", got)
	}
}

func TestManagedMemorySetsGOMEMLIMITToNinetyPercent(t *testing.T) {
	testutil.Proves(t, "T63")
	t.Setenv("GOMEMLIMIT", "")
	withDetectedLimit(t, 10<<30)
	withMemoryLimit(t, math.MaxInt64)
	res, err := isolated(t, spillingRun(200), 1<<40).WithManagedMemory().Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res.Close()
	// The run does not reset it.
	if got := debug.SetMemoryLimit(-1); got != 9<<30 {
		t.Errorf("GOMEMLIMIT = %d, want %d", got, int64(9<<30))
	}
}

func TestLaterRunRemovesEndedRunsButNotOpenResults(t *testing.T) {
	testutil.Proves(t, "T64")
	ctx := context.Background()
	root := t.TempDir()
	// Remains of a run whose process ended: no one holds the lock.
	orphan := filepath.Join(root, "gtable-spill-123")
	if err := os.Mkdir(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan+".lock", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "run"), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}

	open := manyRejects(3000).SpillDir(root)
	open.pool = newMemPool(1 << 40)
	held, err := open.Run(ctx)
	if err != nil || !held.mem.keptSpilled {
		t.Fatalf("err %v, spilled %v", err, held.mem.keptSpilled)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the orphaned directory is still there: %v", err)
	}

	next := spillingRun(200).SpillDir(root)
	next.pool = newMemPool(1 << 40)
	if _, err := next.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if rows := sourceRows(t, held.Table.RejectedRows(), "m.csv"); rows.Len() != 1500 {
		t.Errorf("the open result shows %d rejected rows, want 1500", rows.Len())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	wantEmpty(t, root)
}
