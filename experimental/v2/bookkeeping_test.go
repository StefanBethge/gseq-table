package gtable

import (
	"context"
	"io"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// genReader generates a delivery in the format of the 1BRC file as it is
// read, so that the test holds no records of its own: station and
// measurement, over 50 stations.
type genReader struct {
	n, next int
	off     int64
}

func (r *genReader) Open() (Header, error) {
	r.next, r.off = 0, 0
	return Header{Source: "gen.csv", Columns: []string{"station", "measurement"}, ID: "fp"}, nil
}

func (r *genReader) Next() (Record, error) {
	if r.next >= r.n {
		return Record{}, io.EOF
	}
	i := r.next
	r.next++
	st := "s" + strconv.Itoa(i*7%50)
	val := strconv.FormatFloat(float64(i*37%1999-999)/10, 'f', 1, 64)
	rec := Record{Fields: []string{st, val}, Line: i + 2, Offset: r.off}
	r.off += int64(len(st) + len(val) + 2)
	return rec, nil
}

func (r *genReader) Close() error { return nil }

// heapSink discards the blocks and notes the live heap at every block, so
// that the last note is the heap once all rows are read.
type heapSink struct {
	rows int
	heap uint64
}

func (s *heapSink) Write(_ context.Context, b Block) error {
	s.rows += b.Rows.Len()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.heap = ms.HeapAlloc
	return nil
}

func (s *heapSink) Close() error { return nil }

func TestBookkeepingDoesNotGrowWithTheDelivery(t *testing.T) {
	testutil.Proves(t, "T71")
	ctx := context.Background()
	const small, large = 50_000, 450_000
	plans := map[string]struct {
		ops    []Op
		budget int64 // 0: no cap
		rows   func(n int) int
	}{
		"stream": {
			ops:  []Op{Cast("measurement", TypeFloat), Where(Col("measurement").Gt(Lit(0.0)))},
			rows: func(n int) int { return -1 },
		},
		"onebrc": {
			ops: []Op{Cast("measurement", TypeFloat),
				GroupBy([]string{"station"}, Min("measurement").As("min"), Mean("measurement").As("mean"), Max("measurement").As("max")),
				Sort(Asc("station"))},
			rows: func(int) int { return 50 },
		},
		"spilled sort": {
			ops:    []Op{Cast("measurement", TypeFloat), Sort(Asc("measurement"))},
			budget: 1 << 20,
			rows:   func(n int) int { return n },
		},
	}
	for name, pl := range plans {
		t.Run(name, func(t *testing.T) {
			heap := map[int]uint64{}
			for _, n := range []int{small, large} {
				p := FromSource(NewSource(&genReader{n: n}), 4096)
				for _, op := range pl.ops {
					p.Then(op)
				}
				if pl.budget > 0 {
					p.MemoryBudget(pl.budget)
				}
				out := &heapSink{}
				res, err := isolated(t, p.To(out), 1<<30).Run(ctx)
				if err != nil {
					t.Fatal(err)
				}
				res.Close()
				if want := pl.rows(n); want >= 0 && out.rows != want {
					t.Fatalf("%d rows: sink got %d rows, want %d", n, out.rows, want)
				}
				if res.Counts.Read != n || res.Counts.Passed+res.Counts.Dropped != n {
					t.Fatalf("%d rows: counts %s", n, countsOf(res.Counts))
				}
				heap[n] = out.heap
			}
			perRow := (float64(heap[large]) - float64(heap[small])) / (large - small)
			t.Logf("heap %d B at %d rows, %d B at %d rows: %.2f B per row", heap[small], small, heap[large], large, perRow)
			if perRow > 2 {
				t.Errorf("the heap grows by %.2f bytes per row, want at most 2 (D109)", perRow)
			}
		})
	}

	// A held row carries its origin and its source row.
	if n := reflect.TypeFor[origin]().Size() + reflect.TypeFor[srcRef]().Size(); n > 50 {
		t.Errorf("bookkeeping of a held row is %d bytes, want at most 50 (D109)", n)
	}
}

// floatGroups returns a table of n rows over k groups with floats that do
// not add up exactly, and a null in every 11th row.
func floatGroups(n, k int) Table {
	g := make([]string, n)
	v := make([]float64, n)
	var nulls []int
	for i := range n {
		g[i] = "g" + strconv.Itoa((i*13+i/k)%k)
		v[i] = float64(i%977)*0.1 + 1/float64(i+3)
		if i%11 == 4 {
			nulls = append(nulls, i)
		}
	}
	return NewTable(Texts("g", g...), Floats("v", v...).WithNulls(nulls...))
}

func TestStreamingGroupByEqualsInMemoryAndSpillsOnlyForManyGroups(t *testing.T) {
	testutil.Proves(t, "T72")
	ctx := context.Background()
	streaming := []Agg{Count("v").As("n"), Sum("v").As("sum"), Mean("v").As("mean"), Min("v").As("min"),
		Max("v").As("max"), First("v").As("first"), Last("v").As("last")}
	cases := []struct {
		name   string
		n, k   int
		aggs   []Agg
		budget int64
		spills bool
	}{
		{"few groups", 20_000, 7, streaming, 64 << 10, false},
		{"one group over the budget", 30_000, 1, streaming, 32 << 10, false},
		{"many groups", 20_000, 5_000, streaming, 256 << 10, true},
		{"needs all values", 20_000, 700, append(streaming, Median("v").As("median")), 256 << 10, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := floatGroups(c.n, c.k)
			op := GroupBy([]string{"g"}, c.aggs...)
			want := entries(t, src.Apply(op))
			res, err := isolated(t, From(src, 97).Then(op), c.budget).Run(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Close()
			if got := entries(t, res.Table); got != want {
				t.Errorf("pipeline differs from the table method:\n%s\nwant\n%s", head(got), head(want))
			}
			if spilled := res.mem.spills > 0; spilled != c.spills {
				t.Errorf("spilled %v (%d spills), want %v", spilled, res.mem.spills, c.spills)
			}
			if c.spills && res.mem.peak > c.budget*11/10 {
				t.Errorf("peak %d over the budget %d", res.mem.peak, c.budget)
			}
		})
	}

	// New keys after the budget is reached wait for the sorted way, also
	// when nothing spills.
	src := floatGroups(2_000, 1_500)
	op := GroupBy([]string{"g"}, streaming...)
	steps, _, err := checkPlan(src.s, []step{{name: "group_by", op: op}}, DefaultInfoPrefix)
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	fs := build(steps, &rejector{run: newRun(), shared: &shared{}}, 0, c).(*fullStage)
	for _, b := range src.batches() {
		for _, part := range chunk(b, 500) {
			if err := fs.push(ctx, part); err != nil {
				t.Fatal(err)
			}
			fs.g.full = true // after the first 500 rows
		}
	}
	if fs.x.rows == 0 || fs.x.spilled() {
		t.Fatalf("waiting rows %d, spilled %v", fs.x.rows, fs.x.spilled())
	}
	if err := fs.finish(ctx); err != nil {
		t.Fatal(err)
	}
	got := Table{s: src.Apply(op).s, blocks: blocksOf(c.batches), orig: originsOf(c.batches)}
	if g, w := rowsOf(t, got, false), rowsOf(t, src.Apply(op), false); g != w {
		t.Errorf("waiting keys:\n%s\nwant\n%s", head(g), head(w))
	}

	// An integer sum that overflows rejects the aggregated row, as in memory.
	src = NewTable(Texts("g", "a", "b", "a", "b"), Ints("v", math.MaxInt64, 1, 1, 2))
	op = GroupBy([]string{"g"}, Sum("v"), Count("v").As("n"))
	want := entries(t, src.Apply(op))
	res, err := From(src, 1).Then(op).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := entries(t, res.Table); got != want {
		t.Errorf("overflow:\n%s\nwant\n%s", got, want)
	}
	if countsOf(res.Counts) != "4=2+2+0+0" {
		t.Errorf("overflow: counts %s, want 4=2+2+0+0", countsOf(res.Counts))
	}
}

// head returns the first lines of s.
func head(s string) string {
	n := 0
	for i := range s {
		if s[i] == '\n' {
			if n++; n == 12 {
				return s[:i] + "\n..."
			}
		}
	}
	return s
}

func TestOnlySourceRowsInSeveralWorkingRowsHaveAState(t *testing.T) {
	testutil.Proves(t, "T73")
	ctx := context.Background()

	// Every left row has one partner: no state for the left source.
	ids := make([]string, 3000)
	keys := make([]string, 3000)
	for i := range ids {
		ids[i], keys[i] = strconv.Itoa(i), "k"+strconv.Itoa(i%3)
	}
	left := NewTable(Texts("id", ids...), Texts("k", keys...)).AsSource("left")
	right := NewTable(Texts("k", "k0", "k1", "k2"), Texts("name", "a", "b", "c")).AsSource("right")
	res, err := isolated(t, From(left, 256).Then(InnerJoin(right, On("k"))).To(&memSink{}), 1<<30).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := res.mem.stateRows[left.srcs[0]]; n != 0 {
		t.Errorf("left source: %d rows with a state, want 0", n)
	}
	if n := res.mem.stateRows[right.srcs[0]]; n != 3 {
		t.Errorf("right source: %d rows with a state, want 3", n)
	}
	if countsOf(res.Counts) != "3003=3003+0+0+0" {
		t.Errorf("1:1: counts %s", countsOf(res.Counts))
	}

	// L1 has two partners, one of which fails later; L2 has two partners
	// that pass; L3 has none. Each counts once (D84).
	left = NewTable(Texts("id", "L1", "L2", "L3"), Texts("k", "a", "b", "c")).AsSource("left")
	right = NewTable(Texts("k", "a", "a", "b", "b"), Ints("r", 0, 1, 2, 3)).AsSource("right")
	res, err = isolated(t, From(left, 1).
		Then(InnerJoin(right, On("k"))).
		Step("inverse", With("inv", Lit(1).Div(Col("r")))), 1<<30).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Read: 3 left and 4 right rows. L1 and right row 0 are rejected, L2
	// and right rows 1 to 3 passed, L3 is dropped.
	if countsOf(res.Counts) != "7=4+2+1+0" {
		t.Errorf("1:n: counts %s, want 7=4+2+1+0", countsOf(res.Counts))
	}
	if got := countsOf(stepCounts(t, res, "inverse")); got != "6=4+2+0+0" {
		t.Errorf("inverse: counts %s, want 6=4+2+0+0", got)
	}
	wantBalanced(t, res)
	if res.mem.stateRows[left.srcs[0]] != 2 || res.mem.stateBytes == 0 || res.mem.peak < res.mem.stateBytes {
		t.Errorf("states: %d left rows, %d bytes, peak %d", res.mem.stateRows[left.srcs[0]], res.mem.stateBytes, res.mem.peak)
	}
}
