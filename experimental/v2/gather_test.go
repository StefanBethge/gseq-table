package gtable

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// gatherTable returns n rows with a text key of 97 values, a float with
// nulls and wide texts, so that the cells outweigh the origins of the rows.
func gatherTable(n int) Table {
	codes := make([]string, n)
	vals := make([]float64, n)
	names := make([]string, n)
	notes := make([]string, n)
	var nulls []int
	for i := range n {
		codes[i] = fmt.Sprintf("C%03d", (i*31)%97)
		vals[i] = float64((i * 7) % 13)
		if i%11 == 0 {
			nulls = append(nulls, i)
		}
		names[i] = fmt.Sprintf("name %d of the delivery, padded to be wide", i)
		notes[i] = strings.Repeat("n", 40+i%17)
	}
	return NewTable(Texts("code", codes...), Floats("value", vals...).WithNulls(nulls...),
		Texts("name", names...), Texts("note", notes...))
}

// allocated returns the bytes the run of p allocates, and its result.
func allocated(t *testing.T, p *Pipeline) (uint64, Table) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	res, err := p.Run(context.Background())
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusOK {
		t.Fatalf("status %s: %v", res.Status, res.Causes)
	}
	return after.TotalAlloc - before.TotalAlloc, res.Table
}

// rowStrings returns the rows of tbl as text, one string per row.
func rowStrings(t *testing.T, tbl Table) []string {
	t.Helper()
	var cols [][]string
	for _, c := range tbl.Columns() {
		cols = append(cols, cells(t, tbl, c))
	}
	out := make([]string, tbl.Len())
	for i := range out {
		var sb strings.Builder
		for _, c := range cols {
			sb.WriteString(c[i] + "|")
		}
		out[i] = sb.String()
	}
	return out
}

// T76: sort and join gather every cell from the blocks of their input into
// blocks of the block length once, without joining the input into one block
// first (D113, G75), and their rows stay those of D70 and D6.
func TestSortAndJoinGatherEveryCellOnce(t *testing.T) {
	testutil.Proves(t, "T76")

	const n, blockLen = 30000, 1000
	src := gatherTable(n)
	var data int64
	for _, b := range src.blocks {
		for i := range b.Width() {
			data += b.Column(i).Bytes()
		}
	}
	base, _ := allocated(t, From(src, blockLen))

	wantBlocks := func(t *testing.T, tbl Table) {
		t.Helper()
		for i, b := range tbl.blocks {
			if b.Len() != blockLen {
				t.Fatalf("block %d has %d rows, want %d", i, b.Len(), blockLen)
			}
		}
	}

	t.Run("sort", func(t *testing.T) {
		alloc, got := allocated(t, From(src, blockLen).Then(Sort(Asc("code"), Desc("value"))))
		wantBlocks(t, got)
		// The order of D70: by code, then value descending with nulls last,
		// and equal keys in input order.
		rows := rowStrings(t, src)
		code, val := cells(t, src, "code"), cells(t, src, "value")
		idx := allIndexes(n)
		slices.SortStableFunc(idx, func(a, b int) int {
			if c := strings.Compare(code[a], code[b]); c != 0 {
				return c
			}
			switch an, bn := val[a] == "<null>", val[b] == "<null>"; {
			case an && bn:
				return 0
			case an:
				return 1
			case bn:
				return -1
			}
			fa, _ := srcFloat(src, a)
			fb, _ := srcFloat(src, b)
			switch {
			case fa > fb:
				return -1
			case fa < fb:
				return 1
			}
			return 0
		})
		want := make([]string, n)
		for i, j := range idx {
			want[i] = rows[j]
		}
		if !slices.Equal(rowStrings(t, got), want) {
			t.Fatal("sorted rows differ from a stable sort")
		}
		step := int64(alloc - base)
		t.Logf("data %d bytes, sort allocates %d bytes (%.2f×)", data, step, float64(step)/float64(data))
		// The cells once, and the bookkeeping of the rows (D109).
		if limit := data*11/10 + 100*n; step > limit {
			t.Errorf("sort allocates %d bytes for %d bytes of cells and %d rows, want at most %d", step, data, n, limit)
		}
	})

	t.Run("join", func(t *testing.T) {
		ids := make([]string, 97)
		regions := make([]string, 97)
		for i := range ids {
			ids[i] = fmt.Sprintf("C%03d", i)
			regions[i] = fmt.Sprintf("region %d", i%5)
		}
		dim := NewTable(Texts("code", ids...), Texts("region", regions...)).AsSource("dim")
		alloc, got := allocated(t, From(src, blockLen).Then(InnerJoin(dim, On("code"))))
		wantBlocks(t, got)
		wantCells(t, got, "name", cells(t, src, "name")...)
		region := cells(t, got, "region")
		for i, c := range cells(t, src, "code") {
			var k int
			fmt.Sscanf(c, "C%03d", &k)
			if region[i] != regions[k] {
				t.Fatalf("row %d: region %q, want %q", i, region[i], regions[k])
			}
		}
		step := int64(alloc - base)
		t.Logf("data %d bytes, join allocates %d bytes (%.2f×)", data, step, float64(step)/float64(data))
		// The cells once, and the bookkeeping of the rows (D109).
		if limit := data*11/10 + 100*n; step > limit {
			t.Errorf("join allocates %d bytes for %d bytes of cells and %d rows, want at most %d", step, data, n, limit)
		}
	})
}

// srcFloat returns the value of row i of the float column "value" of tbl.
func srcFloat(tbl Table, i int) (float64, bool) {
	c, _ := tbl.Column("value")
	return c.Float(i)
}
