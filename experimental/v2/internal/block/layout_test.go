package block

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/testutil"
)

// T74: a text column holds its values one after another in one byte buffer
// with offsets and a null bitmap (D113, G75). Reading a cell copies nothing,
// and copying rows or columns costs a few allocations per column, not one per
// value.
func TestTextColumnsHoldTheirValuesInOneBuffer(t *testing.T) {
	testutil.Proves(t, "T74")

	const n = 16384
	vals := make([]string, n)
	b := NewBuilder(Text, n)
	for i := range vals {
		switch i % 5 {
		case 0:
			b.AppendNull()
			continue
		case 1:
			vals[i] = "" // empty text is a value (D30)
		case 2:
			vals[i] = "Müllerstraße " + fmt.Sprint(i)
		default:
			vals[i] = fmt.Sprintf("C%04d", i%1000)
		}
		if i%2 == 0 {
			b.AppendTextBytes([]byte(vals[i]))
		} else {
			b.AppendText(vals[i])
		}
	}
	c := b.Build()

	var data int64
	var prev string
	for i := range n {
		v, ok := c.Text(i)
		if want := i%5 != 0; ok != want || v != vals[i] {
			t.Fatalf("Text(%d) = %q, %v; want %q, %v", i, v, ok, vals[i], want)
		}
		data += int64(len(v))
		// The values lie one after another in one buffer.
		if len(prev) > 0 && len(v) > 0 && unsafe.StringData(v) != (*byte)(unsafe.Add(unsafe.Pointer(unsafe.StringData(prev)), len(prev))) {
			t.Fatalf("cell %d does not follow the one before it in the buffer", i)
		}
		if len(v) > 0 {
			prev = v
		}
	}
	// The budget counts the bytes, 4 bytes of offset per cell and the null
	// bitmap, no string header per value (D28).
	if want := data + 4*(n+1) + 8*(n/64); c.Bytes() != want {
		t.Errorf("Bytes() = %d, want %d", c.Bytes(), want)
	}

	if a := testing.AllocsPerRun(10, func() {
		for i := range n {
			c.Text(i)
		}
	}); a != 0 {
		t.Errorf("reading all cells allocates %v times, want 0", a)
	}
	rows := make([]int, n)
	for i := range rows {
		rows[i] = n - 1 - i
	}
	if a := testing.AllocsPerRun(10, func() { x := c.Take(rows); x.Release() }); a > 6 {
		t.Errorf("Take allocates %v times, want a few per column", a)
	}
	if a := testing.AllocsPerRun(10, func() { x := Concat(Text, c, c, c); x.Release() }); a > 6 {
		t.Errorf("Concat allocates %v times, want a few per column", a)
	}
	tk := c.Take(rows)
	for i, r := range rows {
		if v, ok := tk.Text(i); v != vals[r] || ok != (r%5 != 0) {
			t.Fatalf("Take: cell %d = %q, %v; want %q", i, v, ok, vals[r])
		}
	}

	// A value read before a change keeps its bytes: the buffer of a column
	// is never written again, and a shared column is copied first (D55).
	held, _ := c.Text(2)
	other := c.Share()
	c.Mutate()
	c.SetNull(2)
	if v, ok := other.Text(2); !ok || v != vals[2] || held != vals[2] {
		t.Errorf("after a change of the copy: other %q, %v, held %q; want %q", v, ok, held, vals[2])
	}
	if v, ok := c.Text(2); ok || v != "" {
		t.Errorf("null cell = %q, %v; want empty and not ok", v, ok)
	}
}

// D29: a builder takes an estimate of the bytes of a text column, so that a
// block filled from a reader grows its buffer rarely.
func TestTextBuilderGrowsRarely(t *testing.T) {
	const n = 16384
	a := testing.AllocsPerRun(5, func() {
		b := NewBuilder(Text, n)
		b.ReserveText(n * 8)
		for range n {
			b.AppendText("C0354-12")
		}
		b.Build()
	})
	if a > 4 {
		t.Errorf("building a text column of %d values allocates %v times", n, a)
	}
}
