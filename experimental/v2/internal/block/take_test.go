package block

import "testing"

func TestTakeReordersRepeatsAndPadsWithNull(t *testing.T) {
	b := NewBuilder(Int, 3)
	b.AppendInt(10)
	b.AppendNull()
	b.AppendInt(30)
	c := b.Build()

	got := c.Take([]int{2, 0, 0, 1, -1})
	if got.Len() != 5 || got.Shared() {
		t.Fatalf("Len=%d Shared=%v", got.Len(), got.Shared())
	}
	want := []struct {
		v  int64
		ok bool
	}{{30, true}, {10, true}, {10, true}, {0, false}, {0, false}}
	for i, w := range want {
		if v, ok := got.Int(i); v != w.v || ok != w.ok {
			t.Errorf("cell %d = %d, %v; want %d, %v", i, v, ok, w.v, w.ok)
		}
	}
}

func TestConcatKeepsNulls(t *testing.T) {
	a := NewBuilder(Text, 2)
	a.AppendText("x")
	a.AppendNull()
	b := NewBuilder(Text, 1)
	b.AppendText("")
	c := Concat(Text, a.Build(), b.Build())
	if c.Len() != 3 || c.NullCount() != 1 || !c.IsNull(1) {
		t.Fatalf("Len=%d NullCount=%d", c.Len(), c.NullCount())
	}
	if v, ok := c.Text(2); !ok || v != "" {
		t.Errorf("cell 2 = %q, %v; want empty text", v, ok)
	}
}

func TestBlockTake(t *testing.T) {
	tb, _ := NewTextBuilder(2, 4)
	tb.Add([]string{"a", "1"})
	tb.Add([]string{"b", "2"})
	blk, _ := tb.Flush()
	got := blk.Take([]int{1})
	if got.Len() != 1 || got.Width() != 2 {
		t.Fatalf("Len=%d Width=%d", got.Len(), got.Width())
	}
	if v, _ := got.Column(0).Text(0); v != "b" {
		t.Errorf("got %q", v)
	}
}
