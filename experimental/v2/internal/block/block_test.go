package block

import (
	"strings"
	"testing"
)

func textColumn(values ...string) Column {
	b := NewBuilder(Text, len(values))
	for _, v := range values {
		b.AppendText(v)
	}
	return b.Build()
}

func TestBlockNew(t *testing.T) {
	blk, err := New(textColumn("a", "b"), textColumn("", "d"))
	if err != nil {
		t.Fatal(err)
	}
	if blk.Len() != 2 || blk.Width() != 2 {
		t.Errorf("Len=%d Width=%d", blk.Len(), blk.Width())
	}
	if v, _ := blk.Column(1).Text(0); v != "" {
		t.Errorf("cell (0,1) = %q", v)
	}
}

func TestBlockNewRejectsUnequalLengths(t *testing.T) {
	_, err := New(textColumn("a", "b"), textColumn("c"))
	if err == nil || !strings.Contains(err.Error(), "column 1") {
		t.Errorf("err = %v", err)
	}
}

func TestBlockEmpty(t *testing.T) {
	blk, err := New()
	if err != nil || blk.Len() != 0 || blk.Width() != 0 {
		t.Errorf("empty block: %v len=%d width=%d", err, blk.Len(), blk.Width())
	}
}

// D55 groundwork: raw and working state share the columns of a block until a
// step changes one of them.
func TestBlockShareAndMutateColumn(t *testing.T) {
	raw, _ := New(textColumn("1", "2"), textColumn("x", "y"))
	work := raw.Share()
	for i := range work.Width() {
		if !work.Column(i).Shared() {
			t.Fatalf("column %d not shared", i)
		}
	}

	col := work.MutableColumn(0)
	if !col.Copied {
		t.Error("first change of a shared column did not report a copy")
	}
	col.Column.SetText(0, "changed")

	if v, _ := raw.Column(0).Text(0); v != "1" {
		t.Errorf("raw state changed: %q", v)
	}
	if v, _ := work.Column(0).Text(0); v != "changed" {
		t.Errorf("working state = %q", v)
	}
	if !raw.Column(1).Shared() || !work.Column(1).Shared() {
		t.Error("unchanged column is no longer shared")
	}
	if raw.Column(0).Shared() {
		t.Error("raw column still shared after the working side copied it")
	}
	if again := work.MutableColumn(0); again.Copied {
		t.Error("second change copied again")
	}
}

func TestBlockSetColumn(t *testing.T) {
	blk, _ := New(textColumn("1", "2"))
	b := NewBuilder(Int, 2)
	b.AppendInt(1)
	b.AppendNull()
	if err := blk.SetColumn(0, b.Build()); err != nil {
		t.Fatal(err)
	}
	if blk.Column(0).Kind() != Int || !blk.Column(0).IsNull(1) {
		t.Errorf("column 0 = %v", blk.Column(0).Kind())
	}
	if err := blk.SetColumn(0, textColumn("only one")); err == nil {
		t.Error("SetColumn accepted a column of another length")
	}
}

func TestBlockAppendColumn(t *testing.T) {
	blk, _ := New(textColumn("1", "2"))
	if err := blk.AppendColumn(textColumn("a", "b")); err != nil {
		t.Fatal(err)
	}
	if blk.Width() != 2 {
		t.Errorf("Width = %d", blk.Width())
	}
	if err := blk.AppendColumn(textColumn("a")); err == nil {
		t.Error("AppendColumn accepted a column of another length")
	}
	empty, _ := New()
	if err := empty.AppendColumn(textColumn("a")); err != nil || empty.Len() != 1 {
		t.Errorf("append to empty block: %v len=%d", err, empty.Len())
	}
}

// D29: raw columns from readers are text, and the block length is set by the
// engine, not fixed.
func TestTextBlockBuilderLength(t *testing.T) {
	if _, err := NewTextBuilder(2, 0); err == nil {
		t.Error("length 0 accepted")
	}
	if _, err := NewTextBuilder(-1, 4); err == nil {
		t.Error("negative width accepted")
	}

	for _, length := range []int{1, 3, 1000} {
		tb, err := NewTextBuilder(2, length)
		if err != nil {
			t.Fatal(err)
		}
		var blocks []Block
		for i := range 7 {
			blk, full, err := tb.Add([]string{"k" + string(rune('0'+i)), ""})
			if err != nil {
				t.Fatal(err)
			}
			if full {
				blocks = append(blocks, blk)
			}
		}
		if rest, ok := tb.Flush(); ok {
			blocks = append(blocks, rest)
		}
		if _, ok := tb.Flush(); ok {
			t.Error("second Flush returned a block")
		}

		rows := 0
		for i, blk := range blocks {
			if blk.Len() > length || (i < len(blocks)-1 && blk.Len() != length) {
				t.Errorf("length %d: block %d has %d rows", length, i, blk.Len())
			}
			for c := range blk.Width() {
				col := blk.Column(c)
				if col.Kind() != Text || col.NullCount() != 0 {
					t.Errorf("raw column kind=%v nulls=%d", col.Kind(), col.NullCount())
				}
			}
			if v, ok := blk.Column(1).Text(0); !ok || v != "" {
				t.Errorf("empty field = %q, %v; want empty text (D30)", v, ok)
			}
			rows += blk.Len()
		}
		if rows != 7 {
			t.Errorf("length %d: %d rows, want 7", length, rows)
		}
		if v, _ := blocks[len(blocks)-1].Column(0).Text(blocks[len(blocks)-1].Len() - 1); v != "k6" {
			t.Errorf("last row = %q", v)
		}
	}
}

func TestTextBlockBuilderRejectsWrongWidth(t *testing.T) {
	tb, _ := NewTextBuilder(2, 4)
	if _, _, err := tb.Add([]string{"only one"}); err == nil {
		t.Error("row of wrong width accepted")
	}
}
