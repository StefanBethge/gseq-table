package gtable

import (
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

func TestTypeString(t *testing.T) {
	want := map[Type]string{
		TypeText:      "text",
		TypeInt:       "int",
		TypeFloat:     "float",
		TypeBool:      "bool",
		TypeTimestamp: "timestamp",
	}
	for typ, name := range want {
		if typ.String() != name {
			t.Errorf("%d.String() = %q, want %q", typ, typ.String(), name)
		}
	}
}

func TestTypeMatchesBlockKind(t *testing.T) {
	pairs := map[Type]block.Kind{
		TypeText:      block.Text,
		TypeInt:       block.Int,
		TypeFloat:     block.Float,
		TypeBool:      block.Bool,
		TypeTimestamp: block.Timestamp,
	}
	for typ, kind := range pairs {
		if typeOf(kind) != typ {
			t.Errorf("typeOf(%v) = %v, want %v", kind, typeOf(kind), typ)
		}
	}
}

func TestColumnView(t *testing.T) {
	b := block.NewBuilder(block.Text, 3)
	b.AppendText("a")
	b.AppendText("")
	b.AppendNull()
	c := newColumn("name", b.Build())

	if c.Name() != "name" || c.Type() != TypeText || c.Len() != 3 {
		t.Errorf("Name=%q Type=%v Len=%d", c.Name(), c.Type(), c.Len())
	}
	if v, ok := c.Text(1); !ok || v != "" {
		t.Errorf("Text(1) = %q, %v; want empty text", v, ok)
	}
	if _, ok := c.Text(2); ok || !c.IsNull(2) {
		t.Error("cell 2 is not null")
	}
	if c.NullCount() != 1 {
		t.Errorf("NullCount = %d", c.NullCount())
	}
}

func TestColumnViewTypedAccessors(t *testing.T) {
	ts := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)

	ib := block.NewBuilder(block.Int, 1)
	ib.AppendInt(42)
	if v, ok := newColumn("i", ib.Build()).Int(0); !ok || v != 42 {
		t.Errorf("Int = %d, %v", v, ok)
	}
	fb := block.NewBuilder(block.Float, 1)
	fb.AppendFloat(0.5)
	if v, ok := newColumn("f", fb.Build()).Float(0); !ok || v != 0.5 {
		t.Errorf("Float = %v, %v", v, ok)
	}
	bb := block.NewBuilder(block.Bool, 1)
	bb.AppendNull()
	if _, ok := newColumn("b", bb.Build()).Bool(0); ok {
		t.Error("Bool of a null cell is ok")
	}
	tb := block.NewBuilder(block.Timestamp, 1)
	tb.AppendTimestamp(ts)
	if v, ok := newColumn("t", tb.Build()).Timestamp(0); !ok || !v.Equal(ts) {
		t.Errorf("Timestamp = %v, %v", v, ok)
	}
}
