package block

import (
	"testing"
	"time"
)

func TestColumnKinds(t *testing.T) {
	ts := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		kind  Kind
		build func(b *Builder)
		name  string
	}{
		{Text, func(b *Builder) { b.AppendText("a") }, "text"},
		{Int, func(b *Builder) { b.AppendInt(-7) }, "int"},
		{Float, func(b *Builder) { b.AppendFloat(1.5) }, "float"},
		{Bool, func(b *Builder) { b.AppendBool(true) }, "bool"},
		{Timestamp, func(b *Builder) { b.AppendTimestamp(ts) }, "timestamp"},
	}
	for _, tc := range cases {
		b := NewBuilder(tc.kind, 0)
		tc.build(b)
		c := b.Build()
		if c.Kind() != tc.kind || c.Len() != 1 || c.IsNull(0) {
			t.Errorf("%s: kind=%v len=%d null=%v", tc.name, c.Kind(), c.Len(), c.IsNull(0))
		}
		if got := tc.kind.String(); got != tc.name {
			t.Errorf("Kind.String() = %q, want %q", got, tc.name)
		}
	}

	b := NewBuilder(Int, 0)
	b.AppendInt(3)
	c := b.Build()
	if v, ok := c.Int(0); !ok || v != 3 {
		t.Errorf("Int(0) = %d, %v", v, ok)
	}
	b = NewBuilder(Float, 0)
	b.AppendFloat(2.25)
	if v, ok := b.Build().Float(0); !ok || v != 2.25 {
		t.Errorf("Float(0) = %v, %v", v, ok)
	}
	b = NewBuilder(Bool, 0)
	b.AppendBool(true)
	if v, ok := b.Build().Bool(0); !ok || !v {
		t.Errorf("Bool(0) = %v, %v", v, ok)
	}
	b = NewBuilder(Timestamp, 0)
	b.AppendTimestamp(ts)
	if v, ok := b.Build().Timestamp(0); !ok || !v.Equal(ts) {
		t.Errorf("Timestamp(0) = %v, %v", v, ok)
	}
}

func TestColumnAppendWrongKindPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("AppendInt on a text builder did not panic")
		}
	}()
	NewBuilder(Text, 0).AppendInt(1)
}

func TestColumnReadWrongKindPanics(t *testing.T) {
	b := NewBuilder(Text, 0)
	b.AppendText("1")
	c := b.Build()
	defer func() {
		if recover() == nil {
			t.Error("Int on a text column did not panic")
		}
	}()
	c.Int(0)
}

// D30: null is distinct from empty text.
func TestColumnNullIsNotEmptyText(t *testing.T) {
	b := NewBuilder(Text, 3)
	b.AppendText("")
	b.AppendNull()
	b.AppendText("x")
	c := b.Build()

	if c.IsNull(0) {
		t.Error("empty text is null")
	}
	if v, ok := c.Text(0); !ok || v != "" {
		t.Errorf("Text(0) = %q, %v; want empty text", v, ok)
	}
	if !c.IsNull(1) {
		t.Error("null cell is not null")
	}
	if v, ok := c.Text(1); ok || v != "" {
		t.Errorf("Text(1) = %q, %v; want null", v, ok)
	}
	if c.NullCount() != 1 {
		t.Errorf("NullCount = %d, want 1", c.NullCount())
	}
}

func TestColumnNullsInEveryKind(t *testing.T) {
	for _, k := range []Kind{Text, Int, Float, Bool, Timestamp} {
		b := NewBuilder(k, 0)
		for range 130 { // crosses two bitmap words
			b.AppendNull()
		}
		c := b.Build()
		if c.Len() != 130 || c.NullCount() != 130 {
			t.Errorf("%v: len=%d nulls=%d", k, c.Len(), c.NullCount())
		}
		for i := range 130 {
			if !c.IsNull(i) {
				t.Fatalf("%v: cell %d not null", k, i)
			}
		}
	}
}

func TestColumnWithoutNulls(t *testing.T) {
	b := NewBuilder(Int, 0)
	for i := range 100 {
		b.AppendInt(int64(i))
	}
	c := b.Build()
	if c.NullCount() != 0 {
		t.Errorf("NullCount = %d", c.NullCount())
	}
	if len(c.Ints()) != 100 || c.Ints()[99] != 99 {
		t.Errorf("Ints() = %v", c.Ints())
	}
}

func TestColumnSliceAccessorsMatchKind(t *testing.T) {
	b := NewBuilder(Text, 0)
	b.AppendText("a")
	b.AppendNull()
	c := b.Build()
	if text, offs := c.TextData(); string(text) != "a" || len(offs) != 3 || offs[1] != 1 || offs[2] != 1 {
		t.Errorf("TextData() = %q, %v", text, offs)
	}
	if c.Ints() != nil || c.Floats() != nil || c.Bools() != nil || c.Timestamps() != nil {
		t.Error("slice accessor of another kind is not nil")
	}
}

func TestBuilderIsReusableAfterBuild(t *testing.T) {
	b := NewBuilder(Int, 0)
	b.AppendInt(1)
	first := b.Build()
	b.AppendNull()
	b.AppendInt(2)
	second := b.Build()
	if first.Len() != 1 || first.NullCount() != 0 {
		t.Errorf("first changed after reuse: len=%d nulls=%d", first.Len(), first.NullCount())
	}
	if second.Len() != 2 || !second.IsNull(0) {
		t.Errorf("second: len=%d null(0)=%v", second.Len(), second.IsNull(0))
	}
}

// D55 groundwork: a shared column is copied on its first change, and the
// other owner keeps the original values.
func TestColumnCopyOnWrite(t *testing.T) {
	b := NewBuilder(Text, 0)
	b.AppendText("raw")
	b.AppendText("")
	raw := b.Build()
	if raw.Shared() {
		t.Fatal("new column is shared")
	}

	work := raw.Share()
	if !raw.Shared() || !work.Shared() {
		t.Fatal("columns are not marked shared after Share")
	}
	if work.Len() != 2 {
		t.Fatalf("shared column len = %d", work.Len())
	}

	if copied := work.Mutate(); !copied {
		t.Error("Mutate on a shared column did not copy")
	}
	if work.Shared() || raw.Shared() {
		t.Errorf("after copy: work.Shared=%v raw.Shared=%v", work.Shared(), raw.Shared())
	}
	work.SetText(0, "changed")
	work.SetNull(1)

	if v, _ := raw.Text(0); v != "raw" {
		t.Errorf("raw state changed: %q", v)
	}
	if raw.IsNull(1) {
		t.Error("raw cell became null")
	}
	if v, _ := work.Text(0); v != "changed" || !work.IsNull(1) {
		t.Errorf("work = %q null(1)=%v", v, work.IsNull(1))
	}

	if copied := work.Mutate(); copied {
		t.Error("Mutate on an exclusive column copied")
	}
}

func TestColumnSetOnSharedPanics(t *testing.T) {
	b := NewBuilder(Int, 0)
	b.AppendInt(1)
	c := b.Build()
	other := c.Share()
	_ = other
	defer func() {
		if recover() == nil {
			t.Error("SetInt on a shared column did not panic")
		}
	}()
	c.SetInt(0, 2)
}

func TestColumnReleaseEndsSharing(t *testing.T) {
	b := NewBuilder(Int, 0)
	b.AppendInt(1)
	c := b.Build()
	other := c.Share()
	other.Release()
	if c.Shared() {
		t.Error("column still shared after the other owner released it")
	}
	if copied := c.Mutate(); copied {
		t.Error("Mutate copied although no other owner is left")
	}
	c.SetInt(0, 5)
	if v, _ := c.Int(0); v != 5 {
		t.Errorf("Int(0) = %d", v)
	}
}

func TestColumnSetters(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	check := func(k Kind, set func(c *Column), get func(c Column) bool) {
		t.Helper()
		b := NewBuilder(k, 0)
		b.AppendNull()
		c := b.Build()
		set(&c)
		if c.IsNull(0) || !get(c) || c.NullCount() != 0 {
			t.Errorf("%v: setter did not store a value", k)
		}
	}
	check(Text, func(c *Column) { c.SetText(0, "") }, func(c Column) bool { v, ok := c.Text(0); return ok && v == "" })
	check(Int, func(c *Column) { c.SetInt(0, 4) }, func(c Column) bool { v, ok := c.Int(0); return ok && v == 4 })
	check(Float, func(c *Column) { c.SetFloat(0, 0.5) }, func(c Column) bool { v, ok := c.Float(0); return ok && v == 0.5 })
	check(Bool, func(c *Column) { c.SetBool(0, false) }, func(c Column) bool { v, ok := c.Bool(0); return ok && !v })
	check(Timestamp, func(c *Column) { c.SetTimestamp(0, ts) }, func(c Column) bool { v, ok := c.Timestamp(0); return ok && v.Equal(ts) })
}

func TestColumnBytesCountsValuesAndNulls(t *testing.T) {
	b := NewBuilder(Text, 0)
	b.AppendText("abc")
	b.AppendNull()
	c := b.Build()
	want := int64(3 + 3*4 + 8) // bytes, offsets, null bitmap
	if got := c.Bytes(); got != want {
		t.Errorf("Bytes() = %d, want %d", got, want)
	}
	ib := NewBuilder(Int, 0)
	ib.AppendInt(1)
	if got := ib.Build().Bytes(); got != 8 {
		t.Errorf("int Bytes() = %d, want 8", got)
	}
}

func TestAppendFromCopiesCellsAndNulls(t *testing.T) {
	ts := time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)
	src := []Column{}
	for _, k := range []Kind{Text, Int, Float, Bool, Timestamp} {
		b := NewBuilder(k, 2)
		switch k {
		case Text:
			b.AppendText("x")
		case Int:
			b.AppendInt(3)
		case Float:
			b.AppendFloat(2.5)
		case Bool:
			b.AppendBool(true)
		case Timestamp:
			b.AppendTimestamp(ts)
		}
		b.AppendNull()
		src = append(src, b.Build())
	}
	for _, c := range src {
		b := NewBuilder(c.Kind(), 2)
		b.AppendFrom(c, 1)
		b.AppendFrom(c, 0)
		got := b.Build()
		if !got.IsNull(0) || got.IsNull(1) {
			t.Errorf("%v: nulls not copied", c.Kind())
		}
	}
	b := NewBuilder(Timestamp, 1)
	b.AppendFrom(src[4], 0)
	if v, _ := b.Build().Timestamp(0); !v.Equal(ts) {
		t.Errorf("timestamp = %v, want %v", v, ts)
	}
}
