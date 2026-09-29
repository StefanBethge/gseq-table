package spill

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

func TestBlockRoundTrip(t *testing.T) {
	ts := time.Date(2026, 9, 29, 8, 30, 0, 5, time.FixedZone("", 7200))
	text := block.NewBuilder(block.Text, 3)
	text.AppendText("a")
	text.AppendNull()
	text.AppendText("")
	ints := block.NewBuilder(block.Int, 3)
	ints.AppendInt(-5)
	ints.AppendInt(1 << 40)
	ints.AppendNull()
	flts := block.NewBuilder(block.Float, 3)
	flts.AppendFloat(-0.25)
	flts.AppendNull()
	flts.AppendFloat(1e300)
	bools := block.NewBuilder(block.Bool, 3)
	bools.AppendBool(true)
	bools.AppendBool(false)
	bools.AppendNull()
	times := block.NewBuilder(block.Timestamp, 3)
	times.AppendNull()
	times.AppendTimestamp(ts)
	times.AppendTimestamp(time.Time{})
	b, err := block.New(text.Build(), ints.Build(), flts.Build(), bools.Build(), times.Build())
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Block(b)
	w.String("tail")
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if w.Offset() != int64(buf.Len()) {
		t.Errorf("Offset() = %d, wrote %d", w.Offset(), buf.Len())
	}
	r := NewReader(&buf)
	got := r.Block()
	if s := r.String(); s != "tail" || r.Err() != nil {
		t.Fatalf("tail = %q, err %v", s, r.Err())
	}
	if got.Len() != 3 || got.Width() != 5 {
		t.Fatalf("got %d rows, %d columns", got.Len(), got.Width())
	}
	for c := range b.Width() {
		for i := range 3 {
			if b.Column(c).IsNull(i) != got.Column(c).IsNull(i) {
				t.Errorf("column %d row %d: null differs", c, i)
			}
		}
	}
	if v, _ := got.Column(0).Text(2); v != "" || got.Column(0).IsNull(2) {
		t.Errorf("empty text became %q or null", v)
	}
	if v, _ := got.Column(1).Int(1); v != 1<<40 {
		t.Errorf("int = %d", v)
	}
	if v, _ := got.Column(2).Float(0); v != -0.25 {
		t.Errorf("float = %v", v)
	}
	if v, _ := got.Column(4).Timestamp(1); !v.Equal(ts) || v.Format(time.RFC3339Nano) != ts.Format(time.RFC3339Nano) {
		t.Errorf("timestamp = %v, want %v", v, ts)
	}

	short := NewReader(bytes.NewReader(buf.Bytes()[:0]))
	short.Block()
	if short.Err() == nil {
		t.Error("reading an empty file gave no error")
	}
}

func TestDirIsPrivateAndRemovedWithItsLock(t *testing.T) {
	root := filepath.Join(t.TempDir(), "spill")
	d, err := Create(root)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.CreateFile("run-*")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	for path, want := range map[string]fs.FileMode{d.Path(): 0o700, f.Name(): 0o600} {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := st.Mode().Perm(); got != want {
			t.Errorf("%s: mode %v, want %v", path, got, want)
		}
	}
	if err := d.Remove(); err != nil {
		t.Fatal(err)
	}
	if err := d.Remove(); err != nil {
		t.Errorf("second Remove: %v", err)
	}
	if left, _ := os.ReadDir(root); len(left) != 0 {
		t.Errorf("left in the root: %v", left)
	}
}

func TestCleanOrphansKeepsLockedDirs(t *testing.T) {
	root := t.TempDir()
	live, err := Create(root)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Remove()
	dead, err := Create(root)
	if err != nil {
		t.Fatal(err)
	}
	dead.lock.Close() // as if its process had ended
	half := filepath.Join(root, prefix+"half")
	if err := os.Mkdir(half, 0o700); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(root, "other")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}

	CleanOrphans(root)
	for path, want := range map[string]bool{live.Path(): true, dead.Path(): false, dead.Path() + ".lock": false, half: false, other: true} {
		_, err := os.Stat(path)
		if got := err == nil; got != want {
			t.Errorf("%s exists = %v, want %v", filepath.Base(path), got, want)
		}
	}
}
