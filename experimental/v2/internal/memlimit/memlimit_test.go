package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDetectFromCgroup(t *testing.T) {
	const phys = 64 << 30
	v2 := t.TempDir()
	write(t, filepath.Join(v2, "memory.max"), "1073741824\n")
	nested := t.TempDir()
	write(t, filepath.Join(nested, "app", "memory.max"), "536870912\n")
	write(t, filepath.Join(nested, "self"), "0::/app\n")
	v1 := t.TempDir()
	write(t, filepath.Join(v1, "memory", "memory.limit_in_bytes"), "2147483648\n")
	unlimited := t.TempDir()
	write(t, filepath.Join(unlimited, "memory.max"), "max\n")
	v1unlimited := t.TempDir()
	write(t, filepath.Join(v1unlimited, "memory", "memory.limit_in_bytes"), "9223372036854771712\n")

	for _, c := range []struct {
		name, root, self string
		want             int64
	}{
		{"v2", v2, "", 1 << 30},
		{"v2 nested", nested, filepath.Join(nested, "self"), 512 << 20},
		{"v1", v1, "", 2 << 30},
		{"v2 max", unlimited, "", phys},
		{"v1 unlimited", v1unlimited, "", phys},
		{"none", t.TempDir(), "", phys},
	} {
		if got := DetectFrom(c.root, c.self, phys); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
	if got := DetectFrom(v2, "", 512<<20); got != 512<<20 {
		t.Errorf("limit above physical memory: %d, want the physical memory", got)
	}
}

func TestPhysicalIsKnown(t *testing.T) {
	if Physical() <= 0 {
		t.Skip("physical memory unknown on this system")
	}
	if Detect() <= 0 {
		t.Error("Detect() = 0 with known physical memory")
	}
}
