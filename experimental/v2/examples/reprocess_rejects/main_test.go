package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReprocessRejects(t *testing.T) {
	var out bytes.Buffer
	dir := t.TempDir()
	if err := run(context.Background(), &out, "testdata", dir); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"First run: status ok, 6 read, 3 passed, 3 rejected, exit code 0\n",
		// The two dates in another layout pass; the broken line is split
		// now but its amount fails again (D16).
		"Reprocessing: status ok, 3 read, 2 passed, 1 rejected, exit code 0\n",
		// It points to the original delivery (D82).
		"1004   7\"      orders.csv    5           parse",
		"Whole delivery again: status ok, 6 read, 5 passed, 1 rejected, exit code 0\n",
		// Upserted on row_key, the target has each order once (D47).
		"Target, upserted on row_key (5 rows):\n",
		"  line 3   1002  K2  5  2026-09-28T00:00:00Z\n",
		"  line 6   1005  K4  3.2  2026-09-29T00:00:00Z\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	// The rejected rows of the first run are in a file (D2).
	b, err := os.ReadFile(filepath.Join(dir, "orders-rejects.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "\n"); n != 4 {
		t.Errorf("file of rejected rows has %d lines, want a header and 3 rows:\n%s", n, b)
	}
}

func TestReprocessRejectsMissingData(t *testing.T) {
	if err := run(context.Background(), &bytes.Buffer{}, t.TempDir(), t.TempDir()); err == nil {
		t.Error("no error for a missing delivery")
	}
}
