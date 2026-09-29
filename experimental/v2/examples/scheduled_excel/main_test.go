package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScheduledExcel(t *testing.T) {
	for _, tc := range []struct {
		day  string
		code int
		want []string
	}{
		// A normal day: one placeholder amount is rejected, a cancelled
		// order is dropped, and the new column is reported (D22).
		{"2026-09-27", 0, []string{
			"2026-09-27.xlsx: status ok, exit code 0\n",
			"  6 read, 4 passed, 1 rejected, 1 dropped\n",
			"new_column  note ",
		}},
		// Amounts as text with a decimal comma: a format change, and the
		// threshold fails the run (D20, D23).
		{"2026-09-28", 1, []string{
			"2026-09-28.xlsx: status failed_threshold, exit code 1\n",
			"format_change  amount  3 of 4 rows failed with code parse in step cast_all  3      \"19,90\", \"7,50\", \"3,20\"\n",
		}},
		// A renamed column: a delivery error before the threshold (D42, D63).
		{"2026-09-29", 3, []string{
			"2026-09-29.xlsx: status delivery_error, exit code 3\n",
			"  failed_threshold: threshold exceeded",
			"probably_renamed  ordered     ordered_at",
		}},
	} {
		t.Run(tc.day, func(t *testing.T) {
			var out bytes.Buffer
			dir := t.TempDir()
			code, err := run(context.Background(), &out, filepath.Join("testdata", tc.day+".xlsx"), dir)
			if err != nil || code != tc.code {
				t.Fatalf("exit code %d, err %v, want %d:\n%s", code, err, tc.code, out.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output lacks %q:\n%s", want, out.String())
				}
			}
			// The results and the rejected rows are in their files (D49).
			for name, lines := range map[string]int{
				"orders.csv": map[string]int{"2026-09-27": 5, "2026-09-28": 2, "2026-09-29": 1}[tc.day],
				"rejects-" + tc.day + ".xlsx-Bestellungen.csv": map[string]int{"2026-09-27": 2, "2026-09-28": 4, "2026-09-29": 3}[tc.day],
			} {
				b, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				if n := strings.Count(string(b), "\n"); n != lines {
					t.Errorf("%s has %d lines, want %d:\n%s", name, n, lines, b)
				}
			}
		})
	}
}

func TestScheduledExcelMissingDelivery(t *testing.T) {
	code, err := run(context.Background(), &bytes.Buffer{}, filepath.Join(t.TempDir(), "none.xlsx"), t.TempDir())
	if err == nil || code != 3 {
		t.Errorf("exit code %d, err %v, want 3 and an error", code, err)
	}
}
