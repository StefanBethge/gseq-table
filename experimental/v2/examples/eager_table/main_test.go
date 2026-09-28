package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestEagerTable(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out, "testdata"); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		// The rows that fit are aggregated; an empty amount and "n/a" are
		// null and skipped by Sum and Count (D30).
		"Nordlicht GmbH  142.8        1\n",
		"Bäckerei Sonne  47.6         1\n",
		"Hafen & Co      <null>       0\n",
		// The rows that do not fit are rejected with step, column, value and
		// code (D50).
		`step=cast column=amount value="12,50" code=parse`,
		`step=cast column=ordered value="2026-09-28" code=parse`,
		// The unknown column sticks to the table.
		`Sticky error: plan error in step where: unknown column "revenue"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	if n := strings.Count(got, "step=cast"); n != 2 {
		t.Errorf("%d rejected rows, want 2:\n%s", n, got)
	}
}

func TestEagerTableMissingData(t *testing.T) {
	if err := run(&bytes.Buffer{}, t.TempDir()); err == nil {
		t.Error("no error for a missing delivery")
	}
}
