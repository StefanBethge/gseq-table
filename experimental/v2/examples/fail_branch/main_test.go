package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestFailBranch(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out, "testdata"); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		// Two rows rejected, within the threshold: rescued rows do not
		// count (D46).
		"Status: ok\n",
		// The recovered row flows back in its place (D25, D90).
		"1   K1        2026-09-01T00:00:00Z  12.5\n2   K2        2026-09-03T00:00:00Z  20\n4   K4",
		// Rescued rows passed the step and are shown apart (D46).
		"Step paid_on    read 5, passed 4, rejected 1, rescued 2\n",
		// Failing again in the branch: the whole path and the reason of the
		// main path (D27, D92).
		"3   gestern     5.00    4           paid_on › paid_on_de  not a date in format \"02.01.2006\": \"gestern\"  not a date: \"gestern\"\n",
		// Recovered, then failing later in the main path (D46).
		"5   05.09.2026  n.a.    6           paid_on › amount      not a number: \"n.a.\"                          not a date: \"05.09.2026\"\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
}

func TestFailBranchMissingData(t *testing.T) {
	if err := run(context.Background(), &bytes.Buffer{}, t.TempDir()); err == nil {
		t.Error("no error for a missing delivery")
	}
}
