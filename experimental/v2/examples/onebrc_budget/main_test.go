package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// expected computes the result of the challenge directly from the file,
// skipping the lines the pipeline rejects.
func expected(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type agg struct{ min, max, sum, n float64 }
	stats := map[string]*agg{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ";")
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		a, ok := stats[parts[0]]
		if !ok {
			a = &agg{min: math.Inf(1), max: math.Inf(-1)}
			stats[parts[0]] = a
		}
		a.min, a.max = min(a.min, v), max(a.max, v)
		a.sum += v
		a.n++
	}
	names := make([]string, 0, len(stats))
	for n := range stats {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, n := range names {
		a := stats[n]
		parts[i] = fmt.Sprintf("%s=%.1f/%.1f/%.1f", n, a.min, a.sum/a.n, a.max)
	}
	return "{" + strings.Join(parts, ", ") + "}\n"
}

func TestOneBRCUnderABudget(t *testing.T) {
	const path = "testdata/measurements.txt"
	want := expected(t, path)
	var first string
	// Without a cap of its own, and with a cap far below the delivery, so
	// that group by and sort spill (D6).
	for _, budget := range []int64{0, 16 << 10} {
		spill := t.TempDir()
		var out bytes.Buffer
		code, err := run(context.Background(), &out, config{path: path, budget: budget, blockLen: 64, spillDir: spill})
		if err != nil {
			t.Fatal(err)
		}
		got := out.String()
		if code != 0 || !strings.HasPrefix(got, want) {
			t.Errorf("budget %d: exit code %d, output\n%s\nwant\n%s", budget, code, got, want)
		}
		// Four lines are rejected: two placeholders and a decimal comma
		// fail the cast, the line with a third field cannot be split.
		for _, line := range []string{
			"status ok, read 3000, passed 2996, rejected 4\n",
			"rejected by code: parse 3, unparseable_line 1\n",
		} {
			if !strings.Contains(got, line) {
				t.Errorf("budget %d: output lacks %q:\n%s", budget, line, got)
			}
		}
		if first == "" {
			first = got
		} else if got != first {
			t.Errorf("the spilled run differs:\n%s\nfrom\n%s", got, first)
		}
		if left, _ := os.ReadDir(spill); len(left) != 0 {
			t.Errorf("left in the spill directory: %v", left)
		}
	}
}

func TestOneBRCMissingFile(t *testing.T) {
	code, err := run(context.Background(), &bytes.Buffer{}, config{path: t.TempDir() + "/none.txt", blockLen: 64})
	if err == nil || code != 3 {
		t.Errorf("exit code %d, err %v; want delivery_error (3)", code, err)
	}
}
