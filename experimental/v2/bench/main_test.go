package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Every case gives the same number of result rows in every
// implementation, so that the benchmarks compare the same work.
func TestCasesAgreeAcrossImplementations(t *testing.T) {
	dir := t.TempDir()
	const rows = 5000
	for _, kind := range []string{"num", "text"} {
		if err := generate(dir, kind, rows); err != nil {
			t.Fatal(err)
		}
		for _, kase := range caseNames {
			want := -1
			for _, impl := range []string{"v1t", "v1m", "v2", "v2noraw"} {
				for _, sink := range []bool{false, true} {
					if sink && !strings.HasPrefix(impl, "v2") {
						continue
					}
					c := runConfig{impl: impl, kase: kase, kind: kind, rows: rows, dir: dir, block: 512, sink: sink,
						budget: 64 << 10, spill: t.TempDir()}
					n, err := runCase(context.Background(), c)
					if err != nil {
						t.Fatalf("%s %s %s: %v", kind, kase, impl, err)
					}
					if want < 0 {
						want = n
					} else if n != want {
						t.Errorf("%s %s %s sink %v: %d rows, want %d", kind, kase, impl, sink, n, want)
					}
				}
			}
			if want <= 0 && kase != "join" {
				t.Errorf("%s %s: no rows", kind, kase)
			}
		}
	}
}

func TestExcelAndOneBRCCasesRun(t *testing.T) {
	dir := t.TempDir()
	if err := generateExcel(dir, 300); err != nil {
		t.Fatal(err)
	}
	n, err := runCase(context.Background(), runConfig{impl: "v2", kase: "excel", kind: "num", rows: 300, dir: dir, block: 64})
	if err != nil || n != 300 {
		t.Errorf("excel: %d rows, %v", n, err)
	}
	n, err = runCase(context.Background(), runConfig{impl: "v2", kase: "onebrc", block: 64,
		data: filepath.Join("..", "examples", "onebrc_budget", "testdata", "measurements.txt")})
	if err != nil || n == 0 {
		t.Errorf("onebrc: %d rows, %v", n, err)
	}
}

// measure runs a case in a child process and records it.
func TestMeasureRecordsAChildProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the command")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bench")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	out := filepath.Join(dir, "r.jsonl")
	cmd := exec.Command(bin, "suite", "-dir", dir, "-plan", "compare", "-rows", "2000", "-block", "512", "-reps", "1", "-out", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("suite: %v\n%s", err, b)
	}
	var sb strings.Builder
	if err := cmdReport([]string{out}, &sb); err != nil {
		t.Fatal(err)
	}
	rep := sb.String()
	for _, want := range []string{"### G5", "| groupby |", "### G13: v2 ohne gegen mit Rohzustand"} {
		if !strings.Contains(rep, want) {
			t.Errorf("report lacks %q:\n%s", want, rep)
		}
	}
	if strings.Contains(rep, "| – | – | – | – | – |") {
		t.Errorf("report misses measurements:\n%s", rep)
	}
}
