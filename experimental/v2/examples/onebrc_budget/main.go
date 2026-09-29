// Command onebrc_budget runs the One Billion Row Challenge over a delivery
// in its format: station and measurement separated by ';', no header line.
// It groups by station with min, mean and max and sorts by station, under a
// memory budget (UC6; design decisions D6, D28, D60). Sort and group by
// spill to disk when the budget is reached, and the engine sets GOMEMLIMIT
// on request (D65, D101, D102).
//
// In CI it runs on testdata/measurements.txt, a small synthetic file with
// placeholders, a decimal comma and a line with a third field;
// testdata/gen.go writes it. The real 1BRC file is not part of the repo:
// pass its path with -data or ONEBRC_FILE. README.md shows how to run it in
// Docker with memory limits.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
)

type config struct {
	path     string
	budget   int64 // cap of the run in bytes; 0: the budget of the process
	blockLen int
	managed  bool // let the engine set GOMEMLIMIT
	spillDir string
}

func main() {
	path := os.Getenv("ONEBRC_FILE")
	if path == "" {
		path = "testdata/measurements.txt"
	}
	cfg := config{}
	flag.StringVar(&cfg.path, "data", path, "delivery in 1BRC format (default $ONEBRC_FILE or the test data)")
	flag.Int64Var(&cfg.budget, "budget", 0, "memory cap of the run in bytes (0: a tenth of the memory limit)")
	flag.IntVar(&cfg.blockLen, "block", 1<<14, "rows per block")
	flag.BoolVar(&cfg.managed, "managed", true, "let the engine set GOMEMLIMIT to 90 % of the memory limit")
	flag.StringVar(&cfg.spillDir, "spill", "", "directory to spill to (default: the temporary directory)")
	flag.Parse()
	code, err := run(context.Background(), os.Stdout, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

// run runs the pipeline and writes the stations and a summary to w. It
// returns the exit code of the run (D86).
func run(ctx context.Context, w io.Writer, cfg config) (int, error) {
	src := csv.File(cfg.path, csv.Comma(';'), csv.NoHeader()).Expect("station", "measurement")
	p := gtable.FromSource(src, cfg.blockLen).
		Then(gtable.Cast("measurement", gtable.TypeFloat)).
		Then(gtable.GroupBy([]string{"station"},
			gtable.Min("measurement").As("min"),
			gtable.Mean("measurement").As("mean"),
			gtable.Max("measurement").As("max"))).
		Then(gtable.Sort(gtable.Asc("station")))
	if cfg.budget > 0 {
		p.MemoryBudget(cfg.budget)
	}
	if cfg.managed {
		p.WithManagedMemory()
	}
	if cfg.spillDir != "" {
		p.SpillDir(cfg.spillDir)
	}
	res, err := p.Run(ctx)
	// Rejected rows the result spilled live until Close (D49).
	defer res.Close()
	if err != nil {
		return res.ExitCode(), err
	}

	var parts []string
	for row := range res.Table.Rows() {
		st, _ := row.Text("station")
		lo, _ := row.Float("min")
		mean, _ := row.Float("mean")
		hi, _ := row.Float("max")
		parts = append(parts, fmt.Sprintf("%s=%.1f/%.1f/%.1f", st, lo, mean, hi))
	}
	fmt.Fprintf(w, "{%s}\n", strings.Join(parts, ", "))

	c := res.Counts
	fmt.Fprintf(w, "status %s, read %d, passed %d, rejected %d\n", res.Status, c.Read, c.Passed, c.Rejected)
	codes := make([]string, 0, len(c.ByCode))
	for code, n := range c.ByCode {
		codes = append(codes, fmt.Sprintf("%s %d", code, n))
	}
	sort.Strings(codes)
	if len(codes) > 0 {
		fmt.Fprintf(w, "rejected by code: %s\n", strings.Join(codes, ", "))
	}
	return res.ExitCode(), nil
}
