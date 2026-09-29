// Command scheduled_excel is a run that cron starts every day over the
// Excel delivery of a supplier (UC1, UC2; design decisions D18, D20, D21,
// D22, D23, D42, D45, D49, D63).
//
// The source has an expected layout and a business key (D22, D18). The
// steps cast, derive and filter; a threshold fails the run if more than a
// fifth of the rows are rejected (D20). The results go to a CSV sink, the
// rejected rows of every source to a CSV file of their own through a
// writer in the plan, and the overview to one more file (D49). The run
// prints its status, counts and change report (D23), and exits with the
// exit code of its status, so cron sees a changed delivery (D21, D86).
//
// The deliveries in testdata are small and synthetic (D60): a normal day,
// a day with amounts as text with a decimal comma, and a day with a
// renamed column. testdata/gen.go writes them. A database sink needs the
// writers of F20, which are not part of the prototype.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
)

func main() {
	delivery := flag.String("delivery", "examples/scheduled_excel/testdata/2026-09-27.xlsx", "the Excel delivery of the day")
	out := flag.String("out", os.TempDir(), "directory for the results and the rejected rows")
	flag.Parse()
	code, err := run(context.Background(), os.Stdout, *delivery, *out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

// blockLen is small so the example runs over several blocks.
const blockLen = 2

// run runs the pipeline over one delivery and returns the exit code for
// cron.
func run(ctx context.Context, w io.Writer, delivery, outDir string) (int, error) {
	src := excel.Sheet(delivery, "Bestellungen").
		Expect("order", "customer", "amount", "ordered").
		Key("order")

	res, err := gtable.FromSource(src, blockLen).
		Then(gtable.CastAll(
			gtable.Cast("amount", gtable.TypeFloat),
			gtable.Cast("ordered", gtable.TypeTimestamp),
		)).
		Then(gtable.With("gross", gtable.Col("amount").Mul(gtable.Lit(1.19)).Round(2))).
		Step("cancelled", gtable.Where(gtable.Col("amount").Gt(gtable.Lit(0)))).
		Threshold(gtable.MaxRejectedShare(0.2)).
		To(csv.Create(filepath.Join(outDir, "orders.csv"))).
		RejectsToEach(func(source string) gtable.Sink {
			return csv.Create(filepath.Join(outDir, "rejects-"+strings.ReplaceAll(source, "#", "-")+".csv"))
		}).
		OverviewTo(csv.Create(filepath.Join(outDir, "overview.csv"))).
		Run(ctx)

	c := res.Counts
	fmt.Fprintf(w, "%s: status %v, exit code %d\n", filepath.Base(delivery), res.Status, res.ExitCode())
	fmt.Fprintf(w, "  %d read, %d passed, %d rejected, %d dropped\n", c.Read, c.Passed, c.Rejected, c.Dropped)
	for _, cause := range res.Causes {
		fmt.Fprintf(w, "  %v\n", cause)
	}
	if len(res.Report) > 0 {
		report := res.ChangeReport().Select("kind", "column", "detail", "count", "examples")
		fmt.Fprintf(w, "Change report:\n%s", report)
	}
	return res.ExitCode(), err
}
