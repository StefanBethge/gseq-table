// Command fail_branch runs a delivery whose payment dates come in two
// formats. The rows the date cast fails go into a fail branch that tries
// the other format (UC5; design decisions D25, D26). A recovered row flows
// back into the main path in its place. A row that fails in the branch too
// is rejected with its reject_id, its whole path in step and the reason of
// the main path in prev_reason (D27). A recovered row that fails later in
// the main path keeps that history, and only rows that stay rejected count
// for the threshold (D46).
//
// The delivery in testdata is small, synthetic and dirty (D60).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
)

func main() {
	dir := flag.String("data", "testdata", "directory with payments.csv")
	flag.Parse()
	if err := run(context.Background(), os.Stdout, *dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// blockLen is small so the example runs over several blocks.
const blockLen = 2

func run(ctx context.Context, w io.Writer, dir string) error {
	res, err := gtable.FromSource(csv.File(filepath.Join(dir, "payments.csv")), blockLen).
		Step("paid_on", gtable.Cast("paid_on", gtable.TypeTimestamp)).
		// The branch sees the failed rows as they went into the cast, with
		// the info columns of the error, and tries the German format.
		OnFail(gtable.NewBranch().
			Step("paid_on_de", gtable.Cast("paid_on", gtable.TypeTimestamp, gtable.DateFormat("02.01.2006")))).
		Step("amount", gtable.Cast("amount", gtable.TypeFloat)).
		// Rows recovered by the branch do not count for the threshold.
		Threshold(gtable.MaxRejected(2)).
		Run(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "Status: %s\n\n", res.Status)
	fmt.Fprintf(w, "Payments that passed:\n%s\n", res.Table)

	for _, s := range res.Steps {
		fmt.Fprintf(w, "Step %-10s read %d, passed %d, rejected %d, rescued %d\n",
			s.Step, s.Read, s.Passed, s.Rejected, s.Rescued)
	}
	fmt.Fprintln(w)

	p := gtable.DefaultInfoPrefix
	rows, ok := res.Table.RejectedRows().Source("payments.csv")
	if !ok {
		return fmt.Errorf("no rejected rows of payments.csv")
	}
	rows = rows.Select("id", "paid_on", "amount", p+"line", p+"step", p+"reason", p+"prev_reason")
	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Fprintf(w, "Rejected rows of payments.csv:\n%s", rows)
	return nil
}
