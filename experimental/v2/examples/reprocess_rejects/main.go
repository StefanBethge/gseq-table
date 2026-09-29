// Command reprocess_rejects reprocesses the rejected rows of an earlier run
// after the pipeline was fixed (UC4; design decisions D16, D18, D47, D61).
//
// The first run casts the order date with one fixed layout and rejects the
// rows with another one; a line with a broken quote cannot even be split.
// The rejected rows go to a CSV file through the same writer as results
// (D2). After the fix, FromRejects reads that file back as the source of
// the fixed pipeline (ReplaceSource), with the CSV reader set to read the
// broken line again. Every row keeps its record_key and so its row_key
// (D18, D82), and the target upserts on row_key: the reprocessed rows fill
// the gaps, and running the whole delivery once more adds no duplicate
// (D47).
//
// The prototype has no database writer (F20), so the target is a small
// upsert sink defined here on the Sink interface (D35).
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
)

func main() {
	data := flag.String("data", "examples/reprocess_rejects/testdata", "directory with orders.csv")
	out := flag.String("out", os.TempDir(), "directory for the file of rejected rows")
	flag.Parse()
	if err := run(context.Background(), os.Stdout, *data, *out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// blockLen is small so the example runs over several blocks.
const blockLen = 2

// pipeline casts the columns of an order; lenient is the fix that accepts
// the other date layouts (D74).
func pipeline(delivery string, lenient bool) *gtable.Pipeline {
	date := gtable.DateFormat("02.01.2006")
	if lenient {
		date = gtable.Lenient()
	}
	return gtable.FromSource(csv.File(delivery), blockLen).
		Then(gtable.CastAll(
			gtable.Cast("amount", gtable.TypeFloat),
			gtable.Cast("ordered", gtable.TypeTimestamp, date),
		))
}

func run(ctx context.Context, w io.Writer, dir, outDir string) error {
	delivery := filepath.Join(dir, "orders.csv")
	rejectsFile := filepath.Join(outDir, "orders-rejects.csv")
	target := newUpsertSink()

	// The first run writes the results into the target and the rejected
	// rows into a file (D2, D49).
	res, err := pipeline(delivery, false).
		To(target).
		RejectsTo("orders.csv", csv.Create(rejectsFile)).
		Run(ctx)
	if err != nil {
		return err
	}
	report(w, "First run", res)

	// Read the file back. Its columns carry the info prefix, which is
	// reserved in a delivery, so it is read under another prefix (G65).
	back, err := gtable.FromSource(csv.File(rejectsFile), blockLen).InfoPrefix("_file_").Run(ctx)
	if err != nil {
		return err
	}

	// The fixed pipeline runs the rejected rows as its source, and reads
	// the lines that could not be split again with lazy quotes (D16).
	src := gtable.FromRejects(back.Table, csv.Reparse(csv.LazyQuotes()))
	res, err = pipeline(delivery, true).ReplaceSource(src).To(target).Run(ctx)
	if err != nil {
		return err
	}
	report(w, "Reprocessing", res)
	// A row that fails again points to the original delivery.
	if again, ok := res.Table.RejectedRows().Source("orders.csv"); ok {
		p := gtable.DefaultInfoPrefix
		rows := again.Select("order", "amount", p+"source", p+"line", p+"code", p+"reason")
		if err := rows.Err(); err != nil {
			return err
		}
		fmt.Fprintf(w, "Failed again:\n%s\n", rows)
	}

	// Running the whole delivery again upserts on the same row keys and
	// adds no duplicate (D47, D61).
	res, err = pipeline(delivery, true).To(target).Run(ctx)
	if err != nil {
		return err
	}
	report(w, "Whole delivery again", res)
	fmt.Fprintf(w, "Target, upserted on row_key (%d rows):\n", len(target.order))
	for _, k := range target.order {
		fmt.Fprintf(w, "  line %-3s %s\n", k[strings.LastIndexByte(k, ':')+1:], strings.Join(target.rows[k], "  "))
	}
	return nil
}

func report(w io.Writer, name string, res gtable.Result) {
	c := res.Counts
	fmt.Fprintf(w, "%s: status %v, %d read, %d passed, %d rejected, exit code %d\n",
		name, res.Status, c.Read, c.Passed, c.Rejected, res.ExitCode())
}

// upsertSink is a target that upserts every row on its row_key, as a
// database writer would (D35, D47).
type upsertSink struct {
	rows  map[string][]string
	order []string
}

func newUpsertSink() *upsertSink { return &upsertSink{rows: map[string][]string{}} }

func (s *upsertSink) Write(_ context.Context, b gtable.Block) error {
	cols := b.Rows.Columns()
	for i := range b.Rows.Len() {
		vals := make([]string, len(cols))
		for j, name := range cols {
			c, _ := b.Rows.Column(name)
			vals[j], _ = c.Format(i)
		}
		key := b.RowKeys[i]
		if _, ok := s.rows[key]; !ok {
			s.order = append(s.order, key)
		}
		s.rows[key] = vals
	}
	return nil
}

func (s *upsertSink) Close() error { return nil }
