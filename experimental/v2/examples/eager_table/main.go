// Command eager_table uses a Table directly, without a pipeline: operations
// applied one after another, the rows the table rejected, and the sticky
// error that stops the chain after an unknown column (design decisions D31,
// D50).
//
// The small synthetic deliveries are read at once with the CSV reader into
// tables of raw text columns (D29); each file is a source of its own (D53).
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
	dir := flag.String("data", "testdata", "directory with orders.csv and customers.csv")
	flag.Parse()
	if err := run(os.Stdout, *dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(w io.Writer, dir string) error {
	orders, err := load(filepath.Join(dir, "orders.csv"))
	if err != nil {
		return err
	}
	customers, err := load(filepath.Join(dir, "customers.csv"))
	if err != nil {
		return err
	}

	// Each method applies one operation at once. Rows with values that do not
	// fit are rejected, the rest go on (D5).
	revenue := orders.
		Cast("amount", gtable.TypeFloat, gtable.NullTexts("n/a")).
		Cast("ordered", gtable.TypeTimestamp, gtable.DateFormat("02.01.2006")).
		With("gross", gtable.Col("amount").Mul(gtable.Lit(1.19)).Round(2)).
		InnerJoin(customers, gtable.OnPair("customer", "id")).
		GroupBy([]string{"name"},
			gtable.Sum("gross").As("gross_total"),
			gtable.Count("amount").As("orders_with_amount"),
		).
		Sort(gtable.Desc("gross_total"))
	if err := revenue.Err(); err != nil {
		return err
	}
	fmt.Fprintf(w, "Revenue per customer:\n%s\n", revenue)

	fmt.Fprintln(w, "Rejected rows:")
	for _, r := range revenue.Rejects() {
		fmt.Fprintln(w, " ", r)
	}

	// An unknown column is a plan error. It sticks to the table, and the
	// operations after it do not run (D50).
	broken := revenue.
		Where(gtable.Col("revenue").Gt(gtable.Lit(100.0))).
		Sort(gtable.Asc("name"))
	fmt.Fprintf(w, "\nSticky error: %v\n", broken.Err())
	return nil
}

// load reads a CSV file with a header into a table of text columns, named
// after the file.
func load(path string) (gtable.Table, error) {
	return csv.File(path).Table(context.Background())
}
