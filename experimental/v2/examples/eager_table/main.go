// Command eager_table uses a Table directly, without a pipeline: operations
// applied one after another, the rows the table rejected, and the sticky
// error that stops the chain after an unknown column (design decisions D31,
// D50).
//
// The prototype has no CSV reader yet (slice 4), so the example reads its
// small synthetic delivery with encoding/csv and builds raw text columns,
// the way a reader will deliver them (D29).
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
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

// load reads a CSV file with a header into a table of text columns.
func load(path string) (gtable.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return gtable.Table{}, err
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return gtable.Table{}, fmt.Errorf("%s: %w", path, err)
	}
	if len(records) == 0 {
		return gtable.Table{}, fmt.Errorf("%s: no header", path)
	}
	cols := make([]gtable.Column, len(records[0]))
	for j, name := range records[0] {
		values := make([]string, len(records)-1)
		for i, rec := range records[1:] {
			values[i] = rec[j]
		}
		cols[j] = gtable.Texts(name, values...)
	}
	t := gtable.NewTable(cols...)
	return t, t.Err()
}
