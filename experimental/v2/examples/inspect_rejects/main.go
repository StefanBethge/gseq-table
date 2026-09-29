// Command inspect_rejects runs two deliveries, a CSV file and an Excel
// sheet, and shows what a pipeline developer looks at afterwards: the
// rejected rows of each source with their raw state and the _gseq_ info
// columns, and the overview with one entry per error (UC3; design decisions
// D1, D13, D14, D44). For the Excel source the rows also carry the cell and
// the displayed text (D52, D62).
//
// The deliveries in testdata are small, synthetic and dirty (D60):
// orders.csv has broken quotes, a line with too few fields, amounts with a
// decimal comma and a row that fails in two columns; customers.xlsx has a
// date typed as a plain number and a placeholder text for an amount.
// testdata/gen.go writes customers.xlsx.
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
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
)

func main() {
	dir := flag.String("data", "testdata", "directory with orders.csv and customers.xlsx")
	flag.Parse()
	if err := run(context.Background(), os.Stdout, *dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// blockLen is small so the example runs over several blocks.
const blockLen = 4

func run(ctx context.Context, w io.Writer, dir string) error {
	// The CSV delivery: lines that cannot be split are rejected while
	// reading, with their raw bytes (D10). The row that fails in amount and
	// ordered is rejected once, with an overview entry per column (D44).
	ordersRun, err := gtable.FromSource(csv.File(filepath.Join(dir, "orders.csv")), blockLen).
		Then(gtable.CastAll(
			gtable.Cast("amount", gtable.TypeFloat, gtable.NullTexts("n/a")),
			gtable.Cast("ordered", gtable.TypeTimestamp, gtable.DateFormat("02.01.2006")),
		)).
		Run(ctx)
	if err != nil {
		return err
	}
	orders := ordersRun.Table

	// The Excel delivery: every sheet is a source of its own (D53). The
	// working columns hold the stored values in a fixed text form, so dates
	// cast without a DateFormat (D62).
	customersRun, err := gtable.FromSource(excel.Sheet(filepath.Join(dir, "customers.xlsx"), "Kunden"), blockLen).
		Then(gtable.CastAll(
			gtable.Cast("since", gtable.TypeTimestamp),
			gtable.Cast("credit", gtable.TypeFloat),
		)).
		Run(ctx)
	if err != nil {
		return err
	}
	customers := customersRun.Table

	fmt.Fprintf(w, "Orders that passed: %d, customers that passed: %d\n\n", orders.Len(), customers.Len())

	// One table per source: the raw columns as delivered and the info
	// columns that say where and why (D13, D14).
	p := gtable.DefaultInfoPrefix
	if err := show(w, orders.RejectedRows(), "orders.csv",
		"order", "customer", "amount", "ordered",
		p+"line", p+"offset", p+"error_count", p+"column", p+"code", p+"raw_line"); err != nil {
		return err
	}
	if err := show(w, customers.RejectedRows(), "customers.xlsx#Kunden",
		"id", "since", "credit",
		p+"sheet", p+"line", p+"cell", p+"display", p+"column", p+"code"); err != nil {
		return err
	}

	// The overview holds the info columns only, one entry per error (D15).
	for _, r := range []gtable.Table{orders, customers} {
		ov := r.RejectedRows().Overview().Select(p+"source", p+"line", p+"column", p+"value", p+"code", p+"reason")
		if err := ov.Err(); err != nil {
			return err
		}
		fmt.Fprintf(w, "Overview:\n%s\n", ov)
	}
	return nil
}

// show prints the rejected rows of one source with the given columns.
func show(w io.Writer, rr gtable.RejectedRows, source string, cols ...string) error {
	rows, ok := rr.Source(source)
	if !ok {
		return fmt.Errorf("no rejected rows of %s", source)
	}
	rows = rows.Select(cols...)
	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Fprintf(w, "Rejected rows of %s:\n%s\n", source, rows)
	return nil
}
