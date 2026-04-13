// 02_csv_pipeline demonstrates reading CSV data through an etl.Pipeline.
//
// Topics covered:
//   - csv.New with options (WithSeparator, WithNoHeader)
//   - etl.FromResult wrapping a CSV read result
//   - Pipeline.Then with etl factory functions (etl.Select, etl.Map, etl.DropEmpty)
//   - Pipeline.ThenErr for fallible steps (schema.Infer.Apply)
//   - Pipeline.AssertColumns and Pipeline.Peek for validation / debugging
//   - csv.ToString for quick display
//   - csv.NewWriter.WriteFile for output
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	gcsv "github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/schema"
	"github.com/stefanbethge/gseq-table/table"
)

// salesCSV is an inline CSV dataset — no file required.
const salesCSV = `id,name,region,revenue,date
1,Alice,EU,1200.50,2024-01-15
2,Bob,US,850.00,2024-01-18
3,Carol,EU,2300.75,2024-01-22
4,,APAC,970.25,2024-01-25
5,Eve,US,3100.00,2024-02-01
6,Frank,EU,,2024-02-05
7,Grace,US,640.00,
8,Heidi,APAC,1800.50,2024-02-10
9,Ivan,EU,450.00,2024-02-14
10,Judy,APAC,2750.25,2024-02-18
`

func main() {
	// ── Step 1: Read CSV from an in-memory reader ─────────────────────────────

	fmt.Println("=== Step 1: Read CSV from strings.NewReader ===")

	// csv.New() — comma separator, first row is header (default options).
	// ReadFile / Read both return result.Result[table.Table, error].
	readResult := gcsv.New().Read(strings.NewReader(salesCSV))

	raw := readResult.Unwrap()
	fmt.Printf("Loaded %d rows, %d columns: %v\n\n",
		raw.Len(), len(raw.Headers), []string(raw.Headers))

	// ── Step 2: AssertColumns early — hard-fail if schema is wrong ────────────

	fmt.Println("=== Step 2: AssertColumns validation ===")

	err := raw.AssertColumns("id", "name", "region", "revenue", "date")
	if err != nil {
		fmt.Println("Missing columns:", err)
	} else {
		fmt.Println("All required columns present ✓")
	}
	fmt.Println()

	// ── Step 3: Build a pipeline with factory functions ───────────────────────

	fmt.Println("=== Step 3: Pipeline with etl factory functions ===")

	// schema.Infer inspects a sample table and maps column names → types.
	// We use the raw table as the reference.
	s := schema.Infer(raw)
	fmt.Printf("Inferred types: id=%s  revenue=%s  date=%s  name=%s\n\n",
		s.Col("id"), s.Col("revenue"), s.Col("date"), s.Col("name"))

	result := etl.FromResult(readResult).
		// Validate schema before transforming.
		AssertColumns("id", "name", "region", "revenue", "date").
		// Drop rows with no revenue or no name.
		Then(etl.DropEmpty("revenue", "name")).
		// Normalise region to upper case.
		Then(etl.Map("region", strings.ToUpper)).
		// Add a derived column: revenue as float rounded to 2 decimals.
		Then(etl.AddCol("rev_formatted", func(r table.Row) string {
			v := r.Get("revenue").UnwrapOr("0")
			f, _ := strconv.ParseFloat(v, 64)
			return fmt.Sprintf("%.2f", f)
		})).
		// Fill missing date with an ISO-8601 sentinel (must be parseable by schema).
		Then(etl.FillEmpty("date", "2000-01-01")).
		// Apply schema: normalise numeric columns to canonical form.
		ThenErr(s.Apply).
		// Peek lets you inspect mid-chain without breaking the chain.
		Peek(func(t table.Table) {
			fmt.Printf("After cleaning: %d rows remain\n\n", t.Len())
		}).
		// Keep only the columns we need for output.
		Then(etl.Select("id", "name", "region", "rev_formatted", "date")).
		Result()

	if result.IsErr() {
		fmt.Println("Pipeline error:", result.UnwrapErr())
		return
	}

	processed := result.Unwrap()

	// ── Step 4: Display with csv.ToString ─────────────────────────────────────

	fmt.Println("=== Step 4: Result as CSV string ===")
	fmt.Println(gcsv.ToString(processed))

	// ── Step 5: Write result to a file and read it back ───────────────────────

	fmt.Println("=== Step 5: WriteFile + ReadFile round-trip ===")

	// Write to a temp file in the current directory.
	f, err := os.CreateTemp(".", "sales-out-*.csv")
	if err != nil {
		fmt.Println("Cannot create temp file:", err)
		return
	}
	tmpPath := f.Name()
	f.Close()
	defer os.Remove(tmpPath)

	writeErr := gcsv.NewWriter().WriteFile(tmpPath, processed)
	if writeErr != nil {
		fmt.Println("Write error:", writeErr)
		return
	}
	fmt.Printf("Wrote %d rows to %s\n", processed.Len(), tmpPath)

	// Read it back to confirm the round-trip.
	roundTrip := gcsv.New().ReadFile(tmpPath).Unwrap()
	fmt.Printf("Read back:  %d rows, headers %v\n\n", roundTrip.Len(), []string(roundTrip.Headers))

	// ── Step 6: Semicolon-delimited CSV ───────────────────────────────────────

	fmt.Println("=== Step 6: Semicolon-delimited CSV ===")

	semiCSV := "region;product;amount\nEU;Widget;1200\nUS;Gizmo;850\n"

	semiTable := gcsv.New(gcsv.WithSeparator(';')).
		Read(strings.NewReader(semiCSV)).
		Unwrap()

	fmt.Printf("Semicolon CSV: %d rows, headers %v\n\n", semiTable.Len(), []string(semiTable.Headers))

	// ── Step 7: Compose reusable TableFuncs ───────────────────────────────────

	fmt.Println("=== Step 7: etl.Compose — reusable transform ===")

	// Compose chains multiple TableFunc values into a single reusable function.
	normalize := etl.Compose(
		etl.DropEmpty("revenue"),
		etl.FillEmpty("date", "unknown"),
		etl.Map("region", strings.ToUpper),
	)

	normalised := etl.FromResult(gcsv.New().Read(strings.NewReader(salesCSV))).
		Then(normalize).
		Unwrap()

	fmt.Printf("Composed normalise fn: %d rows after DropEmpty('revenue')\n", normalised.Len())
}
