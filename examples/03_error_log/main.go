// 03_error_log demonstrates Pipeline ErrorLog — the "dead-letter queue" for bad rows.
//
// Scenario: import orders.csv with messy data (bad numbers, bad dates).
//   - Good rows  → processed normally
//   - Bad rows   → filtered out, stored in ErrorLog with original values + context
//   - After Unwrap(): inspect the log, export rejected rows to rejected.csv
//
// Topics covered:
//   - etl.NewErrorLog + Pipeline.WithErrorLog (lax mode)
//   - Multiple TryMap in one chain (price, quantity)
//   - Pipeline.TryTransform for multi-field row validation
//   - log.HasErrors, log.Len, log.Entries for inspection
//   - log.ToTable structure (_source, _step, _row, _error + original cols)
//   - Writing rejected rows: csv.NewWriter().WriteFile("rejected.csv", log.ToTable())
//   - Strict vs lax mode comparison
//   - Hard errors (missing columns) always short-circuit
//   - t.WithSource for dataset tagging
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	gcsv "github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/table"
)

// ordersCSV has a mix of good rows and intentionally bad rows.
const ordersCSV = `order_id,customer,price,quantity,date
1001,Alice,29.99,3,2024-01-10
1002,Bob,not-a-number,2,2024-01-11
1003,Carol,45.00,abc,2024-01-12
1004,Dave,15.50,5,2024-01-13
1005,Eve,99.99,1,2024-01-14
1006,Frank,bad-price,bad-qty,2024-01-15
1007,Grace,12.00,4,2024-01-16
1008,Heidi,75.00,-1,2024-01-17
1009,Ivan,55.50,2,2024-01-18
1010,Judy,20.00,10,2024-01-19
`

func main() {
	// ── Step 1: Strict mode (default) short-circuits on first error ───────────

	fmt.Println("=== Step 1: Strict mode — first bad row short-circuits ===")

	strictResult := etl.FromResult(gcsv.New().Read(strings.NewReader(ordersCSV))).
		TryMap("price", func(v string) (string, error) {
			_, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", fmt.Errorf("invalid price %q: %w", v, err)
			}
			return v, nil
		}).
		Result()

	if strictResult.IsErr() {
		fmt.Printf("Pipeline stopped: %v\n", strictResult.UnwrapErr())
	}
	fmt.Println()

	// ── Step 2: Lax mode — attach an ErrorLog to continue past bad rows ───────

	fmt.Println("=== Step 2: Lax mode with ErrorLog ===")

	log := etl.NewErrorLog()

	// Tag the source so error entries carry the dataset name.
	sourceTagged := gcsv.New().Read(strings.NewReader(ordersCSV)).
		Unwrap().
		WithSource("orders.csv")

	processed := etl.From(sourceTagged).
		WithErrorLog(log). // switch to lax mode
		AssertColumns("order_id", "customer", "price", "quantity", "date"). // hard check
		TryMap("price", func(v string) (string, error) {
			_, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", fmt.Errorf("invalid price %q", v)
			}
			return v, nil
		}).
		TryMap("quantity", func(v string) (string, error) {
			n, err := strconv.Atoi(v)
			if err != nil {
				return "", fmt.Errorf("invalid quantity %q", v)
			}
			if n < 0 {
				return "", fmt.Errorf("quantity must be >= 0, got %d", n)
			}
			return v, nil
		}).
		Unwrap()

	fmt.Printf("Good rows:     %d\n", processed.Len())
	fmt.Printf("Rejected rows: %d\n", log.Len())
	fmt.Println()

	// ── Step 3: Inspect the error log ────────────────────────────────────────

	fmt.Println("=== Step 3: Inspect ErrorLog entries ===")

	for _, entry := range log.Entries() {
		fmt.Printf("  source=%-12s  step=%-22s  row=%d  err=%v\n",
			entry.Source, entry.Step, entry.Row, entry.Err)
		fmt.Printf("    original row: %v\n", entry.OriginalRow)
	}
	fmt.Println()

	// ── Step 4: log.ToTable() — rejected rows as a Table ─────────────────────

	fmt.Println("=== Step 4: log.ToTable() structure ===")

	// ToTable returns a Table with meta-columns plus the original row columns:
	//   _source  _step  _row  _error  + original cols (order_id, customer, ...)
	rejected := log.ToTable()
	fmt.Printf("Rejected table: %d rows, headers: %v\n\n", rejected.Len(), []string(rejected.Headers))

	fmt.Println("Rejected table content:")
	for _, r := range rejected.Rows {
		fmt.Printf("  [row %s] step=%s  err=%s\n",
			r.Get("_row").UnwrapOr(""),
			r.Get("_step").UnwrapOr(""),
			r.Get("_error").UnwrapOr(""))
		fmt.Printf("    original: order_id=%s  customer=%s  price=%s  quantity=%s\n",
			r.Get("order_id").UnwrapOr(""),
			r.Get("customer").UnwrapOr(""),
			r.Get("price").UnwrapOr(""),
			r.Get("quantity").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 5: Export rejected rows to CSV ───────────────────────────────────

	fmt.Println("=== Step 5: Export rejected rows to rejected.csv ===")

	f, err := os.CreateTemp(".", "rejected-*.csv")
	if err != nil {
		fmt.Println("Cannot create file:", err)
		return
	}
	rejectedPath := f.Name()
	f.Close()
	defer os.Remove(rejectedPath)

	writeErr := gcsv.NewWriter().WriteFile(rejectedPath, rejected)
	if writeErr != nil {
		fmt.Println("WriteFile error:", writeErr)
		return
	}
	fmt.Printf("Wrote %d rejected rows to %s\n\n", rejected.Len(), rejectedPath)

	// ── Step 6: TryTransform — multi-field row validation ─────────────────────

	fmt.Println("=== Step 6: TryTransform for multi-field validation ===")

	log2 := etl.NewErrorLog()

	processed2 := etl.FromResult(gcsv.New().Read(strings.NewReader(ordersCSV))).
		WithErrorLog(log2).
		TryTransform(func(r table.Row) (map[string]string, error) {
			priceStr := r.Get("price").UnwrapOr("")
			qtyStr := r.Get("quantity").UnwrapOr("")

			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				return nil, fmt.Errorf("bad price %q", priceStr)
			}

			qty, err := strconv.Atoi(qtyStr)
			if err != nil {
				return nil, fmt.Errorf("bad quantity %q", qtyStr)
			}
			if qty < 0 {
				return nil, fmt.Errorf("quantity %d < 0", qty)
			}

			// Return updated values: add a computed total column.
			return map[string]string{
				"price":    fmt.Sprintf("%.2f", price),
				"quantity": fmt.Sprintf("%d", qty),
			}, nil
		}).
		Then(etl.AddCol("total", func(r table.Row) string {
			p, _ := strconv.ParseFloat(r.Get("price").UnwrapOr("0"), 64)
			q, _ := strconv.Atoi(r.Get("quantity").UnwrapOr("0"))
			return fmt.Sprintf("%.2f", p*float64(q))
		})).
		Unwrap()

	fmt.Printf("Good rows: %d  |  Rejected: %d\n", processed2.Len(), log2.Len())
	fmt.Println("\nGood rows with total:")
	for _, r := range processed2.Rows {
		fmt.Printf("  %-8s  price=%-6s  qty=%-2s  total=%s\n",
			r.Get("customer").UnwrapOr(""),
			r.Get("price").UnwrapOr(""),
			r.Get("quantity").UnwrapOr(""),
			r.Get("total").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 7: Hard errors still short-circuit even in lax mode ─────────────

	fmt.Println("=== Step 7: Hard error (missing column) always short-circuits ===")

	log3 := etl.NewErrorLog()
	hardErrResult := etl.FromResult(gcsv.New().Read(strings.NewReader(ordersCSV))).
		WithErrorLog(log3).
		// AssertColumns is a hard check — lax mode does not soften it.
		AssertColumns("order_id", "THIS_COL_DOES_NOT_EXIST").
		TryMap("price", func(v string) (string, error) { return v, nil }).
		Result()

	fmt.Printf("IsErr: %v\n", hardErrResult.IsErr())
	if hardErrResult.IsErr() {
		fmt.Printf("Error: %v\n", hardErrResult.UnwrapErr())
	}
	fmt.Printf("Log entries (should be 0 — hard error before TryMap): %d\n\n", log3.Len())

	// ── Step 8: Strict vs lax comparison summary ──────────────────────────────

	fmt.Println("=== Step 8: Strict vs lax comparison ===")
	fmt.Println()
	fmt.Println("Strict mode (default):")
	fmt.Println("  - First row error → pipeline short-circuits immediately")
	fmt.Println("  - Result().IsErr() == true")
	fmt.Println("  - No ErrorLog needed; use Result().UnwrapErr() for the error")
	fmt.Println()
	fmt.Println("Lax mode (WithErrorLog):")
	fmt.Println("  - Bad rows are filtered out and logged")
	fmt.Println("  - Pipeline continues with remaining good rows")
	fmt.Println("  - Result().IsOk() == true (unless a hard error occurred)")
	fmt.Println("  - Inspect: log.HasErrors() / log.Len() / log.Entries()")
	fmt.Println("  - Export: csv.NewWriter().WriteFile(\"rejected.csv\", log.ToTable())")
	fmt.Println()
	fmt.Println("Hard errors (always short-circuit regardless of mode):")
	fmt.Println("  - I/O failures (ReadFile, WriteFile)")
	fmt.Println("  - Missing columns (AssertColumns)")
	fmt.Println("  - Any ThenErr step that returns an Err result")
}
