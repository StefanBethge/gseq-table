// 04_mutable demonstrates MutableTable — the in-place variant of Table.
//
// Topics covered:
//   - table.NewMutable from headers + rows
//   - t.Mutable() to get a MutableTable from an existing Table
//   - Method chaining: Map, Sort, AddCol, DropEmpty, FillEmpty, FillForward
//   - Rename
//   - Where (filter rows in place)
//   - Set (update a specific cell by row index and column)
//   - HasErrs / Errs after chain
//   - Freeze() to obtain an immutable Table snapshot
//   - etl.FromMutable + Mut factory methods + Frozen() bridge
package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/table"
)

func main() {
	// ── Step 1: NewMutable from headers + rows ────────────────────────────────

	fmt.Println("=== Step 1: table.NewMutable ===")

	m := table.NewMutable(
		[]string{"id", "region", "product", "revenue", "status"},
		[][]string{
			{"1", "EU", "Widget A", "1200", "active"},
			{"2", "US", "Widget B", "850", ""},       // missing status
			{"3", "EU", "Gizmo X", "2300", "active"},
			{"4", "APAC", "Widget A", "", "active"},   // missing revenue
			{"5", "US", "Gizmo X", "3100", "active"},
			{"6", "EU", "Gadget Z", "450", "inactive"},
		},
	)

	fmt.Printf("Rows: %d  Cols: %d  Headers: %v\n\n", m.Len(), len(m.Headers()), []string(m.Headers()))

	// ── Step 2: Method chaining ───────────────────────────────────────────────

	fmt.Println("=== Step 2: Chaining — FillEmpty, Map, AddCol, DropEmpty ===")

	// MutableTable methods return *MutableTable so you can chain.
	m.FillEmpty("status", "unknown").
		Map("region", strings.ToUpper).
		AddCol("label", func(r table.Row) string {
			return r.Get("region").UnwrapOr("") + ":" + r.Get("product").UnwrapOr("")
		}).
		DropEmpty("revenue") // drop rows with no revenue

	fmt.Printf("After chain: %d rows remain (dropped 1 with empty revenue)\n\n", m.Len())

	// ── Step 3: Where (filter rows) ───────────────────────────────────────────

	fmt.Println("=== Step 3: Where — keep only active rows ===")

	m.Where(func(r table.Row) bool {
		return r.Get("status").UnwrapOr("") == "active"
	})

	fmt.Printf("After Where(status=active): %d rows\n", m.Len())
	for _, r := range m.Freeze().Rows {
		fmt.Printf("  id=%-2s  region=%-4s  product=%-10s  label=%s\n",
			r.Get("id").UnwrapOr(""),
			r.Get("region").UnwrapOr(""),
			r.Get("product").UnwrapOr(""),
			r.Get("label").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 4: Sort ──────────────────────────────────────────────────────────

	fmt.Println("=== Step 4: Sort (region ASC) ===")

	m.Sort("region", true)

	fmt.Println("Sorted by region:")
	for _, r := range m.Freeze().Rows {
		fmt.Printf("  %-4s  %s\n",
			r.Get("region").UnwrapOr(""),
			r.Get("product").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 5: Rename ────────────────────────────────────────────────────────

	fmt.Println("=== Step 5: Rename column ===")

	m.Rename("revenue", "rev_usd")
	fmt.Printf("Headers after Rename: %v\n\n", []string(m.Headers()))

	// ── Step 6: Set — update a specific cell ─────────────────────────────────

	fmt.Println("=== Step 6: Set — update row 0, col 'rev_usd' to '9999' ===")

	m.Set(0, "rev_usd", "9999")
	r0, _ := m.Row(0)
	fmt.Printf("Row 0 rev_usd is now: %s\n\n", r0.Get("rev_usd").UnwrapOr(""))

	// ── Step 7: FillForward ───────────────────────────────────────────────────

	fmt.Println("=== Step 7: FillForward — propagate last non-empty value ===")

	mFF := table.NewMutable(
		[]string{"date", "region", "revenue"},
		[][]string{
			{"2024-01", "EU", "1200"},
			{"", "US", "850"},  // date missing — will be filled forward
			{"2024-02", "EU", "2300"},
			{"", "APAC", "970"}, // date missing — will be filled forward
		},
	)

	mFF.FillForward("date")

	fmt.Println("After FillForward('date'):")
	for _, r := range mFF.Freeze().Rows {
		fmt.Printf("  date=%-8s  region=%s\n",
			r.Get("date").UnwrapOr(""),
			r.Get("region").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 8: Freeze → immutable Table ─────────────────────────────────────

	fmt.Println("=== Step 8: Freeze → immutable Table ===")

	frozen := m.Freeze()
	fmt.Printf("Frozen table: %d rows, type is table.Table\n\n", frozen.Len())

	// ── Step 9: t.Mutable() — get mutable copy of an existing Table ──────────

	fmt.Println("=== Step 9: t.Mutable() — from existing Table ===")

	immutable := table.New(
		[]string{"name", "score"},
		[][]string{{"Alice", "42"}, {"Bob", "88"}, {"Carol", ""}},
	)

	// Mutable() copies the table so the original is unaffected.
	mutable2 := immutable.Mutable()
	mutable2.FillEmpty("score", "0").Map("score", func(v string) string {
		n, _ := strconv.Atoi(v)
		return strconv.Itoa(n * 2) // double each score
	})

	fmt.Println("Original (unchanged):")
	for _, r := range immutable.Rows {
		fmt.Printf("  name=%-6s  score=%s\n", r.Get("name").UnwrapOr(""), r.Get("score").UnwrapOr(""))
	}
	fmt.Println("Mutable copy (doubled):")
	for _, r := range mutable2.Freeze().Rows {
		fmt.Printf("  name=%-6s  score=%s\n", r.Get("name").UnwrapOr(""), r.Get("score").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 10: Error accumulation on MutableTable ───────────────────────────

	fmt.Println("=== Step 10: MutableTable error accumulation ===")

	mErr := table.NewMutable([]string{"x"}, [][]string{{"1"}})
	mErr.Select("no_such_column") // bad — adds error, returns empty table
	fmt.Printf("HasErrs: %v\n", mErr.HasErrs())
	for _, e := range mErr.Errs() {
		fmt.Printf("  error: %v\n", e)
	}
	fmt.Println()

	// ── Step 11: MutablePipeline with etl.Mut factory methods ────────────────

	fmt.Println("=== Step 11: etl.FromMutable + Mut factory methods + Frozen() ===")

	base := table.NewMutable(
		[]string{"city", "sales", "region"},
		[][]string{
			{"berlin", "1200", "eu"},
			{"", "850", "us"},    // empty city — will be dropped
			{"paris", "2300", "eu"},
			{"austin", "3100", "us"},
		},
	)

	// MutablePipeline avoids intermediate immutable allocations.
	// Use Mut.<Method>(...) for concise factory functions.
	// Bridge to immutable Pipeline with Frozen() when needed.
	result := etl.FromMutable(base).
		Then(etl.Mut.DropEmpty("city")).
		Then(etl.Mut.Map("city", strings.ToTitle)).
		Then(etl.Mut.Map("region", strings.ToUpper)).
		Then(etl.Mut.Sort("sales", false)). // descending
		Frozen().                            // → immutable Pipeline
		Then(etl.Select("city", "region", "sales")).
		Unwrap()

	fmt.Println("MutablePipeline → Frozen result:")
	for _, r := range result.Rows {
		fmt.Printf("  city=%-8s  region=%-4s  sales=%s\n",
			r.Get("city").UnwrapOr(""),
			r.Get("region").UnwrapOr(""),
			r.Get("sales").UnwrapOr(""))
	}
}
