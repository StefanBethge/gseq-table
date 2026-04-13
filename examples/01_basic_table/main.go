// 01_basic_table demonstrates the core immutable Table API.
//
// Topics covered:
//   - table.New / table.NewMutable
//   - Where, Select, Map, AddCol, DropEmpty, FillEmpty
//   - SortMulti with table.Asc / table.Desc
//   - GroupBy, Len, Headers, Rows
//   - Distinct, Rename
//   - table.Concat for merging tables
//   - Error accumulation: HasErrs / Errs
package main

import (
	"fmt"
	"strings"

	"github.com/stefanbethge/gseq-table/table"
)

func main() {
	// ── Step 1: Build a table from in-memory data ─────────────────────────────

	fmt.Println("=== Step 1: table.New ===")

	headers := []string{"region", "product", "revenue", "category", "status"}
	rows := [][]string{
		{"EU", "Widget A", "1200", "hardware", "active"},
		{"US", "Widget B", "850", "hardware", "inactive"},
		{"EU", "Gizmo X", "2300", "software", "active"},
		{"APAC", "Widget A", "970", "hardware", "active"},
		{"US", "Gizmo X", "3100", "software", "active"},
		{"EU", "Gadget Z", "", "peripherals", "active"}, // missing revenue
		{"US", "Widget A", "640", "hardware", ""},       // missing status
		{"APAC", "Gadget Z", "1800", "peripherals", "inactive"},
		{"EU", "Widget B", "450", "hardware", "inactive"},
		{"APAC", "Gizmo X", "2750", "software", "active"},
	}

	t := table.New(headers, rows)
	fmt.Printf("Headers: %v\n", []string(t.Headers))
	fmt.Printf("Rows:    %d\n\n", t.Len())

	// ── Step 2: DropEmpty and FillEmpty ───────────────────────────────────────

	fmt.Println("=== Step 2: DropEmpty + FillEmpty ===")

	// Drop rows with no revenue, then default missing status to "unknown".
	clean := t.
		DropEmpty("revenue").
		FillEmpty("status", "unknown")

	fmt.Printf("After DropEmpty('revenue'): %d rows (dropped %d)\n", clean.Len(), t.Len()-clean.Len())
	for _, r := range clean.Rows {
		if r.Get("status").UnwrapOr("") == "unknown" {
			fmt.Printf("  FillEmpty applied → %v\n", r.ToMap())
		}
	}
	fmt.Println()

	// ── Step 3: Where (filtering) ─────────────────────────────────────────────

	fmt.Println("=== Step 3: Where (filter active EU rows) ===")

	euActive := clean.Where(func(r table.Row) bool {
		return r.Get("region").UnwrapOr("") == "EU" &&
			r.Get("status").UnwrapOr("") == "active"
	})
	fmt.Printf("EU active rows: %d\n", euActive.Len())
	for _, r := range euActive.Rows {
		fmt.Printf("  %s – %s – %s\n",
			r.Get("product").UnwrapOr(""),
			r.Get("revenue").UnwrapOr(""),
			r.Get("status").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 4: Select and Rename ─────────────────────────────────────────────

	fmt.Println("=== Step 4: Select + Rename ===")

	slim := clean.
		Select("region", "product", "revenue").
		Rename("revenue", "rev_usd")

	fmt.Printf("Headers after Select+Rename: %v\n\n", []string(slim.Headers))

	// ── Step 5: Map (transform a column) ─────────────────────────────────────

	fmt.Println("=== Step 5: Map (uppercase region) ===")

	upper := clean.Map("region", strings.ToUpper)
	fmt.Printf("First 3 regions: ")
	for i, r := range upper.Rows {
		if i >= 3 {
			break
		}
		fmt.Printf("%q ", r.Get("region").UnwrapOr(""))
	}
	fmt.Println("\n")

	// ── Step 6: AddCol (derived column) ──────────────────────────────────────

	fmt.Println("=== Step 6: AddCol (label column) ===")

	labelled := clean.AddCol("label", func(r table.Row) string {
		return r.Get("region").UnwrapOr("") + "/" + r.Get("product").UnwrapOr("")
	})
	fmt.Println("Sample labels:")
	for i, r := range labelled.Rows {
		if i >= 3 {
			break
		}
		fmt.Printf("  %s\n", r.Get("label").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 7: SortMulti ────────────────────────────────────────────────────

	fmt.Println("=== Step 7: SortMulti (region ASC, revenue DESC) ===")

	sorted := clean.SortMulti(table.Asc("region"), table.Desc("revenue"))
	fmt.Println("Sorted rows (region, product, revenue):")
	for _, r := range sorted.Rows {
		fmt.Printf("  %-6s  %-12s  %s\n",
			r.Get("region").UnwrapOr(""),
			r.Get("product").UnwrapOr(""),
			r.Get("revenue").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 8: GroupBy ───────────────────────────────────────────────────────

	fmt.Println("=== Step 8: GroupBy region ===")

	groups := clean.GroupBy("region")
	for region, group := range groups {
		fmt.Printf("  %s: %d rows\n", region, group.Len())
	}
	fmt.Println()

	// ── Step 9: Distinct ─────────────────────────────────────────────────────

	fmt.Println("=== Step 9: Distinct products ===")

	products := clean.
		Select("product", "category").
		Distinct("product", "category")

	fmt.Printf("Distinct product+category combinations: %d\n", products.Len())
	for _, r := range products.Rows {
		fmt.Printf("  %s (%s)\n",
			r.Get("product").UnwrapOr(""),
			r.Get("category").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 10: table.Concat ─────────────────────────────────────────────────

	fmt.Println("=== Step 10: table.Concat ===")

	q1 := table.New(
		[]string{"region", "product", "revenue"},
		[][]string{
			{"EU", "Widget A", "1200"},
			{"US", "Gizmo X", "3100"},
		},
	)
	q2 := table.New(
		[]string{"region", "product", "revenue"},
		[][]string{
			{"APAC", "Gadget Z", "1800"},
			{"EU", "Widget B", "450"},
		},
	)

	combined := table.Concat(q1, q2)
	fmt.Printf("q1: %d rows  q2: %d rows  combined: %d rows\n\n", q1.Len(), q2.Len(), combined.Len())

	// ── Step 11: Error accumulation ───────────────────────────────────────────

	fmt.Println("=== Step 11: Error accumulation ===")

	bad := t.Select("nonexistent_col")
	fmt.Printf("HasErrs: %v\n", bad.HasErrs())
	for _, e := range bad.Errs() {
		fmt.Printf("  error: %v\n", e)
	}

	// Errors are accumulated silently — no panic. Inspect them with Errs().
	// Operations on a table with errors may still return data (the bad Select
	// here returns the original rows since the unknown column is just skipped).
	fmt.Printf("Rows after bad Select: %d\n\n", bad.Len())

	// ── Step 12: MutableTable overview ───────────────────────────────────────

	fmt.Println("=== Step 12: Build with NewMutable ===")

	m := table.NewMutable(
		[]string{"name", "score"},
		[][]string{
			{"Alice", "42"},
			{"Bob", ""},
			{"Carol", "91"},
		},
	)
	m.FillEmpty("score", "0").Map("name", strings.ToUpper)

	fmt.Println("Mutable result (Freeze → immutable Table):")
	frozen := m.Freeze()
	for _, r := range frozen.Rows {
		fmt.Printf("  name=%-8s  score=%s\n",
			r.Get("name").UnwrapOr(""),
			r.Get("score").UnwrapOr(""))
	}
}
