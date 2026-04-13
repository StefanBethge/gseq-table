// 05_etl_compose demonstrates advanced ETL patterns.
//
// Topics covered:
//   - etl.Compose to build reusable TableFunc
//   - etl.MutCompose for mutable pipelines
//   - Pipeline.IfThen for conditional steps
//   - Pipeline.FanOut for parallel branches
//   - Pipeline.WithTracing + Step + Trace for performance inspection
//   - etl.ConcatPipelines for merging multiple sources
//   - Pipeline.RecoverWith for error recovery
//   - etl.NewTransform + Pipeline.Apply for named reusable transforms
package main

import (
	"fmt"
	"strings"

	gcsv "github.com/stefanbethge/gseq-table/csv"
	"github.com/stefanbethge/gseq-table/etl"
	"github.com/stefanbethge/gseq-table/table"
)

// ─── Inline data ──────────────────────────────────────────────────────────────

const jan = `region,product,revenue,status
EU,Widget A,1200,active
US,Widget B,850,inactive
EU,Gizmo X,2300,active
`

const feb = `region,product,revenue,status
APAC,Widget A,970,active
US,Gizmo X,3100,active
EU,Gadget Z,450,inactive
`

const mar = `region,product,revenue,status
EU,Widget A,1400,active
APAC,Gizmo X,2750,active
US,Widget B,640,active
`

func read(csv string) etl.Pipeline {
	return etl.FromResult(gcsv.New().Read(strings.NewReader(csv)))
}

func main() {
	// ── Step 1: etl.Compose — reusable TableFunc chains ──────────────────────

	fmt.Println("=== Step 1: etl.Compose — reusable TableFunc ===")

	// Compose packages multiple TableFunc values into one reusable function.
	// The returned TableFunc can be passed to Then, or stored as a variable.
	normalize := etl.Compose(
		etl.DropEmpty("revenue"),
		etl.FillEmpty("status", "unknown"),
		etl.Map("region", strings.ToUpper),
	)

	activeOnly := etl.Where(func(r table.Row) bool {
		return r.Get("status").UnwrapOr("") == "active"
	})

	t := read(jan).Then(normalize).Then(activeOnly).Unwrap()
	fmt.Printf("Jan active rows after normalize: %d\n\n", t.Len())

	// ── Step 2: etl.MutCompose — reusable MutableFunc chains ─────────────────

	fmt.Println("=== Step 2: etl.MutCompose — reusable MutableFunc ===")

	mutNorm := etl.MutCompose(
		etl.Mut.DropEmpty("revenue"),
		etl.Mut.Map("region", strings.ToUpper),
		etl.Mut.Sort("revenue", false),
	)

	mResult := etl.FromMutable(
		table.NewMutable(
			[]string{"region", "product", "revenue"},
			[][]string{
				{"EU", "Widget A", "1200"},
				{"us", "Widget B", ""},
				{"APAC", "Gizmo X", "2750"},
			},
		),
	).
		Then(mutNorm).
		Frozen().
		Unwrap()

	fmt.Printf("MutCompose result: %d rows\n", mResult.Len())
	for _, r := range mResult.Rows {
		fmt.Printf("  %-5s  %s\n",
			r.Get("region").UnwrapOr(""),
			r.Get("revenue").UnwrapOr(""))
	}
	fmt.Println()

	// ── Step 3: IfThen — conditional pipeline steps ───────────────────────────

	fmt.Println("=== Step 3: IfThen — conditional step ===")

	deduplicateEnabled := true

	result := read(jan).
		Then(normalize).
		IfThen(deduplicateEnabled, func(t table.Table) table.Table {
			return t.Distinct("product")
		}).
		IfThen(!deduplicateEnabled, func(t table.Table) table.Table {
			// This branch is skipped because deduplicateEnabled is true.
			return t.Head(1)
		}).
		Unwrap()

	fmt.Printf("IfThen(deduplicate=true): %d distinct product rows\n\n", result.Len())

	// ── Step 4: FanOut — parallel branches ───────────────────────────────────

	fmt.Println("=== Step 4: FanOut — parallel branches by region ===")

	// FanOut applies each function concurrently to the same table.
	base := read(jan).Then(normalize).ConcatWith(read(feb).Then(normalize))

	branches := base.FanOut(
		func(t table.Table) table.Table {
			return t.Where(func(r table.Row) bool { return r.Get("region").UnwrapOr("") == "EU" })
		},
		func(t table.Table) table.Table {
			return t.Where(func(r table.Row) bool { return r.Get("region").UnwrapOr("") == "US" })
		},
		func(t table.Table) table.Table {
			return t.Where(func(r table.Row) bool { return r.Get("region").UnwrapOr("") == "APAC" })
		},
	)
	names := []string{"EU", "US", "APAC"}
	for i, branch := range branches {
		fmt.Printf("  %s: %d rows\n", names[i], branch.Unwrap().Len())
	}
	fmt.Println()

	// ── Step 5: WithTracing + Step + Trace ───────────────────────────────────

	fmt.Println("=== Step 5: WithTracing — step-level timing and row counts ===")

	// WithTracing enables recording of StepRecords. Use Step(...) instead of
	// Then(...) to assign a name to each recorded step.
	traced := read(jan).WithTracing()

	_ = traced.
		Step("normalize", normalize).
		Step("active-only", activeOnly).
		Step("select", etl.Select("region", "product", "revenue")).
		Unwrap()

	fmt.Printf("%-18s  %8s  %8s  %10s\n", "step", "in_rows", "out_rows", "duration")
	for _, rec := range traced.Trace() {
		fmt.Printf("%-18s  %8d  %8d  %10v\n",
			rec.Name, rec.InputRows, rec.OutputRows, rec.Duration)
	}
	fmt.Println()

	// ── Step 6: ConcatPipelines — merge multiple sources ─────────────────────

	fmt.Println("=== Step 6: ConcatPipelines — merge three monthly files ===")

	// ConcatPipelines stacks tables vertically. The first error wins.
	merged := etl.ConcatPipelines(
		read(jan).Then(normalize),
		read(feb).Then(normalize),
		read(mar).Then(normalize),
	).Unwrap()

	fmt.Printf("Jan+Feb+Mar merged: %d rows\n\n", merged.Len())

	// ── Step 7: RecoverWith — graceful error recovery ─────────────────────────

	fmt.Println("=== Step 7: RecoverWith — fallback on error ===")

	// Simulate a missing column error, then recover with a fallback table.
	fallback := table.New(
		[]string{"region", "product", "revenue"},
		[][]string{{"UNKNOWN", "UNKNOWN", "0"}},
	)

	recovered := read(jan).
		AssertColumns("region", "product", "revenue", "THIS_COL_MISSING").
		RecoverWith(fallback).
		Unwrap()

	fmt.Printf("Recovered pipeline: %d row(s), first product=%s\n\n",
		recovered.Len(),
		recovered.Rows[0].Get("product").UnwrapOr(""))

	// ── Step 8: NewTransform + Apply — named reusable pipeline transforms ─────

	fmt.Println("=== Step 8: etl.NewTransform + Pipeline.Apply ===")

	// NewTransform packages a pipeline-to-pipeline function with a name.
	// With tracing active, Apply records a StepRecord for the whole transform.
	normalizeTransform := etl.NewTransform("normalize", func(p etl.Pipeline) etl.Pipeline {
		return p.
			Then(etl.DropEmpty("revenue")).
			Then(etl.FillEmpty("status", "unknown")).
			Then(etl.Map("region", strings.ToUpper))
	})

	filterActive := etl.NewTransform("filter-active", func(p etl.Pipeline) etl.Pipeline {
		return p.Then(func(t table.Table) table.Table {
			return t.Where(func(r table.Row) bool {
				return r.Get("status").UnwrapOr("") == "active"
			})
		})
	})

	pTraced := read(jan).WithTracing()
	final := pTraced.
		Apply(normalizeTransform).
		Apply(filterActive).
		Then(etl.Select("region", "product")).
		Unwrap()

	fmt.Printf("Apply result: %d rows\n", final.Len())
	fmt.Printf("%-22s  %8s  %8s\n", "transform", "in_rows", "out_rows")
	for _, rec := range pTraced.Trace() {
		fmt.Printf("%-22s  %8d  %8d\n", rec.Name, rec.InputRows, rec.OutputRows)
	}
	fmt.Println()

	// ── Step 9: GroupBy terminal — split pipeline into sub-pipelines ──────────

	fmt.Println("=== Step 9: Pipeline.GroupBy terminal ===")

	merged2 := etl.ConcatPipelines(
		read(jan).Then(normalize),
		read(feb).Then(normalize),
		read(mar).Then(normalize),
	)

	for region, sub := range merged2.GroupBy("region") {
		t := sub.Unwrap()
		fmt.Printf("  region=%-5s  rows=%d\n", region, t.Len())
	}
	fmt.Println()

	// ── Step 10: OnError — custom recovery logic ──────────────────────────────

	fmt.Println("=== Step 10: OnError — custom recovery with logging ===")

	onErrResult := read(jan).
		AssertColumns("nonexistent").
		OnError(func(err error) (table.Table, error) {
			fmt.Printf("  Caught error: %v\n", err)
			fmt.Println("  Falling back to empty table")
			return table.New([]string{"region", "product", "revenue", "status"}, nil), nil
		}).
		Unwrap()

	fmt.Printf("OnError recovery: %d rows\n", onErrResult.Len())
}
