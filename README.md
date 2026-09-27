# gseq-table

[![CI](https://github.com/StefanBethge/gseq-table/actions/workflows/ci.yml/badge.svg)](https://github.com/StefanBethge/gseq-table/actions/workflows/ci.yml)

ETL and spreadsheet-style data wrangling for Go.

`gseq-table` is an in-memory toolkit for working with messy CSV, JSON, and Excel data:

- string-first tables
- immutable and mutable workflows
- composable transformations
- optional schema inference and validation
- row-level reject handling for real import pipelines

It is built for the gap between raw `[][]string` hacks and heavier typed dataframe systems.

Dependency policy:

- the core module stays minimal
- the core depends only on `gseq`
- heavier integrations are split into optional modules

## Start here

If you are skimming the repo and want the right entry point fast:

- Need CSV read/write and chunked file ingestion? Use `csv`.
- Need JSON read/write with nested object support? Use `json`.
- Need immutable or mutable table transforms? Use `table`.
- Need bad-row rejection, dead-letter style logging, or fallible pipelines? Use `etl`.
- Need type inference, normalization, or validation after cleanup? Use `schema`.
- Need Excel input without bloating the core install? Use `excel`.

Typical progression:

`csv`, `json`, or `excel` -> `table` -> `etl` for fallible cleanup -> `schema` when you want typed checks

## Why gseq-table

Most business data workflows in Go start the same way:

- read CSV or Excel
- normalize headers
- clean broken values
- derive columns
- validate assumptions
- export or hand off to domain logic

At that stage, strongly typed structs are often too early, and low-level row processing is too painful.

`gseq-table` is designed for exactly that phase.

Just as important: the core stays intentionally lean.
If you only need tables, CSV, ETL, and schema handling, you do not pull in Excel dependencies.

## What it is

`gseq-table` is:

- string-first by design
- optimized for import-clean-transform-export workflows
- practical about missing columns and dirty rows
- explicit about immutable vs in-place APIs
- designed for dirty external data, not only clean happy-path inputs

`gseq-table` is not:

- a Pandas clone
- a columnar analytics engine
- a replacement for typed domain models
- a solution for very large datasets that should not live in memory

## Install

Core packages:

```bash
go get github.com/stefanbethge/gseq-table@latest
```

Optional Excel support:

```bash
go get github.com/stefanbethge/gseq-table/excel@latest
```

Requires Go 1.27+.

Core dependency footprint:

- `gseq-table`: depends on `github.com/stefanbethge/gseq`
- `gseq`: no third-party dependencies
- `gseq-table/excel`: separate module so Excel support stays opt-in

## Packages

| Package | Purpose |
|---|---|
| `table` | core `Table`, `MutableTable`, `Row`, joins, aggregations, reshape, validation |
| `csv` | CSV reader/writer, including chunked streaming reads |
| `json` | JSON reader/writer with flat, flatten, and field mapping modes |
| `etl` | composable pipelines with short-circuiting error propagation |
| `schema` | type inference, normalization, validation, typed accessors, stats |
| `excel` | optional Excel reader in a separate module |
| `experimental/simd` | **experimental**: SIMD kernels for `[]float64` / `[]int64` (not covered by the v1 guarantee) |

## Quick example

```go
package main

import (
    "log"
    "strconv"

    "github.com/stefanbethge/gseq-table/csv"
    "github.com/stefanbethge/gseq-table/table"
)

func main() {
    res := csv.New().ReadFile("sales.csv")
    if res.IsErr() {
        log.Fatal(res.UnwrapErr())
    }

    t := res.Unwrap().
        Rename("Customer ID", "customer_id").
        Rename("Revenue", "revenue").
        DropEmpty("customer_id").
        Map("revenue", func(v string) string {
            f, err := strconv.ParseFloat(v, 64)
            if err != nil {
                return ""
            }
            return strconv.FormatFloat(f, 'f', 2, 64)
        }).
        AddRowIndex("row_id").
        Sort("customer_id", true)

    if t.HasErrs() {
        for _, err := range t.Errs() {
            log.Println(err)
        }
    }

    if err := csv.NewWriter().WriteFile("sales_clean.csv", t); err != nil {
        log.Fatal(err)
    }
}
```

## Core idea: strings first, schema when needed

Every cell is stored as a string.

That is intentional.

For messy import data, this gives you:

- predictable ingestion from CSV and Excel
- no premature type failures at read time
- easier normalization and repair passes
- a clean point to introduce schema checks later

When you need types, use the `schema` package:

- infer likely types
- override important columns
- normalize values
- validate strictly or leniently
- access typed row values where needed

This keeps the core pipeline simple without pulling in a large type system or analytics stack.

### Typed access with generic methods

`Row`, `Table`, and `MutableTable` have typed methods built on Go 1.27 generic methods.
Cells are still stored as strings. The methods parse on read and format on write.

```go
// Row: option.Option[T]
qty := row.GetAs[int]("qty").UnwrapOr(0)
price := row.GetAs[float64]("price")          // option.Option[float64]
born := row.GetAs[time.Time]("birthday")
ttl := row.GetWith("ttl", time.ParseDuration) // any type, T inferred from the parser

// Columns
ages, err := t.ColAs[int]("age")        // error on unknown column or any bad cell
prices := t.ColOptAs[float64]("price")  // []option.Option[float64], aligned with rows
ids, err := t.ColWith("id", uuid.Parse) // custom parser

// Typed transforms (types inferred from fn)
t = t.MapAs("price", func(p float64) float64 { return p * 1.19 })
t = t.AddColAs("total", func(r table.Row) float64 {
    return r.GetAs[float64]("price").UnwrapOr(0) * float64(r.GetAs[int]("qty").UnwrapOr(0))
})

// Typed aggregations (empty and unparseable cells are skipped)
units := t.SumAs[int64]("qty")
cheapest := t.MinAs[float64]("price") // option.Option[float64]
latest := t.MaxAs[string]("sku")
active := t.ReduceAs("active", 0, func(n int, b bool) int {
    if b { n++ }
    return n
})

// MutableTable: same methods, in place, plus SetAs
m := t.Mutable()
m.MapAs("qty", func(n int) int { return n + 1 }).SetAs(0, "active", true)
```

| Method | Row | Table | MutableTable |
| --- | :-: | :-: | :-: |
| `GetAs[T]`, `AtAs[T]`, `GetWith` | ✓ | | |
| `ColAs[T]`, `ColWith`, `ColOptAs[T]` | | ✓ | ✓ |
| `MapAs`, `AddColAs` | | ✓ | ✓ (in place) |
| `SumAs[T]`, `MinAs[T]`, `MaxAs[T]`, `ReduceAs` | | ✓ | ✓ |
| `SetAs` | | | ✓ |

Supported types (`table.Value`): all built-in integer and float types, `string`, `bool`, and `time.Time`.
For anything else, use `GetWith` or `ColWith` with your own parser.

Parsing follows the same rules as the `schema` row accessors:

- surrounding whitespace is trimmed
- empty cells count as missing for every type except `string`
- booleans accept `true`/`false`, `1`/`0`, `yes`/`no` (case-insensitive)
- dates use the same layouts as `schema.Time`
- the zero date (`0001-01-01`) counts as not parsed, the same as in `schema.Time`

Formatting writes integers in base 10, floats in their shortest round-trip form, and booleans as `true`/`false`.
A date at midnight UTC is written as `2006-01-02`. Any other time is written as RFC 3339.

`MapAs` leaves empty and unparseable cells unchanged. If the column does not exist, it records a table error like `Map` does.

#### Structs

`FromStructs` builds a table from a slice of structs, and `ToStructs[T]` (on `Table` and `MutableTable`) parses the rows back into them.
Each exported field is one column, named by its `gseq` tag or else by the field name:

```go
type Person struct {
    Name  string                `gseq:"name"`
    Age   int                   `gseq:"age"`
    Email option.Option[string] `gseq:"email"` // empty cell ⇄ None
    Born  *time.Time            `gseq:"born"`  // empty cell ⇄ nil
    Note  string                `gseq:"note,omitempty"`
    Cache string                `gseq:"-"`     // skipped
}

t := table.FromStructs(people)          // also accepts []*Person
people, err := t.ToStructs[Person]()
```

- Field types: `table.Value` types, named types over built-in numbers, strings and bools (`type Status string`), pointers to these, and `option.Option[T]`. Cells use the parse and format rules above.
- Fields of embedded structs are flattened into the parent.
- `ToStructs` returns an error for a missing column, an unparseable cell (for example `ToStructs: column "age" row 3: cannot parse "x" as int`), or an empty cell in a field that is not a pointer, `Option`, or string. It ignores extra columns.
- `omitempty` writes zero values as empty cells. On read, it leaves the field at zero when the cell is empty or the column is missing.
- `FromStructs` records a table error for an unsupported type (it panics under `-tags strict`).

Compatibility notes:

- Generic methods cannot satisfy interfaces, so no existing interface changed. The existing package-level helpers (`table.ColAs`, `table.MapColTo`, `table.AddColOf`) are still there.

### Iterators

`Table` and `MutableTable` provide `iter.Seq` iterators for range-over-func loops.
They do not copy cells or allocate, and `break` stops the iteration.

```go
for i, r := range t.All() { ... }                  // iter.Seq2[int, table.Row]
for r := range t.RowsSeq() { ... }                 // iter.Seq[table.Row]
for city := range t.ColSeq("city") { ... }         // iter.Seq[string]
for i, age := range t.ColSeqAs[int]("age") { ... } // iter.Seq2[int, int], row index + value
```

- `ColSeqAs` skips empty and unparseable cells, like `ReduceAs`. The index tells you which row the value came from.
- An unknown column gives an empty sequence and records no error, like `Col`.
- `MutableTable` iterators read the live table on each step. `Set`, `Map`, and similar calls on rows not yet visited are seen. Rows appended during the loop are visited too. After a structural change (`Select`, `Drop`, `Where`, `Sort`, ...) the loop does not panic, but which values it yields is unspecified. Iterate `m.Freeze()` if you need a stable snapshot.

## Two APIs: immutable and mutable

### Table

`table.Table` is the immutable API.
Every transformation returns a new table.

This is the default when you want:

- easy chaining
- safe branching
- fewer accidental side effects

```go
clean := t.
    DropEmpty("id").
    Where(t.NotEmpty("email")).
    Sort("created_at", true)
```

### MutableTable

`table.MutableTable` is the opt-in in-place API.

Use it when you want:

- lower allocation churn
- incremental building
- explicit ownership of mutation

```go
m := t.Mutable()
m.FillForward("region").Map("status", normalizeStatus)
out := m.Freeze()
```

Rows can also be appended from a map with `AppendMap`. Columns missing from
the map become `""`; keys that are not a column are recorded as table errors
(like `Set` or `Map` on an unknown column) while the row is still appended:

```go
m := table.NewMutable([]string{"id", "name", "city"}, nil)
m.AppendMap(map[string]string{"id": "1", "name": "Alice"}) // city = ""
```

## Error model

`gseq-table` has two distinct error-handling layers.

### 1. Table-level lenient errors

Many table operations do not panic and do not immediately fail.
Instead, they accumulate errors on the table and continue.

That is useful when processing imperfect external data:

- one bad column name should not always destroy a full cleanup pass
- multiple issues can be reported together
- pipelines can stay fluent

```go
out := t.Select("id", "missing_col").Map("also_missing", strings.TrimSpace)

if out.HasErrs() {
    for _, err := range out.Errs() {
        log.Println(err)
    }
}
```

For development and CI, strict mode is available:

```bash
go test -tags strict ./...
go build -tags strict ./...
```

In strict mode, error-accumulating operations panic immediately with a stack trace.

This layer is useful for structural issues inside table transformations:

- missing columns in fluent chains
- invalid column references during cleanup work
- collecting multiple mistakes before reporting them

### 2. Row-level reject handling in ETL pipelines

The stronger ETL feature lives in `etl.WithErrorLog`.

When you attach an `ErrorLog` to a pipeline:

- `TryMap` and `TryTransform` stop failing fast on bad rows
- rejected rows are filtered out of the main flow
- each rejected row is logged with source, step, row index, error, and original values
- the remaining good rows continue through the pipeline

That makes the error path an explicit output of the workflow, not just an exception path.

```go
log := etl.NewErrorLog()

good := etl.FromResult(csv.New().ReadFile("orders.csv")).
    WithErrorLog(log).
    TryMap("price", parsePrice).
    TryMap("quantity", parseQty).
    Unwrap()

rejected := log.ToTable()
reviewQueue := rejected.Select("_source", "_step", "_row", "_error", "order_id", "customer")
byStep := rejected.ValueCounts("_step")
```

This is especially useful for:

- reject CSV exports
- manual review queues
- quality dashboards by error type or pipeline step
- separating recoverable bad rows from hard pipeline failures

Hard errors still stay hard errors:

- I/O failures
- missing required columns via `AssertColumns`
- any explicit pipeline step that returns an `Err`

If you want the full flow, see [`examples/03_error_log`](./examples/03_error_log).

## Dependency philosophy

The library is intentionally split so the common path stays light:

- core table operations live in the main module
- CSV and JSON support live in the main module
- schema and ETL live in the main module
- Excel support lives in its own module

That gives you a practical default:

- no hidden heavy dependency tree
- no spreadsheet dependency unless you explicitly want it
- a small core surface that is easier to audit and maintain

## Typical workflow

### 1. Read raw data

```go
t := csv.New().ReadFile("input.csv").Unwrap()
```

### 2. Clean structure

```go
t = t.
    Rename("Customer ID", "customer_id").
    Rename("E-Mail", "email").
    Drop("unused_notes")
```

### 3. Clean values

```go
t = t.
    FillEmpty("country", "unknown").
    FillForward("account_manager").
    Map("email", strings.TrimSpace)
```

### 4. Derive data

```go
t = t.
    AddCol("domain", func(r table.Row) string {
        email := r.Get("email").UnwrapOr("")
        i := strings.LastIndex(email, "@")
        if i < 0 {
            return ""
        }
        return email[i+1:]
    })
```

### 5. Validate or type-check

```go
err := t.AssertColumns("customer_id", "email")
if err != nil {
    log.Fatal(err)
}
```

### 6. Export or continue downstream

```go
_ = csv.NewWriter().WriteFile("output.csv", t)
```

## Feature highlights

### Table operations

- select, drop, rename, transpose
- filtering, partitioning, sampling
- map and transform by column or row
- add derived or constant-value columns (`AddCol`, `AddColConstValue`)
- typed access, transforms, and aggregations via generic methods
- joins: inner, left, right, outer, anti
- stable sorting and multi-column sorting
- distinct, union, intersect
- melt and pivot
- `GroupByAgg` aggregations: `Sum`, `Mean`, `Count`, `CountDistinct`, `Min`, `Max`, `Median`, `Quantile`, `Var`, `StdDev`, `StringJoin`, `First`, `Last`
- lag, lead, cumulative sums, ranking, rolling aggregations
- window functions per partition with optional ordering, keeping row order
  (`t.PartitionBy("customer").OrderBy(table.Asc("date")).CumSum("revenue", "cum")`)

### IO

- CSV read/write
- chunked CSV streaming for large files
- row-by-row streaming for CSV and JSON/NDJSON (`Stream` → `iter.Seq2[table.Row, error]`), plus chunked JSON streaming (`ReadStream`)
- JSON read/write with three modes: flat (default), recursive flatten, and field mapping
- NDJSON (newline-delimited JSON) support
- optional Excel reading and writing in a separate module, including multi-sheet workbooks (`excel.NewWriter().WriteFileSheets(path, excel.Sheet{Name: "Sales", Table: t}, …)`) and optional native number cells (`excel.WithTypedCells()`)

### Schema

- inference for common scalar types
- normalization and validation
- typed row accessors
- custom date layouts per column (`CastDate`) and for single values (`ParseDate`)
- summary statistics and helper arithmetic
- string helpers for derived columns (`Trim`, `Lower`, `Title`, `Replace`, `RegexExtract`, `SplitPart`, `PadLeft`, `Substr`, `Concat`, …)

```go
s := schema.Infer(t).CastDate("booked", "2.1.2006")
res := s.Apply(t) // "5.3.2024" → "2024-03-05"

d, err := schema.ParseDate("05.03.2024 14:30", "02.01.2006 15:04")

t = t.AddCol("domain", schema.RegexExtract("email", `@(.+)$`, 1)).
	AddCol("zip", schema.PadLeft("zip", 5, '0')).
	AddCol("full_name", schema.Concat(" ", "first", "last"))
```

A custom layout replaces the built-in layouts for that column, and the zero date still counts as not parsed.

### ETL pipelines

The `etl` package is useful when you want explicit short-circuiting over fallible steps:

```go
// read -> clean -> try transform -> write
```

Use direct `Table` chaining for simple in-memory transformations.
Use `etl` when you need pipeline composition around I/O and fallible operations.

For dirty external data, `etl.WithErrorLog` is often the most important mode:

- strict mode: first bad row stops the pipeline
- lax mode with `ErrorLog`: bad rows are rejected and logged, good rows continue

That gives you a dead-letter style workflow for tabular ETL without hiding failures.

## When to use gseq-table

Use it when:

- you regularly ingest CSV, JSON, or Excel files
- your inputs are inconsistent or dirty
- you want a fluent in-memory wrangling API in Go
- you want to delay strong typing until after cleanup
- you need a practical middle ground between structs and dataframes

Do not use it when:

- your data should already be mapped directly into stable typed structs
- you need columnar performance for analytics workloads
- your datasets are too large for in-memory processing
- silent or accumulated errors would be unacceptable in your environment without strict mode

## Relationship to other tools

Compared with raw `encoding/csv` and custom row loops:

- higher-level API
- less repetitive plumbing
- clearer transformation intent

Compared with full dataframe libraries:

- simpler mental model
- stronger focus on import/cleanup/export workflows
- less emphasis on typed analytical computing
- smaller and more controlled dependency footprint in the common case

Compared with directly using `excelize`:

- spreadsheet input becomes a table workflow, not just workbook access
- Excel support remains optional instead of inflating every install

## Design tradeoffs

The main tradeoff is deliberate:

- you gain flexibility and ergonomic ETL operations
- you give up some type safety until validation time

That is usually the right trade for messy external data, and the wrong trade for already-clean domain objects.

## Experimental: SIMD kernels

> **Experimental.** `experimental/simd` is outside the v1 stability guarantee.
> Its API may change or be removed in any release.

`github.com/stefanbethge/gseq-table/experimental/simd` provides numeric kernels on plain slices:

- `SumFloat64`, `SumInt64`, `MeanFloat64`, `MeanInt64`
- `MinFloat64`, `MaxFloat64`, `MinInt64`, `MaxInt64`
- `DotFloat64`, `DotInt64`
- elementwise `AddFloat64`, `AddInt64`, `MulFloat64`, `MulInt64`
- filters: `CompareFloat64` / `CompareInt64` (to a `[]bool` mask) and `IndicesFloat64` / `IndicesInt64` (to matching row indices), with `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`

```go
import "github.com/stefanbethge/gseq-table/experimental/simd"

total := simd.SumFloat64(prices)
rows := simd.IndicesFloat64(nil, prices, simd.Gt, 100)
```

The package always builds. By default it uses a plain-Go scalar implementation.
To enable the vector kernels, build with the `simd` experiment, which uses the standard library's experimental `simd/archsimd` package:

```bash
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=simd go test ./experimental/simd
```

`simd.Accelerated()` reports whether the vector kernels are active.

| Target | With `GOEXPERIMENT=simd` |
|---|---|
| amd64 | AVX2 (256-bit), detected at runtime; scalar on CPUs without AVX2 |
| arm64 | NEON (128-bit) |
| other | scalar fallback |

Some kernels always use the scalar code:

- `MulInt64` and `DotInt64`: NEON has no 64-bit integer multiply, and on amd64 it needs AVX-512.
- `Compare*` and `Indices*` on arm64: NEON has no movemask, and extracting mask lanes costs more than Go's scalar compare.

Results are bit-identical across builds and CPUs.
Float `Sum`, `Mean` and `Dot` accumulate in eight fixed lanes, and the scalar fallback uses the same order.
The last bits can therefore differ from a naive left-to-right loop.
`Min` and `Max` behave like Go's builtin `min` and `max`: NaN propagates, and `-0 < +0`.

Integration with typed table columns is planned separately.

## License

MIT
