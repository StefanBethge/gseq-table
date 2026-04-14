# gseq-table

[![CI](https://github.com/StefanBethge/gseq-table/actions/workflows/ci.yml/badge.svg)](https://github.com/StefanBethge/gseq-table/actions/workflows/ci.yml)

ETL and spreadsheet-style data wrangling for Go.

`gseq-table` is an in-memory toolkit for working with messy CSV and Excel data:

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

Requires Go 1.23+.

Core dependency footprint:

- `gseq-table`: depends on `github.com/stefanbethge/gseq`
- `gseq`: no third-party dependencies
- `gseq-table/excel`: separate module so Excel support stays opt-in

## Packages

| Package | Purpose |
|---|---|
| `table` | core `Table`, `MutableTable`, `Row`, joins, aggregations, reshape, validation |
| `csv` | CSV reader/writer, including chunked streaming reads |
| `etl` | composable pipelines with short-circuiting error propagation |
| `schema` | type inference, normalization, validation, typed accessors, stats |
| `excel` | optional Excel reader in a separate module |

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
- CSV support lives in the main module
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
- joins: inner, left, right, outer, anti
- stable sorting and multi-column sorting
- distinct, union, intersect
- melt and pivot
- lag, lead, cumulative sums, ranking, rolling aggregations

### IO

- CSV read/write
- chunked CSV streaming for large files
- optional Excel reading in a separate module

### Schema

- inference for common scalar types
- normalization and validation
- typed row accessors
- summary statistics and helper arithmetic

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

- you regularly ingest CSV or Excel files
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

## License

MIT
