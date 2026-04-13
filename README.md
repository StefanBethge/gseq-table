# gseq-table

In-memory data tables for Go — a functional ETL toolkit built on [gseq](https://github.com/stefanbethge/gseq).

All values are strings. `table.Table` is the immutable API; `table.MutableTable` is the opt-in in-place variant with full method chaining.

---

## Packages

| Package | Module | Purpose |
|---|---|---|
| `table` | `gseq-table` | Core `Table`, `MutableTable` and `Row` types with all transformation operations |
| `csv` | `gseq-table` | Read and write CSV files |
| `etl` | `gseq-table` | Chainable `Pipeline` wrapper with automatic error propagation |
| `schema` | `gseq-table` | Type inference, validation, typed accessors, and column statistics |
| `excel` | `gseq-table/excel` | Read Excel (.xlsx) files — **separate module** to avoid pulling in excelize |

```bash
go get github.com/stefanbethge/gseq-table@latest        # table, csv, etl, schema
go get github.com/stefanbethge/gseq-table/excel@latest   # excel reader (adds excelize dependency)
```

---

## Reading data

### CSV

```go
import "github.com/stefanbethge/gseq-table/csv"

r := csv.New()                                  // header row, comma-separated
r := csv.New(csv.WithSeparator(';'))            // semicolon-delimited
r := csv.New(csv.WithNoHeader())                // auto-generated col_0, col_1, …
r := csv.New(csv.WithHeaderNames("id", "name")) // explicit header names

// Read from file or io.Reader
res := r.ReadFile("sales.csv")
res := r.Read(strings.NewReader("name,city\nAlice,Berlin\n"))

t := res.Unwrap()

// Stream large files in chunks
for t, err := range csv.New().ReadFileStream("big.csv", 1000) {
    if err != nil { log.Fatal(err) }
    process(t)
}
```

### Excel

```go
import "github.com/stefanbethge/gseq-table/excel"

r := excel.New()                           // first sheet, header row
r := excel.New(excel.WithSheet("Sales"))   // specific sheet by name
r := excel.New(excel.WithSheetIndex(2))    // sheet by index
r := excel.New(excel.WithPassword("pw"))   // encrypted workbook

res := r.ReadFile("report.xlsx")
res := r.Read(ioReader)

// Stream large files
for t, err := range excel.New().ReadFileStream("big.xlsx", 1000) {
    if err != nil { log.Fatal(err) }
    process(t)
}

// Discover sheets
sheets, _ := excel.SheetNames("report.xlsx")
```

### Writing CSV

```go
w := csv.NewWriter()                         // default: comma, with header
w := csv.NewWriter(csv.WithWriteSeparator(';'))
w := csv.NewWriter(csv.WithoutHeader())

w.WriteFile("out.csv", t)
w.Write(os.Stdout, t)
csv.ToString(t)  // quick string serialisation
```

---

## Error handling

Operations on missing columns or invalid inputs never panic — they **accumulate
errors** on the table and continue executing. Check accumulated errors at the
end of a chain with `HasErrs()` / `Errs()`:

```go
result := t.Sort("missing_col", true).Map("also_missing", fn).FillForward("valid_col")

if result.HasErrs() {
    for _, err := range result.Errs() {
        log.Println(err)
        // Sort: unknown column "missing_col"
        // Map: unknown column "also_missing"
    }
}
```

Both `Table` (immutable) and `MutableTable` (mutable) support error accumulation.
Errors propagate through `Mutable()` / `MutableView()` / `Freeze()` / `FreezeView()`.

### Dataset source tagging

Attach a dataset name so error messages identify which file caused the problem:

```go
t = t.WithSource("sales.csv")
// errors now read: "[sales.csv] Sort: unknown column ..."
```

`csv.ReadFile` and `excel.ReadFile` set `WithSource` automatically.

### Strict mode

Build with `-tags strict` to make every error-accumulating call **panic
immediately** with a full stack trace. Useful in development and CI to catch
typos in column names at the point of the mistake:

```bash
go test -tags strict ./...
go build -tags strict ./cmd/myapp
```

---

## Working with Tables (immutable)

### Construction

```go
import "github.com/stefanbethge/gseq-table/table"

t := table.New(
    []string{"name", "city", "revenue"},
    [][]string{
        {"Alice", "Berlin", "4200"},
        {"Bob",   "Munich", "3800"},
        {"Carol", "Berlin", "5100"},
    },
)
```

### Row access

```go
row := t.Rows[0]

row.Get("name")    // option.Some("Alice")
row.Get("unknown") // option.None
row.At(2)          // option.Some("4200")
row.ToMap()        // map[string]string{"name":"Alice", ...}
row.Headers()      // []string{"name", "city", "revenue"}
row.Values()       // []string{"Alice", "Berlin", "4200"}
```

### Shape & inspection

```go
t.Len()                  // 3
rows, cols := t.Shape()  // 3, 3
t.Head(2)                // first 2 rows
t.Tail(1)                // last row
t.Col("revenue")         // []string{"4200", "3800", "5100"}
t.ColIndex("revenue")    // 2  (O(1) lookup, -1 if missing)
```

### Selecting & renaming columns

```go
t.Select("name", "revenue")
t.Drop("city")
t.Rename("revenue", "sales")
t.RenameMany(map[string]string{"revenue": "sales", "city": "location"})
t.AddRowIndex("row_id")   // prepend 0, 1, 2, … column
t.Transpose()             // rows ↔ columns
```

### Filtering rows

```go
t.Where(func(r table.Row) bool {
    return r.Get("city").UnwrapOr("") == "Berlin"
})

t.DropEmpty()                // any empty cell
t.DropEmpty("name", "city")  // only these columns
t.FillEmpty("region", "unknown")
t.FillForward("region")     // fill gaps with value above
t.FillBackward("region")    // fill gaps with value below
t.Distinct()                // fully unique rows
t.Distinct("city")          // one row per city
t.Sample(100)               // random sample
t.SampleFrac(0.1)           // 10% random sample
```

### Predicates

Predicates use pre-computed column indices for O(1) per-row lookups.

```go
t.Where(t.Eq("status", "active"))
t.Where(t.Ne("status", "deleted"))
t.Where(t.Contains("email", "gmail"))
t.Where(t.Prefix("name", "Dr."))
t.Where(t.Suffix("email", ".com"))
t.Where(t.Matches("email", `^[^@]+@gmail\.com$`))
t.Where(t.Empty("notes"))
t.Where(t.NotEmpty("email"))

// Combine with And, Or, Not
t.Where(table.And(t.Eq("status", "active"), t.NotEmpty("email")))
t.Where(table.Or(t.Eq("city", "Berlin"), t.Eq("city", "Munich")))
t.Where(table.Not(t.Empty("email")))
```

### Transforming values

```go
t.Map("revenue", func(v string) string { return "$" + v })

t.AddCol("label", func(r table.Row) string {
    return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
})

t.AddColFloat("ratio", func(r table.Row) float64 { return a / b })
t.AddColInt("year", func(r table.Row) int64 { return y })

t.AddColSwitch("grade",
    []table.Case{
        {When: func(r table.Row) bool { return r.Get("score").UnwrapOr("") == "A" },
         Then: func(r table.Row) string { return "excellent" }},
    },
    func(r table.Row) string { return "other" },  // else
)

// Update multiple columns at once
t.Transform(func(r table.Row) map[string]string {
    return map[string]string{"revenue": v + " EUR", "source": "import"}
})

t.FormatCol("price", 2)   // round floats to 2 decimals
t.Explode("tags", ",")    // split "go,etl" into separate rows
t.Coalesce("display", "nickname", "name", "email")  // first non-empty
t.Lookup("cust_id", "cust_name", customers, "id", "name")
t.Bin("age", "group", []table.BinDef{
    {Max: 18, Label: "minor"}, {Max: 65, Label: "adult"}, {Max: 999, Label: "senior"},
})
```

### Sorting

Sorting is **stable** — equal elements retain their original order.

```go
t.Sort("name", true)   // ascending
t.Sort("date", false)  // descending

// Multiple columns — first key is primary sort
t.SortMulti(table.Asc("city"), table.Desc("revenue"))
```

### Joining

```go
orders.Join(customers, "customer_id", "id")       // inner join
orders.LeftJoin(customers, "customer_id", "id")    // all left rows kept
orders.RightJoin(customers, "customer_id", "id")   // all right rows kept
orders.OuterJoin(customers, "customer_id", "id")   // all rows from both
orders.AntiJoin(customers, "customer_id", "id")    // left rows WITHOUT match

jan.Append(feb)               // stack two tables
table.Concat(jan, feb, mar)   // stack multiple tables
table.CartesianProduct(a, b)  // every combination
```

### Set operations

```go
table.Union(a, b, "id")   // distinct rows from both
a.Intersect(b, "id")      // rows in both a and b
```

### Aggregation

```go
t.GroupBy("city")           // map[string]Table
t.ValueCounts("city")      // frequency table (value, count)

t.GroupByAgg(
    []string{"region", "product"},
    []table.AggDef{
        {Col: "total",  Agg: table.Sum("revenue")},
        {Col: "avg",    Agg: table.Mean("revenue")},
        {Col: "n",      Agg: table.Count("revenue")},
        {Col: "labels", Agg: table.StringJoin("label", ", ")},
        {Col: "first",  Agg: table.First("created_at")},
        {Col: "last",   Agg: table.Last("updated_at")},
    },
)
```

### Reshape

```go
// Wide → long (Melt)
wide.Melt([]string{"name"}, "quarter", "revenue")

// Long → wide (Pivot)
long.Pivot("name", "quarter", "revenue")
```

### Time series

```go
t.Lag("revenue", "prev_revenue", 1)    // value from 1 row back
t.Lead("revenue", "next_revenue", 1)   // value from 1 row ahead
t.CumSum("revenue", "running_total")   // cumulative sum
t.Rank("score", "rank", true)          // dense rank (asc)
t.RollingAgg("avg_3d", 3, table.Mean("revenue"))  // sliding window
```

### Splitting & iteration

```go
active, inactive := t.Partition(t.Eq("status", "active"))
batches := t.Chunk(100)
t.ForEach(func(i int, r table.Row) { log.Println(i, r.ToMap()) })
```

### Parallel operations

```go
t.MapParallel("url", func(v string) string { return fetchTitle(v) })
t.TransformParallel(func(r table.Row) map[string]string { ... })
```

### Fallible operations

`TryMap` and `TryTransform` stop at the first error returned by the callback
and return a `result.Result[table.Table, error]`:

```go
res := t.TryMap("price", func(v string) (string, error) {
    f, err := strconv.ParseFloat(v, 64)
    if err != nil { return "", err }
    return fmt.Sprintf("%.2f EUR", f), nil
})

res := t.TryTransform(func(r table.Row) (map[string]string, error) { ... })

if res.IsOk() {
    t = res.Unwrap()
}
```

If the column itself is missing, the error is accumulated on the Table (not
returned in the Result), so the chain can continue.

### Validation

```go
err := t.AssertColumns("id", "email", "created_at")
err := t.AssertNoEmpty("id", "email")
```

### Generic helpers

```go
table.AddColOf(t, "sq", squareFn, fmtFn)       // typed column with custom formatter
table.ColAs(t, "age", strconv.ParseInt)          // extract column as []T
table.MapColTo(t, "name", strings.ToUpper)       // transform column to []T
```

---

## MutableTable (in-place updates)

`MutableTable` is the opt-in, pointer-based variant. All mutation methods
return `*MutableTable` so you can chain calls fluently.

### Construction & conversion

```go
// From scratch
m := table.NewMutable(
    []string{"name", "city"},
    [][]string{{"Alice", "Berlin"}},
)

// From an existing Table (deep copy)
m = t.Mutable()

// Zero-copy view (shared storage — mutations affect the source)
m = t.MutableView()

// Back to immutable
t = m.Freeze()      // deep copy
t = m.FreezeView()  // zero-copy (careful: later mutations to m affect t)
```

Errors propagate in both directions through `Mutable()`/`MutableView()` and
`Freeze()`/`FreezeView()`.

### Chaining

Every method returns `*MutableTable`, enabling fluent chains:

```go
m.Sort("name", true).
    FillForward("region").
    Map("revenue", func(v string) string { return v + " EUR" }).
    DropEmpty("revenue").
    Rename("revenue", "sales")

if m.HasErrs() {
    fmt.Println(m.Errs())   // all accumulated errors
}
m.ResetErrs()               // clear errors for next chain
```

### Available methods

MutableTable mirrors the full Table API — every operation listed above is also
available on `*MutableTable`, modifying in place instead of creating a copy:

**Column ops:** `Select`, `Drop`, `Rename`, `RenameMany`, `AddCol`,
`AddColFloat`, `AddColInt`, `AddColSwitch`, `AddRowIndex`, `Transpose`

**Row ops:** `Where`, `DropEmpty`, `Distinct`, `Head`, `Tail`, `Sample`,
`SampleFrac`, `Append`, `AppendMutable`, `AppendRow`

**Cell ops:** `Set`, `Map`, `MapParallel`, `FillEmpty`, `FillForward`,
`FillBackward`, `FormatCol`, `Transform`, `TransformParallel`, `TryMap`,
`TryTransform`

**Sorting:** `Sort`, `SortMulti`

**Joining:** `Join`, `LeftJoin`, `RightJoin`, `OuterJoin`, `AntiJoin`

**Aggregation:** `GroupByAgg`, `RollingAgg`, `ValueCounts`

**Reshape:** `Melt`, `Pivot`, `Explode`

**Time series:** `Lag`, `Lead`, `CumSum`, `Rank`

**Set ops:** `Intersect`, `Bin`, `Lookup`, `Coalesce`

**Validation:** `AssertColumns`, `AssertNoEmpty`

**Terminal ops** (return values, not `*MutableTable`):
`Table`/`Freeze`, `FreezeView`, `GroupBy`, `Partition`, `Chunk`, `ForEach`,
`Col`, `Headers`, `Len`, `Shape`, `ColIndex`, `Row`

**Predicates:** `Eq`, `Ne`, `Contains`, `Prefix`, `Suffix`, `Matches`,
`Empty`, `NotEmpty` (use with `Where`, `Partition`, etc.)

---

## Schema

Type inference, validation, typed accessors, and column statistics.

```go
import "github.com/stefanbethge/gseq-table/schema"

// Infer types from data
s := schema.Infer(t)
s.Col("age")   // schema.TypeInt
s.Col("price") // schema.TypeFloat

// Override a type
s = s.Cast("created_at", schema.TypeDate)

// Normalise cell values (trim whitespace, canonical bool/date format, …)
res := s.Apply(t)        // lenient: empty cells pass through
res := s.ApplyStrict(t)  // strict: empty cells in typed columns → error

// Typed row accessors
schema.Int(row, "age")             // option.Option[int64]
schema.Float(row, "price")        // option.Option[float64]
schema.Bool(row, "active")        // option.Option[bool]
schema.Time(row, "date", "")      // option.Option[time.Time]
```

### Column arithmetic

All arithmetic functions return `func(Row) float64` for use with `AddColFloat`:

```go
t.AddColFloat("total",  schema.Add("price", "tax", "shipping"))
t.AddColFloat("net",    schema.Sub("revenue", "cost", "tax", "fees"))
t.AddColFloat("volume", schema.Mul("length", "width", "height"))
t.AddColFloat("avg",    schema.Div("total", "count"))    // 0 if denominator=0
t.AddColFloat("margin", schema.Pct("profit", "revenue")) // (a/b)*100
t.AddColFloat("abs",    schema.Abs("pnl"))
t.AddColFloat("neg",    schema.Neg("cost"))
t.AddColFloat("vat",    schema.MulConst("net", 1.19))
t.AddColFloat("adj",    schema.AddConst("score", -10))
t.AddColFloat("mod",    schema.Mod("total", "batch"))
t.AddColFloat("lowest", schema.Min2("price_a", "price_b", "price_c"))
t.AddColFloat("highest",schema.Max2("price_a", "price_b", "price_c"))
t.AddColFloat("round",  schema.Round("ratio", 2))
t.AddColFloat("capped", schema.Clamp("score", 0, 100))
```

### Date arithmetic

```go
t.AddColFloat("days",   schema.DateDiffDays("end", "start"))
t.AddColFloat("months", schema.DateDiffMonths("end", "start"))
t.AddColFloat("years",  schema.DateDiffYears("end", "start"))
t.AddCol("due",         schema.DateAddDays("created_at", 30))
t.AddCol("review",      schema.DateAddMonths("created_at", 6))
```

### Date extraction

```go
t.AddCol("year",      schema.DateYear("date"))
t.AddCol("month",     schema.DateMonth("date"))
t.AddCol("day",       schema.DateDay("date"))
t.AddCol("quarter",   schema.DateQuarter("date"))       // 1–4
t.AddCol("week",      schema.DateWeek("date"))           // ISO week 1–53
t.AddCol("weekday",   schema.DateWeekday("date"))        // "Monday", …
t.AddCol("formatted", schema.DateFormat("date", "02.01.2006"))
t.AddCol("age",       schema.DateAge("birthday", time.Time{}))  // years until today

// Truncation & boundaries
t.AddCol("period",      schema.DateTrunc("date", "month"))      // 2024-03-15 → 2024-03-01
t.AddCol("month_start", schema.DateStartOfMonth("date"))
t.AddCol("month_end",   schema.DateEndOfMonth("date"))          // handles Feb 29

// Date predicate — use with Where
t.Where(schema.DateBetween("event", "period_start", "period_end"))

// Supported date formats (auto-detected):
// RFC3339, 2006-01-02, 2006-01-02T15:04:05, 02.01.2006,
// 01/02/2006, 02 Jan 2006, Jan 02, 2006
```

### Column statistics

```go
schema.SumCol(t, "revenue")       // float64
schema.MeanCol(t, "revenue")
schema.MinCol(t, "price")
schema.MaxCol(t, "price")
schema.MedianCol(t, "age")        // O(n) quickselect
schema.StdDevCol(t, "revenue")
schema.CountCol(t, "email")       // non-empty count
schema.CountWhere(t, "status", "active")

// Summary table — all stats at once
schema.Describe(t)
// column   count  min  max  mean  std  median

// Frequency map
schema.FreqMap(t, "status")  // map[string]int

// Min-max normalisation
schema.MinMaxNorm(t, "score")
```

---

## ETL Pipeline

The `etl` package provides two pipeline types and a set of composable
transformation functions. Pipelines use short-circuit error propagation: once
an error occurs, all subsequent steps are skipped.

The Pipeline is most useful when starting from an I/O source (`FromResult`) or
using fallible operations (`TryMap`, `TryTransform`, `ApplySchema`). For pure
table-to-table transforms, chaining directly on `Table` is often simpler since
Table has built-in error accumulation.

```go
import (
    "github.com/stefanbethge/gseq-table/csv"
    "github.com/stefanbethge/gseq-table/etl"
    "github.com/stefanbethge/gseq-table/table"
)
```

### Pipeline (immutable)

```go
// Start from an existing Table or *MutableTable
etl.From(t)          // from a Table
etl.From(m)          // from a *MutableTable (calls Freeze() implicitly)

// Start from an I/O source
etl.FromResult(csv.New().ReadFile("data.csv"))
etl.FromResult(excel.New().ReadFile("data.xlsx"))
```

**Core combinators:**

```go
// Infallible step — fn always returns a Table
p.Then(func(t table.Table) table.Table { return t.DropEmpty("id") })

// Fallible step — fn may return an error; if so, all later steps are skipped
p.ThenErr(func(t table.Table) result.Result[table.Table, error] {
    return t.TryMap("price", strconv.ParseFloat)
})
```

**Convenience short-hands** (factory functions for common operations):

```go
// Instead of verbose closures, use etl.* factory functions:
p.Then(etl.Select("name", "city"))
p.Then(etl.Map("revenue", func(v string) string { return v + " EUR" }))
p.Then(etl.DropEmpty("id"))
p.Then(etl.FillEmpty("region", "unknown"))
p.Then(etl.Sort("name", true))
p.Then(etl.SortMulti(table.Desc("revenue"), table.Asc("region")))
p.Then(etl.Join(customers, "customer_id", "id"))
p.Then(etl.GroupByAgg(groupCols, aggs))
// ...and all other table operations: Drop, Rename, RenameMany, AddCol,
// AddColFloat, AddColInt, AddColSwitch, Where, Distinct, Head, Tail,
// Sample, SampleFrac, Append, LeftJoin, Intersect, AddRowIndex,
// Explode, Transpose, ValueCounts, Melt, Pivot, TransformRows,
// RollingAgg, Coalesce, Lookup, FormatCol, Bin, FillForward, FillBackward
```

**Compose** — chain multiple `TableFunc` values into one reusable step:

```go
// TableFunc = func(table.Table) table.Table
normalise := etl.Compose(
    etl.DropEmpty("id"),
    etl.FillEmpty("region", "unknown"),
    etl.Map("name", strings.TrimSpace),
)

p.Then(normalise)           // apply the composed step
p.Then(normalise).Then(...) // chain further
```

**Conditional steps:**

```go
p.IfThen(dedupEnabled, etl.Distinct())
p.IfThenErr(validate, func(t table.Table) result.Result[table.Table, error] { ... })
```

**Error recovery:**

```go
// Replace error state with a fallback table
p.ThenErr(enrichFromAPI).RecoverWith(baseTable)

// Custom recovery logic
p.OnError(func(err error) (table.Table, error) {
    log.Printf("enrichment failed: %v — using base table", err)
    return baseTable, nil
})

// Wrap or transform the error itself
p.MapErr(func(err error) error {
    return fmt.Errorf("processing sales.csv: %w", err)
})
```

**Multi-source merge:**

```go
// Stack multiple pipelines vertically — first error wins
p := etl.ConcatPipelines(
    etl.FromResult(csv.New().ReadFile("jan.csv")),
    etl.FromResult(csv.New().ReadFile("feb.csv")),
    etl.FromResult(csv.New().ReadFile("mar.csv")),
)

// Or on an existing pipeline
p.ConcatWith(etl.FromResult(csv.New().ReadFile("apr.csv")))
```

**Parallel branches (FanOut):**

```go
branches := p.FanOut(
    func(t table.Table) table.Table { return t.Where(t.Eq("region", "EU")) },
    func(t table.Table) table.Table { return t.Where(t.Eq("region", "US")) },
)
eu, us := branches[0], branches[1]
```

**Named reusable transforms:**

```go
// Package a pipeline-to-pipeline function with a name (useful for tracing)
normalise := etl.NewTransform("normalise", func(p etl.Pipeline) etl.Pipeline {
    return p.
        Then(etl.DropEmpty("id")).
        Then(etl.FillEmpty("region", "unknown")).
        Then(etl.Map("name", strings.TrimSpace))
})

validate := etl.NewTransform("validate", func(p etl.Pipeline) etl.Pipeline {
    return p.AssertColumns("id", "region").AssertNoEmpty("id")
})

p.Apply(normalise).Apply(validate).Unwrap()
```

**Execution tracing:**

```go
p := etl.FromResult(csv.New().ReadFile("data.csv")).WithTracing()

result := p.
    Step("filter", func(t table.Table) table.Table { return t.Where(...) }).
    Step("enrich", func(t table.Table) table.Table { return t.AddCol(...) }).
    Unwrap()

for _, rec := range p.Trace() {
    fmt.Printf("%s: %d → %d rows (%v)\n", rec.Name, rec.InputRows, rec.OutputRows, rec.Duration)
}
// filter: 10000 → 4200 rows (1.2ms)
// enrich: 4200 → 4200 rows (0.8ms)
```

**Schema validation:**

```go
p.ApplySchema(s)        // normalise cell values, lenient (empty ok)
p.ApplySchemaStrict(s)  // normalise + error on empty typed cells
p.AssertColumns("id", "email", "created_at")
p.AssertNoEmpty("id", "email")
```

**Terminal operations:**

```go
p.Result()  // result.Result[table.Table, error]
p.Unwrap()  // table.Table (panics on error)
p.IsOk()    // bool
p.IsErr()   // bool

// Split into sub-pipelines
for region, sub := range p.GroupBy("region") {
    fmt.Println(region, sub.Unwrap().Len())
}
active, rest := p.Partition(t.Eq("status", "active"))
batches := p.Chunk(100)
```

### MutablePipeline (in-place)

`MutablePipeline` avoids intermediate immutable Table allocations — each step
mutates the same underlying `*MutableTable`. Use `Frozen()` to bridge to an
immutable `Pipeline` when you need schema validation or joins.

```go
mp := etl.FromMutable(m).
    Then(etl.Mut.Map("city", strings.ToUpper)).
    Then(etl.Mut.DropEmpty("id")).
    Then(etl.Mut.FillEmpty("region", "unknown")).
    Then(etl.Mut.Sort("name", true))

if mp.IsErr() {
    log.Fatal(mp.Result().UnwrapErr())
}

// Bridge to immutable pipeline for schema validation
t := mp.Frozen().
    ApplySchema(s).
    AssertNoEmpty("id").
    Unwrap()
```

**`etl.Mut` namespace** — factory functions for `MutableFunc`:

```go
etl.Mut.Map("col", fn)
etl.Mut.Select("a", "b")
etl.Mut.Drop("tmp")
etl.Mut.DropEmpty("id")
etl.Mut.FillEmpty("region", "unknown")
etl.Mut.FillForward("region")
etl.Mut.FillBackward("region")
etl.Mut.Sort("name", true)
etl.Mut.SortMulti(table.Asc("city"), table.Desc("rev"))
etl.Mut.Rename("old", "new")
etl.Mut.RenameMany(map[string]string{"a": "b"})
etl.Mut.AddCol("label", fn)
etl.Mut.AddColFloat("ratio", fn)
etl.Mut.AddColInt("year", fn)
etl.Mut.AddColSwitch("grade", cases, else_)
etl.Mut.Where(predFn)
etl.Mut.Distinct("id")
etl.Mut.Head(100)
etl.Mut.Tail(100)
etl.Mut.Sample(50)
etl.Mut.SampleFrac(0.1)
etl.Mut.Append(other)
etl.Mut.AppendMutable(otherMutable)
etl.Mut.Join(other, "left_id", "right_id")
etl.Mut.LeftJoin(other, "left_id", "right_id")
etl.Mut.RightJoin(other, "left_id", "right_id")
etl.Mut.OuterJoin(other, "left_id", "right_id")
etl.Mut.AntiJoin(other, "left_id", "right_id")
etl.Mut.Intersect(other, "id")
etl.Mut.Transform(fn)
etl.Mut.TransformParallel(fn)
etl.Mut.Explode("tags", ",")
etl.Mut.Transpose()
etl.Mut.AddRowIndex("row")
etl.Mut.ValueCounts("status")
etl.Mut.Melt(idCols, "var", "val")
etl.Mut.Pivot("index", "col", "val")
etl.Mut.GroupByAgg(groupCols, aggs)
etl.Mut.RollingAgg("avg_3", 3, table.Mean("rev"))  // not yet in Mut, use Then directly
etl.Mut.Coalesce("display", "nickname", "name")
etl.Mut.Lookup("id", "name", ref, "id", "name")
etl.Mut.FormatCol("price", 2)
etl.Mut.Bin("age", "group", bins)
```

**MutCompose** — reusable in-place transform:

```go
// MutableFunc = func(*table.MutableTable) *table.MutableTable
normalise := etl.MutCompose(
    etl.Mut.DropEmpty("id"),
    etl.Mut.FillEmpty("region", "unknown"),
    etl.Mut.Map("name", strings.TrimSpace),
)

mp.Then(normalise)
```

**MutablePipeline — conditional steps and error recovery:**

```go
mp.IfThen(cond, etl.Mut.Distinct())
mp.IfThenErr(cond, fn)
mp.RecoverWith(fallbackMutable)
mp.OnError(func(err error) (*table.MutableTable, error) { ... })
mp.MapErr(func(err error) error { return fmt.Errorf("context: %w", err) })
```

**MutablePipeline tracing:**

```go
mp := etl.FromMutable(m).WithTracing()
mp.Step("clean", etl.Mut.DropEmpty("id")).
   Step("sort", etl.Mut.Sort("name", true))
for _, rec := range mp.Trace() { fmt.Println(rec) }
```

### Pipeline vs direct Table chaining

| | Direct Table chain | Pipeline / MutablePipeline |
|---|---|---|
| Error model | Accumulated (`HasErrs()`) | Short-circuit (`Result.IsErr()`) |
| I/O errors | Handle before chain | Built-in via `FromResult` |
| Fallible callbacks | `TryMap` returns `Result` | `ThenErr` short-circuits |
| Reusable steps | Not composable | `Compose` / `MutCompose` / `Transform` |
| Tracing | No | `WithTracing` / `Step` / `Trace` |
| Best for | Pure transforms, simple scripts | I/O → transform → validate, production ETL |

**Note:** Table-level errors (accumulated via `HasErrs()`) are **not** surfaced
through the Pipeline's `Result` error. When using Pipeline, check both
`p.IsErr()` for hard errors and `p.Unwrap().HasErrs()` for soft errors.

---

## Best practices

### Choose the right table type

- **`Table`** (immutable, value type): Default choice. Safe to branch, share,
  and pass across goroutines. Every method returns a new Table.
- **`MutableTable`** (mutable, pointer type): Use when building tables
  incrementally or when minimising allocations matters. Chain with
  `.Sort(...).Map(...).FillForward(...)`. Call `Freeze()` when done.
- **`MutableView` / `FreezeView`**: Zero-copy shortcuts for hot paths where you
  know ownership is exclusive. Avoid if the source is still used elsewhere.

### Error handling patterns

```go
// Immutable: check at end of chain
result := t.Sort("x", true).Map("y", fn).Select("x", "y")
if result.HasErrs() {
    log.Println(result.Errs())
}

// Mutable: check at end of chain
m.Sort("x", true).Map("y", fn).Select("x", "y")
if m.HasErrs() {
    log.Println(m.Errs())
    m.ResetErrs()
}

// Strict mode in CI: build with -tags strict to panic on any error
// go test -tags strict ./...
```

### Source tagging for multi-file workflows

```go
sales := csv.New().ReadFile("sales.csv").Unwrap()     // source set automatically
costs := csv.New().ReadFile("costs.csv").Unwrap()     // source set automatically

merged := sales.Join(costs, "id", "id")
if merged.HasErrs() {
    // errors show "[sales.csv] Join: ..." or "[costs.csv] Join: ..."
}
```

### Performance tips

- For bulk transforms, prefer `Transform` / `TransformParallel` over multiple
  `Map` calls — it makes a single pass over all rows.
- Use `MapParallel` / `TransformParallel` only for expensive per-row work (HTTP
  calls, heavy computation). For simple string ops, the sequential version is faster.
- Use `MutableTable` with `MutableView()` / `FreezeView()` to avoid copies in
  tight loops — but only when you own the data exclusively.
- `schema.MedianCol` uses O(n) quickselect, not O(n log n) sort.

---

## Full example

```go
package main

import (
    "fmt"
    "strings"

    "github.com/stefanbethge/gseq-table/csv"
    "github.com/stefanbethge/gseq-table/table"
)

const data = `region,product,revenue,status
EU,Widget,4200,closed
EU,Gadget,3100,open
US,Widget,5800,closed
US,Gadget,,closed
EU,Widget,2900,closed
`

func main() {
    t := csv.New().Read(strings.NewReader(data)).Unwrap()

    result := t.
        Where(t.Eq("status", "closed")).
        DropEmpty("revenue").
        Map("revenue", func(v string) string { return v + " USD" }).
        SortMulti(table.Asc("region"), table.Desc("revenue"))

    if result.HasErrs() {
        fmt.Println("Errors:", result.Errs())
        return
    }

    fmt.Println(result.Headers)
    for _, row := range result.Rows {
        fmt.Println(row.Values())
    }

    for region, sub := range result.GroupBy("region") {
        fmt.Printf("%s: %d rows\n", region, sub.Len())
    }
}
```

### Mutable example

```go
m := table.NewMutable(
    []string{"name", "city", "score"},
    [][]string{
        {"Alice", "Berlin", "85"},
        {"Bob", "Munich", ""},
        {"Carol", "Berlin", "92"},
    },
)

m.FillEmpty("score", "0").
    Sort("name", true).
    AddCol("label", func(r table.Row) string {
        return r.Get("name").UnwrapOr("") + " (" + r.Get("city").UnwrapOr("") + ")"
    })

if m.HasErrs() {
    fmt.Println(m.Errs())
}

t := m.Freeze()
fmt.Println(csv.ToString(t))
```
