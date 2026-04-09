# gseq-table

In-memory data tables for Go — a functional ETL toolkit built on [gseq](https://github.com/stefanbethge/gseq).

All values are strings. Every operation returns a new Table; nothing is mutated in place.

---

## Packages

| Package | Module | Purpose |
|---|---|---|
| `table` | `gseq-table` | Core `Table` and `Row` types with all transformation operations |
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

## Working with Tables

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

```go
res := t.TryMap("price", func(v string) (string, error) { ... })
res := t.TryTransform(func(r table.Row) (map[string]string, error) { ... })
// result.Result[table.Table, error]
```

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

// Column arithmetic — returns func(Row) float64, use with AddColFloat
t.AddColFloat("total",  schema.Add("price", "tax"))
t.AddColFloat("profit", schema.Sub("revenue", "cost"))
t.AddColFloat("amount", schema.Mul("qty", "unit_price"))
t.AddColFloat("avg",    schema.Div("total", "count"))    // 0 if denominator=0
t.AddColFloat("margin", schema.Pct("profit", "revenue")) // (a/b)*100
t.AddColFloat("abs",    schema.Abs("pnl"))
t.AddColFloat("neg",    schema.Neg("cost"))
t.AddColFloat("vat",    schema.MulConst("net", 1.19))
t.AddColFloat("adj",    schema.AddConst("score", -10))
t.AddColFloat("mod",    schema.Mod("total", "batch"))
t.AddColFloat("lower",  schema.Min2("bid", "ask"))
t.AddColFloat("upper",  schema.Max2("bid", "ask"))
t.AddColFloat("round",  schema.Round("ratio", 2))
t.AddColFloat("capped", schema.Clamp("score", 0, 100))

// Date arithmetic
t.AddColFloat("days",   schema.DateDiffDays("end", "start"))
t.AddColFloat("months", schema.DateDiffMonths("end", "start"))
t.AddColFloat("years",  schema.DateDiffYears("end", "start"))
t.AddCol("due",         schema.DateAddDays("created_at", 30))
t.AddCol("review",      schema.DateAddMonths("created_at", 6))

// Date extraction
t.AddCol("year",      schema.DateYear("date"))
t.AddCol("month",     schema.DateMonth("date"))
t.AddCol("day",       schema.DateDay("date"))
t.AddCol("quarter",   schema.DateQuarter("date"))       // 1–4
t.AddCol("week",      schema.DateWeek("date"))           // ISO week 1–53
t.AddCol("weekday",   schema.DateWeekday("date"))        // "Monday", …
t.AddCol("formatted", schema.DateFormat("date", "02.01.2006"))
t.AddCol("age",       schema.DateAge("birthday", time.Time{}))  // years until today

// Date truncation & boundaries
t.AddCol("period",      schema.DateTrunc("date", "month"))      // 2024-03-15 → 2024-03-01
t.AddCol("month_start", schema.DateStartOfMonth("date"))
t.AddCol("month_end",   schema.DateEndOfMonth("date"))          // handles Feb 29

// Date predicate — use with Where
t.Where(schema.DateBetween("event", "period_start", "period_end"))

// Supported date formats (auto-detected):
// RFC3339, 2006-01-02, 2006-01-02T15:04:05, 02.01.2006,
// 01/02/2006, 02 Jan 2006, Jan 02, 2006

// Column statistics
schema.SumCol(t, "revenue")       // float64
schema.MeanCol(t, "revenue")
schema.MinCol(t, "price")
schema.MaxCol(t, "price")
schema.MedianCol(t, "age")
schema.StdDevCol(t, "revenue")
schema.CountCol(t, "email")       // non-empty count
schema.CountWhere(t, "status", "active")

// Summary table
schema.Describe(t)
// column   count  min  max  mean  std  median

// Frequency map
schema.FreqMap(t, "status")  // map[string]int

// Min-max normalisation
schema.MinMaxNorm(t, "score")
```

---

## ETL Pipeline

`etl.Pipeline` wraps a `result.Result[table.Table, error]`. Every method
returns a new Pipeline; if an error occurs at any step all subsequent steps are
skipped and the error is forwarded to `Result()`.

```go
import (
    "github.com/stefanbethge/gseq-table/csv"
    "github.com/stefanbethge/gseq-table/etl"
    "github.com/stefanbethge/gseq-table/table"
)

res := etl.FromResult(csv.New().ReadFile("sales.csv")).
    AssertColumns("revenue", "region").
    DropEmpty("revenue", "region").
    FillEmpty("category", "other").
    Where(func(r table.Row) bool {
        return r.Get("status").UnwrapOr("") == "closed"
    }).
    Map("revenue", func(v string) string { return v + " EUR" }).
    SortMulti(table.Desc("revenue"), table.Asc("region")).
    Result()

if res.IsErr() {
    log.Fatal(res.UnwrapErr())
}
t := res.Unwrap()
```

### Starting a pipeline

```go
etl.From(t)                                      // from an existing Table
etl.FromResult(csv.New().ReadFile("data.csv"))   // from a Result
etl.FromResult(excel.New().ReadFile("data.xlsx")) // from Excel
```

### Terminal operations

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

The pipeline exposes all table operations: `Select`, `Drop`, `Where`, `Map`,
`AddCol`, `Rename`, `RenameMany`, `Sort`, `SortMulti`, `Join`, `LeftJoin`,
`RightJoin`, `OuterJoin`, `AntiJoin`, `Append`, `Concat`, `Distinct`,
`DropEmpty`, `FillEmpty`, `FillForward`, `FillBackward`, `Transform`,
`TransformParallel`, `MapParallel`, `TryTransform`, `TryMap`, `Head`, `Tail`,
`Sample`, `SampleFrac`, `AddColSwitch`, `AddColFloat`, `AddColInt`, `Melt`,
`Pivot`, `Transpose`, `Explode`, `AddRowIndex`, `Coalesce`, `Lookup`,
`FormatCol`, `Lag`, `Lead`, `CumSum`, `Rank`, `RollingAgg`, `Bin`,
`Intersect`, `ValueCounts`, `GroupByAgg`, `AssertColumns`, `AssertNoEmpty`,
`ApplySchema`, `ApplySchemaStrict`, `Peek`, `ForEach`.

---

## Full example

```go
package main

import (
    "fmt"
    "strings"

    "github.com/stefanbethge/gseq-table/csv"
    "github.com/stefanbethge/gseq-table/etl"
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
    t := etl.FromResult(csv.New().Read(strings.NewReader(data))).
        Where(func(r table.Row) bool {
            return r.Get("status").UnwrapOr("") == "closed"
        }).
        DropEmpty("revenue").
        Map("revenue", func(v string) string { return v + " USD" }).
        SortMulti(table.Asc("region"), table.Desc("revenue")).
        Unwrap()

    fmt.Println(t.Headers)
    for _, row := range t.Rows {
        fmt.Println(row.Values())
    }

    for region, sub := range etl.From(t).GroupBy("region") {
        fmt.Printf("%s: %d rows\n", region, sub.Unwrap().Len())
    }
}
```
