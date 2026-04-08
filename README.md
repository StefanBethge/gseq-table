# gseq-table

In-memory data tables for Go — a functional, pandas-inspired ETL toolkit built on top of [gseq](https://github.com/stefanbethge/gseq).

All values are strings. Every operation returns a new Table; nothing is mutated in place.

---

## Packages

| Package | Purpose |
|---|---|
| `table` | Core `Table` and `Row` types with all transformation operations |
| `csv` | Read CSV files or `io.Reader` streams into a `Table` |
| `etl` | Chainable `Pipeline` wrapper with automatic error propagation |

---

## Reading CSV

```go
import "github.com/stefanbethge/gseq-table/csv"

// Default: first row is the header, comma-separated
r := csv.New()

// Semicolon delimiter
r := csv.New(csv.WithSeparator(';'))

// No header row — columns get auto-names: col_0, col_1, …
r := csv.New(csv.WithNoHeader())

// No header row — provide explicit names
r := csv.New(csv.WithHeaderNames("id", "name", "amount"))

// Read from a file
res := r.ReadFile("sales.csv")
if res.IsErr() {
    log.Fatal(res.UnwrapErr())
}
t := res.Unwrap()

// Read from any io.Reader
res := r.Read(strings.NewReader("name,city\nAlice,Berlin\n"))
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
row.Headers()      // slice.Slice[string]{"name", "city", "revenue"}
row.Values()       // slice.Slice[string]{"Alice", "Berlin", "4200"}
```

### Shape & inspection

```go
t.Len()                  // 3
rows, cols := t.Shape()  // 3, 3
t.Head(2)                // first 2 rows
t.Tail(1)                // last row
t.Col("revenue")         // slice.Slice[string]{"4200", "3800", "5100"}
```

### Selecting & filtering columns

```go
// Keep only specific columns (in given order)
t.Select("name", "revenue")

// Remove columns
t.Drop("city")

// Rename a column
t.Rename("revenue", "sales")
```

### Filtering rows

```go
// Keep rows matching a predicate
t.Where(func(r table.Row) bool {
    return r.Get("city").UnwrapOr("") == "Berlin"
})

// Remove rows with empty values
t.DropEmpty()                // any empty cell
t.DropEmpty("name", "city")  // only in these columns

// Fill empty values
t.FillEmpty("region", "unknown")

// Deduplicate
t.Distinct()        // fully unique rows
t.Distinct("city")  // one row per city (keeps first occurrence)
```

### Transforming values

```go
// Transform a single column
t.Map("revenue", func(v string) string { return "$" + v })

// Add a computed column
t.AddCol("label", func(r table.Row) string {
    return r.Get("name").UnwrapOr("") + "@" + r.Get("city").UnwrapOr("")
})

// Update multiple columns at once (partial update — other columns unchanged)
t.Transform(func(r table.Row) map[string]string {
    return map[string]string{
        "revenue": r.Get("revenue").UnwrapOr("0") + " EUR",
        "source":  "import",
    }
})
```

### Sorting

```go
// Single column
t.Sort("name", true)   // ascending  (A → Z)
t.Sort("date", false)  // descending (newest first)

// Multiple columns — first key is primary sort
t.SortMulti(table.Asc("city"), table.Desc("revenue"))
```

### Joining tables

```go
orders    := table.New([]string{"order_id", "customer_id", "amount"}, ...)
customers := table.New([]string{"id", "name", "country"}, ...)

// Inner join — only matched rows are kept
orders.Join(customers, "customer_id", "id")

// Left join — all orders kept, unmatched customers → empty strings
orders.LeftJoin(customers, "customer_id", "id")

// Stack tables vertically (same schema)
jan.Append(feb).Append(mar)
```

### Aggregation

```go
// Split into sub-tables by column value
groups := t.GroupBy("city")
groups["Berlin"].Len() // 2

// Frequency table, sorted by count descending
t.ValueCounts("city")
// value   count
// Berlin  2
// Munich  1
```

### Reshape

```go
// Wide → long (Melt)
wide := table.New(
    []string{"name", "q1", "q2", "q3"},
    [][]string{
        {"Alice", "100", "200", "150"},
        {"Bob",   "120", "180", "210"},
    },
)

long := wide.Melt([]string{"name"}, "quarter", "revenue")
// name   quarter  revenue
// Alice  q1       100
// Alice  q2       200
// Alice  q3       150
// Bob    q1       120
// ...

// Long → wide (Pivot)
long.Pivot("name", "quarter", "revenue")
// name   q1   q2   q3
// Alice  100  200  150
// Bob    120  180  210
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
    DropEmpty("revenue", "region").
    FillEmpty("category", "other").
    Where(func(r table.Row) bool {
        return r.Get("status").UnwrapOr("") == "closed"
    }).
    Map("revenue", func(v string) string { return v + " EUR" }).
    AddCol("label", func(r table.Row) string {
        return r.Get("region").UnwrapOr("") + "/" + r.Get("category").UnwrapOr("")
    }).
    SortMulti(table.Desc("revenue"), table.Asc("region")).
    Result()

if res.IsErr() {
    log.Fatal(res.UnwrapErr())
}
t := res.Unwrap()
```

### Starting a pipeline

```go
etl.From(t)                       // from an existing Table
etl.FromResult(csvReader.Read(f)) // from a Result (e.g. CSV reader)
```

### Terminal operations

```go
p.Result()  // result.Result[table.Table, error]
p.Unwrap()  // table.Table — panics if error
p.IsOk()    // bool
p.IsErr()   // bool

// Split into one sub-pipeline per distinct value
for region, sub := range p.GroupBy("region") {
    fmt.Println(region, sub.Unwrap().Len())
}
```

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

    // Print result
    fmt.Println(t.Headers)
    for _, row := range t.Rows {
        fmt.Println(row.Values())
    }

    // Group by region
    for region, sub := range etl.From(t).GroupBy("region") {
        fmt.Printf("%s: %d rows\n", region, sub.Unwrap().Len())
    }

    // Pivot: revenue per product per region
    long := t.Drop("status")
    wide := long.Pivot("product", "region", "revenue")
    fmt.Println(wide.Headers) // [product EU US]
}
```
