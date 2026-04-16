// 06_json_io demonstrates every feature of the json package.
//
// Topics covered:
//   - Flat JSON array reading (default mode)
//   - NDJSON (newline-delimited JSON) reading
//   - Nested values as JSON strings in default mode
//   - ReadString and ReadBytes convenience methods
//   - WithFlatten for recursive object/array flattening
//   - WithMaxDepth to limit flatten depth
//   - WithFlattenSeparator for custom key separators
//   - WithFieldMapping for explicit path-based extraction
//   - WithSortedHeaders for deterministic column order
//   - Writing JSON arrays
//   - Writing NDJSON
//   - WithPrettyPrint and WithIndent for formatted output
//   - ReadFile with automatic source tagging
//   - Round-trip: read -> transform -> write -> read back
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/stefanbethge/gseq-table/csv"
	gjson "github.com/stefanbethge/gseq-table/json"
	"github.com/stefanbethge/gseq-table/table"
)

func main() {
	// ── 1. Flat JSON array ───────────────────────────────────────────────────

	fmt.Println("=== 1. Flat JSON array (default mode) ===")

	flat := `[
		{"id": "1", "name": "Alice", "city": "Berlin"},
		{"id": "2", "name": "Bob",   "city": "Munich"},
		{"id": "3", "name": "Carol", "city": "Hamburg"}
	]`

	t := gjson.New(gjson.WithSortedHeaders()).ReadString(flat).Unwrap()
	fmt.Printf("Rows: %d, Headers: %v\n", t.Len(), []string(t.Headers))
	fmt.Println(csv.ToString(t))

	// ── 2. NDJSON (newline-delimited JSON) ───────────────────────────────────

	fmt.Println("=== 2. NDJSON ===")

	ndjson := `{"event":"login","user":"alice","ts":"2024-01-15T09:00:00Z"}
{"event":"purchase","user":"bob","ts":"2024-01-15T09:05:00Z"}
{"event":"logout","user":"alice","ts":"2024-01-15T09:10:00Z"}
`

	t = gjson.New(gjson.WithNDJSON(), gjson.WithSortedHeaders()).ReadString(ndjson).Unwrap()
	fmt.Printf("Rows: %d, Headers: %v\n", t.Len(), []string(t.Headers))
	fmt.Println(csv.ToString(t))

	// ── 3. Nested values as JSON strings (default mode) ──────────────────────

	fmt.Println("=== 3. Nested values as JSON strings (default mode) ===")

	nested := `[
		{
			"id": "1",
			"user": {"name": "Alice", "role": "admin"},
			"tags": ["go", "data"]
		}
	]`

	t = gjson.New(gjson.WithSortedHeaders()).ReadString(nested).Unwrap()
	fmt.Printf("Headers: %v\n", []string(t.Headers))
	for _, h := range t.Headers {
		fmt.Printf("  %s = %s\n", h, t.Rows[0].Get(h).UnwrapOr(""))
	}
	fmt.Println()

	// ── 4. ReadBytes convenience ─────────────────────────────────────────────

	fmt.Println("=== 4. ReadBytes convenience ===")

	payload := []byte(`[{"status":"ok","code":200}]`)
	t = gjson.New(gjson.WithSortedHeaders()).ReadBytes(payload).Unwrap()
	fmt.Printf("From bytes: %d rows, code=%s\n\n",
		t.Len(), t.Rows[0].Get("code").UnwrapOr(""))

	// ── 5. WithFlatten ───────────────────────────────────────────────────────

	fmt.Println("=== 5. WithFlatten — recursive flattening ===")

	deepJSON := `[
		{
			"user": {
				"name": "Alice",
				"address": {"city": "Berlin", "zip": "10115"}
			},
			"scores": [95, 87, 92],
			"metadata": {"source": "api", "version": 2}
		},
		{
			"user": {
				"name": "Bob",
				"address": {"city": "Munich", "zip": "80331"}
			},
			"scores": [78, 91],
			"metadata": {"source": "import", "version": 1}
		}
	]`

	t = gjson.New(gjson.WithFlatten(), gjson.WithSortedHeaders()).ReadString(deepJSON).Unwrap()
	fmt.Printf("Flattened headers: %v\n", []string(t.Headers))
	fmt.Println(csv.ToString(t))

	// ── 6. WithFlatten + WithMaxDepth ────────────────────────────────────────

	fmt.Println("=== 6. WithFlatten + WithMaxDepth(1) ===")

	t = gjson.New(
		gjson.WithFlatten(),
		gjson.WithMaxDepth(1),
		gjson.WithSortedHeaders(),
	).ReadString(deepJSON).Unwrap()

	fmt.Printf("Depth-limited headers: %v\n", []string(t.Headers))
	for _, h := range t.Headers {
		fmt.Printf("  %s = %s\n", h, t.Rows[0].Get(h).UnwrapOr(""))
	}
	fmt.Println()

	// ── 7. WithFlattenSeparator ──────────────────────────────────────────────

	fmt.Println("=== 7. WithFlattenSeparator(\"__\") ===")

	t = gjson.New(
		gjson.WithFlatten(),
		gjson.WithFlattenSeparator("__"),
		gjson.WithSortedHeaders(),
	).ReadString(`[{"a":{"b":{"c":"deep"}}}]`).Unwrap()

	fmt.Printf("Headers with __ separator: %v\n", []string(t.Headers))
	fmt.Printf("  a__b__c = %s\n\n", t.Rows[0].Get("a__b__c").UnwrapOr(""))

	// ── 8. WithFieldMapping ──────────────────────────────────────────────────

	fmt.Println("=== 8. WithFieldMapping — explicit path extraction ===")

	apiResponse := `[
		{
			"data": {
				"user": {"first_name": "Alice", "last_name": "Smith"},
				"scores": [95, 87, 92],
				"address": {"city": "Berlin", "country": "DE"}
			},
			"meta": {"request_id": "abc-123"}
		},
		{
			"data": {
				"user": {"first_name": "Bob", "last_name": "Jones"},
				"scores": [78, 91, 85],
				"address": {"city": "Munich", "country": "DE"}
			},
			"meta": {"request_id": "def-456"}
		}
	]`

	t = gjson.New(gjson.WithFieldMapping(map[string]string{
		"first_name": ".data.user.first_name",
		"last_name":  ".data.user.last_name",
		"city":       ".data.address.city",
		"country":    ".data.address.country",
		"top_score":  ".data.scores[0]",
		"request_id": ".meta.request_id",
	})).ReadString(apiResponse).Unwrap()

	fmt.Printf("Mapped headers: %v\n", []string(t.Headers))
	fmt.Println(csv.ToString(t))

	// ── 9. Field mapping — non-leaf values become JSON strings ────────────────

	fmt.Println("=== 9. Field mapping — non-leaf as JSON string ===")

	t = gjson.New(gjson.WithFieldMapping(map[string]string{
		"name":       ".data.user.first_name",
		"all_scores": ".data.scores",
		"full_addr":  ".data.address",
	})).ReadString(apiResponse).Unwrap()

	for _, h := range t.Headers {
		fmt.Printf("  %s = %s\n", h, t.Rows[0].Get(h).UnwrapOr(""))
	}
	fmt.Println()

	// ── 10. Write JSON array ─────────────────────────────────────────────────

	fmt.Println("=== 10. Write JSON array ===")

	t = table.New(
		[]string{"name", "score"},
		[][]string{{"Alice", "95"}, {"Bob", "87"}},
	)
	var buf bytes.Buffer
	_ = gjson.NewWriter().Write(&buf, t)
	fmt.Println(strings.TrimSpace(buf.String()))
	fmt.Println()

	// ── 11. Write NDJSON ─────────────────────────────────────────────────────

	fmt.Println("=== 11. Write NDJSON ===")

	buf.Reset()
	_ = gjson.NewWriter(gjson.WithWriteNDJSON()).Write(&buf, t)
	fmt.Print(buf.String())
	fmt.Println()

	// ── 12. Write with PrettyPrint ───────────────────────────────────────────

	fmt.Println("=== 12. Write with PrettyPrint ===")

	buf.Reset()
	_ = gjson.NewWriter(gjson.WithPrettyPrint()).Write(&buf, t)
	fmt.Println(strings.TrimSpace(buf.String()))
	fmt.Println()

	// ── 13. Write with custom indent ─────────────────────────────────────────

	fmt.Println("=== 13. Write with custom tab indent ===")

	buf.Reset()
	_ = gjson.NewWriter(gjson.WithPrettyPrint(), gjson.WithIndent("\t")).Write(&buf, t)
	fmt.Println(strings.TrimSpace(buf.String()))
	fmt.Println()

	// ── 14. ReadFile + WriteFile with source tagging ─────────────────────────

	fmt.Println("=== 14. ReadFile + WriteFile round-trip ===")

	tmpFile, err := os.CreateTemp("", "example-*.json")
	if err != nil {
		fmt.Println("Cannot create temp file:", err)
		return
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	_ = gjson.NewWriter(gjson.WithPrettyPrint()).WriteFile(tmpPath, t)
	tmpFile.Close()

	res := gjson.New(gjson.WithSortedHeaders()).ReadFile(tmpPath)
	if res.IsErr() {
		fmt.Println("Read error:", res.UnwrapErr())
		return
	}
	t2 := res.Unwrap()
	fmt.Printf("Source: %q, Rows: %d, Headers: %v\n", t2.Source(), t2.Len(), []string(t2.Headers))
	fmt.Println(csv.ToString(t2))

	// ── 15. ToString convenience ─────────────────────────────────────────────

	fmt.Println("=== 15. ToString convenience ===")

	t = table.New([]string{"x"}, [][]string{{"1"}, {"2"}})
	fmt.Println(strings.TrimSpace(gjson.ToString(t)))
	fmt.Println()

	// ── 16. Sparse rows — missing keys become empty strings ──────────────────

	fmt.Println("=== 16. Sparse rows ===")

	sparse := `[
		{"a": "1", "b": "2"},
		{"a": "3", "c": "4"},
		{"b": "5", "c": "6"}
	]`

	t = gjson.New(gjson.WithSortedHeaders()).ReadString(sparse).Unwrap()
	fmt.Printf("Headers: %v\n", []string(t.Headers))
	fmt.Println(csv.ToString(t))

	// ── 17. Type coercion — numbers, booleans, null ──────────────────────────

	fmt.Println("=== 17. Type coercion ===")

	types := `[{"str":"hello","int":42,"float":3.14,"bool_t":true,"bool_f":false,"null_val":null}]`

	t = gjson.New(gjson.WithSortedHeaders()).ReadString(types).Unwrap()
	for _, h := range t.Headers {
		v := t.Rows[0].Get(h).UnwrapOr("")
		repr := v
		if repr == "" {
			repr = "(empty)"
		}
		fmt.Printf("  %-10s = %s\n", h, repr)
	}
	fmt.Println()

	// ── 18. Flatten with mixed array content ─────────────────────────────────

	fmt.Println("=== 18. Flatten — arrays of objects and primitives ===")

	mixed := `[{
		"name": "project-x",
		"contributors": [
			{"login": "alice", "commits": 42},
			{"login": "bob",   "commits": 17}
		],
		"labels": ["bug", "enhancement", "help wanted"]
	}]`

	t = gjson.New(gjson.WithFlatten(), gjson.WithSortedHeaders()).ReadString(mixed).Unwrap()
	fmt.Printf("Headers: %v\n", []string(t.Headers))
	for _, h := range t.Headers {
		fmt.Printf("  %-25s = %s\n", h, t.Rows[0].Get(h).UnwrapOr(""))
	}
	fmt.Println()

	// ── 19. Pipeline: JSON read -> transform -> CSV write ────────────────────

	fmt.Println("=== 19. Pipeline: JSON -> transform -> CSV ===")

	orders := `[
		{"order_id": "A001", "customer": {"name": "Alice"}, "items": [{"sku": "W1", "qty": 2}]},
		{"order_id": "A002", "customer": {"name": "Bob"},   "items": [{"sku": "G3", "qty": 1}]}
	]`

	t = gjson.New(gjson.WithFieldMapping(map[string]string{
		"order_id":     ".order_id",
		"customer":     ".customer.name",
		"first_sku":    ".items[0].sku",
		"first_qty":    ".items[0].qty",
	})).ReadString(orders).Unwrap()

	t = t.AddCol("summary", func(r table.Row) string {
		return r.Get("customer").UnwrapOr("") + " ordered " +
			r.Get("first_qty").UnwrapOr("0") + "x " +
			r.Get("first_sku").UnwrapOr("")
	})

	fmt.Println(csv.ToString(t))
}
