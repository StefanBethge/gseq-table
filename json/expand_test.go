package json

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/slice"
)

func makeTable(headers []string, records [][]string) table.Table {
	return table.New(slice.Slice[string](headers), records)
}

func TestExpandColDefault(t *testing.T) {
	// Column "meta" contains a JSON object → expand top-level keys.
	tbl := makeTable(
		[]string{"id", "meta"},
		[][]string{
			{"1", `{"role":"admin","level":"3"}`},
			{"2", `{"role":"user","level":"1"}`},
		},
	)
	out := ExpandCol(tbl, "meta", WithSortedHeaders())

	// "meta" should be removed, "meta.level" and "meta.role" added.
	wantHeaders := []string{"id", "meta.level", "meta.role"}
	if len(out.Headers) != len(wantHeaders) {
		t.Fatalf("headers = %v, want %v", []string(out.Headers), wantHeaders)
	}
	for i, w := range wantHeaders {
		if out.Headers[i] != w {
			t.Errorf("header[%d] = %q, want %q", i, out.Headers[i], w)
		}
	}

	if got := out.Rows[0].Get("meta.role").UnwrapOr(""); got != "admin" {
		t.Errorf("row[0] meta.role = %q, want %q", got, "admin")
	}
	if got := out.Rows[1].Get("meta.level").UnwrapOr(""); got != "1" {
		t.Errorf("row[1] meta.level = %q, want %q", got, "1")
	}
	// Original id should still be there.
	if got := out.Rows[0].Get("id").UnwrapOr(""); got != "1" {
		t.Errorf("row[0] id = %q, want %q", got, "1")
	}
}

func TestExpandColDefaultArray(t *testing.T) {
	tbl := makeTable(
		[]string{"id", "tags"},
		[][]string{{"1", `["go","data","json"]`}},
	)
	out := ExpandCol(tbl, "tags", WithSortedHeaders())

	if got := out.Rows[0].Get("tags.0").UnwrapOr(""); got != "go" {
		t.Errorf("tags.0 = %q, want %q", got, "go")
	}
	if got := out.Rows[0].Get("tags.2").UnwrapOr(""); got != "json" {
		t.Errorf("tags.2 = %q, want %q", got, "json")
	}
}

func TestExpandColWithFieldMapping(t *testing.T) {
	tbl := makeTable(
		[]string{"id", "payload"},
		[][]string{
			{"1", `{"user":{"name":"Alice","addr":{"city":"Berlin"}},"score":95}`},
			{"2", `{"user":{"name":"Bob","addr":{"city":"Munich"}},"score":87}`},
		},
	)
	out := ExpandCol(tbl, "payload", WithFieldMapping(map[string]string{
		"name":  ".user.name",
		"city":  ".user.addr.city",
		"score": ".score",
	}))

	if got := out.Rows[0].Get("name").UnwrapOr(""); got != "Alice" {
		t.Errorf("row[0] name = %q, want %q", got, "Alice")
	}
	if got := out.Rows[1].Get("city").UnwrapOr(""); got != "Munich" {
		t.Errorf("row[1] city = %q, want %q", got, "Munich")
	}
	if got := out.Rows[0].Get("score").UnwrapOr(""); got != "95" {
		t.Errorf("row[0] score = %q, want %q", got, "95")
	}
	// Original column should be gone.
	if got := out.ColIndex("payload"); got >= 0 {
		t.Error("original 'payload' column should be removed")
	}
}

func TestExpandColWithFieldMappingArrayIndex(t *testing.T) {
	tbl := makeTable(
		[]string{"data"},
		[][]string{{`{"items":[{"id":"a"},{"id":"b"}]}`}},
	)
	out := ExpandCol(tbl, "data", WithFieldMapping(map[string]string{
		"first":  ".items[0].id",
		"second": ".items[1].id",
	}))

	if got := out.Rows[0].Get("first").UnwrapOr(""); got != "a" {
		t.Errorf("first = %q, want %q", got, "a")
	}
	if got := out.Rows[0].Get("second").UnwrapOr(""); got != "b" {
		t.Errorf("second = %q, want %q", got, "b")
	}
}

func TestExpandColWithFlatten(t *testing.T) {
	tbl := makeTable(
		[]string{"id", "details"},
		[][]string{
			{"1", `{"user":{"name":"Alice"},"tags":["a","b"]}`},
		},
	)
	out := ExpandCol(tbl, "details", WithFlatten(), WithSortedHeaders())

	wantCols := map[string]string{
		"details.tags.0":     "a",
		"details.tags.1":     "b",
		"details.user.name":  "Alice",
	}
	for col, want := range wantCols {
		got := out.Rows[0].Get(col).UnwrapOr("MISSING")
		if got != want {
			t.Errorf("col %q = %q, want %q", col, got, want)
		}
	}
}

func TestExpandColWithFlattenMaxDepth(t *testing.T) {
	tbl := makeTable(
		[]string{"data"},
		[][]string{{`{"a":{"b":{"c":"deep"}}}`}},
	)
	out := ExpandCol(tbl, "data", WithFlatten(), WithMaxDepth(1))

	got := out.Rows[0].Get("data.a.b").UnwrapOr("MISSING")
	if got != `{"c":"deep"}` {
		t.Errorf("data.a.b = %q, want JSON string", got)
	}
}

func TestExpandColWithFlattenSeparator(t *testing.T) {
	tbl := makeTable(
		[]string{"x"},
		[][]string{{`{"a":{"b":"1"}}`}},
	)
	out := ExpandCol(tbl, "x", WithFlatten(), WithFlattenSeparator("__"))

	got := out.Rows[0].Get("x__a__b").UnwrapOr("MISSING")
	if got != "1" {
		t.Errorf("x__a__b = %q, want %q", got, "1")
	}
}

func TestExpandColInvalidJSON(t *testing.T) {
	tbl := makeTable(
		[]string{"id", "data"},
		[][]string{
			{"1", `{"valid":"ok"}`},
			{"2", `{broken`},
			{"3", `{"also":"ok"}`},
		},
	)
	out := ExpandCol(tbl, "data", WithSortedHeaders())

	// Row 0 and 2 should have expanded values.
	if got := out.Rows[0].Get("data.valid").UnwrapOr(""); got != "ok" {
		t.Errorf("row[0] data.valid = %q, want %q", got, "ok")
	}
	if got := out.Rows[2].Get("data.also").UnwrapOr(""); got != "ok" {
		t.Errorf("row[2] data.also = %q, want %q", got, "ok")
	}
	// Row 1 (broken JSON) should have empty values.
	if got := out.Rows[1].Get("data.valid").UnwrapOr(""); got != "" {
		t.Errorf("row[1] broken JSON should give empty, got %q", got)
	}
}

func TestExpandColEmptyCell(t *testing.T) {
	tbl := makeTable(
		[]string{"id", "data"},
		[][]string{
			{"1", `{"a":"1"}`},
			{"2", ""},
		},
	)
	out := ExpandCol(tbl, "data", WithSortedHeaders())

	if got := out.Rows[0].Get("data.a").UnwrapOr(""); got != "1" {
		t.Errorf("row[0] data.a = %q, want %q", got, "1")
	}
	// Empty cell should produce empty values.
	if got := out.Rows[1].Get("data.a").UnwrapOr(""); got != "" {
		t.Errorf("row[1] empty cell should give empty, got %q", got)
	}
}

func TestExpandColUnknownColumn(t *testing.T) {
	tbl := makeTable([]string{"id"}, [][]string{{"1"}})
	out := ExpandCol(tbl, "nonexistent")
	if !out.HasErrs() {
		t.Error("expected error for unknown column")
	}
}

func TestExpandColPreservesSource(t *testing.T) {
	tbl := makeTable([]string{"data"}, [][]string{{`{"a":"1"}`}}).
		WithSource("test.json")
	out := ExpandCol(tbl, "data")
	if got := out.Source(); got != "test.json" {
		t.Errorf("Source() = %q, want %q", got, "test.json")
	}
}

func TestExpandColSparseRows(t *testing.T) {
	// Different rows have different keys.
	tbl := makeTable(
		[]string{"data"},
		[][]string{
			{`{"a":"1","b":"2"}`},
			{`{"b":"3","c":"4"}`},
		},
	)
	out := ExpandCol(tbl, "data", WithSortedHeaders())

	// Row 0 has a and b, missing c → ""
	if got := out.Rows[0].Get("data.c").UnwrapOr(""); got != "" {
		t.Errorf("row[0] data.c = %q, want empty", got)
	}
	// Row 1 has b and c, missing a → ""
	if got := out.Rows[1].Get("data.a").UnwrapOr(""); got != "" {
		t.Errorf("row[1] data.a = %q, want empty", got)
	}
	if got := out.Rows[1].Get("data.c").UnwrapOr(""); got != "4" {
		t.Errorf("row[1] data.c = %q, want %q", got, "4")
	}
}

func TestExpandColMappingNonLeaf(t *testing.T) {
	tbl := makeTable(
		[]string{"data"},
		[][]string{{`{"user":{"name":"Alice","age":"30"}}`}},
	)
	out := ExpandCol(tbl, "data", WithFieldMapping(map[string]string{
		"user_obj": ".user",
	}))

	got := out.Rows[0].Get("user_obj").UnwrapOr("MISSING")
	// Non-leaf should be serialised as JSON.
	if got != `{"age":"30","name":"Alice"}` && got != `{"name":"Alice","age":"30"}` {
		t.Errorf("user_obj = %q, want JSON object", got)
	}
}

func TestExpandColScalarJSON(t *testing.T) {
	// Cell contains a JSON scalar (string, number), not an object/array.
	tbl := makeTable(
		[]string{"val"},
		[][]string{{`"just a string"`}, {`42`}},
	)
	out := ExpandCol(tbl, "val")

	if got := out.Rows[0].Get("val").UnwrapOr(""); got != "just a string" {
		t.Errorf("row[0] val = %q, want %q", got, "just a string")
	}
	if got := out.Rows[1].Get("val").UnwrapOr(""); got != "42" {
		t.Errorf("row[1] val = %q, want %q", got, "42")
	}
}
