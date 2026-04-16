package table

import (
	"testing"

	"github.com/stefanbethge/gseq/slice"
)

func makeTestTable(headers []string, records [][]string) Table {
	return New(slice.Slice[string](headers), records)
}

// --- Immutable Table tests ---

func TestExpandJSON_Default(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "meta"},
		[][]string{
			{"1", `{"role":"admin","level":"3"}`},
			{"2", `{"role":"user","level":"1"}`},
		},
	)
	out := tbl.ExpandJSON("meta", WithJSONSortedHeaders())

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
	if got := out.Rows[0].Get("id").UnwrapOr(""); got != "1" {
		t.Errorf("row[0] id = %q, want %q", got, "1")
	}
}

func TestExpandJSON_DefaultArray(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "tags"},
		[][]string{{"1", `["go","data","json"]`}},
	)
	out := tbl.ExpandJSON("tags", WithJSONSortedHeaders())

	if got := out.Rows[0].Get("tags.0").UnwrapOr(""); got != "go" {
		t.Errorf("tags.0 = %q, want %q", got, "go")
	}
	if got := out.Rows[0].Get("tags.2").UnwrapOr(""); got != "json" {
		t.Errorf("tags.2 = %q, want %q", got, "json")
	}
}

func TestExpandJSON_FieldMapping(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "payload"},
		[][]string{
			{"1", `{"user":{"name":"Alice","addr":{"city":"Berlin"}},"score":95}`},
			{"2", `{"user":{"name":"Bob","addr":{"city":"Munich"}},"score":87}`},
		},
	)
	out := tbl.ExpandJSON("payload", WithJSONFieldMapping(map[string]string{
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
	if got := out.ColIndex("payload"); got >= 0 {
		t.Error("original 'payload' column should be removed")
	}
}

func TestExpandJSON_FieldMappingArrayIndex(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"items":[{"id":"a"},{"id":"b"}]}`}},
	)
	out := tbl.ExpandJSON("data", WithJSONFieldMapping(map[string]string{
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

func TestExpandJSON_Flatten(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "details"},
		[][]string{{"1", `{"user":{"name":"Alice"},"tags":["a","b"]}`}},
	)
	out := tbl.ExpandJSON("details", WithJSONFlatten(), WithJSONSortedHeaders())

	wantCols := map[string]string{
		"details.tags.0":    "a",
		"details.tags.1":    "b",
		"details.user.name": "Alice",
	}
	for col, want := range wantCols {
		got := out.Rows[0].Get(col).UnwrapOr("MISSING")
		if got != want {
			t.Errorf("col %q = %q, want %q", col, got, want)
		}
	}
}

func TestExpandJSON_FlattenMaxDepth(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"a":{"b":{"c":"deep"}}}`}},
	)
	out := tbl.ExpandJSON("data", WithJSONFlatten(), WithJSONMaxDepth(1))

	got := out.Rows[0].Get("data.a.b").UnwrapOr("MISSING")
	if got != `{"c":"deep"}` {
		t.Errorf("data.a.b = %q, want JSON string", got)
	}
}

func TestExpandJSON_FlattenSeparator(t *testing.T) {
	tbl := makeTestTable(
		[]string{"x"},
		[][]string{{`{"a":{"b":"1"}}`}},
	)
	out := tbl.ExpandJSON("x", WithJSONFlatten(), WithJSONFlattenSeparator("__"))

	got := out.Rows[0].Get("x__a__b").UnwrapOr("MISSING")
	if got != "1" {
		t.Errorf("x__a__b = %q, want %q", got, "1")
	}
}

func TestExpandJSON_InvalidJSON(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "data"},
		[][]string{
			{"1", `{"valid":"ok"}`},
			{"2", `{broken`},
			{"3", `{"also":"ok"}`},
		},
	)
	out := tbl.ExpandJSON("data", WithJSONSortedHeaders())

	if got := out.Rows[0].Get("data.valid").UnwrapOr(""); got != "ok" {
		t.Errorf("row[0] data.valid = %q, want %q", got, "ok")
	}
	if got := out.Rows[1].Get("data.valid").UnwrapOr(""); got != "" {
		t.Errorf("row[1] broken JSON should give empty, got %q", got)
	}
}

func TestExpandJSON_EmptyCell(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "data"},
		[][]string{{"1", `{"a":"1"}`}, {"2", ""}},
	)
	out := tbl.ExpandJSON("data", WithJSONSortedHeaders())

	if got := out.Rows[1].Get("data.a").UnwrapOr(""); got != "" {
		t.Errorf("empty cell should give empty, got %q", got)
	}
}

func TestExpandJSON_UnknownColumn(t *testing.T) {
	tbl := makeTestTable([]string{"id"}, [][]string{{"1"}})
	out := tbl.ExpandJSON("nonexistent")
	if !out.HasErrs() {
		t.Error("expected error for unknown column")
	}
}

func TestExpandJSON_PreservesSource(t *testing.T) {
	tbl := makeTestTable([]string{"data"}, [][]string{{`{"a":"1"}`}}).
		WithSource("test.json")
	out := tbl.ExpandJSON("data")
	if got := out.Source(); got != "test.json" {
		t.Errorf("Source() = %q, want %q", got, "test.json")
	}
}

func TestExpandJSON_SparseRows(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"a":"1","b":"2"}`}, {`{"b":"3","c":"4"}`}},
	)
	out := tbl.ExpandJSON("data", WithJSONSortedHeaders())

	if got := out.Rows[0].Get("data.c").UnwrapOr(""); got != "" {
		t.Errorf("row[0] data.c = %q, want empty", got)
	}
	if got := out.Rows[1].Get("data.a").UnwrapOr(""); got != "" {
		t.Errorf("row[1] data.a = %q, want empty", got)
	}
}

// --- MapJSON tests (immutable) ---

func TestMapJSON_PathExtract(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "data"},
		[][]string{
			{"1", `{"user":{"name":"Alice"},"score":95}`},
			{"2", `{"user":{"name":"Bob"},"score":87}`},
		},
	)
	out := tbl.MapJSON("data", ".user.name")

	if got := out.Rows[0].Get("data").UnwrapOr(""); got != "Alice" {
		t.Errorf("row[0] data = %q, want %q", got, "Alice")
	}
	if got := out.Rows[1].Get("data").UnwrapOr(""); got != "Bob" {
		t.Errorf("row[1] data = %q, want %q", got, "Bob")
	}
}

func TestMapJSON_PathExtractNonLeaf(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"user":{"name":"Alice","age":"30"}}`}},
	)
	out := tbl.MapJSON("data", ".user")

	got := out.Rows[0].Get("data").UnwrapOr("")
	// Non-leaf → JSON string.
	if got != `{"age":"30","name":"Alice"}` && got != `{"name":"Alice","age":"30"}` {
		t.Errorf("data = %q, want JSON object", got)
	}
}

func TestMapJSON_PathExtractArrayIndex(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"items":[{"id":"a"},{"id":"b"}]}`}},
	)
	out := tbl.MapJSON("data", ".items[1].id")

	if got := out.Rows[0].Get("data").UnwrapOr(""); got != "b" {
		t.Errorf("data = %q, want %q", got, "b")
	}
}

func TestMapJSON_PathMissing(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"a":"1"}`}},
	)
	out := tbl.MapJSON("data", ".nonexistent")

	if got := out.Rows[0].Get("data").UnwrapOr("FAIL"); got != "" {
		t.Errorf("missing path should give empty, got %q", got)
	}
}

func TestMapJSON_FieldMapping(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"user":{"name":"Alice","age":"30"},"city":"Berlin"}`}},
	)
	out := tbl.MapJSON("data", WithJSONFieldMapping(map[string]string{
		"name": ".user.name",
		"city": ".city",
	}))

	got := out.Rows[0].Get("data").UnwrapOr("")
	if got != `{"city":"Berlin","name":"Alice"}` {
		t.Errorf("data = %q, want restructured JSON", got)
	}
}

func TestMapJSON_InvalidJSON(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{broken`}},
	)
	out := tbl.MapJSON("data", ".a")

	// Invalid JSON should be left unchanged.
	if got := out.Rows[0].Get("data").UnwrapOr(""); got != `{broken` {
		t.Errorf("invalid JSON should be unchanged, got %q", got)
	}
}

func TestMapJSON_EmptyCell(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{""}},
	)
	out := tbl.MapJSON("data", ".a")

	if got := out.Rows[0].Get("data").UnwrapOr("FAIL"); got != "" {
		t.Errorf("empty cell should stay empty, got %q", got)
	}
}

func TestMapJSON_UnknownColumn(t *testing.T) {
	tbl := makeTestTable([]string{"id"}, [][]string{{"1"}})
	out := tbl.MapJSON("nonexistent", ".a")
	if !out.HasErrs() {
		t.Error("expected error for unknown column")
	}
}

func TestMapJSON_PreservesOtherColumns(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "data", "extra"},
		[][]string{{"1", `{"a":"val"}`, "keep"}},
	)
	out := tbl.MapJSON("data", ".a")

	if got := out.Rows[0].Get("id").UnwrapOr(""); got != "1" {
		t.Errorf("id = %q, want %q", got, "1")
	}
	if got := out.Rows[0].Get("extra").UnwrapOr(""); got != "keep" {
		t.Errorf("extra = %q, want %q", got, "keep")
	}
	if got := out.Rows[0].Get("data").UnwrapOr(""); got != "val" {
		t.Errorf("data = %q, want %q", got, "val")
	}
}

// --- MapJSON Mutable ---

func TestMapJSON_Mutable_Path(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"user":{"name":"Alice"}}`}},
	)
	m := tbl.Mutable()
	m.MapJSON("data", ".user.name")

	if got := m.Freeze().Rows[0].Get("data").UnwrapOr(""); got != "Alice" {
		t.Errorf("data = %q, want %q", got, "Alice")
	}
}

func TestMapJSON_Mutable_FieldMapping(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"a":"1","b":"2","c":"3"}`}},
	)
	m := tbl.Mutable()
	m.MapJSON("data", WithJSONFieldMapping(map[string]string{
		"x": ".a",
		"y": ".b",
	}))

	got := m.Freeze().Rows[0].Get("data").UnwrapOr("")
	if got != `{"x":"1","y":"2"}` {
		t.Errorf("data = %q, want restructured JSON", got)
	}
}

func TestMapJSON_Mutable_Chaining(t *testing.T) {
	tbl := makeTestTable(
		[]string{"a", "b"},
		[][]string{{`{"x":"1"}`, `{"y":"2"}`}},
	)
	out := tbl.Mutable().
		MapJSON("a", ".x").
		MapJSON("b", ".y").
		Freeze()

	if got := out.Rows[0].Get("a").UnwrapOr(""); got != "1" {
		t.Errorf("a = %q, want %q", got, "1")
	}
	if got := out.Rows[0].Get("b").UnwrapOr(""); got != "2" {
		t.Errorf("b = %q, want %q", got, "2")
	}
}

// --- MutableTable tests ---

func TestExpandJSON_Mutable_Default(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "meta"},
		[][]string{
			{"1", `{"role":"admin","level":"3"}`},
			{"2", `{"role":"user","level":"1"}`},
		},
	)
	m := tbl.Mutable()
	m.ExpandJSON("meta", WithJSONSortedHeaders())

	if got := m.Freeze().Rows[0].Get("meta.role").UnwrapOr(""); got != "admin" {
		t.Errorf("row[0] meta.role = %q, want %q", got, "admin")
	}
	if got := m.Freeze().Rows[0].Get("id").UnwrapOr(""); got != "1" {
		t.Errorf("row[0] id = %q, want %q", got, "1")
	}
}

func TestExpandJSON_Mutable_FieldMapping(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "payload"},
		[][]string{
			{"1", `{"user":{"name":"Alice"},"score":95}`},
		},
	)
	m := tbl.Mutable()
	m.ExpandJSON("payload", WithJSONFieldMapping(map[string]string{
		"name":  ".user.name",
		"score": ".score",
	}))

	out := m.Freeze()
	if got := out.Rows[0].Get("name").UnwrapOr(""); got != "Alice" {
		t.Errorf("name = %q, want %q", got, "Alice")
	}
	if got := out.ColIndex("payload"); got >= 0 {
		t.Error("original column should be removed")
	}
}

func TestExpandJSON_Mutable_Flatten(t *testing.T) {
	tbl := makeTestTable(
		[]string{"data"},
		[][]string{{`{"a":{"b":"1"}}`}},
	)
	m := tbl.Mutable()
	m.ExpandJSON("data", WithJSONFlatten())

	out := m.Freeze()
	if got := out.Rows[0].Get("data.a.b").UnwrapOr(""); got != "1" {
		t.Errorf("data.a.b = %q, want %q", got, "1")
	}
}

func TestExpandJSON_Mutable_UnknownColumn(t *testing.T) {
	tbl := makeTestTable([]string{"id"}, [][]string{{"1"}})
	m := tbl.Mutable()
	m.ExpandJSON("nonexistent")
	if !m.HasErrs() {
		t.Error("expected error for unknown column")
	}
}

func TestExpandJSON_Mutable_Chaining(t *testing.T) {
	tbl := makeTestTable(
		[]string{"id", "data"},
		[][]string{{"1", `{"a":"val"}`}},
	)
	out := tbl.Mutable().
		ExpandJSON("data", WithJSONSortedHeaders()).
		Freeze()

	if got := out.Rows[0].Get("data.a").UnwrapOr(""); got != "val" {
		t.Errorf("data.a = %q, want %q", got, "val")
	}
}

func TestExpandJSON_Mutable_PreservesSource(t *testing.T) {
	tbl := makeTestTable([]string{"data"}, [][]string{{`{"a":"1"}`}}).
		WithSource("src.json")
	m := tbl.Mutable()
	m.ExpandJSON("data")
	if got := m.Source(); got != "src.json" {
		t.Errorf("Source() = %q, want %q", got, "src.json")
	}
}