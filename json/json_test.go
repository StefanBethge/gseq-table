package json

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadStringFlatArray(t *testing.T) {
	input := `[{"id":"1","name":"Alice"},{"id":"2","name":"Bob"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := len(tbl.Headers); got != 2 {
		t.Fatalf("headers: got %d, want 2", got)
	}
	if got, want := tbl.Headers[0], "id"; got != want {
		t.Errorf("header[0] = %q, want %q", got, want)
	}
	if got, want := tbl.Headers[1], "name"; got != want {
		t.Errorf("header[1] = %q, want %q", got, want)
	}
	if got := len(tbl.Rows); got != 2 {
		t.Fatalf("rows: got %d, want 2", got)
	}
	if got := tbl.Rows[0].Get("id").UnwrapOr(""); got != "1" {
		t.Errorf("row[0].id = %q, want %q", got, "1")
	}
	if got := tbl.Rows[1].Get("name").UnwrapOr(""); got != "Bob" {
		t.Errorf("row[1].name = %q, want %q", got, "Bob")
	}
}

func TestReadStringNDJSON(t *testing.T) {
	input := "{\"id\":\"1\",\"name\":\"Alice\"}\n{\"id\":\"2\",\"name\":\"Bob\"}\n"
	res := New(WithNDJSON()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := len(tbl.Rows); got != 2 {
		t.Fatalf("rows: got %d, want 2", got)
	}
	if got := tbl.Rows[0].Get("name").UnwrapOr(""); got != "Alice" {
		t.Errorf("row[0].name = %q, want %q", got, "Alice")
	}
}

func TestReadStringEmptyArray(t *testing.T) {
	res := New().ReadString(`[]`)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := len(tbl.Headers); got != 0 {
		t.Errorf("headers: got %d, want 0", got)
	}
	if got := len(tbl.Rows); got != 0 {
		t.Errorf("rows: got %d, want 0", got)
	}
}

func TestReadStringEmptyInput(t *testing.T) {
	res := New().ReadString(``)
	if res.IsErr() {
		t.Fatal("expected Ok for empty input, got Err:", res.UnwrapErr())
	}
}

func TestReadStringSparseRows(t *testing.T) {
	input := `[{"a":"1","b":"2"},{"a":"3","c":"4"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := len(tbl.Headers); got != 3 {
		t.Fatalf("headers: got %d, want 3", got)
	}

	// Row 0 has a and b, missing c → ""
	if got := tbl.Rows[0].Get("c").UnwrapOr(""); got != "" {
		t.Errorf("row[0].c = %q, want empty", got)
	}
	// Row 1 has a and c, missing b → ""
	if got := tbl.Rows[1].Get("b").UnwrapOr(""); got != "" {
		t.Errorf("row[1].b = %q, want empty", got)
	}
	if got := tbl.Rows[1].Get("c").UnwrapOr(""); got != "4" {
		t.Errorf("row[1].c = %q, want %q", got, "4")
	}
}

func TestReadStringTypes(t *testing.T) {
	input := `[{"s":"hello","n":42,"f":3.14,"bt":true,"bf":false,"null":null}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	tests := []struct {
		col  string
		want string
	}{
		{"s", "hello"},
		{"n", "42"},
		{"f", "3.14"},
		{"bt", "true"},
		{"bf", "false"},
		{"null", ""},
	}
	for _, tt := range tests {
		got := tbl.Rows[0].Get(tt.col).UnwrapOr("MISSING")
		if got != tt.want {
			t.Errorf("col %q = %q, want %q", tt.col, got, tt.want)
		}
	}
}

func TestReadStringNestedAsJSON(t *testing.T) {
	input := `[{"name":"Alice","meta":{"role":"admin"},"tags":["go","data"]}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	meta := tbl.Rows[0].Get("meta").UnwrapOr("")
	if meta != `{"role":"admin"}` {
		t.Errorf("meta = %q, want %q", meta, `{"role":"admin"}`)
	}
	tags := tbl.Rows[0].Get("tags").UnwrapOr("")
	if tags != `["go","data"]` {
		t.Errorf("tags = %q, want %q", tags, `["go","data"]`)
	}
}

func TestReadStringSortedHeaders(t *testing.T) {
	input := `[{"z":"1","a":"2","m":"3"}]`
	res := New(WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got, want := tbl.Headers[0], "a"; got != want {
		t.Errorf("header[0] = %q, want %q", got, want)
	}
	if got, want := tbl.Headers[1], "m"; got != want {
		t.Errorf("header[1] = %q, want %q", got, want)
	}
	if got, want := tbl.Headers[2], "z"; got != want {
		t.Errorf("header[2] = %q, want %q", got, want)
	}
}

func TestReadStringInvalidJSON(t *testing.T) {
	res := New().ReadString(`{invalid`)
	if !res.IsErr() {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestReadStringNonArrayTopLevel(t *testing.T) {
	res := New().ReadString(`{"not":"an array"}`)
	if !res.IsErr() {
		t.Fatal("expected error for non-array top level")
	}
}

func TestReadStringNDJSONInvalidLine(t *testing.T) {
	input := "{\"a\":\"1\"}\n{invalid}\n"
	res := New(WithNDJSON()).ReadString(input)
	if !res.IsErr() {
		t.Fatal("expected error for invalid NDJSON line")
	}
}

func TestReadFileSetsSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	if err := os.WriteFile(path, []byte(`[{"x":"1"}]`), 0o644); err != nil {
		t.Fatal(err)
	}

	res := New().ReadFile(path)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got, want := tbl.Source(), "test.json"; got != want {
		t.Errorf("Source() = %q, want %q", got, want)
	}
}

func TestReadFileNotFound(t *testing.T) {
	res := New().ReadFile("/nonexistent/path.json")
	if !res.IsErr() {
		t.Fatal("expected error for missing file")
	}
}

func TestReadBytesDelegates(t *testing.T) {
	data := []byte(`[{"k":"v"}]`)
	res := New().ReadBytes(data)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := tbl.Rows[0].Get("k").UnwrapOr(""); got != "v" {
		t.Errorf("got %q, want %q", got, "v")
	}
}

func TestReadStringNumberPrecision(t *testing.T) {
	// Ensure large integers are not mangled by float conversion.
	input := `[{"id":9007199254740993}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	got := res.Unwrap().Rows[0].Get("id").UnwrapOr("")
	if got != "9007199254740993" {
		t.Errorf("id = %q, want %q", got, "9007199254740993")
	}
}

func TestReadStringSingleRow(t *testing.T) {
	input := `[{"only":"one"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := len(tbl.Rows); got != 1 {
		t.Fatalf("rows: got %d, want 1", got)
	}
}

func TestReadStringNDJSONSingleObject(t *testing.T) {
	res := New(WithNDJSON()).ReadString(`{"a":"1"}`)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := len(tbl.Rows); got != 1 {
		t.Fatalf("rows: got %d, want 1", got)
	}
}

func TestFlattenNestedObjects(t *testing.T) {
	input := `[{"user":{"name":"Alice","addr":{"city":"Berlin"}},"age":"30"}]`
	res := New(WithFlatten(), WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	tests := []struct {
		col  string
		want string
	}{
		{"age", "30"},
		{"user.addr.city", "Berlin"},
		{"user.name", "Alice"},
	}
	for _, tt := range tests {
		got := tbl.Rows[0].Get(tt.col).UnwrapOr("MISSING")
		if got != tt.want {
			t.Errorf("col %q = %q, want %q", tt.col, got, tt.want)
		}
	}
}

func TestFlattenArrays(t *testing.T) {
	input := `[{"tags":["go","data"],"nested":[{"id":"1"},{"id":"2"}]}]`
	res := New(WithFlatten(), WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	tests := []struct {
		col  string
		want string
	}{
		{"nested.0.id", "1"},
		{"nested.1.id", "2"},
		{"tags.0", "go"},
		{"tags.1", "data"},
	}
	for _, tt := range tests {
		got := tbl.Rows[0].Get(tt.col).UnwrapOr("MISSING")
		if got != tt.want {
			t.Errorf("col %q = %q, want %q", tt.col, got, tt.want)
		}
	}
}

func TestFlattenMaxDepth(t *testing.T) {
	input := `[{"user":{"name":"Alice","addr":{"city":"Berlin"}}}]`
	res := New(WithFlatten(), WithMaxDepth(1), WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	// At depth 1, user.name is a leaf (depth 1), but user.addr is an object
	// at depth 1 and should be serialised as JSON.
	name := tbl.Rows[0].Get("user.name").UnwrapOr("MISSING")
	if name != "Alice" {
		t.Errorf("user.name = %q, want %q", name, "Alice")
	}
	addr := tbl.Rows[0].Get("user.addr").UnwrapOr("MISSING")
	if addr != `{"city":"Berlin"}` {
		t.Errorf("user.addr = %q, want %q", addr, `{"city":"Berlin"}`)
	}
}

func TestFlattenCustomSeparator(t *testing.T) {
	input := `[{"a":{"b":"1"}}]`
	res := New(WithFlatten(), WithFlattenSeparator("_")).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	got := tbl.Rows[0].Get("a_b").UnwrapOr("MISSING")
	if got != "1" {
		t.Errorf("a_b = %q, want %q", got, "1")
	}
}

func TestFlattenEmptyArrayAndObject(t *testing.T) {
	input := `[{"arr":[],"obj":{}}]`
	res := New(WithFlatten()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	arr := tbl.Rows[0].Get("arr").UnwrapOr("MISSING")
	if arr != "[]" {
		t.Errorf("arr = %q, want %q", arr, "[]")
	}
	obj := tbl.Rows[0].Get("obj").UnwrapOr("MISSING")
	if obj != "{}" {
		t.Errorf("obj = %q, want %q", obj, "{}")
	}
}

func TestFlattenDeeplyNested(t *testing.T) {
	input := `[{"a":{"b":{"c":{"d":"deep"}}}}]`
	res := New(WithFlatten()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	got := tbl.Rows[0].Get("a.b.c.d").UnwrapOr("MISSING")
	if got != "deep" {
		t.Errorf("a.b.c.d = %q, want %q", got, "deep")
	}
}

func TestFlattenAndMappingMutuallyExclusive(t *testing.T) {
	res := New(WithFlatten(), WithFieldMapping(map[string]string{"x": ".y"})).
		ReadString(`[{"y":"1"}]`)
	if !res.IsErr() {
		t.Fatal("expected error for flatten + mapping")
	}
}

func TestFlattenSparseRows(t *testing.T) {
	input := `[{"a":{"x":"1"}},{"a":{"y":"2"}}]`
	res := New(WithFlatten(), WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	// Row 0 has a.x but not a.y → ""
	if got := tbl.Rows[0].Get("a.y").UnwrapOr(""); got != "" {
		t.Errorf("row[0] a.y = %q, want empty", got)
	}
	// Row 1 has a.y but not a.x → ""
	if got := tbl.Rows[1].Get("a.x").UnwrapOr(""); got != "" {
		t.Errorf("row[1] a.x = %q, want empty", got)
	}
}

func TestFieldMappingSimple(t *testing.T) {
	input := `[{"user":{"name":"Alice","addr":{"city":"Berlin"}},"tags":["go","data"]}]`
	res := New(WithFieldMapping(map[string]string{
		"name":      ".user.name",
		"city":      ".user.addr.city",
		"first_tag": ".tags[0]",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := len(tbl.Headers); got != 3 {
		t.Fatalf("headers: got %d, want 3", got)
	}

	tests := []struct {
		col  string
		want string
	}{
		{"name", "Alice"},
		{"city", "Berlin"},
		{"first_tag", "go"},
	}
	for _, tt := range tests {
		got := tbl.Rows[0].Get(tt.col).UnwrapOr("MISSING")
		if got != tt.want {
			t.Errorf("col %q = %q, want %q", tt.col, got, tt.want)
		}
	}
}

func TestFieldMappingNonLeafAsJSON(t *testing.T) {
	input := `[{"user":{"name":"Alice","age":"30"}}]`
	res := New(WithFieldMapping(map[string]string{
		"user_obj": ".user",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	got := tbl.Rows[0].Get("user_obj").UnwrapOr("MISSING")
	// Non-leaf value should be serialised as JSON string.
	if got != `{"age":"30","name":"Alice"}` && got != `{"name":"Alice","age":"30"}` {
		t.Errorf("user_obj = %q, want JSON object", got)
	}
}

func TestFieldMappingMissingPath(t *testing.T) {
	input := `[{"a":"1"}]`
	res := New(WithFieldMapping(map[string]string{
		"x": ".nonexistent.path",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	got := tbl.Rows[0].Get("x").UnwrapOr("FAIL")
	if got != "" {
		t.Errorf("missing path should give empty string, got %q", got)
	}
}

func TestFieldMappingInvalidPath(t *testing.T) {
	res := New(WithFieldMapping(map[string]string{
		"x": "no_leading_dot",
	})).ReadString(`[{"a":"1"}]`)
	if !res.IsErr() {
		t.Fatal("expected error for invalid path syntax")
	}
}

func TestFieldMappingArrayIndex(t *testing.T) {
	input := `[{"items":[{"id":"a"},{"id":"b"},{"id":"c"}]}]`
	res := New(WithFieldMapping(map[string]string{
		"second": ".items[1].id",
		"third":  ".items[2].id",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := tbl.Rows[0].Get("second").UnwrapOr(""); got != "b" {
		t.Errorf("second = %q, want %q", got, "b")
	}
	if got := tbl.Rows[0].Get("third").UnwrapOr(""); got != "c" {
		t.Errorf("third = %q, want %q", got, "c")
	}
}

func TestFieldMappingOutOfBoundsIndex(t *testing.T) {
	input := `[{"items":["a"]}]`
	res := New(WithFieldMapping(map[string]string{
		"x": ".items[99]",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	got := res.Unwrap().Rows[0].Get("x").UnwrapOr("FAIL")
	if got != "" {
		t.Errorf("out-of-bounds index should give empty string, got %q", got)
	}
}

func TestFieldMappingDeterministicHeaders(t *testing.T) {
	res := New(WithFieldMapping(map[string]string{
		"z_col": ".a",
		"a_col": ".b",
		"m_col": ".c",
	})).ReadString(`[{"a":"1","b":"2","c":"3"}]`)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	// Headers should be sorted alphabetically by output column name.
	want := []string{"a_col", "m_col", "z_col"}
	for i, w := range want {
		if tbl.Headers[i] != w {
			t.Errorf("header[%d] = %q, want %q", i, tbl.Headers[i], w)
		}
	}
}

func TestFirstSeenHeaderOrder(t *testing.T) {
	// Second row introduces "c" which should appear after "a" and "b".
	input := `[{"a":"1","b":"2"},{"c":"3","a":"4"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	want := []string{"a", "b", "c"}
	if len(tbl.Headers) != len(want) {
		t.Fatalf("headers = %v, want %v", []string(tbl.Headers), want)
	}
	for i, w := range want {
		if tbl.Headers[i] != w {
			t.Errorf("header[%d] = %q, want %q", i, tbl.Headers[i], w)
		}
	}
}

// --- Validation and error handling ---

func TestNDJSONNonObjectLine(t *testing.T) {
	input := `{"a":"1"}
["not","an","object"]
`
	res := New(WithNDJSON()).ReadString(input)
	if !res.IsErr() {
		t.Fatal("expected error for non-object NDJSON line")
	}
	err := res.UnwrapErr().Error()
	if !strings.Contains(err, "expected JSON object") {
		t.Errorf("error should mention 'expected JSON object', got: %s", err)
	}
}

func TestArrayNonObjectElement(t *testing.T) {
	input := `[{"a":"1"}, "not an object"]`
	res := New().ReadString(input)
	if !res.IsErr() {
		t.Fatal("expected error for non-object element in array")
	}
	err := res.UnwrapErr().Error()
	if !strings.Contains(err, "expected JSON object") {
		t.Errorf("error should mention 'expected JSON object', got: %s", err)
	}
}

func TestNegativeMaxDepth(t *testing.T) {
	res := New(WithFlatten(), WithMaxDepth(-1)).ReadString(`[{"a":"1"}]`)
	if !res.IsErr() {
		t.Fatal("expected error for negative MaxDepth")
	}
}

func TestNegativeArrayIndexInMapping(t *testing.T) {
	res := New(WithFieldMapping(map[string]string{
		"x": ".items[-1]",
	})).ReadString(`[{"items":["a"]}]`)
	if !res.IsErr() {
		t.Fatal("expected error for negative array index")
	}
}

func TestMappingPathTrailingDot(t *testing.T) {
	res := New(WithFieldMapping(map[string]string{
		"x": ".a.",
	})).ReadString(`[{"a":"1"}]`)
	// Trailing dot should be handled (either error or empty key).
	// The parser produces an empty key segment at the end.
	_ = res // should not panic
}

func TestMappingPathEmptyBrackets(t *testing.T) {
	res := New(WithFieldMapping(map[string]string{
		"x": ".a[]",
	})).ReadString(`[{"a":["1"]}]`)
	if !res.IsErr() {
		t.Fatal("expected error for empty brackets in path")
	}
}

func TestMappingPathThroughNull(t *testing.T) {
	input := `[{"user": null}]`
	res := New(WithFieldMapping(map[string]string{
		"name": ".user.name",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	got := res.Unwrap().Rows[0].Get("name").UnwrapOr("FAIL")
	if got != "" {
		t.Errorf("path through null should give empty string, got %q", got)
	}
}

// --- Unicode and special characters ---

func TestUnicodeKeysAndValues(t *testing.T) {
	input := `[{"名前":"太郎","Stadt":"München","emoji":"🎉"}]`
	res := New(WithSortedHeaders()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := tbl.Rows[0].Get("名前").UnwrapOr(""); got != "太郎" {
		t.Errorf("名前 = %q, want %q", got, "太郎")
	}
	if got := tbl.Rows[0].Get("Stadt").UnwrapOr(""); got != "München" {
		t.Errorf("Stadt = %q, want %q", got, "München")
	}
	if got := tbl.Rows[0].Get("emoji").UnwrapOr(""); got != "🎉" {
		t.Errorf("emoji = %q, want %q", got, "🎉")
	}
}

func TestKeysWithSpecialChars(t *testing.T) {
	input := `[{"key with spaces":"1","key.with.dots":"2","key[0]":"3"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := tbl.Rows[0].Get("key with spaces").UnwrapOr(""); got != "1" {
		t.Errorf("got %q, want %q", got, "1")
	}
	if got := tbl.Rows[0].Get("key.with.dots").UnwrapOr(""); got != "2" {
		t.Errorf("got %q, want %q", got, "2")
	}
}

// --- Duplicate keys ---

func TestDuplicateKeysLastWins(t *testing.T) {
	// JSON spec: duplicate keys are undefined behavior; Go uses last value.
	input := `[{"a":"first","a":"second"}]`
	res := New().ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	got := res.Unwrap().Rows[0].Get("a").UnwrapOr("")
	if got != "second" {
		t.Errorf("duplicate key should use last value, got %q", got)
	}
}

// --- NDJSON edge cases ---

func TestNDJSONEmptyInput(t *testing.T) {
	res := New(WithNDJSON()).ReadString(``)
	if res.IsErr() {
		t.Fatal("expected Ok for empty NDJSON, got:", res.UnwrapErr())
	}
	if got := len(res.Unwrap().Rows); got != 0 {
		t.Errorf("rows: got %d, want 0", got)
	}
}

func TestNDJSONWithFlatten(t *testing.T) {
	input := "{\"a\":{\"b\":\"1\"}}\n{\"a\":{\"b\":\"2\"}}\n"
	res := New(WithNDJSON(), WithFlatten()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := len(tbl.Rows); got != 2 {
		t.Fatalf("rows: got %d, want 2", got)
	}
	if got := tbl.Rows[0].Get("a.b").UnwrapOr(""); got != "1" {
		t.Errorf("row[0] a.b = %q, want %q", got, "1")
	}
}

func TestNDJSONWithFieldMapping(t *testing.T) {
	input := "{\"user\":{\"name\":\"Alice\"}}\n{\"user\":{\"name\":\"Bob\"}}\n"
	res := New(WithNDJSON(), WithFieldMapping(map[string]string{
		"name": ".user.name",
	})).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()
	if got := tbl.Rows[1].Get("name").UnwrapOr(""); got != "Bob" {
		t.Errorf("row[1].name = %q, want %q", got, "Bob")
	}
}

// --- Flatten with mixed array types ---

func TestFlattenMixedArrayTypes(t *testing.T) {
	// Array with mixed types: string, number, object, null.
	input := `[{"items":["text", 42, {"nested":"val"}, null]}]`
	res := New(WithFlatten()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	tests := map[string]string{
		"items.0":          "text",
		"items.1":          "42",
		"items.2.nested":   "val",
		"items.3":          "",
	}
	for col, want := range tests {
		got := tbl.Rows[0].Get(col).UnwrapOr("MISSING")
		if got != want {
			t.Errorf("col %q = %q, want %q", col, got, want)
		}
	}
}

func TestFlattenNullArrayElement(t *testing.T) {
	input := `[{"items":[null, {"id":"1"}]}]`
	res := New(WithFlatten()).ReadString(input)
	if res.IsErr() {
		t.Fatal(res.UnwrapErr())
	}
	tbl := res.Unwrap()

	if got := tbl.Rows[0].Get("items.0").UnwrapOr("MISSING"); got != "" {
		t.Errorf("null element should be empty, got %q", got)
	}
	if got := tbl.Rows[0].Get("items.1.id").UnwrapOr("MISSING"); got != "1" {
		t.Errorf("items.1.id = %q, want %q", got, "1")
	}
}

// --- Writer edge cases ---

func TestWriteUnicode(t *testing.T) {
	tbl := newTable([]string{"名前"}, [][]string{{"太郎"}})
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(buf.String())
	if !strings.Contains(got, "太郎") {
		t.Errorf("output should contain unicode value, got: %s", got)
	}
}

func TestWriteSpecialCharsInValues(t *testing.T) {
	tbl := newTable([]string{"v"}, [][]string{{`he said "hello" & <bye>`}})
	var buf bytes.Buffer
	if err := NewWriter().Write(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	// Read back to verify round-trip.
	tbl2 := New(WithSortedHeaders()).ReadString(strings.TrimSpace(buf.String())).Unwrap()
	got := tbl2.Rows[0].Get("v").UnwrapOr("")
	if got != `he said "hello" & <bye>` {
		t.Errorf("round-trip failed for special chars: got %q", got)
	}
}

func TestWriteFileError(t *testing.T) {
	tbl := newTable([]string{"x"}, [][]string{{"1"}})
	err := NewWriter().WriteFile("/nonexistent/dir/file.json", tbl)
	if err == nil {
		t.Fatal("expected error for writing to nonexistent directory")
	}
}
