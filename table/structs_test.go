package table

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stefanbethge/gseq/option"
)

type structPerson struct {
	Name   string                `gseq:"name"`
	Age    int                   `gseq:"age"`
	Score  float64               `gseq:"score"`
	Active bool                  `gseq:"active"`
	Born   time.Time             `gseq:"born"`
	Email  option.Option[string] `gseq:"email"`
	Rank   *int                  `gseq:"rank"`
	Note   string                `gseq:",omitempty"`
	Temp   string                `gseq:"-"`
	secret string
}

type structStatus string

type structCode uint16

type structBase struct {
	ID    int64 `gseq:"id"`
	Label string
}

type structEmbedded struct {
	structBase
	Status structStatus `gseq:"status"`
	Code   structCode   `gseq:"code"`
	Level  *structCode  `gseq:"level"`
}

type structAllTypes struct {
	I   int
	I8  int8
	I16 int16
	I32 int32
	I64 int64
	U   uint
	U8  uint8
	U16 uint16
	U32 uint32
	U64 uint64
	F32 float32
	F64 float64
	S   string
	B   bool
	T   time.Time
	OI  option.Option[int]
	OF  option.Option[float32]
	OT  option.Option[time.Time]
	PS  *string
	PB  *bool
}

func intPtr(n int) *int { return &n }

func TestFromStructs(t *testing.T) {
	people := []structPerson{
		{Name: "Alice", Age: 30, Score: 1.5, Active: true, Born: time.Date(1994, 5, 1, 0, 0, 0, 0, time.UTC),
			Email: option.Some("a@x.io"), Rank: intPtr(1), Note: "vip", Temp: "x", secret: "s"},
		{Name: "Bob", Age: 0, Score: 0.25, Born: time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)},
	}
	tb := FromStructs(people)
	assertEqual(t, tb.HasErrs(), false)
	assertStrings(t, []string(tb.Headers), []string{"name", "age", "score", "active", "born", "email", "rank", "Note"})
	assertStrings(t, []string(tb.Rows[0].Values()), []string{"Alice", "30", "1.5", "true", "1994-05-01", "a@x.io", "1", "vip"})
	assertStrings(t, []string(tb.Rows[1].Values()), []string{"Bob", "0", "0.25", "false", "2000-01-02T03:04:05Z", "", "", ""})
}

func TestFromStructs_Pointers(t *testing.T) {
	tb := FromStructs([]*structPerson{{Name: "Alice", Age: 3}, nil})
	assertEqual(t, tb.HasErrs(), false)
	assertEqual(t, len(tb.Rows), 2)
	assertEqual(t, tb.Rows[0].Get("age").UnwrapOr(""), "3")
	assertStrings(t, []string(tb.Rows[1].Values()), []string{"", "", "", "", "", "", "", ""})
}

func TestFromStructs_EmptyKeepsHeaders(t *testing.T) {
	tb := FromStructs[structPerson](nil)
	assertEqual(t, len(tb.Rows), 0)
	assertEqual(t, len(tb.Headers), 8)
}

func TestFromStructs_EmbeddedAndNamed(t *testing.T) {
	lvl := structCode(7)
	tb := FromStructs([]structEmbedded{
		{structBase: structBase{ID: 9, Label: "x"}, Status: "open", Code: 404, Level: &lvl},
	})
	assertStrings(t, []string(tb.Headers), []string{"id", "Label", "status", "code", "level"})
	assertStrings(t, []string(tb.Rows[0].Values()), []string{"9", "x", "open", "404", "7"})
}

func TestStructs_RoundTripAllTypes(t *testing.T) {
	s := "hi"
	b := false
	in := []structAllTypes{
		{I: -1, I8: -8, I16: -16, I32: -32, I64: -64, U: 1, U8: 8, U16: 16, U32: 32, U64: 64,
			F32: 1.25, F64: 0.1, S: " padded ", B: true, T: time.Date(2024, 3, 1, 10, 30, 0, 0, time.UTC),
			OI: option.Some(5), OF: option.Some[float32](2.5), OT: option.Some(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			PS: &s, PB: &b},
		{T: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
	}
	tb := FromStructs(in)
	assertEqual(t, tb.HasErrs(), false)
	out, err := tb.ToStructs[structAllTypes]()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", out, in)
	}

	mout, err := tb.Mutable().ToStructs[structAllTypes]()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mout, in) {
		t.Fatalf("mutable round trip mismatch:\n got  %+v\n want %+v", mout, in)
	}
}

func TestToStructs(t *testing.T) {
	tb := New(
		[]string{"name", "age", "score", "active", "born", "email", "rank", "extra"},
		[][]string{
			{"Alice", " 30 ", "1.5", "yes", "01.05.1994", "a@x.io", "2", "ignored"},
			{"Bob", "25", "2", "0", "2000-01-02", "", " ", "ignored"},
		},
	)
	got, err := tb.ToStructs[structPerson]()
	if err != nil {
		t.Fatal(err)
	}
	want := []structPerson{
		{Name: "Alice", Age: 30, Score: 1.5, Active: true, Born: time.Date(1994, 5, 1, 0, 0, 0, 0, time.UTC),
			Email: option.Some("a@x.io"), Rank: intPtr(2)},
		{Name: "Bob", Age: 25, Score: 2, Born: time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
			Email: option.None[string]()},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestToStructs_EmbeddedAndNamed(t *testing.T) {
	tb := New(
		[]string{"id", "Label", "status", "code", "level"},
		[][]string{{"9", "x", "open", "404", ""}, {"1", "", "done", "1", "3"}},
	)
	got, err := tb.ToStructs[structEmbedded]()
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got[0].ID, int64(9))
	assertEqual(t, got[0].Label, "x")
	assertEqual(t, got[0].Status, structStatus("open"))
	assertEqual(t, got[0].Code, structCode(404))
	assertEqual(t, got[0].Level == nil, true)
	assertEqual(t, *got[1].Level, structCode(3))
}

func TestToStructs_OmitEmpty(t *testing.T) {
	type rec struct {
		ID  int    `gseq:"id"`
		Qty int    `gseq:"qty,omitempty"`
		Tag string `gseq:"tag,omitempty"`
	}
	// missing column with omitempty is fine
	got, err := New([]string{"id"}, [][]string{{"1"}}).ToStructs[rec]()
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got[0], rec{ID: 1})

	// empty cell with omitempty keeps the zero value
	got, err = New([]string{"id", "qty"}, [][]string{{"1", " "}, {"2", "5"}}).ToStructs[rec]()
	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, got[0].Qty, 0)
	assertEqual(t, got[1].Qty, 5)

	// omitempty writes the zero value as an empty cell
	tb := FromStructs([]rec{{ID: 0, Qty: 0}, {ID: 1, Qty: 2, Tag: "t"}})
	assertStrings(t, []string(tb.Rows[0].Values()), []string{"0", "", ""})
	assertStrings(t, []string(tb.Rows[1].Values()), []string{"1", "2", "t"})
}

func TestToStructs_Errors(t *testing.T) {
	type rec struct {
		ID  int     `gseq:"id"`
		Val float64 `gseq:"val"`
	}
	cases := []struct {
		name string
		tb   Table
		want string
	}{
		{"unknown column", New([]string{"id"}, [][]string{{"1"}}), `ToStructs: unknown column "val"`},
		{"bad cell", New([]string{"id", "val"}, [][]string{{"1", "2"}, {"2", "abc"}}),
			`ToStructs: column "val" row 1: cannot parse "abc" as float64`},
		{"empty cell", New([]string{"id", "val"}, [][]string{{"", "2"}}),
			`ToStructs: column "id" row 0: cannot parse "" as int`},
		{"with source", New([]string{"id"}, nil).WithSource("in.csv"), `[in.csv] ToStructs: unknown column "val"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.tb.ToStructs[rec]()
			if err == nil {
				t.Fatal("expected error")
			}
			assertEqual(t, err.Error(), tc.want)
			assertEqual(t, got == nil, true)

			_, err = tc.tb.Mutable().ToStructs[rec]()
			if err == nil {
				t.Fatal("expected mutable error")
			}
			assertEqual(t, err.Error(), tc.want)
		})
	}
}

func TestToStructs_OverflowAndBadOption(t *testing.T) {
	type rec struct {
		N int8               `gseq:"n"`
		O option.Option[int] `gseq:"o"`
		P *bool              `gseq:"p"`
	}
	tb := New([]string{"n", "o", "p"}, [][]string{{"300", "", ""}})
	_, err := tb.ToStructs[rec]()
	if err == nil || !strings.Contains(err.Error(), `column "n" row 0`) {
		t.Fatalf("want overflow error, got %v", err)
	}
	_, err = New([]string{"n", "o", "p"}, [][]string{{"1", "x", ""}}).ToStructs[rec]()
	if err == nil || !strings.Contains(err.Error(), `column "o" row 0`) {
		t.Fatalf("want option error, got %v", err)
	}
	_, err = New([]string{"n", "o", "p"}, [][]string{{"1", "", "maybe"}}).ToStructs[rec]()
	if err == nil || !strings.Contains(err.Error(), `column "p" row 0`) {
		t.Fatalf("want pointer error, got %v", err)
	}
}

func TestToStructs_UnsupportedTypes(t *testing.T) {
	type badField struct {
		M map[string]int
	}
	type dup struct {
		A string `gseq:"x"`
		B string `gseq:"x"`
	}
	_, err := New(nil, nil).ToStructs[badField]()
	assertEqual(t, err.Error(), "ToStructs: field M: unsupported type map[string]int")
	_, err = New(nil, nil).ToStructs[dup]()
	assertEqual(t, err.Error(), `ToStructs: field B: duplicate column "x"`)
	_, err = New(nil, nil).ToStructs[int]()
	assertEqual(t, err.Error(), "ToStructs: int is not a struct type")
	_, err = New(nil, nil).ToStructs[*structPerson]()
	assertEqual(t, err.Error(), "ToStructs: *table.structPerson is not a struct type")
}

func TestToStructs_TagOptions(t *testing.T) {
	type rec struct {
		A string `gseq:"-,"`
		B string `gseq:"b, omitempty"`
		C string `gseq:""`
	}
	tb := FromStructs([]rec{{A: "a", C: "c"}})
	assertStrings(t, []string(tb.Headers), []string{"-", "b", "C"})
	assertStrings(t, []string(tb.Rows[0].Values()), []string{"a", "", "c"})
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
