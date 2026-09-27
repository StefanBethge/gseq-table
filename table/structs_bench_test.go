package table

import (
	"strconv"
	"testing"

	"github.com/stefanbethge/gseq/option"
)

type benchStruct struct {
	ID    int64                 `gseq:"id"`
	Name  string                `gseq:"name"`
	Price float64               `gseq:"price"`
	Qty   int                   `gseq:"qty"`
	Note  option.Option[string] `gseq:"note"`
}

func benchStructs(n int) []benchStruct {
	out := make([]benchStruct, n)
	for i := range out {
		out[i] = benchStruct{ID: int64(i), Name: "item" + strconv.Itoa(i), Price: float64(i) * 0.5, Qty: i % 100}
		if i%2 == 0 {
			out[i].Note = option.Some("n")
		}
	}
	return out
}

// ── FromStructs / ToStructs ──────────────────────────────────────────────────

func BenchmarkFromStructs(b *testing.B) {
	for _, sz := range benchSizes {
		items := benchStructs(sz.n)
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = FromStructs(items)
			}
		})
	}
}

func BenchmarkToStructs(b *testing.B) {
	for _, sz := range benchSizes {
		tb := FromStructs(benchStructs(sz.n))
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := tb.ToStructs[benchStruct](); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMutableToStructs(b *testing.B) {
	for _, sz := range benchSizes {
		m := FromStructs(benchStructs(sz.n)).Mutable()
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := m.ToStructs[benchStruct](); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
