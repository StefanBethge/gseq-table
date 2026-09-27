package table

import "testing"

// Iterator benchmarks. Each op has a "slice" sub-benchmark with the
// equivalent hand-written loop over Rows / Col / ColAs as the baseline.

var iterSinkInt int

func BenchmarkIterRows(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name+"/slice", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for i, r := range tb.Rows {
					n += i + len(r.values)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/All", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for i, r := range tb.All() {
					n += i + len(r.values)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/RowsSeq", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for r := range tb.RowsSeq() {
					n += len(r.values)
				}
				iterSinkInt = n
			}
		})
	}
}

func BenchmarkIterCol(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name+"/Col", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for _, v := range tb.Col("city") {
					n += len(v)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/ColSeq", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for v := range tb.ColSeq("city") {
					n += len(v)
				}
				iterSinkInt = n
			}
		})
	}
}

func BenchmarkIterColAs(b *testing.B) {
	for _, sz := range benchSizes {
		tb := benchTable(sz.n)
		b.Run(sz.name+"/ColAs", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				vals, _ := tb.ColAs[int]("revenue")
				n := 0
				for _, v := range vals {
					n += v
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/ColSeqAs", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for _, v := range tb.ColSeqAs[int]("revenue") {
					n += v
				}
				iterSinkInt = n
			}
		})
	}
}

func BenchmarkMutableIterRows(b *testing.B) {
	for _, sz := range benchSizes {
		m := benchMutableView(sz.n)
		b.Run(sz.name+"/slice", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for i := range m.Len() {
					r, _ := m.Row(i)
					n += i + len(r.values)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/All", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for i, r := range m.All() {
					n += i + len(r.values)
				}
				iterSinkInt = n
			}
		})
	}
}

func BenchmarkMutableIterCol(b *testing.B) {
	for _, sz := range benchSizes {
		m := benchMutableView(sz.n)
		b.Run(sz.name+"/Col", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for _, v := range m.Col("city") {
					n += len(v)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/ColSeq", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for v := range m.ColSeq("city") {
					n += len(v)
				}
				iterSinkInt = n
			}
		})
		b.Run(sz.name+"/ColSeqAs", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for _, v := range m.ColSeqAs[int]("revenue") {
					n += v
				}
				iterSinkInt = n
			}
		})
	}
}
