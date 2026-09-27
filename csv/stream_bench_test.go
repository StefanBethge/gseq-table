package csv

import (
	"runtime"
	"strings"
	"testing"
)

// heapProbe tracks the peak live heap above a baseline taken after a GC.
type heapProbe struct {
	base, peak uint64
	ms         runtime.MemStats
}

func newHeapProbe() *heapProbe {
	p := &heapProbe{}
	runtime.GC()
	runtime.ReadMemStats(&p.ms)
	p.base = p.ms.HeapAlloc
	return p
}

func (p *heapProbe) sample() {
	runtime.ReadMemStats(&p.ms)
	if p.ms.HeapAlloc > p.base && p.ms.HeapAlloc-p.base > p.peak {
		p.peak = p.ms.HeapAlloc - p.base
	}
}

// BenchmarkStreamMemory compares the peak live heap of Read (whole table in
// memory) against Stream and ReadStream (one row / one chunk at a time) on
// the same input. The custom peak-heap-B metric is the maximum HeapAlloc
// above the pre-read baseline, sampled every 1000 rows.
func BenchmarkStreamMemory(b *testing.B) {
	const rows = 100_000
	data := buildCSV(rows, 5, ',')

	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		var peak uint64
		for b.Loop() {
			p := newHeapProbe()
			t := New().Read(strings.NewReader(data)).Unwrap()
			p.sample()
			runtime.KeepAlive(t)
			peak = max(peak, p.peak)
		}
		b.ReportMetric(float64(peak), "peak-heap-B")
	})

	b.Run("Stream", func(b *testing.B) {
		b.ReportAllocs()
		var peak uint64
		for b.Loop() {
			p := newHeapProbe()
			n := 0
			for _, err := range New().Stream(strings.NewReader(data)) {
				if err != nil {
					b.Fatal(err)
				}
				if n++; n%1000 == 0 {
					p.sample()
				}
			}
			peak = max(peak, p.peak)
		}
		b.ReportMetric(float64(peak), "peak-heap-B")
	})

	b.Run("ReadStream_chunk1k", func(b *testing.B) {
		b.ReportAllocs()
		var peak uint64
		for b.Loop() {
			p := newHeapProbe()
			for _, err := range New().ReadStream(strings.NewReader(data), 1000) {
				if err != nil {
					b.Fatal(err)
				}
				p.sample()
			}
			peak = max(peak, p.peak)
		}
		b.ReportMetric(float64(peak), "peak-heap-B")
	})
}

// BenchmarkStream measures row-by-row throughput for the standard sizes.
func BenchmarkStream(b *testing.B) {
	for _, sz := range csvBenchSizes {
		data := buildCSV(sz.n, 5, ',')
		b.Run(sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, err := range New().Stream(strings.NewReader(data)) {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
