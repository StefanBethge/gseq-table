package json

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

// BenchmarkStreamMemory compares the peak live heap of Read (all records and
// the whole table in memory) against Stream and ReadStream on the same NDJSON
// input. The custom peak-heap-B metric is the maximum HeapAlloc above the
// pre-read baseline, sampled every 1000 rows.
func BenchmarkStreamMemory(b *testing.B) {
	const rows = 50_000
	data := buildNDJSON(rows, 5)
	r := New(WithNDJSON())

	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		var peak uint64
		for b.Loop() {
			p := newHeapProbe()
			t := r.ReadString(data).Unwrap()
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
			for _, err := range r.Stream(strings.NewReader(data)) {
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
			for _, err := range r.ReadStream(strings.NewReader(data), 1000) {
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

// BenchmarkStream measures throughput of Read, Stream and ReadStream on the
// same NDJSON input.
func BenchmarkStream(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"1k", 1_000},
		{"10k", 10_000},
	}
	r := New(WithNDJSON())
	for _, sz := range sizes {
		data := buildNDJSON(sz.n, 5)
		b.Run("Read/"+sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if res := r.ReadString(data); res.IsErr() {
					b.Fatal(res.UnwrapErr())
				}
			}
		})
		b.Run("Stream/"+sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, err := range r.Stream(strings.NewReader(data)) {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run("ReadStream/"+sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, err := range r.ReadStream(strings.NewReader(data), 1000) {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
