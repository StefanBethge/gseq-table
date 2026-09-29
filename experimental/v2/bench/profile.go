package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sync"
	"time"
)

// profiles are the pprof profiles of one case (#66): the heap at the peak
// of the live heap, all allocations, and the CPU.
type profiles struct {
	heap, allocs, cpu string

	stop chan struct{}
	done sync.WaitGroup
	peak uint64
}

func profileFlags(fs *flag.FlagSet) *profiles {
	p := &profiles{}
	fs.StringVar(&p.heap, "heapprofile", "", "write the heap profile at the peak of the live heap to this file")
	fs.StringVar(&p.allocs, "allocprofile", "", "write the profile of all allocations to this file")
	fs.StringVar(&p.cpu, "cpuprofile", "", "write the CPU profile to this file")
	return p
}

// start starts the CPU profile and the sampling of the live heap.
func (p *profiles) start() error {
	if p.cpu != "" {
		f, err := os.Create(p.cpu)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
	}
	if p.heap != "" {
		p.stop = make(chan struct{})
		p.done.Add(1)
		go p.sample()
	}
	return nil
}

// sample writes the heap profile whenever the live heap reaches a new
// peak, checked every 200 ms.
func (p *profiles) sample() {
	defer p.done.Done()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		metrics.Read(s)
		if live := s[0].Value.Uint64(); live > p.peak+p.peak/20 {
			p.peak = live
			p.write(p.heap, "heap")
		}
	}
}

func (p *profiles) write(path, name string) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	pprof.Lookup(name).WriteTo(f, 0)
}

// finish stops the profiles and writes the allocations.
func (p *profiles) finish() {
	if p.cpu != "" {
		pprof.StopCPUProfile()
	}
	if p.stop != nil {
		close(p.stop)
		p.done.Wait()
	}
	if p.allocs != "" {
		runtime.GC()
		p.write(p.allocs, "allocs")
	}
}
