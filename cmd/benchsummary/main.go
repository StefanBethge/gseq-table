// Command benchsummary reads Go benchmark output from stdin (or files),
// groups results by benchmark name, averages ns/op / B/op / allocs/op across
// multiple -count runs, and prints a sorted summary table.
//
// Usage:
//
//	go test -bench=. -benchmem -count=5 ./... | go run ./cmd/benchsummary
//	go run ./cmd/benchsummary bench_latest.txt
package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// benchLine holds the parsed fields of one Go benchmark result line.
type benchLine struct {
	pkg     string
	name    string
	n       int     // iterations (b.N)
	nsOp    float64 // ns/op
	bOp     float64 // B/op  (0 if not reported)
	allocs  float64 // allocs/op (0 if not reported)
}

// key groups lines that belong to the same benchmark.
type key struct{ pkg, name string }

// stats accumulates values for averaging.
type stats struct {
	count  int
	nsOp   float64
	bOp    float64
	allocs float64
}

func (s *stats) add(l benchLine) {
	s.count++
	s.nsOp += l.nsOp
	s.bOp += l.bOp
	s.allocs += l.allocs
}

func (s stats) avg() (nsOp, bOp, allocs float64) {
	if s.count == 0 {
		return
	}
	return s.nsOp / float64(s.count),
		s.bOp / float64(s.count),
		s.allocs / float64(s.count)
}

// fmtNs formats a nanosecond value as a human-readable string.
func fmtNs(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2f s", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2f ms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2f µs", ns/1e3)
	default:
		return fmt.Sprintf("%.0f ns", ns)
	}
}

// fmtBytes formats a byte value as a human-readable string.
func fmtBytes(b float64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

// benchRe matches a Go benchmark result line.
// Format: BenchmarkName-N   <iterations>   <ns/op> ns/op   [<B/op> B/op   <allocs> allocs/op]
var benchRe = regexp.MustCompile(`^(Benchmark\S+?)-\d+\s+(\d+)\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+B/op\s+([\d.]+)\s+allocs/op)?`)

// pkgRe matches the "pkg:" line in benchmark output.
var pkgRe = regexp.MustCompile(`^pkg:\s+(\S+)`)

func parseLine(line, currentPkg string) (benchLine, bool) {
	m := benchRe.FindStringSubmatch(line)
	if m == nil {
		return benchLine{}, false
	}
	n, _ := strconv.Atoi(m[2])
	nsOp, _ := strconv.ParseFloat(m[3], 64)
	var bOp, allocs float64
	if m[4] != "" {
		bOp, _ = strconv.ParseFloat(m[4], 64)
		allocs, _ = strconv.ParseFloat(m[5], 64)
	}
	return benchLine{
		pkg:    currentPkg,
		name:   m[1],
		n:      n,
		nsOp:   nsOp,
		bOp:    bOp,
		allocs: allocs,
	}, true
}

func readLines(r io.Reader) ([]benchLine, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var lines []benchLine
	currentPkg := "(unknown)"
	for scanner.Scan() {
		text := scanner.Text()
		if m := pkgRe.FindStringSubmatch(text); m != nil {
			currentPkg = m[1]
			continue
		}
		if l, ok := parseLine(text, currentPkg); ok {
			lines = append(lines, l)
		}
	}
	if err := scanner.Err(); err != nil {
		return lines, err
	}
	return lines, nil
}

// splitSubtest splits a benchmark name like "BenchmarkFoo/10k" into
// base "BenchmarkFoo" and sub "10k".
func splitSubtest(name string) (base, sub string) {
	idx := strings.LastIndex(name, "/")
	if idx == -1 {
		return name, ""
	}
	return name[:idx], name[idx+1:]
}

func main() {
	var readers []io.Reader
	var files []*os.File
	if len(os.Args) > 1 {
		for _, path := range os.Args[1:] {
			f, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "open %s: %v\n", path, err)
				os.Exit(1)
			}
			files = append(files, f)
			readers = append(readers, f)
		}
		defer func() {
			for _, f := range files {
				f.Close()
			}
		}()
	} else {
		readers = []io.Reader{os.Stdin}
	}

	var allLines []benchLine
	for _, r := range readers {
		lines, err := readLines(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read error: %v\n", err)
			os.Exit(1)
		}
		allLines = append(allLines, lines...)
	}

	if len(allLines) == 0 {
		fmt.Fprintln(os.Stderr, "no benchmark lines found")
		os.Exit(1)
	}

	// Group by (pkg, baseName, subName) and average across -count runs.
	type groupKey struct{ pkg, base, sub string }
	groups := make(map[groupKey]*stats)
	var order []groupKey

	for _, l := range allLines {
		base, sub := splitSubtest(l.name)
		k := groupKey{l.pkg, base, sub}
		if _, ok := groups[k]; !ok {
			groups[k] = &stats{}
			order = append(order, k)
		}
		groups[k].add(l)
	}

	// Sort: package → base name → sub name.
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if a.pkg != b.pkg {
			return a.pkg < b.pkg
		}
		if a.base != b.base {
			return a.base < b.base
		}
		return naturalLess(a.sub, b.sub)
	})

	// Identify which bases have sub-benchmarks so we can print aggregated rows.
	type baseKey struct{ pkg, base string }
	baseGroups := make(map[baseKey][]groupKey)
	for _, k := range order {
		bk := baseKey{k.pkg, k.base}
		baseGroups[bk] = append(baseGroups[bk], k)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	currentPkg := ""
	printedBases := make(map[baseKey]bool)

	fmt.Fprintln(w, "Benchmark\tSub\tRuns\tns/op\tB/op\tallocs/op")
	fmt.Fprintln(w, strings.Repeat("-", 80))

	for _, k := range order {
		if k.pkg != currentPkg {
			if currentPkg != "" {
				fmt.Fprintln(w, "")
			}
			fmt.Fprintf(w, "── %s ──\n", k.pkg)
			currentPkg = k.pkg
		}

		s := groups[k]
		nsOp, bOp, allocs := s.avg()

		// Print individual line.
		sub := k.sub
		if sub == "" {
			sub = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%d×\t%s\t%s\t%.0f\n",
			shortName(k.base), sub, s.count,
			fmtNs(nsOp), fmtBytes(bOp), allocs)

		// After last sub of a multi-sub base, print the aggregate.
		bk := baseKey{k.pkg, k.base}
		if !printedBases[bk] && len(baseGroups[bk]) > 1 && k == baseGroups[bk][len(baseGroups[bk])-1] {
			printedBases[bk] = true
			var agg stats
			var minNs, maxNs float64 = math.MaxFloat64, 0
			for _, gk := range baseGroups[bk] {
				gs := groups[gk]
				ns, bo, al := gs.avg()
				agg.count += gs.count
				agg.nsOp += ns * float64(gs.count)
				agg.bOp += bo * float64(gs.count)
				agg.allocs += al * float64(gs.count)
				if ns < minNs {
					minNs = ns
				}
				if ns > maxNs {
					maxNs = ns
				}
			}
			total := float64(agg.count)
			avgNs := agg.nsOp / total
			avgBo := agg.bOp / total
			avgAl := agg.allocs / total
			ratio := maxNs / minNs
			fmt.Fprintf(w, "%s\t▶ avg\t-\t%s\t%s\t%.0f\t(×%.1f scaling)\n",
				shortName(k.base), fmtNs(avgNs), fmtBytes(avgBo), avgAl, ratio)
		}
	}

	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Total benchmark lines parsed: %d\n", len(allLines))
}

// shortName strips the "Benchmark" prefix.
func shortName(name string) string {
	return strings.TrimPrefix(name, "Benchmark")
}

// naturalLess sorts strings containing numbers in numeric order.
func naturalLess(a, b string) bool {
	for a != "" && b != "" {
		ai := leadingDigits(a)
		bi := leadingDigits(b)
		if ai > 0 && bi > 0 {
			na, _ := strconv.Atoi(a[:ai])
			nb, _ := strconv.Atoi(b[:bi])
			if na != nb {
				return na < nb
			}
			a, b = a[ai:], b[bi:]
			continue
		}
		if a[0] != b[0] {
			return a[0] < b[0]
		}
		a, b = a[1:], b[1:]
	}
	return len(a) < len(b)
}

func leadingDigits(s string) int {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return i
}
