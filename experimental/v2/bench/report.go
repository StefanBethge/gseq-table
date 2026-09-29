package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
)

// key identifies the repetitions of one measured configuration.
type key struct {
	label, impl, kase, kind string
	rows, block             int
	sink, managed           bool
	budget                  int64
}

// stat is the median of the repetitions of one configuration.
type stat struct {
	wall   float64
	rss    int64
	reps   int
	result string
	failed int
}

func median[T int64 | float64](v []T) T {
	s := slices.Clone(v)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func load(paths []string) (map[key]stat, error) {
	walls := map[key][]float64{}
	rsss := map[key][]int64{}
	out := map[key]stat{}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var m Measurement
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				f.Close()
				return nil, fmt.Errorf("%s: %w", p, err)
			}
			k := key{m.Label, m.Impl, m.Case, m.Kind, m.Rows, m.Block, m.Sink, m.Managed, m.Budget}
			s := out[k]
			if m.Exit != 0 {
				s.failed++
				out[k] = s
				continue
			}
			walls[k] = append(walls[k], m.Wall)
			rsss[k] = append(rsss[k], m.MaxRSS)
			s.result = m.Result
			out[k] = s
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	for k, s := range out {
		if len(walls[k]) > 0 {
			s.wall, s.rss, s.reps = median(walls[k]), median(rsss[k]), len(walls[k])
		}
		out[k] = s
	}
	return out, nil
}

func mib(b int64) string { return fmt.Sprintf("%.0f", float64(b)/(1<<20)) }

func rowsLabel(n int) string {
	if n%1000000 == 0 {
		return fmt.Sprintf("%dM", n/1000000)
	}
	return fmt.Sprint(n)
}

func cmdReport(args []string, w io.Writer) error {
	res, err := load(args)
	if err != nil {
		return err
	}
	var rows []int
	for k := range res {
		if k.label == "compare" && !slices.Contains(rows, k.rows) {
			rows = append(rows, k.rows)
		}
	}
	sort.Ints(rows)
	get := func(label, impl, kase, kind string, n int) stat {
		for k, s := range res {
			if k.label == label && k.impl == impl && k.kase == kase && k.kind == kind && k.rows == n && !k.sink {
				return s
			}
		}
		return stat{}
	}
	verdict := func(ok bool) string {
		if ok {
			return "ja"
		}
		return "**nein**"
	}
	// compare pairs a v2 variant against a v1 variant with the limits for
	// time and memory of D58; timeMax 0 means no criterion.
	compare := func(title, base, cand string, kinds []string, timeMax, rssMax float64) {
		fmt.Fprintf(w, "### %s\n\n", title)
		for _, kind := range kinds {
			for _, n := range rows {
				fmt.Fprintf(w, "%s, %s Zeilen:\n\n", map[string]string{"num": "Zahlenlastig", "text": "Textlastig"}[kind], rowsLabel(n))
				fmt.Fprintf(w, "| Fall | %s s | %s s | Zeit × | %s MiB | %s MiB | Speicher × | Zeit ok | Speicher ok |\n", base, cand, base, cand)
				fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---|---|")
				for _, kase := range caseNames {
					b, c := get("compare", base, kase, kind, n), get("compare", cand, kase, kind, n)
					if b.reps == 0 || c.reps == 0 {
						fmt.Fprintf(w, "| %s | – | – | – | – | – | – | – | – |\n", kase)
						continue
					}
					tr, mr := c.wall/b.wall, float64(c.rss)/float64(b.rss)
					tok, mok := "–", "–"
					if timeMax > 0 {
						tok, mok = verdict(tr <= timeMax), verdict(mr <= rssMax)
					}
					fmt.Fprintf(w, "| %s | %.2f | %.2f | %.2f | %s | %s | %.2f | %s | %s |\n",
						kase, b.wall, c.wall, tr, mib(b.rss), mib(c.rss), mr, tok, mok)
				}
				fmt.Fprintln(w)
			}
		}
	}
	if len(rows) > 0 {
		compare("G5: v2 automatisch mit Rohzustand gegen v1 MutableTable", "v1m", "v2", []string{"num", "text"}, 1.2, 1.0)
		compare("G13: v2 gegen v1 Table, zahlenlastig", "v1t", "v2", []string{"num"}, 1.0, 0.7)
		compare("G13: v2 gegen v1 Table, textlastig (ohne Kriterium)", "v1t", "v2", []string{"text"}, 0, 0)
		compare("G13: v2 ohne gegen mit Rohzustand", "v2", "v2noraw", []string{"num", "text"}, 0, 0)
	}

	var other []key
	for k := range res {
		if k.label != "compare" {
			other = append(other, k)
		}
	}
	sort.Slice(other, func(i, j int) bool {
		a, b := other[i], other[j]
		return fmt.Sprint(a.label, a.kind, a.kase, a.impl, a.rows, a.block, a.sink, a.budget, a.managed) <
			fmt.Sprint(b.label, b.kind, b.kase, b.impl, b.rows, b.block, b.sink, b.budget, b.managed)
	})
	last := ""
	for _, k := range other {
		if k.label != last {
			fmt.Fprintf(w, "\n### %s\n\n", k.label)
			fmt.Fprintln(w, "| Art | Fall | Impl | Zeilen | Block | Ziel | Budget MiB | GOMEMLIMIT | s | MiB | Wdh. | Fehlgeschlagen | Ergebnis |")
			fmt.Fprintln(w, "|---|---|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|")
			last = k.label
		}
		s := res[k]
		fmt.Fprintf(w, "| %s | %s | %s | %s | %d | %v | %s | %v | %.2f | %s | %d | %d | %s |\n", k.kind, k.kase, k.impl,
			rowsLabel(k.rows), k.block, k.sink, mib(k.budget), k.managed, s.wall, mib(s.rss), s.reps, s.failed,
			strings.ReplaceAll(s.result, "|", "/"))
	}
	return nil
}
