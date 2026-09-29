package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// cmdSuite runs a plan of measurements, one case after the other:
//
//	block    v2 on 10M numeric-heavy rows with several block lengths (G13)
//	compare  v1 Table, v1 MutableTable, v2 with and without raw state, on
//	         numeric- and text-heavy deliveries with 1M and 10M rows (G5, G13)
//	sink     v2 with the result to a sink, with and without raw state (G13)
//	spill    sort and group by under a cap of the run, with and without raw
//	         state (G13)
//	excel    reading an Excel delivery (G13)
func cmdSuite(args []string) error {
	fs := flag.NewFlagSet("suite", flag.ExitOnError)
	dir := fs.String("dir", "", "directory of the deliveries")
	plan := fs.String("plan", "compare", "block, compare, sink, spill or excel")
	out := fs.String("out", "results.jsonl", "JSONL file to append to")
	reps := fs.Int("reps", 3, "repetitions")
	sizes := fs.String("rows", "1000000,10000000", "row counts")
	block := fs.Int("block", 0, "v2: rows per block (0: the default of the engine)")
	budget := fs.Int64("budget", 256<<20, "spill: cap of the run in bytes")
	spill := fs.String("spill", "", "v2: spill directory")
	fs.Parse(args)

	var rows []int
	for _, s := range strings.Split(*sizes, ",") {
		n, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		rows = append(rows, n)
	}
	base := runConfig{dir: *dir, block: *block, spill: *spill}
	var plans []runConfig
	add := func(c runConfig) { plans = append(plans, c) }
	for _, kind := range []string{"num", "text"} {
		for _, n := range rows {
			if err := generate(*dir, kind, n); err != nil {
				return err
			}
		}
	}
	switch *plan {
	case "block":
		for _, kase := range []string{"read", "filter", "cast", "sort", "groupby"} {
			for _, b := range []int{1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18} {
				c := base
				c.impl, c.kase, c.kind, c.rows, c.block = "v2", kase, "num", rows[len(rows)-1], b
				add(c)
			}
		}
	case "compare":
		for _, n := range rows {
			for _, kind := range []string{"num", "text"} {
				for _, kase := range caseNames {
					for _, impl := range []string{"v1t", "v1m", "v2", "v2noraw"} {
						c := base
						c.impl, c.kase, c.kind, c.rows = impl, kase, kind, n
						add(c)
					}
				}
			}
		}
	case "sink":
		for _, kind := range []string{"num", "text"} {
			for _, kase := range caseNames {
				for _, impl := range []string{"v2", "v2noraw"} {
					c := base
					c.impl, c.kase, c.kind, c.rows, c.sink = impl, kase, kind, rows[len(rows)-1], true
					add(c)
				}
			}
		}
	case "spill":
		for _, kind := range []string{"num", "text"} {
			for _, kase := range []string{"sort", "groupby"} {
				for _, impl := range []string{"v2", "v2noraw"} {
					for _, sink := range []bool{false, true} {
						c := base
						c.impl, c.kase, c.kind, c.rows, c.sink, c.budget = impl, kase, kind, rows[len(rows)-1], sink, *budget
						add(c)
					}
				}
			}
		}
	case "excel":
		for _, n := range rows {
			if err := generateExcel(*dir, n); err != nil {
				return err
			}
			for _, sink := range []bool{false, true} {
				c := base
				c.impl, c.kase, c.kind, c.rows, c.sink = "v2", "excel", "num", n, sink
				add(c)
			}
		}
	default:
		return fmt.Errorf("unknown plan %q", *plan)
	}
	fmt.Fprintf(os.Stderr, "plan %s: %d cases, %d repetitions each\n", *plan, len(plans), *reps)
	for _, c := range plans {
		if err := measure(c, *plan, *reps, *out); err != nil {
			return err
		}
	}
	return nil
}
