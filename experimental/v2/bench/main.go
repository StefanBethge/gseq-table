// Command bench measures v2 against v1 and under memory limits for the
// gaps G5 and G13 and the test case T23 (design decisions D58, D60, D64,
// D65). RESULTS.md records the results and how they were taken.
//
// Every measurement runs one case in a child process of its own, so that
// the peak resident memory of the child (maxrss) is the peak of that case
// alone:
//
//	bench gen -dir /tmp/bench -kind num -rows 1000000
//	bench measure -dir /tmp/bench -impl v2 -case sort -kind num -rows 1000000 -out r.jsonl
//	bench suite -dir /tmp/bench -plan g5 -out r.jsonl
//	bench exec -label onebrc-1g -out r.jsonl -- ./onebrc_budget -data measurements.txt
//	bench report r.jsonl
//
// The deliveries are generated (gen.go); the 1BRC file is passed with -data
// and is never part of the repository.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/memlimit"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bench gen|run|measure|suite|exec|report ...")
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "gen":
		err = cmdGen(args)
	case "run":
		err = cmdRun(args)
	case "measure":
		err = cmdMeasure(args)
	case "suite":
		err = cmdSuite(args)
	case "exec":
		err = cmdExec(args)
	case "report":
		err = cmdReport(args, os.Stdout)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cmdGen(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	dir := fs.String("dir", "", "directory of the deliveries")
	kind := fs.String("kind", "num", "num, text or excel (numeric-heavy as Excel)")
	rows := fs.Int("rows", 1000000, "rows")
	fs.Parse(args)
	if *kind == "excel" {
		return generateExcel(*dir, *rows)
	}
	return generate(*dir, *kind, *rows)
}

// runFlags declares the flags of one case on fs.
func runFlags(fs *flag.FlagSet) *runConfig {
	c := &runConfig{}
	fs.StringVar(&c.impl, "impl", "v2", "v1t, v1m, v2 or v2noraw")
	fs.StringVar(&c.kase, "case", "read", "read, filter, cast, derive, sort, groupby, join, excel or onebrc")
	fs.StringVar(&c.kind, "kind", "num", "num or text")
	fs.IntVar(&c.rows, "rows", 1000000, "rows of the generated delivery")
	fs.StringVar(&c.dir, "dir", "", "directory of the generated deliveries")
	fs.StringVar(&c.data, "data", "", "path of the delivery (default: the generated one)")
	fs.IntVar(&c.block, "block", 0, "v2: rows per block (0: the default of the engine)")
	fs.BoolVar(&c.sink, "sink", false, "v2: write the result to a sink that discards it")
	fs.Int64Var(&c.budget, "budget", 0, "v2: memory cap of the run in bytes")
	fs.BoolVar(&c.managed, "managed", false, "v2: let the engine set GOMEMLIMIT")
	fs.StringVar(&c.spill, "spill", "", "v2: spill directory")
	return c
}

func (c runConfig) args() []string {
	a := []string{"-impl", c.impl, "-case", c.kase, "-kind", c.kind, "-rows", strconv.Itoa(c.rows),
		"-dir", c.dir, "-block", strconv.Itoa(c.block), "-budget", strconv.FormatInt(c.budget, 10)}
	if c.data != "" {
		a = append(a, "-data", c.data)
	}
	if c.sink {
		a = append(a, "-sink")
	}
	if c.managed {
		a = append(a, "-managed")
	}
	if c.spill != "" {
		a = append(a, "-spill", c.spill)
	}
	return a
}

// cmdRun is the child: it runs one case and prints the result rows.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	c := runFlags(fs)
	prof := profileFlags(fs)
	fs.Parse(args)
	if err := prof.start(); err != nil {
		return err
	}
	n, err := runCase(context.Background(), *c)
	prof.finish()
	if err != nil {
		return err
	}
	fmt.Println(n)
	return nil
}

// Measurement is one measured child process, a line of the JSONL output.
type Measurement struct {
	Label   string  `json:"label,omitempty"`
	Impl    string  `json:"impl,omitempty"`
	Case    string  `json:"case,omitempty"`
	Kind    string  `json:"kind,omitempty"`
	Rows    int     `json:"rows,omitempty"`
	Block   int     `json:"block,omitempty"`
	Sink    bool    `json:"sink,omitempty"`
	Budget  int64   `json:"budget,omitempty"`
	Managed bool    `json:"managed,omitempty"`
	Rep     int     `json:"rep"`
	Wall    float64 `json:"wall_s"`
	MaxRSS  int64   `json:"maxrss"`
	Result  string  `json:"result,omitempty"`
	Exit    int     `json:"exit"`
	Limit   int64   `json:"limit"` // detected memory limit of the measuring process
	GOOS    string  `json:"goos"`
	Env     string  `json:"env,omitempty"`
}

// measureCmd runs cmd and returns its wall time, peak resident memory and
// exit code; the output of cmd goes to out.
func measureCmd(cmd *exec.Cmd, out io.Writer) (Measurement, error) {
	var m Measurement
	var stdout strings.Builder
	cmd.Stdout = io.MultiWriter(&stdout, out)
	cmd.Stderr = os.Stderr
	start := time.Now()
	err := cmd.Run()
	m.Wall = time.Since(start).Seconds()
	if cmd.ProcessState == nil {
		return m, err
	}
	m.Exit = cmd.ProcessState.ExitCode()
	if ru, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		m.MaxRSS = int64(ru.Maxrss)
		if runtime.GOOS == "linux" {
			m.MaxRSS *= 1024 // KiB on Linux, bytes on macOS
		}
	}
	m.Result = strings.TrimSpace(stdout.String())
	if i := strings.LastIndexByte(m.Result, '\n'); i >= 0 {
		m.Result = m.Result[i+1:]
	}
	m.Limit = memlimit.Detect()
	m.GOOS = runtime.GOOS
	m.Env = os.Getenv("GOMEMLIMIT")
	return m, nil
}

func appendJSON(path string, m Measurement) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(m)
}

// measure runs case c reps times, each in a child process, and appends
// the measurements to out.
func measure(c runConfig, label string, reps int, out string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	for rep := range reps {
		cmd := exec.Command(self, append([]string{"run"}, c.args()...)...)
		m, err := measureCmd(cmd, io.Discard)
		if err != nil && cmd.ProcessState == nil {
			return err
		}
		m.Label, m.Impl, m.Case, m.Kind, m.Rows = label, c.impl, c.kase, c.kind, c.rows
		m.Block, m.Sink, m.Budget, m.Managed, m.Rep = c.block, c.sink, c.budget, c.managed, rep
		fmt.Fprintf(os.Stderr, "%-10s %-8s %-8s %-4s %9d block %6d: %7.2fs %8.1f MiB exit %d rows %s\n",
			label, c.impl, c.kase, c.kind, c.rows, c.block, m.Wall, float64(m.MaxRSS)/(1<<20), m.Exit, m.Result)
		if err := appendJSON(out, m); err != nil {
			return err
		}
	}
	return nil
}

func cmdMeasure(args []string) error {
	fs := flag.NewFlagSet("measure", flag.ExitOnError)
	c := runFlags(fs)
	label := fs.String("label", "", "label of the measurement")
	reps := fs.Int("reps", 3, "repetitions")
	out := fs.String("out", "results.jsonl", "JSONL file to append to")
	fs.Parse(args)
	return measure(*c, *label, *reps, *out)
}

// cmdExec measures any command, such as the onebrc_budget example in a
// container.
func cmdExec(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	label := fs.String("label", "", "label of the measurement")
	out := fs.String("out", "results.jsonl", "JSONL file to append to")
	budget := fs.Int64("budget", 0, "budget of the run to record, in bytes")
	managed := fs.Bool("managed", false, "record that the engine sets GOMEMLIMIT")
	fs.Parse(args)
	if fs.NArg() == 0 {
		return fmt.Errorf("exec: no command")
	}
	cmd := exec.Command(fs.Arg(0), fs.Args()[1:]...)
	m, err := measureCmd(cmd, os.Stdout)
	if err != nil && cmd.ProcessState == nil {
		return err
	}
	m.Label, m.Case, m.Budget, m.Managed = *label, "exec", *budget, *managed
	fmt.Fprintf(os.Stderr, "%s: %.1fs, maxrss %.1f MiB, exit %d, limit %d\n", *label, m.Wall, float64(m.MaxRSS)/(1<<20), m.Exit, m.Limit)
	return appendJSON(*out, m)
}
