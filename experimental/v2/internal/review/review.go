// Package review runs pipelines over the synthetic sample deliveries of the
// maintainer review (D60) and prints what the review looks at: the status
// and counts of every run, the rejected rows of every source, the overview
// and the change report (D13, D14, D23, D44). The printed output is kept as
// testdata/review.golden, so the review is reproducible and every change
// to the shape of the rejected rows shows up in a diff.
//
// testdata/gen.go writes the deliveries; the package comment there lists
// the problems each one has.
package review

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	gtable "github.com/stefanbethge/gseq-table/experimental/v2"
	"github.com/stefanbethge/gseq-table/experimental/v2/csv"
	"github.com/stefanbethge/gseq-table/experimental/v2/excel"
)

// blockLen is small so the runs go over several blocks.
const blockLen = 3

// maxField is the field limit of the pipelines, small so the sample
// deliveries can carry an oversized field (D56).
const maxField = 64

// columns are the expected columns of the article list.
var columns = []string{"sku", "name", "price", "stock", "valid_from"}

// casts type the article list. NULL is the placeholder the pipeline
// developer knows; other placeholders are rejected (D29, D74).
func casts(cols ...string) gtable.Op {
	all := map[string]gtable.Op{
		"price":      gtable.Cast("price", gtable.TypeFloat, gtable.NullTexts("NULL")),
		"stock":      gtable.Cast("stock", gtable.TypeInt, gtable.NullTexts("NULL")),
		"valid_from": gtable.Cast("valid_from", gtable.TypeTimestamp, gtable.DateFormat("02.01.2006")),
	}
	ops := make([]gtable.Op, len(cols))
	for i, c := range cols {
		ops[i] = all[c]
	}
	return gtable.CastAll(ops...)
}

// Run runs the pipelines over the deliveries in dir and prints the review
// to w. Every run gets its own run ID; runIDs replaces it so the output is
// the same on every run.
func Run(ctx context.Context, w io.Writer, dir string) error {
	for _, name := range []string{"01-placeholders.csv", "02-formats.csv", "03-columns.csv"} {
		src := csv.File(filepath.Join(dir, name), csv.Comma(';'), csv.MaxFieldSize(maxField)).
			Expect(columns...).
			Key("sku")
		p := gtable.FromSource(src, blockLen).
			Then(casts("price", "stock", "valid_from")).
			Then(gtable.With("value", gtable.Col("price").Mul(gtable.Col("stock")).Round(2)))
		if err := show(ctx, w, name, p); err != nil {
			return err
		}
	}
	return multiSheet(ctx, w, filepath.Join(dir, "04-multisheet.xlsx"))
}

// multiSheet runs the Excel delivery: every sheet is a source (D53). The
// prices are read and cast at once and joined to the articles, so the run
// has two sources, and the overview spans both (D13, D75).
func multiSheet(ctx context.Context, w io.Writer, path string) error {
	prices, err := excel.Sheet(path, "Preise", excel.MaxFieldSize(maxField)).
		Expect("sku", "price", "stock").
		Key("sku").
		Table(ctx)
	if err != nil {
		return err
	}
	prices = prices.Apply(casts("price", "stock"))

	src := excel.Sheet(path, "Artikel", excel.MaxFieldSize(maxField)).
		Expect("sku", "name", "valid_from").
		Key("sku")
	// The working columns of Excel hold dates in a fixed text form (D62),
	// so valid_from casts without a DateFormat.
	p := gtable.FromSource(src, blockLen).
		Then(gtable.Cast("valid_from", gtable.TypeTimestamp)).
		Then(gtable.LeftJoin(prices, gtable.On("sku"))).
		Then(gtable.With("value", gtable.Col("price").Mul(gtable.Col("stock")).Round(2)))
	return show(ctx, w, filepath.Base(path), p)
}

// show runs p and prints its result.
func show(ctx context.Context, w io.Writer, name string, p *gtable.Pipeline) error {
	res, err := p.Run(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	defer res.Close()
	out := &strings.Builder{}
	c := res.Counts
	fmt.Fprintf(out, "=== %s: status %v, exit code %d\n", name, res.Status, res.ExitCode())
	fmt.Fprintf(out, "%d read, %d passed, %d rejected, %d dropped\n", c.Read, c.Passed, c.Rejected, c.Dropped)
	for _, cause := range res.Causes {
		fmt.Fprintf(out, "cause %v\n", cause)
	}
	rr := res.Table.RejectedRows()
	for _, s := range rr.Sources() {
		fmt.Fprintf(out, "\n--- Rejected rows of %s (%d)\n%s", s.Source, s.Rows.Len(), s.Rows)
	}
	fmt.Fprintf(out, "\n--- Overview\n%s", rr.Overview())
	if len(res.Report) > 0 {
		fmt.Fprintf(out, "\n--- Change report\n%s", res.ChangeReport())
	}
	fmt.Fprintln(out)
	_, err = io.WriteString(w, runIDs(res.Table.Rejects()).Replace(out.String()))
	return err
}

// runIDs returns a replacer that puts a fixed placeholder of the same
// width, run0…01, in place of every run ID the rejects carry, the part of reject_id
// before the counter (D76), so the tables stay aligned.
func runIDs(rejects []gtable.Reject) *strings.Replacer {
	var pairs []string
	seen := map[string]bool{}
	for _, r := range rejects {
		id, _, _ := strings.Cut(r.ID, "-")
		if !seen[id] {
			seen[id] = true
			pairs = append(pairs, id, fmt.Sprintf("run%0*d", len(id)-3, len(seen)))
		}
	}
	return strings.NewReplacer(pairs...)
}
