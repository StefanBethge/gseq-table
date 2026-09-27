package table

import (
	"io"
	"strconv"
	"strings"

	"github.com/stefanbethge/gseq-table/internal/cell"
)

// Default limits used by String and Pretty.
const (
	// DefaultPrettyMaxRows is the number of rows String renders before
	// summarising the rest as "… N more rows".
	DefaultPrettyMaxRows = 20

	// DefaultPrettyMaxColWidth is the display width at which String truncates
	// a header or cell value with an ellipsis.
	DefaultPrettyMaxColWidth = 32
)

// PrettyConfig holds the resolved settings for Pretty.
// Use the WithPretty* options to change individual fields.
type PrettyConfig struct {
	// MaxRows is the number of rows rendered. Remaining rows are summarised
	// in a trailing "… N more rows" line. Zero or less means all rows.
	// Defaults to DefaultPrettyMaxRows.
	MaxRows int

	// MaxColWidth is the maximum display width of a column. Longer headers
	// and values are truncated with "…". Zero or less means unlimited.
	// Defaults to DefaultPrettyMaxColWidth.
	MaxColWidth int
}

// PrettyOption is a functional option for configuring Pretty.
type PrettyOption func(*PrettyConfig)

// WithPrettyMaxRows sets the number of rows Pretty renders. Zero or less
// renders all rows.
func WithPrettyMaxRows(n int) PrettyOption { return func(c *PrettyConfig) { c.MaxRows = n } }

// WithPrettyMaxColWidth sets the maximum display width of a column. Zero or
// less disables truncation.
func WithPrettyMaxColWidth(n int) PrettyOption {
	return func(c *PrettyConfig) { c.MaxColWidth = n }
}

// String renders t as an aligned text table for debugging and logs, using the
// default limits (DefaultPrettyMaxRows, DefaultPrettyMaxColWidth). It makes
// Table a fmt.Stringer:
//
//	fmt.Println(t)
//	// +-------+--------+-----+
//	// | name  | city   | age |
//	// +-------+--------+-----+
//	// | Alice | Berlin |  30 |
//	// | Bob   | Munich |  25 |
//	// +-------+--------+-----+
//
// Use Pretty to change the limits.
func (t Table) String() string { return t.Pretty() }

// Pretty renders t as an aligned text table. Column widths are measured in
// display columns (runes, with wide East Asian characters counting double),
// numeric columns are right-aligned, and control characters such as newlines
// are shown escaped so each row stays on one line. Rows beyond MaxRows are
// summarised as "… N more rows"; the result has no trailing newline.
//
//	s := t.Pretty(table.WithPrettyMaxRows(5), table.WithPrettyMaxColWidth(12))
func (t Table) Pretty(opts ...PrettyOption) string {
	return renderPretty(t.Headers, len(t.Rows), func(r int) []string { return t.Rows[r].values }, opts)
}

// WritePretty writes the Pretty rendering of t to w, followed by a newline.
func (t Table) WritePretty(w io.Writer, opts ...PrettyOption) error {
	_, err := io.WriteString(w, t.Pretty(opts...)+"\n")
	return err
}

// String renders m as an aligned text table using the default limits.
// See Table.String.
func (m *MutableTable) String() string { return m.Pretty() }

// Pretty renders m as an aligned text table. See Table.Pretty.
func (m *MutableTable) Pretty(opts ...PrettyOption) string {
	return renderPretty(m.headers, len(m.rows), func(r int) []string { return m.rows[r] }, opts)
}

// WritePretty writes the Pretty rendering of m to w, followed by a newline.
func (m *MutableTable) WritePretty(w io.Writer, opts ...PrettyOption) error {
	_, err := io.WriteString(w, m.Pretty(opts...)+"\n")
	return err
}

// renderPretty lays out headers and the first rows of a table. row returns
// the raw values of row r, which may be shorter than headers.
func renderPretty(headers []string, n int, row func(r int) []string, opts []PrettyOption) string {
	cfg := PrettyConfig{MaxRows: DefaultPrettyMaxRows, MaxColWidth: DefaultPrettyMaxColWidth}
	for _, opt := range opts {
		opt(&cfg)
	}
	if len(headers) == 0 {
		return "(empty table)"
	}

	shown := n
	if cfg.MaxRows > 0 && shown > cfg.MaxRows {
		shown = cfg.MaxRows
	}

	display := func(s string) string { return cell.Truncate(cell.EscapeControl(s), cfg.MaxColWidth) }

	cols := len(headers)
	head := make([]string, cols)
	widths := make([]int, cols)
	for c, h := range headers {
		head[c] = display(h)
		widths[c] = cell.Width(head[c])
	}

	// Numeric columns are decided on the raw values of the rendered rows: a
	// column is numeric if it has at least one value and every non-empty
	// value parses as a number.
	numeric := make([]bool, cols)
	seen := make([]bool, cols)
	for c := range numeric {
		numeric[c] = true
	}
	cells := make([][]string, shown)
	for r := range shown {
		vals := row(r)
		line := make([]string, cols)
		for c := range cols {
			var v string
			if c < len(vals) {
				v = vals[c]
			}
			if v != "" && numeric[c] {
				seen[c] = true
				numeric[c] = cell.IsNumeric(v)
			}
			line[c] = display(v)
			widths[c] = max(widths[c], cell.Width(line[c]))
		}
		cells[r] = line
	}
	for c := range numeric {
		numeric[c] = numeric[c] && seen[c]
	}

	var sb strings.Builder
	border := func() {
		for c := range cols {
			sb.WriteByte('+')
			sb.WriteString(strings.Repeat("-", widths[c]+2))
		}
		sb.WriteString("+\n")
	}
	line := func(vals []string) {
		for c, v := range vals {
			sb.WriteString("| ")
			sb.WriteString(cell.Pad(v, widths[c], numeric[c]))
			sb.WriteByte(' ')
		}
		sb.WriteString("|\n")
	}

	border()
	line(head)
	border()
	for _, vals := range cells {
		line(vals)
	}
	if shown > 0 {
		border()
	}
	if rest := n - shown; rest > 0 {
		sb.WriteString(cell.Ellipsis + " " + strconv.Itoa(rest) + " more row")
		if rest != 1 {
			sb.WriteByte('s')
		}
		sb.WriteByte('\n')
	}
	return strings.TrimSuffix(sb.String(), "\n")
}
