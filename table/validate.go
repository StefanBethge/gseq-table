package table

import "fmt"

// AssertColumns returns an error if any of the required column names are
// absent from the table. Use this at the start of a pipeline to catch schema
// mismatches early.
//
//	if err := t.AssertColumns("id", "email", "created_at"); err != nil {
//	    return err
//	}
func (t Table) AssertColumns(cols ...string) error {
	have := make(map[string]bool, len(t.Headers))
	for _, h := range t.Headers {
		have[h] = true
	}
	for _, col := range cols {
		if !have[col] {
			return fmt.Errorf("missing required column: %q", col)
		}
	}
	return nil
}

// AssertNoEmpty returns an error for the first empty cell found in any of the
// specified columns. If no columns are given, all columns are checked.
//
//	if err := t.AssertNoEmpty("id", "email"); err != nil {
//	    return err
//	}
func (t Table) AssertNoEmpty(cols ...string) error {
	check := cols
	if len(check) == 0 {
		check = t.Headers
	}
	if err := t.AssertColumns(check...); err != nil {
		return err
	}
	for ri, row := range t.Rows {
		for _, col := range check {
			if row.Get(col).UnwrapOr("") == "" {
				return fmt.Errorf("row %d: column %q is empty", ri, col)
			}
		}
	}
	return nil
}
