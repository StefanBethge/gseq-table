//go:build go1.27

package schema

import (
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/table"
)

// TestTypedRowAccessorsMatchSchema guards that Row.GetAs follows the same
// parsing conventions as the schema row accessors.
func TestTypedRowAccessorsMatchSchema(t *testing.T) {
	inputs := []string{
		"", " ", "0", "1", "-7", " 42 ", "+3", "1.5", "-0.25", "1e3", "1,000",
		"true", "FALSE", "yes", "No", "maybe",
		"2024-01-15", "15.02.2024", "01/02/2024", "02 Jan 2024", "Jan 02, 2024",
		"2024-01-15T10:00:00", "2024-01-15T10:00:00Z", "not a date",
	}
	for _, in := range inputs {
		r := table.NewRow([]string{"v"}, []string{in})
		if got, want := r.GetAs[int64]("v"), Int(r, "v"); got != want {
			t.Errorf("int64 %q: GetAs=%v schema=%v", in, got, want)
		}
		if got, want := r.GetAs[float64]("v"), Float(r, "v"); got != want {
			t.Errorf("float64 %q: GetAs=%v schema=%v", in, got, want)
		}
		if got, want := r.GetAs[bool]("v"), Bool(r, "v"); got != want {
			t.Errorf("bool %q: GetAs=%v schema=%v", in, got, want)
		}
		got, want := r.GetAs[time.Time]("v"), Time(r, "v", "")
		gv, gok := got.Get()
		wv, wok := want.Get()
		if gok != wok || !gv.Equal(wv) {
			t.Errorf("time %q: GetAs=%v schema=%v", in, got, want)
		}
	}
}
