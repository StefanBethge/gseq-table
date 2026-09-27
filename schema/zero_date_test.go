package schema

import (
	"testing"

	"github.com/stefanbethge/gseq-table/table"
)

// The zero time ("0001-01-01") is not a date for inference or normalization
// (rule in internal/cell.ParseDate).
func TestZeroDate_InferAndApply(t *testing.T) {
	tb := table.New([]string{"d"}, [][]string{{"2024-01-15"}, {"0001-01-01"}})
	if got := Infer(tb).Col("d"); got != TypeString {
		t.Errorf("Infer: got %s, want %s (zero date is not a date)", got, TypeString)
	}
	res := Schema{}.Cast("d", TypeDate).ApplyStrict(tb)
	if res.IsOk() {
		t.Error("ApplyStrict: expected error for zero date")
	}
}
