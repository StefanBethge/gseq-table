package schema

import (
	"testing"
	"time"

	"github.com/stefanbethge/gseq-table/table"
)

// schema.Time and the typed table methods share internal/cell.ParseDate, so
// both treat the zero time as "not parsed".
func TestZeroDate_SchemaAndTypedAgree(t *testing.T) {
	for _, in := range []string{"0001-01-01", "0001-01-01T00:00:00Z", "01.01.0001", "2024-01-15"} {
		r := table.NewRow([]string{"d"}, []string{in})
		s := Time(r, "d", "")
		g := r.GetAs[time.Time]("d")
		sv, sok := s.Get()
		gv, gok := g.Get()
		if sok != gok || !sv.Equal(gv) {
			t.Errorf("%q: schema.Time=%v GetAs=%v", in, s, g)
		}
		wantOK := in == "2024-01-15"
		if sok != wantOK {
			t.Errorf("%q: parsed=%v, want %v", in, sok, wantOK)
		}
	}
}
