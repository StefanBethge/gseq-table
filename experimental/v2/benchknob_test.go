package gtable

import (
	"context"
	"testing"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/benchknob"
)

// The switch of D105 drops the raw state of the rows read: nothing of it
// is counted, and the result is the same as with the raw state.
func TestNoRawStateSwitchDropsTheRawState(t *testing.T) {
	run := func() Result {
		p := FromSource(NewSource(measurements(2000, 40)), 64).
			Then(Cast("value", TypeFloat)).
			Then(Sort(Asc("station"), Asc("value")))
		res, err := isolated(t, p, 0).Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { res.Close() })
		return res
	}
	with := run()
	benchknob.NoRawState.Store(true)
	defer benchknob.NoRawState.Store(false)
	without := run()

	if with.mem.read == 0 {
		t.Fatal("the run with the raw state counted none")
	}
	if without.mem.read != 0 {
		t.Errorf("the run without the raw state counted %d bytes of it", without.mem.read)
	}
	if got, want := rowsOf(t, without.Table, false), rowsOf(t, with.Table, false); got != want {
		t.Errorf("rows without the raw state differ:\n%s\nwant\n%s", got, want)
	}
	if without.Counts.Rejected != with.Counts.Rejected || without.Counts.Passed != with.Counts.Passed {
		t.Errorf("counts %+v, want %+v", without.Counts, with.Counts)
	}
}
