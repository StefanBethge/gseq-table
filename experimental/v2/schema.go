package gtable

import (
	"fmt"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
)

// field is a named, typed column of a plan. The plan knows the schema after
// every step, so unknown columns and type conflicts are plan errors before
// the run (D19, D32).
type field struct {
	name string
	kind block.Kind
}

type schema []field

func (s schema) index(name string) int {
	for i, f := range s {
		if f.name == name {
			return i
		}
	}
	return -1
}

func (s schema) lookup(name string) (int, error) {
	if i := s.index(name); i >= 0 {
		return i, nil
	}
	return -1, fmt.Errorf("unknown column %q", name)
}

func (s schema) names() []string {
	out := make([]string, len(s))
	for i, f := range s {
		out[i] = f.name
	}
	return out
}

func (s schema) clone() schema { return append(schema(nil), s...) }
