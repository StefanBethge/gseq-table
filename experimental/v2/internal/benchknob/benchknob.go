// Package benchknob holds switches of the engine that only the benchmarks
// and tests of the module set, never a pipeline (design decision D105).
package benchknob

import "sync/atomic"

// NoRawState makes the engine drop the raw state of the rows a Reader
// reads: the blocks are neither kept as raw state nor counted in the
// budget nor shared with it. The location and the counts stay. Rejected
// rows of such a run carry no raw values. It exists to measure the cost of
// the raw state (G13).
var NoRawState atomic.Bool
