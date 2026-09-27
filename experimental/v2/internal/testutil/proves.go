// Package testutil holds test helpers and the docs gates of the v2 design set
// (design decision D37).
package testutil

import (
	"regexp"
	"testing"
)

var tCaseID = regexp.MustCompile(`^T[1-9][0-9]*$`)

// Proves declares that the calling test proves the given T cases of the test
// plan (docs/explanation/design/v2/30-test-plan.md). TestPlanMappingConsistency
// reads these calls from the source and checks them against the mapping table
// in the test plan, so the arguments must be string literals.
//
// Each id is also recorded as a test attribute "proves", visible in
// `go test -json` output.
func Proves(t testing.TB, ids ...string) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("Proves: no T case given")
	}
	for _, id := range ids {
		if !tCaseID.MatchString(id) {
			t.Fatalf("Proves: %q is not a T case id (want T<n>)", id)
		}
		t.Attr("proves", id)
	}
}
