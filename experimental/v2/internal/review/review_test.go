package review

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/review.golden")

// TestReview runs the pipelines over the sample deliveries and compares
// the printed review with testdata/review.golden (D60). Run it with
// -update after a change to the deliveries or the shape of the rejected
// rows, and look at the diff.
func TestReview(t *testing.T) {
	var out bytes.Buffer
	if err := Run(context.Background(), &out, "testdata"); err != nil {
		t.Fatal(err)
	}
	golden := filepath.Join("testdata", "review.golden")
	if *update {
		if err := os.WriteFile(golden, out.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Errorf("review differs from %s (run with -update and look at the diff):\n%s", golden, out.String())
	}
}
