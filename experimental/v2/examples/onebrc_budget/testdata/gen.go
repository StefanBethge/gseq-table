//go:build ignore

// Command gen writes measurements.txt, a small synthetic delivery in the
// format of the 1BRC file: station and measurement separated by ';', no
// header line. It is deliberately dirty: a placeholder for a missing value,
// a decimal comma and a line with a third field. Run it with
// `go run gen.go` in testdata.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
)

func main() {
	stations := []string{"Hamburg", "Bulawayo", "Palembang", "St. John's", "Cracow", "Bridgetown",
		"Istanbul", "Roseau", "Conakry", "Zürich", "Oslo", "Lissabon"}
	r := rand.New(rand.NewPCG(1, 2))
	var b strings.Builder
	for i := range 3000 {
		st := stations[r.IntN(len(stations))]
		v := fmt.Sprintf("%.1f", float64(r.IntN(1200)-400)/10)
		switch i {
		case 17, 911:
			v = "n/a"
		case 1203:
			v = strings.Replace(v, ".", ",", 1)
		case 2500:
			v += ";extra"
		}
		fmt.Fprintf(&b, "%s;%s\n", st, v)
	}
	if err := os.WriteFile("measurements.txt", []byte(b.String()), 0o644); err != nil {
		log.Fatal(err)
	}
}
