package main

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"

	"github.com/xuri/excelize/v2"
)

// The generated deliveries (D60, D106). Both have eight columns and a text
// key code with 1000 values. A numeric-heavy delivery holds five floats
// and two integers besides the key; a text-heavy one five texts, one
// float and one integer. The dimension table for the join maps every code
// to a region and a name.

var numColumns = []string{"id", "code", "f1", "f2", "f3", "f4", "f5", "n"}
var textColumns = []string{"id", "code", "city", "name", "street", "email", "comment", "amount"}
var dimColumns = []string{"code", "region", "label"}

const codes = 1000

var (
	cities = []string{"Berlin", "Hamburg", "München", "Köln", "Frankfurt am Main", "Stuttgart", "Düsseldorf",
		"Leipzig", "Dortmund", "Essen", "Bremen", "Dresden", "Hannover", "Nürnberg", "Duisburg", "Bochum",
		"Wuppertal", "Bielefeld", "Bonn", "Münster", "Mannheim", "Karlsruhe", "Augsburg", "Wiesbaden"}
	first = []string{"Anna", "Ben", "Clara", "David", "Emma", "Felix", "Greta", "Hannah", "Jonas", "Lena",
		"Leon", "Mia", "Noah", "Paul", "Sophie", "Tim", "Lukas", "Marie", "Elias", "Lea"}
	last = []string{"Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker",
		"Schulz", "Hoffmann", "Schäfer", "Koch", "Bauer", "Richter", "Klein", "Wolf", "Schröder"}
	words = []string{"Lieferung", "verspätet", "Rechnung", "offen", "bitte", "prüfen", "Kunde", "meldet",
		"Schaden", "Rückfrage", "erledigt", "Termin", "vereinbart", "Anruf", "Mahnung", "storniert",
		"Gutschrift", "Ersatzteil", "bestellt", "Nachlieferung"}
)

func code(i int) string { return fmt.Sprintf("C%04d", i%codes) }

// genRow writes the fields of row i of a delivery of kind into rec.
func genRow(kind string, r *rand.Rand, i int, rec []string) {
	rec[0] = strconv.Itoa(i + 1)
	rec[1] = code(r.IntN(codes))
	switch kind {
	case "num":
		for j := 2; j < 7; j++ {
			rec[j] = strconv.FormatFloat(float64(r.IntN(200000)-100000)/100, 'f', 2, 64)
		}
		rec[7] = strconv.Itoa(r.IntN(1000000))
	case "text":
		rec[2] = cities[r.IntN(len(cities))]
		f, l := first[r.IntN(len(first))], last[r.IntN(len(last))]
		rec[3] = f + " " + l
		rec[4] = fmt.Sprintf("%sstraße %d", last[r.IntN(len(last))], 1+r.IntN(200))
		rec[5] = fmt.Sprintf("%s.%s%d@example.org", f, l, r.IntN(100))
		n := 4 + r.IntN(8)
		c := words[r.IntN(len(words))]
		for range n - 1 {
			c += " " + words[r.IntN(len(words))]
		}
		rec[6] = c
		rec[7] = strconv.FormatFloat(float64(r.IntN(200000)-100000)/100, 'f', 2, 64)
	}
}

func columnsOf(kind string) []string {
	if kind == "text" {
		return textColumns
	}
	return numColumns
}

// deliveryPath returns the path of the generated delivery of kind with
// rows rows in dir.
func deliveryPath(dir, kind string, rows int, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%d.%s", kind, rows, ext))
}

func dimPath(dir string) string { return filepath.Join(dir, "dim.csv") }

// generate writes the delivery of kind with rows rows as CSV into dir,
// and the dimension table, unless they exist.
func generate(dir, kind string, rows int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := genDim(dimPath(dir)); err != nil {
		return err
	}
	path := deliveryPath(dir, kind, rows, "csv")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	cols := columnsOf(kind)
	return writeCSV(path, cols, rows, func(r *rand.Rand, i int, rec []string) { genRow(kind, r, i, rec) })
}

func genDim(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return writeCSV(path, dimColumns, codes, func(r *rand.Rand, i int, rec []string) {
		rec[0] = code(i)
		rec[1] = "R" + strconv.Itoa(i%7)
		rec[2] = "Kunde " + strconv.Itoa(i)
	})
}

// writeCSV writes a CSV file. The fields contain no separator, quote or
// line break, so they are written unquoted, the same for v1 and v2.
func writeCSV(path string, cols []string, rows int, row func(*rand.Rand, int, []string)) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	r := rand.New(rand.NewPCG(53, uint64(len(cols))))
	rec := make([]string, len(cols))
	writeLine := func(fields []string) {
		for j, s := range fields {
			if j > 0 {
				w.WriteByte(',')
			}
			w.WriteString(s)
		}
		w.WriteByte('\n')
	}
	writeLine(cols)
	for i := range rows {
		row(r, i, rec)
		writeLine(rec)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// generateExcel writes the numeric-heavy delivery with rows rows as an
// Excel file into dir, unless it exists. Numbers are numeric cells.
func generateExcel(dir string, rows int) error {
	path := deliveryPath(dir, "num", rows, "xlsx")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f := excelize.NewFile()
	defer f.Close()
	sw, err := f.NewStreamWriter("Sheet1")
	if err != nil {
		return err
	}
	head := make([]any, len(numColumns))
	for i, c := range numColumns {
		head[i] = c
	}
	if err := sw.SetRow("A1", head); err != nil {
		return err
	}
	r := rand.New(rand.NewPCG(53, uint64(len(numColumns))))
	rec := make([]string, len(numColumns))
	cells := make([]any, len(numColumns))
	for i := range rows {
		genRow("num", r, i, rec)
		for j, s := range rec {
			if j == 1 {
				cells[j] = s
				continue
			}
			v, _ := strconv.ParseFloat(s, 64)
			cells[j] = v
		}
		addr, _ := excelize.CoordinatesToCellName(1, i+2)
		if err := sw.SetRow(addr, cells); err != nil {
			return err
		}
	}
	if err := sw.Flush(); err != nil {
		return err
	}
	return f.SaveAs(path)
}
