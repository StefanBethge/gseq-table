//go:build ignore

// Command gen writes customers.xlsx, the synthetic Excel delivery of the
// example: dates and amounts with number formats, a date typed as a plain
// number, and a placeholder text. Run it with `go run gen.go` in testdata.
package main

import (
	"log"
	"strconv"
	"time"

	x "github.com/stefanbethge/gseq-table/experimental/v2/internal/xlsxtest"
)

func serial(t time.Time) string {
	d := t.Sub(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC))
	return strconv.FormatFloat(d.Hours()/24, 'f', -1, 64)
}

func day(y int, m time.Month, d int) string { return serial(time.Date(y, m, d, 0, 0, 0, 0, time.UTC)) }

func main() {
	const date, amount, plain = 1, 2, 3
	b := x.Book{
		Formats: []string{"dd.mm.yyyy", "#,##0.00", "#,##0"},
		Sheets: []x.Sheet{{Name: "Kunden", Rows: []x.Row{
			{Num: 1, Cells: []x.Cell{x.Text("A1", "id"), x.Text("B1", "name"), x.Text("C1", "since"), x.Text("D1", "credit")}},
			{Num: 2, Cells: []x.Cell{x.Text("A2", "K1"), x.Text("B2", "Nordlicht GmbH"), x.Num("C2", day(2024, 3, 1), date), x.Num("D2", "1500", amount)}},
			// Someone typed the date as a number.
			{Num: 3, Cells: []x.Cell{x.Text("A3", "K2"), x.Text("B3", "Bäckerei Sonne"), x.Num("C3", "20190301", plain), x.Num("D3", "250.5", amount)}},
			{Num: 4, Cells: []x.Cell{x.Text("A4", "K3"), x.Text("B4", "Hafen & Co"), x.Num("C4", day(2025, 1, 15), date), x.Text("D4", "auf Anfrage")}},
			{Num: 5, Cells: []x.Cell{x.Text("A5", "K4"), x.Text("B5", "Werkstatt Nord"), x.Num("C5", day(2026, 9, 27), date), x.Num("D5", "80", amount)}},
		}}},
	}
	if err := x.Write("customers.xlsx", b); err != nil {
		log.Fatal(err)
	}
}
