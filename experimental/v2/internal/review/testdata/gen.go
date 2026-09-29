//go:build ignore

// Command gen writes the synthetic sample deliveries of the maintainer
// review (D60). They copy typical problems of supplier files; no real
// customer data goes into them. Run it with `go run gen.go` in testdata.
//
// A supplier sends a daily article list with the columns sku, name, price,
// stock and valid_from, as CSV with ';' or as Excel.
//
//   - 01-placeholders.csv: a normal day with dirt: the placeholders NULL,
//     n/a and -, stray whitespace, a bare quote, a line with too few
//     fields, an oversized name and a row that fails in two columns.
//   - 02-formats.csv: the supplier switched most prices to a decimal
//     comma, some dates to ISO and some stock values to a thousands
//     separator, so the three columns fail at different shares.
//   - 03-columns.csv: stock is missing, name and valid_from are renamed
//     (Name, Valid From), and a column comment is new.
//   - 04-multisheet.xlsx: the same data split over the sheets Artikel and
//     Preise, with a sheet Hinweise that no pipeline reads: a date typed as
//     text, a date typed as a plain number, a price as text with a decimal
//     comma, placeholders, a number with whitespace and an oversized cell.
package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	x "github.com/stefanbethge/gseq-table/experimental/v2/internal/xlsxtest"
)

// long is a field over the limit of 64 bytes the pipelines set.
var long = "Schraube M8x40 verzinkt DIN 933 " + strings.Repeat("(Restposten) ", 6)

func writeCSV(name string, lines ...string) {
	if err := os.WriteFile(name, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		log.Fatal(err)
	}
}

func serial(y int, m time.Month, d int) string {
	t := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	return strconv.FormatFloat(t.Sub(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)).Hours()/24, 'f', -1, 64)
}

func main() {
	writeCSV("01-placeholders.csv",
		"sku;name;price;stock;valid_from",
		"A-100;Schraube M6;0.12;1500;27.09.2026",
		"A-101;Schraube M8;0.18;NULL;27.09.2026", // the placeholder the pipeline knows
		"A-102;Mutter M6;n/a;800;27.09.2026",     // a placeholder it does not know
		"A-103;Mutter M8;0.09;-;27.09.2026",      // another one
		"A-104;Scheibe 6mm; 0.03;2000;27.09.2026",
		`A-105;Dübel 5/16";0.05;400;27.09.2026`, // an inch sign, a bare quote
		"A-106;Dübel 10mm;0.07;27.09.2026",      // stock is missing in this line
		"A-107;"+long+";0.21;120;27.09.2026",
		"A-108;Winkel 40mm;n/a;30;2026-09-27", // fails in price and valid_from
		"A-109;Winkel 60mm;0.95;25;27.09.2026",
	)
	writeCSV("02-formats.csv",
		"sku;name;price;stock;valid_from",
		"A-100;Schraube M6;0,12;1500;28.09.2026",
		"A-101;Schraube M8;0,18;1.200;28.09.2026",
		"A-102;Mutter M6;0,08;800;2026-09-28",
		"A-103;Mutter M8;0,09;650;28.09.2026",
		"A-104;Scheibe 6mm;0,03;2.000;2026-09-28",
		"A-105;Dübel 8mm;0,05;400;28.09.2026",
		"A-106;Dübel 10mm;0,07;350;2026-09-28",
		"A-107;Schraube M10;0.21;120;28.09.2026",
		"A-108;Winkel 40mm;0.55;30;28.09.2026",
		"A-109;Winkel 60mm;0.95;25;28.09.2026",
	)
	writeCSV("03-columns.csv",
		"sku;Name;price;Valid From;comment",
		"A-100;Schraube M6;0.12;29.09.2026;",
		"A-101;Schraube M8;0.18;29.09.2026;neu im Sortiment",
		"A-102;Mutter M6;0.08;29.09.2026;",
		"A-103;Mutter M8;0.09;29.09.2026;Preis vorläufig",
	)

	const date, money = 1, 2
	head := func(names ...string) x.Row {
		r := x.Row{Num: 1}
		for i, n := range names {
			r.Cells = append(r.Cells, x.Text(string(rune('A'+i))+"1", n))
		}
		return r
	}
	b := x.Book{
		Formats: []string{"dd.mm.yyyy", "#,##0.00"},
		Sheets: []x.Sheet{
			{Name: "Artikel", Rows: []x.Row{
				head("sku", "name", "valid_from"),
				{Num: 2, Cells: []x.Cell{x.Text("A2", "A-100"), x.Text("B2", "Schraube M6"), x.Num("C2", serial(2026, 9, 29), date)}},
				{Num: 3, Cells: []x.Cell{x.Text("A3", "A-101"), x.Text("B3", "Schraube M8"), x.Text("C3", "29.09.2026")}},    // a date typed as text
				{Num: 4, Cells: []x.Cell{x.Text("A4", "A-102"), x.Text("B4", "Mutter M6"), x.Num("C4", "20260929", 0)}},      // a date typed as a number
				{Num: 5, Cells: []x.Cell{x.Text("A5", "A-103"), x.Text("B5", long), x.Num("C5", serial(2026, 9, 29), date)}}, // an oversized cell
				{Num: 6, Cells: []x.Cell{x.Text("A6", "A-104"), x.Text("B6", "Scheibe 6mm"), x.Num("C6", serial(2026, 9, 29), date)}},
				{Num: 7, Cells: []x.Cell{x.Text("A7", "A-105"), x.Text("B7", "Dübel 8mm"), x.Num("C7", serial(2026, 9, 29), date)}},
			}},
			{Name: "Preise", Rows: []x.Row{
				head("sku", "price", "stock"),
				{Num: 2, Cells: []x.Cell{x.Text("A2", "A-100"), x.Num("B2", "0.12", money), x.Num("C2", "1500", 0)}},
				{Num: 3, Cells: []x.Cell{x.Text("A3", "A-101"), x.Text("B3", "0,18"), x.Num("C3", "1200", 0)}}, // a price as text
				{Num: 4, Cells: []x.Cell{x.Text("A4", "A-102"), x.Text("B4", "n/a"), x.Num("C4", "800", 0)}},
				{Num: 5, Cells: []x.Cell{x.Text("A5", "A-103"), x.Num("B5", "0.09", money), x.Text("C5", "-")}},
				{Num: 6, Cells: []x.Cell{x.Text("A6", "A-104"), x.Num("B6", "0.03", money), x.Text("C6", " 2000")}}, // whitespace
				{Num: 7, Cells: []x.Cell{x.Text("A7", "A-105"), x.Num("B7", "0.05", money), x.Num("C7", "400", 0)}},
			}},
			{Name: "Hinweise", Rows: []x.Row{
				{Num: 1, Cells: []x.Cell{x.Text("A1", "Preise ohne MwSt., Bestand vom Vortag")}},
			}},
		},
	}
	if err := x.Write("04-multisheet.xlsx", b); err != nil {
		log.Fatal(err)
	}
}
