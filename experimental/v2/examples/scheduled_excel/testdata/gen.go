//go:build ignore

// Command gen writes the synthetic Excel deliveries of the example, one per
// day, sheet Bestellungen. Run it with `go run gen.go` in testdata.
//
//   - 2026-09-27.xlsx: a normal delivery with a new column note, a
//     placeholder text for an amount and a cancelled order with a negative
//     amount.
//   - 2026-09-28.xlsx: the supplier sends the amounts as text with a
//     decimal comma.
//   - 2026-09-29.xlsx: the supplier renamed the column ordered to
//     ordered_at.
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

const date, amount = 1, 2

type order struct {
	id, customer, amount string
	text                 bool // amount as text
	day                  int
	note                 string
}

func row(num int, o order, withNote bool) x.Row {
	ref := func(col string) string { return col + strconv.Itoa(num) }
	a := x.Num(ref("C"), o.amount, amount)
	if o.text {
		a = x.Text(ref("C"), o.amount)
	}
	cells := []x.Cell{
		x.Text(ref("A"), o.id),
		x.Text(ref("B"), o.customer),
		a,
		x.Num(ref("D"), serial(time.Date(2026, 9, o.day, 0, 0, 0, 0, time.UTC)), date),
	}
	if withNote && o.note != "" {
		cells = append(cells, x.Text(ref("E"), o.note))
	}
	return x.Row{Num: num, Cells: cells}
}

func write(name string, header []string, orders []order) {
	h := x.Row{Num: 1}
	for i, c := range header {
		h.Cells = append(h.Cells, x.Text(string(rune('A'+i))+"1", c))
	}
	rows := []x.Row{h}
	for i, o := range orders {
		rows = append(rows, row(i+2, o, len(header) > 4))
	}
	b := x.Book{
		Formats: []string{"dd.mm.yyyy", "#,##0.00"},
		Sheets:  []x.Sheet{{Name: "Bestellungen", Rows: rows}},
	}
	if err := x.Write(name, b); err != nil {
		log.Fatal(err)
	}
}

func main() {
	write("2026-09-27.xlsx", []string{"order", "customer", "amount", "ordered", "note"}, []order{
		{id: "1001", customer: "K1", amount: "19.9", day: 26},
		{id: "1002", customer: "K2", amount: "auf Anfrage", text: true, day: 26, note: "Preis folgt"},
		{id: "1003", customer: "K3", amount: "-5", day: 26, note: "storniert"},
		{id: "1004", customer: "K1", amount: "12.5", day: 27},
		{id: "1005", customer: "K4", amount: "3.2", day: 27},
		{id: "1006", customer: "K2", amount: "8", day: 27},
	})
	write("2026-09-28.xlsx", []string{"order", "customer", "amount", "ordered"}, []order{
		{id: "1007", customer: "K1", amount: "19,90", text: true, day: 28},
		{id: "1008", customer: "K3", amount: "7,50", text: true, day: 28},
		{id: "1009", customer: "K2", amount: "12", day: 28},
		{id: "1010", customer: "K4", amount: "3,20", text: true, day: 28},
	})
	write("2026-09-29.xlsx", []string{"order", "customer", "amount", "ordered_at"}, []order{
		{id: "1011", customer: "K1", amount: "4", day: 29},
		{id: "1012", customer: "K2", amount: "6", day: 29},
	})
}
