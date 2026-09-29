package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestInspectRejects(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out, "testdata"); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"Orders that passed: 3, customers that passed: 2\n",
		// Lines that cannot be split keep their raw bytes, line and offset
		// and have no cell values (D10).
		"<null>  <null>    <null>  <null>      5           106           1                  <null>        unparseable_line  1004,K3,80.50\",28.09.2026\n",
		"<null>  <null>    <null>  <null>      6           132           1                  <null>        unparseable_line  1005,K2,99.90\n",
		// The raw state is the value as delivered (D1).
		"1002    K2        12,50   27.09.2026  3           56            1                  amount        parse             <null>\n",
		// A row failing in two columns stands once, with error_count 2 (D44) ...
		"1006    K4        abc     2026-09-28  7           146           2                  amount        parse             <null>\n",
		// ... and has an entry per column in the overview (D15).
		"orders.csv    7           amount        abc          parse             not a number: \"abc\"\n",
		"orders.csv    7           ordered       2026-09-28   parse             not a date in format \"02.01.2006\": \"2026-09-28\"\n",
		// Excel rows carry the stored value, the cell and the displayed
		// text (D52, D62).
		"K2  20190301    250.5        Kunden       3           C3          20,190,301     since         parse\n",
		"K3  2025-01-15  auf Anfrage  Kunden       4           D4          auf Anfrage    credit        parse\n",
		"customers.xlsx#Kunden  3           since         20190301     parse       not a date: \"20190301\"\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
}

func TestInspectRejectsMissingData(t *testing.T) {
	if err := run(context.Background(), &bytes.Buffer{}, t.TempDir()); err == nil {
		t.Error("no error for a missing delivery")
	}
}
