// Package xlsxtest writes small xlsx files cell by cell for tests and
// example data: shared and inline strings, numbers, booleans, error cells,
// formulas with cached results and number formats, exactly as given.
package xlsxtest

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

// Book is a workbook.
type Book struct {
	Sheets []Sheet
	// Formats are number formats: a custom format code, or "#<id>" for a
	// built-in one such as "#14". A cell with Style i uses Formats[i-1];
	// Style 0 is General.
	Formats  []string
	Date1904 bool
}

// Sheet is a worksheet.
type Sheet struct {
	Name string
	Rows []Row
}

// Row is one row with its Excel row number.
type Row struct {
	Num   int
	Cells []Cell
}

// Cell is one cell. Type is the cell type of the file format: "s" for a
// shared string, "inlineStr", "str" for a formula text result, "b", "e",
// or "" for a number.
type Cell struct {
	Ref     string
	Type    string
	Value   string
	Formula string
	Style   int
}

// Text returns a shared string cell.
func Text(ref, v string) Cell { return Cell{Ref: ref, Type: "s", Value: v} }

// Num returns a number cell with the given stored value and style.
func Num(ref, v string, style int) Cell { return Cell{Ref: ref, Value: v, Style: style} }

// Write writes b as an xlsx file to path.
func Write(path string, b Book) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(f)
	var sst []string
	index := map[string]int{}
	sheets := make([]string, len(b.Sheets))
	for i, s := range b.Sheets {
		var sb strings.Builder
		sb.WriteString(xml.Header + `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`)
		for _, r := range s.Rows {
			fmt.Fprintf(&sb, `<row r="%d">`, r.Num)
			for _, c := range r.Cells {
				fmt.Fprintf(&sb, `<c r="%s"`, c.Ref)
				if c.Style > 0 {
					fmt.Fprintf(&sb, ` s="%d"`, c.Style)
				}
				if c.Type != "" {
					fmt.Fprintf(&sb, ` t="%s"`, c.Type)
				}
				sb.WriteString(">")
				if c.Formula != "" {
					fmt.Fprintf(&sb, "<f>%s</f>", esc(c.Formula))
				}
				switch c.Type {
				case "s":
					n, ok := index[c.Value]
					if !ok {
						n = len(sst)
						index[c.Value] = n
						sst = append(sst, c.Value)
					}
					fmt.Fprintf(&sb, "<v>%d</v>", n)
				case "inlineStr":
					fmt.Fprintf(&sb, "<is><t>%s</t></is>", esc(c.Value))
				default:
					if c.Value != "" {
						fmt.Fprintf(&sb, "<v>%s</v>", esc(c.Value))
					}
				}
				sb.WriteString("</c>")
			}
			sb.WriteString("</row>")
		}
		sb.WriteString("</sheetData></worksheet>")
		sheets[i] = sb.String()
	}

	var ct, wb, rels strings.Builder
	ct.WriteString(xml.Header + `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>` +
		`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` +
		`<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>`)
	wb.WriteString(xml.Header + `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">`)
	if b.Date1904 {
		wb.WriteString(`<workbookPr date1904="1"/>`)
	}
	wb.WriteString("<sheets>")
	rels.WriteString(xml.Header + `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for i, s := range b.Sheets {
		fmt.Fprintf(&ct, `<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`, i+1)
		fmt.Fprintf(&wb, `<sheet name="%s" sheetId="%d" r:id="rId%d"/>`, esc(s.Name), i+1, i+1)
		fmt.Fprintf(&rels, `<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`, i+1, i+1)
	}
	n := len(b.Sheets)
	fmt.Fprintf(&rels, `<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`, n+1)
	fmt.Fprintf(&rels, `<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>`, n+2)
	ct.WriteString("</Types>")
	wb.WriteString("</sheets></workbook>")
	rels.WriteString("</Relationships>")

	var st strings.Builder
	st.WriteString(xml.Header + `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">`)
	var custom []string
	ids := make([]int, len(b.Formats))
	for i, code := range b.Formats {
		var id int
		if _, err := fmt.Sscanf(code, "#%d", &id); err == nil {
			ids[i] = id
			continue
		}
		ids[i] = 164 + len(custom)
		custom = append(custom, fmt.Sprintf(`<numFmt numFmtId="%d" formatCode="%s"/>`, ids[i], esc(code)))
	}
	if len(custom) > 0 {
		fmt.Fprintf(&st, `<numFmts count="%d">%s</numFmts>`, len(custom), strings.Join(custom, ""))
	}
	st.WriteString(`<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>` +
		`<fills count="1"><fill><patternFill patternType="none"/></fill></fills>` +
		`<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>` +
		`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>`)
	fmt.Fprintf(&st, `<cellXfs count="%d"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>`, len(ids)+1)
	for _, id := range ids {
		fmt.Fprintf(&st, `<xf numFmtId="%d" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>`, id)
	}
	st.WriteString(`</cellXfs></styleSheet>`)

	var ss strings.Builder
	fmt.Fprintf(&ss, xml.Header+`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="%d" uniqueCount="%d">`, len(sst), len(sst))
	for _, s := range sst {
		fmt.Fprintf(&ss, `<si><t xml:space="preserve">%s</t></si>`, esc(s))
	}
	ss.WriteString("</sst>")

	parts := []struct{ name, body string }{
		{"[Content_Types].xml", ct.String()},
		{"_rels/.rels", xml.Header + `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`},
		{"xl/workbook.xml", wb.String()},
		{"xl/_rels/workbook.xml.rels", rels.String()},
		{"xl/styles.xml", st.String()},
		{"xl/sharedStrings.xml", ss.String()},
	}
	for i, s := range sheets {
		parts = append(parts, struct{ name, body string }{fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1), s})
	}
	for _, p := range parts {
		w, err := zw.Create(p.name)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write([]byte(p.body)); err != nil {
			f.Close()
			return err
		}
	}
	if err := zw.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func esc(s string) string {
	var sb strings.Builder
	xml.EscapeText(&sb, []byte(s))
	return sb.String()
}
