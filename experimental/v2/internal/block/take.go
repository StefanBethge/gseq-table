package block

import "fmt"

// Take returns a new column with one owner that holds the cells rows of c,
// in that order. A row may appear more than once, as after a 1:n join; -1
// yields a null cell, as for a left join without a partner.
func (c Column) Take(rows []int) Column {
	b := NewBuilder(c.d.kind, len(rows))
	for _, r := range rows {
		if r < 0 {
			b.AppendNull()
			continue
		}
		c.checkIndex(r)
		if c.d.isNull(r) {
			b.AppendNull()
			continue
		}
		switch c.d.kind {
		case Text:
			b.AppendText(c.d.texts[r])
		case Int:
			b.AppendInt(c.d.ints[r])
		case Float:
			b.AppendFloat(c.d.floats[r])
		case Bool:
			b.AppendBool(c.d.bools[r])
		case Timestamp:
			b.AppendTimestamp(c.d.times[r])
		}
	}
	return b.Build()
}

// Concat returns a new column with one owner that holds the cells of cols one
// after another. All columns must have kind k.
func Concat(k Kind, cols ...Column) Column {
	n := 0
	for _, c := range cols {
		if c.d.kind != k {
			panic(fmt.Sprintf("block: concat of a %s column into %s", c.d.kind, k))
		}
		n += c.d.length
	}
	b := NewBuilder(k, n)
	for _, c := range cols {
		for i := range c.d.length {
			if c.d.isNull(i) {
				b.AppendNull()
				continue
			}
			switch k {
			case Text:
				b.AppendText(c.d.texts[i])
			case Int:
				b.AppendInt(c.d.ints[i])
			case Float:
				b.AppendFloat(c.d.floats[i])
			case Bool:
				b.AppendBool(c.d.bools[i])
			case Timestamp:
				b.AppendTimestamp(c.d.times[i])
			}
		}
	}
	return b.Build()
}

// Take returns a new block with the rows of b, in that order (see
// Column.Take).
func (b Block) Take(rows []int) Block {
	out := Block{length: len(rows), cols: make([]Column, len(b.cols))}
	for i, c := range b.cols {
		out.cols[i] = c.Take(rows)
	}
	return out
}
