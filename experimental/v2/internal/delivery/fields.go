package delivery

// Fields is one record split into fields: the bytes of all fields one after
// another in Buf, and the end of every field in Ends. A reader fills it in
// place of a slice of strings, so that reading a record allocates nothing
// (D113, G75). A Fields is reused from record to record.
type Fields struct {
	Buf  []byte
	Ends []int
}

// Reset empties f and keeps its storage.
func (f *Fields) Reset() {
	f.Buf, f.Ends = f.Buf[:0], f.Ends[:0]
}

// Len returns the number of fields.
func (f *Fields) Len() int { return len(f.Ends) }

// Field returns the bytes of field i. They are valid until f changes.
func (f *Fields) Field(i int) []byte {
	start := 0
	if i > 0 {
		start = f.Ends[i-1]
	}
	return f.Buf[start:f.Ends[i]:f.Ends[i]]
}

// End ends the field whose bytes were appended to Buf last.
func (f *Fields) End() { f.Ends = append(f.Ends, len(f.Buf)) }

// Append appends a field.
func (f *Fields) Append(s string) {
	f.Buf = append(f.Buf, s...)
	f.End()
}

// Strings returns the fields as strings, which are copies.
func (f *Fields) Strings() []string {
	s := string(f.Buf)
	out := make([]string, len(f.Ends))
	start := 0
	for i, end := range f.Ends {
		out[i] = s[start:end]
		start = end
	}
	return out
}
