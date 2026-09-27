package json

import (
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"

	"github.com/stefanbethge/gseq-table/table"
	"github.com/stefanbethge/gseq/slice"
)

// Stream reads JSON from rd and yields one table.Row per object, without
// loading the whole input into memory. Both JSON-array and NDJSON input are
// supported; all options apply exactly as in Read.
//
// Headers accumulate while streaming: each Row carries the columns seen so
// far (first-seen order, or alphabetical with WithSortedHeaders), and keys
// missing from an object read as "". When every object has the same keys,
// the Rows match those produced by Read. Header slices are never mutated, so
// Rows yielded earlier keep their snapshot when new keys appear later.
//
// The iterator stops early if the caller returns false from yield or an error
// is encountered; the error is yielded once with a zero Row. If rd contains
// no objects, nothing is yielded.
//
//	for row, err := range json.New(json.WithNDJSON()).Stream(f) {
//	    if err != nil { log.Fatal(err) }
//	    fmt.Println(row.Get("name").UnwrapOr(""))
//	}
func (r *Reader) Stream(rd io.Reader) iter.Seq2[table.Row, error] {
	return func(yield func(table.Row, error) bool) {
		var ex *rowExtractor
		for rec, err := range r.records(rd) {
			if err == nil && ex == nil {
				ex, err = r.newRowExtractor()
			}
			if err != nil {
				yield(table.Row{}, err)
				return
			}
			row := ex.extract(rec)
			if !yield(table.NewRow(ex.headers, ex.values(row)), nil) {
				return
			}
		}
	}
}

// StreamFile opens the file at path and streams it row by row. See Stream for
// details. The file is closed when iteration ends.
func (r *Reader) StreamFile(path string) iter.Seq2[table.Row, error] {
	return func(yield func(table.Row, error) bool) {
		f, err := os.Open(path)
		if err != nil {
			yield(table.Row{}, err)
			return
		}
		defer f.Close()
		for row, err := range r.Stream(f) {
			if !yield(row, err) {
				return
			}
		}
	}
}

// ReadStream reads JSON from rd and yields chunks of at most chunkSize rows as
// Tables. chunkSize <= 0 defaults to 1000. Headers accumulate as in Stream:
// each chunk carries every column seen up to and including its last row, so
// a later chunk may have more columns than an earlier one. When all keys
// appear in the first chunk, concatenating the chunks yields the same rows
// as Read.
//
// The iterator stops early if the caller returns false from yield or an error
// is encountered. If rd contains no objects, no Tables are yielded.
//
//	for t, err := range json.New(json.WithNDJSON()).ReadStream(f, 1000) {
//	    if err != nil { log.Fatal(err) }
//	    process(t)
//	}
func (r *Reader) ReadStream(rd io.Reader, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		if chunkSize <= 0 {
			chunkSize = 1000
		}
		var ex *rowExtractor
		chunk := make([]map[string]string, 0, chunkSize)
		for rec, err := range r.records(rd) {
			if err == nil && ex == nil {
				ex, err = r.newRowExtractor()
			}
			if err != nil {
				yield(table.Table{}, err)
				return
			}
			chunk = append(chunk, ex.extract(rec))
			if len(chunk) >= chunkSize {
				if !yield(ex.table(chunk), nil) {
					return
				}
				chunk = chunk[:0]
			}
		}
		if len(chunk) > 0 {
			yield(ex.table(chunk), nil)
		}
	}
}

// ReadFileStream opens the file at path and streams it in chunks of chunkSize
// rows. See ReadStream for details. The basename of path is attached as each
// chunk's source name, as in ReadFile.
func (r *Reader) ReadFileStream(path string, chunkSize int) iter.Seq2[table.Table, error] {
	return func(yield func(table.Table, error) bool) {
		f, err := os.Open(path)
		if err != nil {
			yield(table.Table{}, err)
			return
		}
		defer f.Close()
		source := filepath.Base(path)
		for t, err := range r.ReadStream(f, chunkSize) {
			if err == nil {
				t = t.WithSource(source)
			}
			if !yield(t, err) {
				return
			}
		}
	}
}

// records returns the record iterator for the configured input mode.
func (r *Reader) records(rd io.Reader) iter.Seq2[record, error] {
	if r.config.NDJSON {
		return iterNDJSON(rd)
	}
	return iterArray(rd)
}

// rowExtractor converts records into flat rows one at a time, tracking the
// cumulative header set of a stream.
type rowExtractor struct {
	flatten  bool
	sep      string
	maxDepth int
	sorted   bool
	mapping  []parsedMapping
	headers  slice.Slice[string]
	seen     map[string]bool
}

// newRowExtractor validates the Reader's options and prepares an extractor.
// With WithFieldMapping the headers are fixed up front.
func (r *Reader) newRowExtractor() (*rowExtractor, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	ex := &rowExtractor{
		flatten:  r.config.Flatten,
		sep:      r.config.FlattenSeparator,
		maxDepth: r.config.MaxDepth,
		sorted:   r.config.SortedHeaders,
		seen:     make(map[string]bool),
	}
	if ex.sep == "" {
		ex.sep = "."
	}
	if len(r.config.FieldMapping) > 0 {
		parsed, err := parseMappings(r.config.FieldMapping)
		if err != nil {
			return nil, err
		}
		ex.mapping = parsed
		ex.headers = make(slice.Slice[string], len(parsed))
		for i, pm := range parsed {
			ex.headers[i] = pm.col
		}
	}
	return ex, nil
}

// extract returns the flat row for rec and extends the header set with any
// keys rec introduces.
func (ex *rowExtractor) extract(rec record) map[string]string {
	switch {
	case ex.mapping != nil:
		return mapRecord(rec, ex.mapping)
	case ex.flatten:
		row := make(map[string]string)
		ex.addKeys(flattenRecord(rec, "", ex.sep, 0, ex.maxDepth, row))
		return row
	default:
		ex.addKeys(rec.keys)
		return defaultRecord(rec)
	}
}

// addKeys appends unseen keys to the header set. The header slice is replaced
// rather than grown in place so previously yielded Rows keep their snapshot.
func (ex *rowExtractor) addKeys(keys []string) {
	var grown slice.Slice[string]
	for _, k := range keys {
		if ex.seen[k] {
			continue
		}
		ex.seen[k] = true
		if grown == nil {
			grown = ex.headers[:len(ex.headers):len(ex.headers)]
		}
		grown = append(grown, k)
	}
	if grown == nil {
		return
	}
	if ex.sorted {
		sort.Strings(grown)
	}
	ex.headers = grown
}

// values lays row out in the current header order; missing keys become "".
func (ex *rowExtractor) values(row map[string]string) slice.Slice[string] {
	vals := make(slice.Slice[string], len(ex.headers))
	for i, h := range ex.headers {
		vals[i] = row[h]
	}
	return vals
}

// table builds a Table from rows using the current header set.
func (ex *rowExtractor) table(rows []map[string]string) table.Table {
	records := make([][]string, len(rows))
	for i, row := range rows {
		records[i] = ex.values(row)
	}
	return table.New(ex.headers, records)
}
