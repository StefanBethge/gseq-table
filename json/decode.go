package json

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
)

// record is a decoded JSON object that preserves key insertion order.
type record struct {
	fields map[string]any
	keys   []string // insertion order
}

// decodeArray reads a JSON array of objects from rd.
// Each element must be a JSON object; non-object elements cause an error.
func decodeArray(rd io.Reader) ([]record, error) {
	return collectRecords(iterArray(rd))
}

// decodeNDJSON reads newline-delimited JSON (one object per line) from rd.
// Empty lines are silently skipped by the decoder.
func decodeNDJSON(rd io.Reader) ([]record, error) {
	return collectRecords(iterNDJSON(rd))
}

// collectRecords drains seq into a slice, stopping at the first error.
func collectRecords(seq iter.Seq2[record, error]) ([]record, error) {
	var records []record
	for rec, err := range seq {
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// iterArray yields the objects of a top-level JSON array one at a time.
// An empty input yields nothing. The first error is yielded once and ends
// the sequence.
func iterArray(rd io.Reader) iter.Seq2[record, error] {
	return func(yield func(record, error) bool) {
		dec := stdjson.NewDecoder(rd)
		dec.UseNumber()

		// Expect opening bracket.
		tok, err := dec.Token()
		if err == io.EOF {
			return
		}
		if err != nil {
			yield(record{}, err)
			return
		}
		delim, ok := tok.(stdjson.Delim)
		if !ok || delim != '[' {
			yield(record{}, errors.New("json: expected JSON array at top level"))
			return
		}

		idx := 0
		for dec.More() {
			rec, err := decodeObject(dec, idx)
			if err != nil {
				yield(record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
			idx++
		}

		// Consume closing bracket.
		if _, err := dec.Token(); err != nil {
			yield(record{}, err)
		}
	}
}

// iterNDJSON yields newline-delimited JSON objects one at a time.
// Empty lines are silently skipped by the decoder. The first error is
// yielded once and ends the sequence.
func iterNDJSON(rd io.Reader) iter.Seq2[record, error] {
	return func(yield func(record, error) bool) {
		dec := stdjson.NewDecoder(rd)
		dec.UseNumber()

		idx := 0
		for dec.More() {
			rec, err := decodeObject(dec, idx)
			if err != nil {
				yield(record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
			idx++
		}
	}
}

// decodeObject decodes a single JSON object from the decoder, preserving
// key order. Returns a clear error if the element is not an object.
func decodeObject(dec *stdjson.Decoder, idx int) (record, error) {
	// Peek at the next token to check it's an object.
	tok, err := dec.Token()
	if err != nil {
		return record{}, fmt.Errorf("json: record %d: %w", idx, err)
	}
	delim, ok := tok.(stdjson.Delim)
	if !ok || delim != '{' {
		return record{}, fmt.Errorf("json: record %d: expected JSON object, got %T", idx, tok)
	}

	fields := make(map[string]any)
	var keys []string

	for dec.More() {
		// Read key.
		keyTok, err := dec.Token()
		if err != nil {
			return record{}, fmt.Errorf("json: record %d: %w", idx, err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return record{}, fmt.Errorf("json: record %d: expected string key, got %T", idx, keyTok)
		}

		// Read value.
		var val any
		if err := dec.Decode(&val); err != nil {
			return record{}, fmt.Errorf("json: record %d: key %q: %w", idx, key, err)
		}

		// Duplicate keys: last value wins (encoding/json behavior), but
		// only add key to order slice on first occurrence.
		if _, exists := fields[key]; !exists {
			keys = append(keys, key)
		}
		fields[key] = val
	}

	// Consume closing brace.
	if _, err := dec.Token(); err != nil {
		return record{}, fmt.Errorf("json: record %d: %w", idx, err)
	}

	return record{fields: fields, keys: keys}, nil
}
