package json

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
)

// record is a decoded JSON object that preserves key insertion order.
type record struct {
	fields map[string]any
	keys   []string // insertion order
}

// decodeArray reads a JSON array of objects from rd.
// Each element must be a JSON object; non-object elements cause an error.
func decodeArray(rd io.Reader) ([]record, error) {
	dec := stdjson.NewDecoder(rd)
	dec.UseNumber()

	// Expect opening bracket.
	tok, err := dec.Token()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(stdjson.Delim)
	if !ok || delim != '[' {
		return nil, errors.New("json: expected JSON array at top level")
	}

	var records []record
	idx := 0
	for dec.More() {
		rec, err := decodeObject(dec, idx)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		idx++
	}

	// Consume closing bracket.
	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	return records, nil
}

// decodeNDJSON reads newline-delimited JSON (one object per line) from rd.
// Empty lines are silently skipped by the decoder.
func decodeNDJSON(rd io.Reader) ([]record, error) {
	dec := stdjson.NewDecoder(rd)
	dec.UseNumber()

	var records []record
	idx := 0
	for dec.More() {
		rec, err := decodeObject(dec, idx)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		idx++
	}
	return records, nil
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
