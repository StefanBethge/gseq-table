// Package cell holds the parsing and formatting rules for table cell strings.
//
// It is the single implementation shared by the schema package (inference,
// normalization, typed row accessors) and the typed generic methods in the
// table package, so both always agree on what a valid int, float, bool or
// date looks like.
//
// Rules:
//   - surrounding whitespace is trimmed before parsing
//   - booleans accept true/false, 1/0 and yes/no (case-insensitive)
//   - dates are tried against DateLayouts in order; the zero time
//     ("0001-01-01") counts as not parsed
//   - floats are formatted in their shortest round-tripping form
package cell

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// DateLayouts are the layouts ParseDate tries, in order.
var DateLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02",
	"02.01.2006",
	"01/02/2006",
	"02 Jan 2006",
	"Jan 02, 2006",
}

// DateFormat is the canonical output layout for dates.
const DateFormat = "2006-01-02"

var (
	errNoDateLayout = errors.New("no date layout matches")
	errZeroDate     = errors.New("zero date is treated as not parsed")
)

// ParseInt parses s as a base-10 integer that fits in bitSize bits.
func ParseInt(s string, bitSize int) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, bitSize)
}

// ParseUint parses s as a base-10 unsigned integer that fits in bitSize bits.
func ParseUint(s string, bitSize int) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(s), 10, bitSize)
}

// ParseFloat parses s as a floating-point number of the given bitSize.
func ParseFloat(s string, bitSize int) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), bitSize)
}

// ParseBool parses s as a boolean. The second result is false if s is not a
// recognised boolean value.
func ParseBool(s string) (value, ok bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes":
		return true, true
	case "false", "0", "no":
		return false, true
	}
	return false, false
}

// ParseDate parses s using the first matching layout in DateLayouts.
//
// A value that parses to the zero time (time.Time.IsZero, e.g. "0001-01-01")
// is rejected with an error. The zero time doubles as the "no date" marker in
// schema, so it is never reported as a successfully parsed date.
func ParseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range DateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			if t.IsZero() {
				return time.Time{}, errZeroDate
			}
			return t, nil
		}
	}
	return time.Time{}, errNoDateLayout
}

// ParseDateLayout parses s with an explicit layout.
func ParseDateLayout(layout, s string) (time.Time, error) {
	return time.Parse(layout, strings.TrimSpace(s))
}

// FormatInt formats n in base 10.
func FormatInt(n int64) string { return strconv.FormatInt(n, 10) }

// FormatUint formats n in base 10.
func FormatUint(n uint64) string { return strconv.FormatUint(n, 10) }

// FormatFloat formats f in its shortest round-tripping decimal form.
func FormatFloat(f float64, bitSize int) string {
	return strconv.FormatFloat(f, 'f', -1, bitSize)
}

// FormatBool formats b as "true" or "false".
func FormatBool(b bool) string { return strconv.FormatBool(b) }

// FormatDate formats t using DateFormat.
func FormatDate(t time.Time) string { return t.Format(DateFormat) }

// FormatTime formats t as a date (DateFormat) when it is midnight UTC, and as
// RFC 3339 otherwise, so values with a time or zone component survive a round
// trip through ParseDate.
func FormatTime(t time.Time) string {
	if t.Location() == time.UTC && t.Equal(t.Truncate(24*time.Hour)) {
		return FormatDate(t)
	}
	return t.Format(time.RFC3339Nano)
}
