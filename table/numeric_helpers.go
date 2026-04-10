package table

import (
	"sort"
	"strconv"
	"strings"
)

type numericEntry struct {
	valid      bool
	intOnly    bool
	intValue   int64
	floatValue float64
}

func parseNumericEntry(raw string) numericEntry {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return numericEntry{}
	}
	if i, ok := parseIntFast(trimmed); ok {
		return numericEntry{
			valid:      true,
			intOnly:    true,
			intValue:   i,
			floatValue: float64(i),
		}
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return numericEntry{}
	}
	return numericEntry{valid: true, floatValue: f}
}

func parseIntFast(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	start := 0
	if s[0] == '+' || s[0] == '-' {
		if len(s) == 1 {
			return 0, false
		}
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}

func denseRankValues(entries []numericEntry, asc bool) []string {
	allInts := true
	intSeen := make(map[int64]struct{}, len(entries))
	intVals := make([]int64, 0, len(entries))
	for _, entry := range entries {
		if !entry.valid {
			continue
		}
		if !entry.intOnly {
			allInts = false
			break
		}
		if _, ok := intSeen[entry.intValue]; ok {
			continue
		}
		intSeen[entry.intValue] = struct{}{}
		intVals = append(intVals, entry.intValue)
	}
	if allInts {
		sort.Slice(intVals, func(i, j int) bool {
			if asc {
				return intVals[i] < intVals[j]
			}
			return intVals[i] > intVals[j]
		})
		rankMap := make(map[int64]string, len(intVals))
		for i, v := range intVals {
			rankMap[v] = strconv.Itoa(i + 1)
		}
		ranks := make([]string, len(entries))
		for i, entry := range entries {
			if entry.valid {
				ranks[i] = rankMap[entry.intValue]
			}
		}
		return ranks
	}

	floatSeen := make(map[float64]struct{}, len(entries))
	floatVals := make([]float64, 0, len(entries))
	for _, entry := range entries {
		if !entry.valid {
			continue
		}
		if _, ok := floatSeen[entry.floatValue]; ok {
			continue
		}
		floatSeen[entry.floatValue] = struct{}{}
		floatVals = append(floatVals, entry.floatValue)
	}
	sort.Float64s(floatVals)
	if !asc {
		for i, j := 0, len(floatVals)-1; i < j; i, j = i+1, j-1 {
			floatVals[i], floatVals[j] = floatVals[j], floatVals[i]
		}
	}
	rankMap := make(map[float64]string, len(floatVals))
	for i, v := range floatVals {
		rankMap[v] = strconv.Itoa(i + 1)
	}
	ranks := make([]string, len(entries))
	for i, entry := range entries {
		if entry.valid {
			ranks[i] = rankMap[entry.floatValue]
		}
	}
	return ranks
}
