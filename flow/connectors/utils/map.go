package utils

import (
	"maps"
	"slices"
	"strings"
)

func KeysToString(m map[string]struct{}) string {
	if m == nil {
		return ""
	}

	return strings.Join(slices.Sorted(maps.Keys(m)), ",")
}
