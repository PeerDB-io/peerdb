package utils

import (
	"slices"
	"sort"
	"strings"

	"golang.org/x/exp/maps"
)

func KeysToString(m map[string]struct{}) string {
	if m == nil {
		return ""
	}

	sm := maps.Keys(m)
	sort.Strings(sm)
	slices.Sort[[]string](sm)
	return strings.Join(sm, ",")
}
