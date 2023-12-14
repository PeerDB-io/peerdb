package utils

import (
	"strings"

	"golang.org/x/exp/maps"
)

func KeysToString(m map[string]struct{}) string {
	if m == nil {
		return ""
	}

	return strings.Join(maps.Keys(m), ",")
}
