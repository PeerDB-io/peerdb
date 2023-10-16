package utils

import "strings"

func KeysToString(m map[string]struct{}) string {
	if m == nil {
		return ""
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return strings.Join(keys, ",")
}
