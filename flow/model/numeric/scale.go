package numeric

import "strings"

const PeerDBNumericScale = 9

func StripTrailingZeros(value string) string {
	value = strings.TrimRight(value, "0")
	value = strings.TrimSuffix(value, ".")
	return value
}
