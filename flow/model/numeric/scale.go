package numeric

import (
	"strings"
)

const (
	PeerDBNumericPrecision = 38
	PeerDBNumericScale     = 9
)

func StripTrailingZeros(value string) string {
	value = strings.TrimRight(value, "0")
	value = strings.TrimSuffix(value, ".")
	return value
}

func ParseNumericTypmod(typmod int32) (int, int) {
	offsetMod := typmod - 4
	precision := int((offsetMod >> 16) & 0x7FFF)
	scale := int(offsetMod & 0x7FFF)
	return precision, scale
}
