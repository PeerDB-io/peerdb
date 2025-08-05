package shared

import (
	"net"
	"regexp"
	"strconv"
	"unsafe"

	"golang.org/x/exp/constraints"
)

func UnsafeFastStringToReadOnlyBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeFastReadOnlyBytesToString(s []byte) string {
	return unsafe.String(unsafe.SliceData(s), len(s))
}

var (
	reIllegalIdentifierCharacters = regexp.MustCompile("[^a-zA-Z0-9_]+")
	reLegalIdentifierLower        = regexp.MustCompile("^[a-z_][a-z0-9_]*$")
)

func ReplaceIllegalCharactersWithUnderscores(s string) string {
	return reIllegalIdentifierCharacters.ReplaceAllString(s, "_")
}

func IsValidReplicationName(s string) bool {
	return reLegalIdentifierLower.MatchString(s)
}

func JoinHostPort[I constraints.Integer](host string, port I) string {
	return net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
}
