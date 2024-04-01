package shared

import "unsafe"

func UnsafeFastStringToReadOnlyBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeFastReadOnlyBytesToString(s []byte) string {
	return unsafe.String(unsafe.SliceData(s), len(s))
}
