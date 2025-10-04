package mysql

import "testing"

func TestPrettySize(t *testing.T) {
	tests := []struct {
		text string
		val  int64
	}{
		{"1024 bytes", 1024},
		{"977 kB", 1000000},
		{"954 MB", 1000000000},
		{"931 GB", 1000000000000},
		{"909 TB", 1000000000000000},
		{"10239 bytes", 10239},
		{"10 kB", 10240},
		{"10239 kB", 10485247},
		{"10 MB", 10485248},
		{"10239 MB", 10736893951},
		{"10 GB", 10736893952},
		{"10239 GB", 10994579406847},
		{"10 TB", 10994579406848},
		{"10239 TB", 11258449312612351},
		{"10 PB", 11258449312612352},
		{"8192 PB", 9223372036854775807},
	}

	for _, tc := range tests {
		got := PrettyBytes(tc.val)
		if got != tc.text {
			t.Errorf("PrettySize(%d) = %s; expected %s", tc.val, got, tc.text)
		}
	}
}
