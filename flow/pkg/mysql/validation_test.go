package mysql

import "testing"

func TestIsCompressedColumnType(t *testing.T) {
	for _, tc := range []struct {
		columnType string
		want       bool
	}{
		{"varchar(100) /*M!100301 COMPRESSED*/", true},
		{"text /*M!100301 COMPRESSED*/", true},
		{"blob /*M!100301 COMPRESSED*/", true},
		// version-agnostic and case-insensitive
		{"tinytext /*M!100701 compressed*/", true},
		{"varchar(100)", false},
		{"text", false},
		{"blob", false},
		{"int", false},
	} {
		t.Run(tc.columnType, func(t *testing.T) {
			if got := IsCompressedColumnType(tc.columnType); got != tc.want {
				t.Errorf("IsCompressedColumnType(%q) = %v, want %v", tc.columnType, got, tc.want)
			}
		})
	}
}
