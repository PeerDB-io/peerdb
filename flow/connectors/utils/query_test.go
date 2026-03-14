package utils

import (
	"testing"
)

func TestBuildQuery(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected string
		err      bool
	}{
		{
			name:     "Date range in template",
			query:    "SELECT * FROM table WHERE date BETWEEN {{.start}} AND {{.end}}",
			expected: "SELECT * FROM table WHERE date BETWEEN $1 AND $2",
		},
		{
			name:     "Integer range in template",
			query:    "SELECT * FROM table WHERE id BETWEEN {{.start}} AND {{.end}}",
			expected: "SELECT * FROM table WHERE id BETWEEN $1 AND $2",
		},
		{
			name:     "Time range in template",
			query:    "SELECT * FROM table WHERE timestamp BETWEEN {{.start}} AND {{.end}}",
			expected: "SELECT * FROM table WHERE timestamp BETWEEN $1 AND $2",
		},
		{
			name:     "Float range in template",
			query:    "SELECT * FROM table WHERE value BETWEEN {{.start}} AND {{.end}}",
			expected: "SELECT * FROM table WHERE value BETWEEN $1 AND $2",
		},

		{
			name:  "Wrong template",
			query: "SELECT * FROM table WHERE value BETWEEN {{.start}} AND {{.end",
			err:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ExecuteTemplate(tc.query, map[string]string{"start": "$1", "end": "$2"})
			if !tc.err && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if tc.err && err == nil {
				t.Fatal("Expected error, got nil")
			}

			if actual != tc.expected {
				t.Fatalf("Expected query %q, got %q", tc.expected, actual)
			}
		})
	}
}
