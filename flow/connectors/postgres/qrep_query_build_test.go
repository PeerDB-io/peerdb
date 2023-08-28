package connpostgres

import (
	"testing"
)

func TestBuildQuery(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected string
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := BuildQuery(tc.query, "test_flow")
			if err != nil {
				t.Fatalf("Error returned by BuildQuery: %v", err)
			}

			if actual != tc.expected {
				t.Fatalf("Expected query %q, got %q", tc.expected, actual)
			}
		})
	}
}
