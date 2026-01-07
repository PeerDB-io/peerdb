package connbigquery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple table name",
			input:    "my_table",
			expected: "`my_table`",
		},
		{
			name:     "dataset.table",
			input:    "dataset.table",
			expected: "`dataset`.`table`",
		},
		{
			name:     "backtick injection attempt",
			input:    "table`; DROP TABLE users; --",
			expected: "`table``; DROP TABLE users; --`",
		},
		{
			name:     "multiple backticks",
			input:    "a]b``c```d",
			expected: "`a]b````c``````d`",
		},
		{
			name:     "special chars preserved",
			input:    "table-name with spaces!@#$%",
			expected: "`table-name with spaces!@#$%`",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := quoteIdentifier(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
