package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractSingleQuotedStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "standard distributed engine_full",
			input:    "Distributed('cicluster', 'mydb', 'mytable_shard', rand())",
			expected: []string{"cicluster", "mydb", "mytable_shard"},
		},
		{
			name:     "shard table with timestamp suffix",
			input:    "Distributed('cicluster', 'e2e_test_abc', 'test_table_shard1710425678', rand())",
			expected: []string{"cicluster", "e2e_test_abc", "test_table_shard1710425678"},
		},
		{
			name:     "with policy parameter",
			input:    "Distributed('cluster', 'db', 'shard_tbl', rand(), 'default')",
			expected: []string{"cluster", "db", "shard_tbl", "default"},
		},
		{
			name:     "escaped single quote",
			input:    `Distributed('clust\'er', 'db', 'tbl')`,
			expected: []string{"clust'er", "db", "tbl"},
		},
		{
			name:     "escaped backslash",
			input:    `Distributed('clus\\ter', 'db', 'tbl')`,
			expected: []string{"clus\\ter", "db", "tbl"},
		},
		{
			name:     "escaped tab and newline",
			input:    `Distributed('a\tb', 'c\nd', 'tbl')`,
			expected: []string{"atb", "cnd", "tbl"},
		},
		{
			name:     "escaped backtick",
			input:    "Distributed('a\\`b', 'db', 'tbl')",
			expected: []string{"a`b", "db", "tbl"},
		},
		{
			name:     "no quotes",
			input:    "ReplacingMergeTree()",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single unclosed quote",
			input:    "'abc",
			expected: nil,
		},
		{
			name:     "backslash at end of quoted string",
			input:    `'abc\`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSingleQuotedStrings(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
