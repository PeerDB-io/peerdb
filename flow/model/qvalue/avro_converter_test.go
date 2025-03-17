package qvalue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

func TestColumnNameAvroFieldConvert(t *testing.T) {
	testColumnNames := []string{
		"valid_column",             // Already valid
		"ColumnWithCaps",           // Mixed case
		"column-name",              // Hyphen should be replaced with _
		"column.name",              // Dot should be replaced with _
		"column@name",              // Special character should be replaced with _
		"!invalid_start",           // Invalid start character
		"123numericStart",          // Starts with a number, should be prefixed with _
		"UPPER_CASE_COLUMN",        // Already valid
		"column name",              // Space should be replaced with _
		"column$name",              // Special character should be replaced with _
		"column#name",              // Special character should be replaced with _
		"column&name",              // Special character should be replaced with _
		"123",                      // Fully numeric, should be prefixed with _
		"_already_valid",           // Already valid
		"column__name",             // Already valid with underscores
		"table.column",             // Dotted name (common in queries), should be replaced with _
		"column-name-with-hyphens", // Multiple hyphens
		" spaces  in  name ",       // Multiple spaces should be replaced
		"trailing_",                // Should remain unchanged
		"__leading_underscores",    // Already valid, should remain
		"CAPS-WITH-HYPHEN",         // Hyphen should be replaced with _
		"",                         // Empty input, should return "_"
	}

	expectedColumnNames := []string{
		"valid_column",
		"ColumnWithCaps",
		"column_name",
		"column_name",
		"column_name",
		"_invalid_start",
		"_123numericStart",
		"UPPER_CASE_COLUMN",
		"column_name",
		"column_name",
		"column_name",
		"column_name",
		"_123",
		"_already_valid",
		"column__name",
		"table_column",
		"column_name_with_hyphens",
		"_spaces__in__name_",
		"trailing_",
		"__leading_underscores",
		"CAPS_WITH_HYPHEN",
		"_",
	}

	for i, columnName := range testColumnNames {
		t.Run(columnName, func(t *testing.T) {
			assert.Equal(t, expectedColumnNames[i], qvalue.ConvertToAvroCompatibleName(columnName))
		})
	}
}
