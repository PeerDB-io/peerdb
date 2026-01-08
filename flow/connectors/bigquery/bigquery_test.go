package connbigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatasetTableStringQuoted(t *testing.T) {
	testCases := []struct {
		datasetTable datasetTable
		expected     string
	}{
		{
			datasetTable: datasetTable{
				dataset: "my_dataset",
				table:   "my_table",
			},
			expected: "`my_dataset`.`my_table`",
		},
		{
			datasetTable: datasetTable{
				dataset: `data"set`,
				table:   `ta"ble`,
			},
			expected: "`data\"set`.`ta\"ble`",
		},
		{
			datasetTable: datasetTable{
				dataset: "data`set",
				table:   "ta`ble",
			},
			expected: "`data\\`set`.`ta\\`ble`",
		},
		{
			datasetTable: datasetTable{
				project: "my_project`; DROP TABLE users;--",
				dataset: "my_dataset",
				table:   "my_table",
			},
			expected: "`my_project\\`; DROP TABLE users;--`.`my_dataset`.`my_table`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.datasetTable.stringQuoted())
		})
	}
}
