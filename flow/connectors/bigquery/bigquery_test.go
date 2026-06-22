package connbigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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

func TestConvertToDatasetTable(t *testing.T) {
	c := &BigQueryConnector{datasetID: "default_dataset"}

	testCases := []struct {
		name     string
		input    common.QualifiedTable
		expected datasetTable
		errors   bool
	}{
		{
			name:     "table only falls back to connector dataset",
			input:    common.QualifiedTable{Table: "my_table"},
			expected: datasetTable{dataset: "default_dataset", table: "my_table"},
		},
		{
			name:     "dataset and table",
			input:    common.QualifiedTable{Namespace: "my_dataset", Table: "my_table"},
			expected: datasetTable{dataset: "my_dataset", table: "my_table"},
		},
		{
			name:     "project packed into namespace",
			input:    common.QualifiedTable{Namespace: "my_project.my_dataset", Table: "my_table"},
			expected: datasetTable{project: "my_project", dataset: "my_dataset", table: "my_table"},
		},
		{
			name:     "legacy 3-part identifier split at first dot",
			input:    common.QualifiedTable{Namespace: "my_project", Table: "my_dataset.my_table"},
			expected: datasetTable{project: "my_project", dataset: "my_dataset", table: "my_table"},
		},
		{
			name:     "legacy 2-part identifier packed into table",
			input:    common.QualifiedTable{Table: "my_dataset.my_table"},
			expected: datasetTable{dataset: "my_dataset", table: "my_table"},
		},
		{
			name:   "four part identifier errors",
			input:  common.QualifiedTable{Namespace: "a.b.c", Table: "my_table"},
			errors: true,
		},
		{
			name:   "dotted namespace and dotted table errors",
			input:  common.QualifiedTable{Namespace: "my_project.my_dataset", Table: "my_table.extra"},
			errors: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.convertToDatasetTable(tc.input)
			if tc.errors {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
