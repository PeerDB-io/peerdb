package connmysql

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildSelectedColumns(t *testing.T) {
	testCases := []struct {
		name                      string
		expectedSelectedColumns   string
		cols                      []*protos.FieldDescription
		exclude                   []string
		isBinlogMetadataSupported bool
	}{
		{
			name: "no excluded columns, binlog metadata supported",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "name", Type: string(types.QValueKindString)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                   []string{},
			isBinlogMetadataSupported: true,
			expectedSelectedColumns:   "*",
		},
		{
			name: "one excluded column, binlog metadata supported",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "name", Type: string(types.QValueKindString)},
			},
			exclude:                   []string{"name"},
			isBinlogMetadataSupported: true,
			expectedSelectedColumns:   "`id`",
		},
		{
			name: "one enum column, binlog metadata not supported",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                   []string{},
			isBinlogMetadataSupported: false,
			expectedSelectedColumns:   "`id`, `status` + 0 as `status`",
		},
		{
			name: "one enum column, one excluded non enum column, binlog metadata supported",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
				{Name: "created_at", Type: string(types.QValueKindTimestamp)},
			},
			exclude:                   []string{"created_at"},
			isBinlogMetadataSupported: true,
			expectedSelectedColumns:   "`id`, `status`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selectedColumns := buildSelectedColumns(tc.cols, tc.exclude, tc.isBinlogMetadataSupported)
			if selectedColumns != tc.expectedSelectedColumns {
				t.Errorf("expected selected columns to be %s, but got %s", tc.expectedSelectedColumns, selectedColumns)
			}
		})
	}
}
