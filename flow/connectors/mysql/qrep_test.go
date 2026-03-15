package connmysql

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildSelectedColumns(t *testing.T) {
	testCases := []struct {
		name                      string
		expectedSelectedColumns   string
		cols                      []*protos.FieldDescription
		exclude                   []string
		isBinlogMetadataSupported bool
		mirrorVersion             uint32
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
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts,
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
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts,
			expectedSelectedColumns:   "`id`",
		},
		{
			name: "one enum column, binlog metadata not supported, new version",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                   []string{},
			isBinlogMetadataSupported: false,
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts,
			expectedSelectedColumns:   "`id`, CAST(`status` AS UNSIGNED) AS `status`",
		},
		{
			name: "one enum column, binlog metadata not supported, old version",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                   []string{},
			isBinlogMetadataSupported: false,
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts - 1,
			expectedSelectedColumns:   "*",
		},
		{
			name: "one enum column, binlog metadata not supported, version zero",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                   []string{},
			isBinlogMetadataSupported: false,
			mirrorVersion:             0,
			expectedSelectedColumns:   "*",
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
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts,
			expectedSelectedColumns:   "`id`, `status`",
		},
		{
			name: "enum with exclude, binlog metadata not supported, old version",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
				{Name: "created_at", Type: string(types.QValueKindTimestamp)},
			},
			exclude:                   []string{"created_at"},
			isBinlogMetadataSupported: false,
			mirrorVersion:             shared.InternalVersion_MySQLConvertEnumsToInts - 1,
			expectedSelectedColumns:   "`id`, `status`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selectedColumns := buildSelectedColumns(tc.cols, tc.exclude, tc.isBinlogMetadataSupported, tc.mirrorVersion)
			if selectedColumns != tc.expectedSelectedColumns {
				t.Errorf("expected selected columns to be %s, but got %s", tc.expectedSelectedColumns, selectedColumns)
			}
		})
	}
}
