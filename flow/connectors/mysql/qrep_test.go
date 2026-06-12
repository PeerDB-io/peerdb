package connmysql

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildSelectedColumns(t *testing.T) {
	testCases := []struct {
		name                    string
		expectedSelectedColumns string
		cols                    []*protos.FieldDescription
		exclude                 []string
	}{
		{
			name: "no excluded columns, string enums",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "name", Type: string(types.QValueKindString)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                 []string{},
			expectedSelectedColumns: "*",
		},
		{
			name: "one excluded column",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "name", Type: string(types.QValueKindString)},
			},
			exclude:                 []string{"name"},
			expectedSelectedColumns: "`id`",
		},
		{
			name: "uint16enum column is cast to unsigned",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindUint16Enum)},
			},
			exclude:                 []string{},
			expectedSelectedColumns: "`id`, CAST(`status` AS UNSIGNED) AS `status`",
		},
		{
			name: "string enum column is not cast",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
			},
			exclude:                 []string{},
			expectedSelectedColumns: "*",
		},
		{
			name: "uint16enum with exclude",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindUint16Enum)},
				{Name: "created_at", Type: string(types.QValueKindTimestamp)},
			},
			exclude:                 []string{"created_at"},
			expectedSelectedColumns: "`id`, CAST(`status` AS UNSIGNED) AS `status`",
		},
		{
			name: "string enum with exclude",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt32)},
				{Name: "status", Type: string(types.QValueKindEnum)},
				{Name: "created_at", Type: string(types.QValueKindTimestamp)},
			},
			exclude:                 []string{"created_at"},
			expectedSelectedColumns: "`id`, `status`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selectedColumns := buildSelectedColumns(tc.cols, tc.exclude)
			if selectedColumns != tc.expectedSelectedColumns {
				t.Errorf("expected selected columns to be %s, but got %s", tc.expectedSelectedColumns, selectedColumns)
			}
		})
	}
}

func TestMySQLDefaultPartitionKeyEligible(t *testing.T) {
	for _, dataType := range []string{
		"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year",
		"date", "datetime", "timestamp", "BIGINT", "DateTime",
	} {
		if !supportsRangePartition(dataType) {
			t.Errorf("expected %q to be eligible as a default partition key", dataType)
		}
	}
	for _, dataType := range []string{
		"varchar", "char", "text", "binary", "varbinary", "decimal", "float", "double",
		"time", "enum", "set", "json", "bit", "",
	} {
		if supportsRangePartition(dataType) {
			t.Errorf("expected %q to not be eligible as a default partition key", dataType)
		}
	}
}

func TestMySQLQuotedList(t *testing.T) {
	if got := mysqlQuotedList([]string{"db"}); got != "'db'" {
		t.Errorf("mysqlQuotedList single = %q", got)
	}
	if got := mysqlQuotedList([]string{"a", "b'c"}); got != `'a','b\'c'` {
		t.Errorf("mysqlQuotedList escaping = %q", got)
	}
}
