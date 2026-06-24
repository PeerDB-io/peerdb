package connmysql

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

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

func TestGetDefaultPartitionKeyForTables(t *testing.T) {
	c := &MySqlConnector{logger: log.NewStructuredLogger(slog.Default())}

	tableMapping := func(name string) *protos.TableMapping {
		return &protos.TableMapping{SourceTableIdentifier: name, DestinationTableIdentifier: name}
	}

	fieldDesc := func(name string, qkind types.QValueKind) *protos.FieldDescription {
		return &protos.FieldDescription{Name: name, Type: string(qkind)}
	}

	testCases := []struct {
		schemas       map[string]*protos.TableSchema
		expected      map[string]string
		name          string
		tableMappings []*protos.TableMapping
	}{
		{
			name:          "integer primary key is supported",
			tableMappings: []*protos.TableMapping{tableMapping("db.ints")},
			schemas: map[string]*protos.TableSchema{
				"db.ints": {
					PrimaryKeyColumns: []string{"id"},
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindInt64)},
				},
			},
			expected: map[string]string{"db.ints": "id"},
		},
		{
			name:          "timestamp primary key is supported",
			tableMappings: []*protos.TableMapping{tableMapping("db.ts")},
			schemas: map[string]*protos.TableSchema{
				"db.ts": {
					PrimaryKeyColumns: []string{"created_at"},
					Columns:           []*protos.FieldDescription{fieldDesc("created_at", types.QValueKindTimestamp)},
				},
			},
			expected: map[string]string{"db.ts": "created_at"},
		},
		{
			name:          "composite primary key with valid first column",
			tableMappings: []*protos.TableMapping{tableMapping("db.composite")},
			schemas: map[string]*protos.TableSchema{
				"db.composite": {
					PrimaryKeyColumns: []string{"id", "created_at"},
					Columns: []*protos.FieldDescription{
						fieldDesc("id", types.QValueKindInt32),
						fieldDesc("created_at", types.QValueKindTimestamp),
					},
				},
			},
			expected: map[string]string{"db.composite": "id"},
		},
		{
			name:          "composite primary key with invalid first column",
			tableMappings: []*protos.TableMapping{tableMapping("db.composite2")},
			schemas: map[string]*protos.TableSchema{
				"db.composite2": {
					PrimaryKeyColumns: []string{"name", "id"},
					Columns: []*protos.FieldDescription{
						fieldDesc("name", types.QValueKindString),
						fieldDesc("id", types.QValueKindInt32),
					},
				},
			},
			expected: map[string]string{},
		},
		{
			name:          "no primary key",
			tableMappings: []*protos.TableMapping{tableMapping("db.nopk")},
			schemas: map[string]*protos.TableSchema{
				"db.nopk": {
					PrimaryKeyColumns: nil,
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindInt64)},
				},
			},
			expected: map[string]string{},
		},
		{
			name: "multiple table schemas",
			tableMappings: []*protos.TableMapping{
				tableMapping("db.a"),
				tableMapping("db.b"),
				tableMapping("db.c"),
			},
			schemas: map[string]*protos.TableSchema{
				"db.a": {
					PrimaryKeyColumns: []string{"id"},
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindInt16)},
				},
				"db.b": {
					PrimaryKeyColumns: []string{"uuid"},
					Columns: []*protos.FieldDescription{
						fieldDesc("uuid", types.QValueKindString),
					},
				},
				"db.c": {
					PrimaryKeyColumns: []string{"date"},
					Columns:           []*protos.FieldDescription{fieldDesc("date", types.QValueKindDate)},
				},
			},
			expected: map[string]string{"db.a": "id", "db.c": "date"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := c.GetDefaultPartitionKeyForTables(context.Background(), &protos.GetDefaultPartitionKeyForTablesInput{
				TableMappings:      tc.tableMappings,
				TableSchemaMapping: tc.schemas,
			})
			require.NoError(t, err)
			require.NotNil(t, output)
			require.Equal(t, tc.expected, output.TableDefaultPartitionKeyMapping)
		})
	}
}
