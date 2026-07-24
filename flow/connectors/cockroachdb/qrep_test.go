package conncockroachdb

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildSelectedColumns(t *testing.T) {
	testCases := []struct {
		name     string
		expected string
		cols     []*protos.FieldDescription
		exclude  []string
	}{
		{
			name: "no excluded columns",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt64)},
				{Name: "name", Type: string(types.QValueKindString)},
			},
			exclude:  nil,
			expected: `"id", "name"`,
		},
		{
			name: "one excluded column",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt64)},
				{Name: "secret", Type: string(types.QValueKindString)},
			},
			exclude:  []string{"secret"},
			expected: `"id"`,
		},
		{
			name: "identifier quoting",
			cols: []*protos.FieldDescription{
				{Name: "select", Type: string(types.QValueKindInt64)},
				{Name: `we"ird`, Type: string(types.QValueKindString)},
			},
			exclude:  nil,
			expected: `"select", "we""ird"`,
		},
		{
			name: "all columns excluded",
			cols: []*protos.FieldDescription{
				{Name: "id", Type: string(types.QValueKindInt64)},
			},
			exclude:  []string{"id"},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, buildSelectedColumns(tc.cols, tc.exclude))
		})
	}
}

func TestBuildMinMaxQuery(t *testing.T) {
	table := &common.QualifiedTable{Namespace: "public", Table: "events"}
	const systemTime = "1712345678901234567.0000000001"

	testCases := []struct {
		name      string
		expected  string
		resuming  bool
		withCount bool
	}{
		{
			name:     "fresh start",
			expected: `SELECT MIN("id"),MAX("id") FROM "public"."events" AS OF SYSTEM TIME '1712345678901234567.0000000001'`,
		},
		{
			name:     "resuming",
			resuming: true,
			expected: `SELECT MIN("id"),MAX("id") FROM "public"."events" AS OF SYSTEM TIME '1712345678901234567.0000000001' WHERE "id" > $1`,
		},
		{
			name:      "resuming with count",
			resuming:  true,
			withCount: true,
			expected: `SELECT MIN("id"),MAX("id"),COUNT(*) FROM "public"."events"` +
				` AS OF SYSTEM TIME '1712345678901234567.0000000001' WHERE "id" > $1`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, buildMinMaxQuery(table, "id", systemTime, tc.resuming, tc.withCount))
		})
	}
}

func TestBuildPullQuery(t *testing.T) {
	srcTable := &common.QualifiedTable{Namespace: "public", Table: "events"}
	const systemTime = "1712345678901234567.0000000001"
	const selectedColumns = `"id", "name"`

	intPartition := &protos.QRepPartition{
		PartitionId: "p1",
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_IntRange{
				IntRange: &protos.IntPartitionRange{Start: 1, End: 100},
			},
		},
	}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	timestampPartition := &protos.QRepPartition{
		PartitionId: "p2",
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_TimestampRange{
				TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(start),
					End:   timestamppb.New(end),
				},
			},
		},
	}
	nullPartition := &protos.QRepPartition{
		PartitionId: "p3",
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_NullRange{NullRange: &protos.NullPartitionRange{}},
		},
	}
	stringPartition := &protos.QRepPartition{
		PartitionId: "p4",
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_StringRange{
				StringRange: &protos.StringPartitionRange{Start: "a", End: "b"},
			},
		},
	}

	testCases := []struct {
		config        *protos.QRepConfig
		partition     *protos.QRepPartition
		name          string
		expectedQuery string
		expectedArgs  []any
		expectErr     bool
	}{
		{
			name:      "full table partition",
			config:    &protos.QRepConfig{WatermarkColumn: "id"},
			partition: &protos.QRepPartition{PartitionId: "p0", FullTablePartition: true},
			expectedQuery: `SELECT "id", "name" FROM "public"."events"` +
				` AS OF SYSTEM TIME '1712345678901234567.0000000001'`,
		},
		{
			name:          "full table partition with custom query",
			config:        &protos.QRepConfig{Query: "SELECT * FROM public.events"},
			partition:     &protos.QRepPartition{PartitionId: "p0", FullTablePartition: true},
			expectedQuery: "SELECT * FROM public.events",
		},
		{
			name:      "int range partition",
			config:    &protos.QRepConfig{WatermarkColumn: "id"},
			partition: intPartition,
			expectedQuery: `SELECT "id", "name" FROM "public"."events"` +
				` AS OF SYSTEM TIME '1712345678901234567.0000000001' WHERE "id" BETWEEN $1 AND $2`,
			expectedArgs: []any{int64(1), int64(100)},
		},
		{
			name:      "timestamp range partition",
			config:    &protos.QRepConfig{WatermarkColumn: "updated_at"},
			partition: timestampPartition,
			expectedQuery: `SELECT "id", "name" FROM "public"."events"` +
				` AS OF SYSTEM TIME '1712345678901234567.0000000001' WHERE "updated_at" BETWEEN $1 AND $2`,
			expectedArgs: []any{start, end},
		},
		{
			name:      "null range partition",
			config:    &protos.QRepConfig{WatermarkColumn: "updated_at"},
			partition: nullPartition,
			expectedQuery: `SELECT "id", "name" FROM "public"."events"` +
				` AS OF SYSTEM TIME '1712345678901234567.0000000001' WHERE "updated_at" IS NULL`,
		},
		{
			name: "custom query with range template",
			config: &protos.QRepConfig{
				WatermarkColumn: "id",
				Query:           "SELECT id FROM public.events WHERE id BETWEEN {{.start}} AND {{.end}}",
			},
			partition:     intPartition,
			expectedQuery: "SELECT id FROM public.events WHERE id BETWEEN $1 AND $2",
			expectedArgs:  []any{int64(1), int64(100)},
		},
		{
			name:      "null range partition with custom query errors",
			config:    &protos.QRepConfig{WatermarkColumn: "id", Query: "SELECT 1"},
			partition: nullPartition,
			expectErr: true,
		},
		{
			name:      "string range partition is unsupported",
			config:    &protos.QRepConfig{WatermarkColumn: "id"},
			partition: stringPartition,
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, args, err := buildPullQuery(tc.config, tc.partition, selectedColumns, srcTable, systemTime)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedQuery, query)
			require.Equal(t, tc.expectedArgs, args)
		})
	}
}

func TestSnapshotSystemTime(t *testing.T) {
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}

	systemTime, err := c.snapshotSystemTime(t.Context(), &protos.QRepConfig{
		SnapshotName: "1712345678901234567.0000000001",
	})
	require.NoError(t, err)
	require.Equal(t, "1712345678901234567.0000000001", systemTime)

	systemTime, err = c.snapshotSystemTime(t.Context(), &protos.QRepConfig{SnapshotName: "1712345678901234567"})
	require.NoError(t, err)
	require.Equal(t, "1712345678901234567", systemTime)

	for _, invalid := range []string{"-1s", "now()'; DROP TABLE x;--", "1712.34.56", "abc"} {
		_, err := c.snapshotSystemTime(t.Context(), &protos.QRepConfig{SnapshotName: invalid})
		require.Error(t, err, "expected error for %q", invalid)
	}
}

func TestSupportsRangePartition(t *testing.T) {
	supported := []types.QValueKind{
		types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ,
	}
	for _, qkind := range supported {
		require.True(t, supportsRangePartition(qkind), "expected %s to support range partitioning", qkind)
	}

	unsupported := []types.QValueKind{
		types.QValueKindString, types.QValueKindUUID, types.QValueKindFloat64,
		types.QValueKindNumeric, types.QValueKindBytes, types.QValueKindJSON,
	}
	for _, qkind := range unsupported {
		require.False(t, supportsRangePartition(qkind), "expected %s to not support range partitioning", qkind)
	}
}

func TestGetDefaultPartitionKeyForTables(t *testing.T) {
	c := &CockroachDBConnector{logger: log.NewStructuredLogger(slog.Default())}

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
			tableMappings: []*protos.TableMapping{tableMapping("public.ints")},
			schemas: map[string]*protos.TableSchema{
				"public.ints": {
					PrimaryKeyColumns: []string{"id"},
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindInt64)},
				},
			},
			expected: map[string]string{"public.ints": "id"},
		},
		{
			name:          "timestamptz primary key is supported",
			tableMappings: []*protos.TableMapping{tableMapping("public.ts")},
			schemas: map[string]*protos.TableSchema{
				"public.ts": {
					PrimaryKeyColumns: []string{"created_at"},
					Columns:           []*protos.FieldDescription{fieldDesc("created_at", types.QValueKindTimestampTZ)},
				},
			},
			expected: map[string]string{"public.ts": "created_at"},
		},
		{
			name:          "uuid primary key falls back to full table snapshot",
			tableMappings: []*protos.TableMapping{tableMapping("public.uuidpk")},
			schemas: map[string]*protos.TableSchema{
				"public.uuidpk": {
					PrimaryKeyColumns: []string{"id"},
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindUUID)},
				},
			},
			expected: map[string]string{},
		},
		{
			name:          "composite primary key uses first column",
			tableMappings: []*protos.TableMapping{tableMapping("public.composite")},
			schemas: map[string]*protos.TableSchema{
				"public.composite": {
					PrimaryKeyColumns: []string{"id", "created_at"},
					Columns: []*protos.FieldDescription{
						fieldDesc("id", types.QValueKindInt32),
						fieldDesc("created_at", types.QValueKindTimestamp),
					},
				},
			},
			expected: map[string]string{"public.composite": "id"},
		},
		{
			name:          "no primary key",
			tableMappings: []*protos.TableMapping{tableMapping("public.nopk")},
			schemas: map[string]*protos.TableSchema{
				"public.nopk": {
					PrimaryKeyColumns: nil,
					Columns:           []*protos.FieldDescription{fieldDesc("id", types.QValueKindInt64)},
				},
			},
			expected: map[string]string{},
		},
		{
			name:          "missing table schema",
			tableMappings: []*protos.TableMapping{tableMapping("public.missing")},
			schemas:       map[string]*protos.TableSchema{},
			expected:      map[string]string{},
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

func TestQRecordSchemaFromFieldDescriptions(t *testing.T) {
	tableSchema := &protos.TableSchema{
		TableIdentifier: "public.events",
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64), Nullable: false},
			{Name: "status", Type: string(types.QValueKindEnum), Nullable: true},
			{Name: "price", Type: string(types.QValueKindNumeric), Nullable: true},
		},
	}
	// numeric(10,2) typmod: ((10 << 16) | 2) + 4
	numericTypmod := int32(10<<16|2) + 4
	fds := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int8OID, TypeModifier: -1},
		{Name: "status", DataTypeOID: 100500, TypeModifier: -1},
		{Name: "price", DataTypeOID: pgtype.NumericOID, TypeModifier: numericTypmod},
		{Name: "computed", DataTypeOID: pgtype.TextOID, TypeModifier: -1},
	}

	schema := qRecordSchemaFromFieldDescriptions(tableSchema, fds)
	require.Equal(t, []types.QField{
		{Name: "id", Type: types.QValueKindInt64, Nullable: false},
		{Name: "status", Type: types.QValueKindEnum, Nullable: true},
		{Name: "price", Type: types.QValueKindNumeric, Nullable: true, Precision: 10, Scale: 2},
		{Name: "computed", Type: types.QValueKindString, Nullable: true},
	}, schema.Fields)
}
