package connclickhouse

import (
	"context"
	"fmt"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	chinternal "github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestBuildInsertFromTableFunctionQuery(t *testing.T) {
	ctx := context.Background()

	schema := types.QRecordSchema{
		Fields: []types.QField{
			{Name: "id", Type: types.QValueKindInt64, Nullable: false},
			{Name: "name", Type: types.QValueKindString, Nullable: false},
		},
	}

	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema:           schema,
		config: &protos.QRepConfig{
			Env: map[string]string{
				"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
			},
		},
	}

	tableFunctionExpr := "s3('s3://bucket/key', 'format')"
	chSettings := chinternal.NewCHSettings(&chproto.Version{Major: 25, Minor: 8})
	chSettings.Add(chinternal.SettingTypeJsonSkipDuplicatedPaths, "1")

	// without partitioning
	query, err := buildInsertFromTableFunctionQuery(ctx, config, tableFunctionExpr, chSettings)
	require.NoError(t, err)
	require.Equal(t,
		fmt.Sprintf("INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format') SETTINGS %s=%s",
			string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
		query)

	// with partitioning
	totalPartitions := uint64(8)
	for idx := range totalPartitions {
		query, err := buildInsertFromTableFunctionQueryWithPartitioning(ctx, config, tableFunctionExpr, idx, totalPartitions, chSettings)
		require.NoError(t, err)
		require.Equal(t,
			"INSERT INTO `t1`(`id`,`name`) SELECT `id`,`name` FROM s3('s3://bucket/key', 'format')"+
				fmt.Sprintf(" WHERE cityHash64(`id`) %% 8 = %d SETTINGS %s=%s",
					idx, string(chinternal.SettingTypeJsonSkipDuplicatedPaths), "1"),
			query)
	}
}

func TestBuildInsertFromTableFunctionQueryArrayTime(t *testing.T) {
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema: types.QRecordSchema{Fields: []types.QField{
			{Name: "array_time", Type: types.QValueKindArrayTime},
		}},
		config: &protos.QRepConfig{Env: map[string]string{
			"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
		}},
	}

	query, err := buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`array_time`) SELECT "+
			"arrayMap(x -> fromUnixTimestamp64Micro(toInt64(x)), `array_time`) "+
			"FROM s3('s3://bucket/key', 'Avro')",
		query)
}

// TestBuildInsertFromTableFunctionQueryDestinationName checks a renamed column is inserted under its
// destination name while still being read from the staged field with the source name.
func TestBuildInsertFromTableFunctionQueryDestinationName(t *testing.T) {
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema: types.QRecordSchema{Fields: []types.QField{
			{Name: "id", Type: types.QValueKindInt64},
			{Name: "note", Type: types.QValueKindString},
		}},
		config: &protos.QRepConfig{
			Env:     map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false"},
			Columns: []*protos.ColumnSetting{{SourceName: "note", DestinationName: "memo"}},
		},
	}

	query, err := buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`id`,`memo`) SELECT `id`,`note` FROM s3('s3://bucket/key', 'Avro')",
		query)
}

// TestBuildInsertFromTableFunctionQueryArrayOfJSON checks staged JSON arrays are parsed into their array column's type:
// an override's, or for a column without one the type table creation picked.
func TestBuildInsertFromTableFunctionQueryArrayOfJSON(t *testing.T) {
	const overrides = "JSONExtract(ifNull(`docs`, ''), 'Array(Nullable(JSON))')," +
		"arrayMap(x -> ifNull(x, CAST('{}', 'JSON')), JSONExtract(ifNull(`docsb`, ''), 'Array(Nullable(JSON))')),"
	const asText = overrides + "`docs_default`,`docs_as_string`"
	for _, tc := range []struct {
		name           string
		nativeJSON     string
		nullable       string
		want           string
		sourceType     protos.DBType
		version        uint32
		columnNullable bool
	}{
		{
			name: "default mapping", nativeJSON: "true", nullable: "false",
			sourceType: protos.DBType_POSTGRES, version: shared.InternalVersion_Latest,
			want: overrides +
				"arrayMap(x -> ifNull(x, CAST('{}', 'JSON')), JSONExtract(ifNull(`docs_default`, ''), 'Array(Nullable(JSON))'))," +
				"`docs_as_string`",
		},
		{
			name: "default mapping with PEERDB_NULLABLE", nativeJSON: "true", nullable: "true",
			sourceType: protos.DBType_POSTGRES, version: shared.InternalVersion_Latest,
			want: overrides + "JSONExtract(ifNull(`docs_default`, ''), 'Array(Nullable(JSON))'),`docs_as_string`",
		},
		{
			name: "default mapping with a nullable column setting", nativeJSON: "true", nullable: "false",
			sourceType: protos.DBType_POSTGRES, version: shared.InternalVersion_Latest, columnNullable: true,
			want: overrides + "JSONExtract(ifNull(`docs_default`, ''), 'Array(Nullable(JSON))'),`docs_as_string`",
		},
		{
			name: "setting off", nativeJSON: "false", nullable: "true",
			sourceType: protos.DBType_POSTGRES, version: shared.InternalVersion_Latest, want: asText,
		},
		{
			name: "non-Postgres source", nativeJSON: "true", nullable: "true",
			sourceType: protos.DBType_COCKROACHDB, version: shared.InternalVersion_Latest, want: asText,
		},
		{
			name: "mirror from before the mapping", nativeJSON: "true", nullable: "true",
			sourceType: protos.DBType_POSTGRES, version: shared.InternalVersion_JSONArrayAsArrayJSON - 1, want: asText,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &insertFromTableFunctionConfig{
				destinationTable: "t1",
				schema: types.QRecordSchema{Fields: []types.QField{
					{Name: "docs", Type: types.QValueKindArrayJSON, Nullable: true},
					{Name: "docsb", Type: types.QValueKindArrayJSONB, Nullable: true},
					{Name: "docs_default", Type: types.QValueKindArrayJSON, Nullable: true},
					{Name: "docs_as_string", Type: types.QValueKindArrayJSON, Nullable: true},
				}},
				config: &protos.QRepConfig{
					Env: map[string]string{
						"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
						"PEERDB_CLICKHOUSE_ENABLE_JSON":              tc.nativeJSON,
						"PEERDB_NULLABLE":                            tc.nullable,
					},
					Columns: []*protos.ColumnSetting{
						{SourceName: "docs", DestinationType: "Array(Nullable(JSON))"},
						{SourceName: "docsb", DestinationType: "Array(JSON)"},
						{SourceName: "docs_default", NullableEnabled: tc.columnNullable},
						{SourceName: "docs_as_string", DestinationType: "String"},
					},
					SourceType: tc.sourceType,
					Version:    tc.version,
				},
				connector: &ClickHouseConnector{chVersion: &chproto.Version{Major: 25, Minor: 8}},
			}

			query, err := buildInsertFromTableFunctionQuery(
				context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
			)
			require.NoError(t, err)
			require.Equal(t,
				"INSERT INTO `t1`(`docs`,`docsb`,`docs_default`,`docs_as_string`) SELECT "+tc.want+
					" FROM s3('s3://bucket/key', 'Avro')",
				query)
		})
	}
}

// TestBuildInsertFromTableFunctionQueryJSON checks the native JSON casts: CAST(NULL, 'JSON') fails, so a
// nullable JSON field has to be cast to Nullable(JSON) while a non-nullable one is cast to JSON.
func TestBuildInsertFromTableFunctionQueryJSON(t *testing.T) {
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema: types.QRecordSchema{Fields: []types.QField{
			{Name: "js", Type: types.QValueKindJSON, Nullable: true},
			{Name: "jsb", Type: types.QValueKindJSONB, Nullable: true},
			{Name: "js_required", Type: types.QValueKindJSON, Nullable: false},
		}},
		config: &protos.QRepConfig{Env: map[string]string{
			"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
			"PEERDB_CLICKHOUSE_ENABLE_JSON":              "true",
		}},
		// ClickHouse 25.8 supports the native JSON type
		connector: &ClickHouseConnector{chVersion: &chproto.Version{Major: 25, Minor: 8}},
	}

	query, err := buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`js`,`jsb`,`js_required`) SELECT "+
			"CAST(`js`, 'Nullable(JSON)'),CAST(`jsb`, 'Nullable(JSON)'),CAST(`js_required`, 'JSON') "+
			"FROM s3('s3://bucket/key', 'Avro')",
		query)
	// the classifier relies on Exec recognizing these casts
	require.True(t, peerdb_clickhouse.QueryCastsToJSON(query))
}

// TestBuildInsertFromTableFunctionQueryJSONOverride checks that an explicit destination type on a JSON
// column wins over the mirror-wide native JSON setting, as it does in the DDL and the normalize query.
func TestBuildInsertFromTableFunctionQueryJSONOverride(t *testing.T) {
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema: types.QRecordSchema{Fields: []types.QField{
			{Name: "as_string", Type: types.QValueKindJSON, Nullable: true},
			{Name: "as_json", Type: types.QValueKindJSON, Nullable: true},
			{Name: "as_bare_json", Type: types.QValueKindJSON, Nullable: true},
			{Name: "as_bare_json_required", Type: types.QValueKindJSON, Nullable: false},
			{Name: "untouched", Type: types.QValueKindJSON, Nullable: true},
		}},
		config: &protos.QRepConfig{
			Env: map[string]string{
				"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false",
				"PEERDB_CLICKHOUSE_ENABLE_JSON":              "false",
			},
			Columns: []*protos.ColumnSetting{
				{SourceName: "as_string", DestinationType: "String"},
				{SourceName: "as_json", DestinationType: "Nullable(JSON)"},
				// a bare JSON override is promoted for a nullable column, as the DDL does
				{SourceName: "as_bare_json", DestinationType: "JSON"},
				{SourceName: "as_bare_json_required", DestinationType: "JSON"},
			},
		},
		connector: &ClickHouseConnector{chVersion: &chproto.Version{Major: 25, Minor: 8}},
	}

	query, err := buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`as_string`,`as_json`,`as_bare_json`,`as_bare_json_required`,`untouched`) SELECT "+
			"`as_string`,CAST(`as_json`, 'Nullable(JSON)'),CAST(`as_bare_json`, 'Nullable(JSON)'),"+
			"CAST(`as_bare_json_required`, 'JSON'),`untouched` "+
			"FROM s3('s3://bucket/key', 'Avro')",
		query)

	// with native JSON on, the String override still keeps its column out of the cast
	config.config.Env["PEERDB_CLICKHOUSE_ENABLE_JSON"] = "true"
	query, err = buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`as_string`,`as_json`,`as_bare_json`,`as_bare_json_required`,`untouched`) SELECT "+
			"`as_string`,CAST(`as_json`, 'Nullable(JSON)'),CAST(`as_bare_json`, 'Nullable(JSON)'),"+
			"CAST(`as_bare_json_required`, 'JSON'),CAST(`untouched`, 'Nullable(JSON)') "+
			"FROM s3('s3://bucket/key', 'Avro')",
		query)
}
