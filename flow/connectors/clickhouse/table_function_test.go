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

// TestBuildInsertFromTableFunctionQueryArrayOfJSON checks arrays of JSON, staged as a string holding the
// whole array, are parsed when their column is declared an array, a null one into the empty array, and
// left as is otherwise, as when they land in a String column.
func TestBuildInsertFromTableFunctionQueryArrayOfJSON(t *testing.T) {
	config := &insertFromTableFunctionConfig{
		destinationTable: "t1",
		schema: types.QRecordSchema{Fields: []types.QField{
			{Name: "docs", Type: types.QValueKindArrayJSON, Nullable: true},
			{Name: "docsb", Type: types.QValueKindArrayJSONB, Nullable: true},
			{Name: "docs_as_string", Type: types.QValueKindArrayJSON, Nullable: true},
		}},
		config: &protos.QRepConfig{
			Env: map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false"},
			Columns: []*protos.ColumnSetting{
				{SourceName: "docs", DestinationType: "Array(Nullable(JSON))"},
				{SourceName: "docsb", DestinationType: "Array(JSON)"},
			},
		},
	}

	query, err := buildInsertFromTableFunctionQuery(
		context.Background(), config, "s3('s3://bucket/key', 'Avro')", nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		"INSERT INTO `t1`(`docs`,`docsb`,`docs_as_string`) SELECT "+
			"JSONExtract(ifNull(`docs`, ''), 'Array(Nullable(JSON))'),"+
			"JSONExtract(ifNull(`docsb`, ''), 'Array(JSON)'),"+
			"`docs_as_string` "+
			"FROM s3('s3://bucket/key', 'Avro')",
		query)
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

// TestBuildInsertFromTableFunctionQuerySourceSchema checks who supplies _peerdb_source_schema: under the snapshot
// gate the Postgres source projects it per row and the insert reads it from the staged file, otherwise the insert
// stamps the watermark table's schema as a literal, as before.
func TestBuildInsertFromTableFunctionQuerySourceSchema(t *testing.T) {
	const tableFunctionExpr = "s3('s3://bucket/key', 'Avro')"
	settingOn := map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "true"}
	withSourceSchema := types.QRecordSchema{Fields: []types.QField{
		{Name: "id", Type: types.QValueKindInt64},
		{Name: sourceSchemaColName, Type: types.QValueKindString},
	}}
	withoutSourceSchema := types.QRecordSchema{Fields: []types.QField{
		{Name: "id", Type: types.QValueKindInt64},
	}}
	gated := func() *protos.QRepConfig {
		return &protos.QRepConfig{
			Env:            settingOn,
			Version:        shared.InternalVersion_SourceSchemaFromInheritanceChild,
			SourceType:     protos.DBType_POSTGRES,
			WatermarkTable: "public.parent",
		}
	}

	for _, tc := range []struct {
		name            string
		config          func() *protos.QRepConfig
		schema          types.QRecordSchema
		columnNameMap   map[string]string
		excludedColumns []string
		expected        string
		expectedErr     string
	}{
		{
			name:     "projected by the source",
			config:   gated,
			schema:   withSourceSchema,
			expected: "INSERT INTO `t1`(`id`,`_peerdb_source_schema`) SELECT `id`,`_peerdb_source_schema` FROM " + tableFunctionExpr,
		},
		{
			name:          "projected by the source, staged field names",
			config:        gated,
			schema:        withSourceSchema,
			columnNameMap: map[string]string{"id": "id_0", sourceSchemaColName: "_peerdb_source_schema_1"},
			expected: "INSERT INTO `t1`(`id`,`_peerdb_source_schema`) SELECT `id_0`,`_peerdb_source_schema_1` FROM " +
				tableFunctionExpr,
		},
		{
			name:            "projected by the source, despite an exclusion of the same name",
			config:          gated,
			schema:          withSourceSchema,
			excludedColumns: []string{sourceSchemaColName},
			expected:        "INSERT INTO `t1`(`id`,`_peerdb_source_schema`) SELECT `id`,`_peerdb_source_schema` FROM " + tableFunctionExpr,
		},
		{
			name:        "projection missing",
			config:      gated,
			schema:      withoutSourceSchema,
			expectedErr: "snapshot of public.parent is missing column _peerdb_source_schema",
		},
		{
			name: "mirror created before the internal version",
			config: func() *protos.QRepConfig {
				c := gated()
				c.Version = shared.InternalVersion_SourceSchemaFromInheritanceChild - 1
				return c
			},
			schema:   withoutSourceSchema,
			expected: "INSERT INTO `t1`(`id`,_peerdb_source_schema) SELECT `id`,'public' FROM " + tableFunctionExpr,
		},
		{
			name: "custom query",
			config: func() *protos.QRepConfig {
				c := gated()
				c.Query = "SELECT * FROM public.parent"
				return c
			},
			schema:   withoutSourceSchema,
			expected: "INSERT INTO `t1`(`id`,_peerdb_source_schema) SELECT `id`,'public' FROM " + tableFunctionExpr,
		},
		{
			name: "non-Postgres source",
			config: func() *protos.QRepConfig {
				c := gated()
				c.SourceType = protos.DBType_BIGQUERY
				return c
			},
			schema:   withoutSourceSchema,
			expected: "INSERT INTO `t1`(`id`,_peerdb_source_schema) SELECT `id`,'public' FROM " + tableFunctionExpr,
		},
		{
			name: "setting off",
			config: func() *protos.QRepConfig {
				c := gated()
				c.Env = map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false"}
				return c
			},
			schema:   withoutSourceSchema,
			expected: "INSERT INTO `t1`(`id`) SELECT `id` FROM " + tableFunctionExpr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := buildInsertFromTableFunctionQuery(t.Context(), &insertFromTableFunctionConfig{
				destinationTable: "t1",
				schema:           tc.schema,
				columnNameMap:    tc.columnNameMap,
				excludedColumns:  tc.excludedColumns,
				config:           tc.config(),
			}, tableFunctionExpr, nil)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, query)
		})
	}
}
