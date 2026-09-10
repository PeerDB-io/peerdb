package connclickhouse

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func Test_GetOrderByColumns_WithColMap_AndOrdering(t *testing.T) {
	sourceColumns := []*protos.FieldDescription{
		{
			Name: "my id",
			Type: string(types.QValueKindInt32),
		},
		{
			Name:     "name",
			Type:     string(types.QValueKindString),
			Nullable: true,
		},
	}

	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
				Ordering:        1,
			},
			{
				SourceName:      "name",
				DestinationName: "name",
				Ordering:        2,
			},
		},
	}

	sourcePkeys := []string{"my id", "name"}
	colNameMap := map[string]string{
		"my id": "my id",
		"name":  "name",
	}

	nullableKeyFn := buildIsNullableKeyFn(tableMappingForTest, sourceColumns, true)

	expected := []string{"`my id`", "`name`"}
	actual, allowNullableKey := getOrderedOrderByColumns(tableMappingForTest, colNameMap, sourcePkeys, nullableKeyFn)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}

	// nullable field exists and enabled at schema level
	require.True(t, allowNullableKey)
}

func Test_GetOrderByColumns_NoOrdering_NoColMap(t *testing.T) {
	sourceColumns := []*protos.FieldDescription{
		{
			Name:     "my id",
			Type:     string(types.QValueKindInt32),
			Nullable: true,
		},
		{
			Name: "name",
			Type: string(types.QValueKindString),
		},
	}

	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
				NullableEnabled: true,
			},
			{
				SourceName:      "name",
				DestinationName: "name",
			},
		},
	}

	nullableKeyFn := buildIsNullableKeyFn(tableMappingForTest, sourceColumns, false)

	sourcePkeys := []string{"my id"}
	expected := []string{"`my id`"}
	actual, allowNullableKey := getOrderedOrderByColumns(tableMappingForTest, nil, sourcePkeys, nullableKeyFn)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}

	// nullable field exists and enabled at column level
	require.True(t, allowNullableKey)
}

func Test_GetOrderByColumns_WithColMap_NoOrdering(t *testing.T) {
	sourceColumns := []*protos.FieldDescription{
		{
			Name: "my id",
			Type: string(types.QValueKindInt32),
		},
		{
			Name: "name",
			Type: string(types.QValueKindString),
		},
	}

	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
			},
			{
				SourceName:      "name",
				DestinationName: "name",
			},
		},
	}

	nullableKeyFn := buildIsNullableKeyFn(tableMappingForTest, sourceColumns, true)

	sourcePkeys := []string{"my id", "name"}
	colNameMap := map[string]string{
		"my id": "my id destination",
		"name":  "name",
	}
	expected := []string{"`my id destination`", "`name`"}
	actual, allowNullableKey := getOrderedOrderByColumns(tableMappingForTest, colNameMap, sourcePkeys, nullableKeyFn)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}

	// nullable enabled but not exists
	require.False(t, allowNullableKey)
}

func Test_GetOrderByColumns_NoColMap_WithOrdering(t *testing.T) {
	sourceColumns := []*protos.FieldDescription{
		{
			Name: "my id",
			Type: string(types.QValueKindInt32),
		},
		{
			Name:     "name",
			Type:     string(types.QValueKindString),
			Nullable: true,
		},
	}

	tableMappingForTest := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "my id",
				DestinationName: "my id",
				Ordering:        1,
			},
			{
				SourceName:      "name",
				DestinationName: "name",
				Ordering:        2,
			},
		},
	}

	nullableKeyFn := buildIsNullableKeyFn(tableMappingForTest, sourceColumns, false)

	sourcePkeys := []string{"my id", "name"}
	expected := []string{"`my id`", "`name`"}
	actual, allowNullableKey := getOrderedOrderByColumns(tableMappingForTest, nil, sourcePkeys, nullableKeyFn)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}

	// nullable field exists but not enabled
	require.False(t, allowNullableKey)
}

func TestBuildQuery_Basic(t *testing.T) {
	ctx := t.Context()
	tableName := "my_table"
	rawTableName := "raw_my_table"
	endBatchID := int64(10)
	lastNormBatchID := int64(5)
	enablePrimaryUpdate := false
	sourceSchemaAsDestinationColumn := false
	env := map[string]string{}

	// Table schema with two columns
	tableSchema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "name", Type: string(types.QValueKindString)},
		},
		NullableEnabled: false,
	}
	tableNameSchemaMapping := map[string]*protos.TableSchema{
		tableName: tableSchema,
	}

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      "public.my_table",
			DestinationTableIdentifier: tableName,
		},
	}

	g := NewNormalizeQueryGenerator(
		tableName,
		tableNameSchemaMapping,
		tableMappings,
		endBatchID,
		lastNormBatchID,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
		nil,
		false,
		"",
		shared.InternalVersion_Latest,
		nil,
	)

	query, err := g.BuildQuery(ctx)
	require.NoError(t, err)
	require.Contains(t, query, "INSERT INTO")
	require.Contains(t, query, "SELECT")
	require.Contains(t, query, "JSONExtract(_peerdb_data, 'id', 'Int64') AS `id`")
	require.Contains(t, query, "JSONExtract(_peerdb_data, 'name', 'String') AS `name`")
	require.Contains(t, query, "FROM `raw_my_table`")
	require.Contains(t, query, "_peerdb_batch_id > 5 AND _peerdb_batch_id <= 10")
	require.Contains(t, query, "_peerdb_destination_table_name = 'my_table'")
}

func TestBuildQuery_WithPrimaryUpdate(t *testing.T) {
	ctx := t.Context()
	tableName := "my_table"
	rawTableName := "raw_my_table"
	endBatchID := int64(10)
	lastNormBatchID := int64(5)
	enablePrimaryUpdate := true
	sourceSchemaAsDestinationColumn := false
	env := map[string]string{}

	tableSchema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
		},
		NullableEnabled: false,
	}
	tableNameSchemaMapping := map[string]*protos.TableSchema{
		tableName: tableSchema,
	}

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      "public.my_table",
			DestinationTableIdentifier: tableName,
		},
	}

	g := NewNormalizeQueryGenerator(
		tableName,
		tableNameSchemaMapping,
		tableMappings,
		endBatchID,
		lastNormBatchID,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
		nil,
		false,
		"",
		shared.InternalVersion_Latest,
		nil,
	)

	query, err := g.BuildQuery(ctx)
	require.NoError(t, err)
	require.Contains(t, query, "UNION ALL SELECT")
	require.Contains(t, query, "JSONExtract(_peerdb_match_data, 'id', 'Int64') AS `id`")
	require.Contains(t, query, "_peerdb_match_data != ''")
	require.Contains(t, query, "_peerdb_record_type = 1")
}

func TestBuildQuery_WithSourceSchemaAsDestinationColumn(t *testing.T) {
	ctx := t.Context()
	tableName := "my_table"
	rawTableName := "raw_my_table"
	endBatchID := int64(10)
	lastNormBatchID := int64(5)
	enablePrimaryUpdate := false
	sourceSchemaAsDestinationColumn := true
	env := map[string]string{}

	tableSchema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
		},
		NullableEnabled: false,
	}
	tableNameSchemaMapping := map[string]*protos.TableSchema{
		tableName: tableSchema,
	}

	tableMappings := []*protos.TableMapping{
		{
			SourceTableIdentifier:      "public.my_table",
			DestinationTableIdentifier: tableName,
		},
	}

	g := NewNormalizeQueryGenerator(
		tableName,
		tableNameSchemaMapping,
		tableMappings,
		endBatchID,
		lastNormBatchID,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
		nil,
		true,
		"",
		shared.InternalVersion_Latest,
		nil,
	)

	query, err := g.BuildQuery(ctx)
	require.NoError(t, err)
	require.Contains(t, query, " AS `_peerdb_source_schema`")
	require.Contains(t, query, "parallel_distributed_insert_select=0")
}

func TestGetOrderedPartitionByColumns(t *testing.T) {
	sourceColumns := []*protos.FieldDescription{
		{
			Name: "col1",
			Type: string(types.QValueKindInt32),
		},
		{
			Name:     "col2",
			Type:     string(types.QValueKindString),
			Nullable: true,
		},
		{
			Name: "col3",
			Type: string(types.QValueKindBoolean),
		},
	}

	tableMapping := &protos.TableMapping{
		SourceTableIdentifier:      "test_table",
		DestinationTableIdentifier: "test_table_ch",
		Columns: []*protos.ColumnSetting{
			{
				SourceName:      "col1",
				DestinationName: "col1",
				Partitioning:    1,
			},
			{
				SourceName:      "col2",
				DestinationName: "col2",
				Partitioning:    2,
				NullableEnabled: true,
			},
			{
				SourceName:      "col3",
				DestinationName: "col3",
				Partitioning:    0, // Not partitioned
			},
		},
	}

	colNameMap := map[string]string{
		"col1": "column_one",
		"col2": "column_two",
	}

	nullableKeyFn := buildIsNullableKeyFn(tableMapping, sourceColumns, false)

	expected := []string{"`column_one`", "`column_two`"}
	actual, hasNullablePartitionKeys := getOrderedPartitionByColumns(tableMapping, colNameMap, nullableKeyFn)

	require.Equal(t, expected, actual)

	require.True(t, hasNullablePartitionKeys)
}

func TestGenerateCreateTableSQLForNormalizedTable(t *testing.T) {
	tableIdentifier := "tbl"
	tableSchema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "col", Type: string(types.QValueKindString)},
		},
		PrimaryKeyColumns: []string{"id"},
	}

	tests := []struct {
		chVersion   *chproto.Version
		name        string
		contains    []string
		notContains []string
		isResync    bool
	}{
		{
			name:      "basic non-resync 'create table if not exists' test",
			chVersion: &chproto.Version{Major: 25, Minor: 8, Patch: 0},
			isResync:  false,
			contains: []string{
				"CREATE TABLE IF NOT EXISTS `tbl`",
				"`id` Int64",
				"`col` String",
				"`_peerdb_is_deleted` UInt8",
				"`_peerdb_version` UInt64",
				"ENGINE = ReplacingMergeTree(`_peerdb_version`)",
				"PRIMARY KEY (`id`) ORDER BY (`id`)",
			},
			notContains: []string{"max_table_size_to_drop"},
		},
		{
			name:      "basic resync 'create or replace table' test",
			chVersion: &chproto.Version{Major: 25, Minor: 8, Patch: 0},
			isResync:  true,
			contains: []string{
				"CREATE OR REPLACE TABLE `tbl`",
				"`id` Int64",
				"`col` String",
				"`_peerdb_is_deleted` UInt8",
				"`_peerdb_version` UInt64",
				"ENGINE = ReplacingMergeTree(`_peerdb_version`)",
				"SETTINGS max_table_size_to_drop=0",
				"PRIMARY KEY (`id`) ORDER BY (`id`)",
			},
		},
		{
			name:        "resync on old CH version omits max_table_size_to_drop",
			chVersion:   &chproto.Version{Major: 23, Minor: 11, Patch: 0},
			isResync:    true,
			contains:    []string{"CREATE OR REPLACE TABLE `tbl`"},
			notContains: []string{"max_table_size_to_drop"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			c := &ClickHouseConnector{
				Config:    &protos.ClickhouseConfig{Database: "db"},
				chVersion: tc.chVersion,
			}
			config := &protos.SetupNormalizedTableBatchInput{
				Env: map[string]string{"PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN": "false"},
				TableMappings: []*protos.TableMapping{
					{
						SourceTableIdentifier:      tableIdentifier,
						DestinationTableIdentifier: tableIdentifier,
					},
				},
				IsResync: tc.isResync,
			}

			result, err := c.generateCreateTableSQLForNormalizedTable(ctx, config, tableIdentifier, tableSchema, tc.chVersion, nil)
			require.NoError(t, err)
			require.Len(t, result, 1)
			sql := result[0]
			for _, contain := range tc.contains {
				require.Contains(t, sql, contain)
			}
			for _, notContain := range tc.notContains {
				require.NotContains(t, sql, notContain)
			}
		})
	}
}

func Test_FilterSchemaColumnsByDestination(t *testing.T) {
	makeSchema := func(colNames ...string) *protos.TableSchema {
		columns := make([]*protos.FieldDescription, 0, len(colNames))
		for _, name := range colNames {
			columns = append(columns, &protos.FieldDescription{
				Name: name,
				Type: string(types.QValueKindString),
			})
		}
		return &protos.TableSchema{
			TableIdentifier:   "public.tbl",
			PrimaryKeyColumns: []string{"id"},
			NullableEnabled:   true,
			TableOid:          42,
			Columns:           columns,
		}
	}
	destColumns := func(colNames ...string) map[string]struct{} {
		set := make(map[string]struct{}, len(colNames))
		for _, name := range colNames {
			set[name] = struct{}{}
		}
		return set
	}
	columnNames := func(schema *protos.TableSchema) []string {
		names := make([]string, 0, len(schema.Columns))
		for _, col := range schema.Columns {
			names = append(names, col.Name)
		}
		return names
	}

	tests := []struct {
		name            string
		schema          *protos.TableSchema
		tableMapping    *protos.TableMapping
		actualDestCols  map[string]struct{}
		expectedKept    []string
		expectedRemoved []string
	}{
		{
			name:            "single column dropped without rename",
			schema:          makeSchema("id", "c1", "val"),
			actualDestCols:  destColumns("id", "c1"),
			expectedKept:    []string{"id", "c1"},
			expectedRemoved: []string{"val"},
		},
		{
			name:   "renamed column dropped returns source name",
			schema: makeSchema("id", "val"),
			tableMapping: &protos.TableMapping{
				DestinationTableIdentifier: "tbl_dst",
				Columns: []*protos.ColumnSetting{
					{SourceName: "val", DestinationName: "val_dst"},
				},
			},
			actualDestCols:  destColumns("id"),
			expectedKept:    []string{"id"},
			expectedRemoved: []string{"val"},
		},
		{
			name:   "renamed column present on destination is kept",
			schema: makeSchema("id", "val"),
			tableMapping: &protos.TableMapping{
				DestinationTableIdentifier: "tbl_dst",
				Columns: []*protos.ColumnSetting{
					{SourceName: "val", DestinationName: "val_dst"},
				},
			},
			actualDestCols:  destColumns("id", "val_dst"),
			expectedKept:    []string{"id", "val"},
			expectedRemoved: nil,
		},
		{
			name:   "empty destination name falls back to source name",
			schema: makeSchema("id", "val"),
			tableMapping: &protos.TableMapping{
				DestinationTableIdentifier: "tbl_dst",
				Columns: []*protos.ColumnSetting{
					{SourceName: "val", DestinationName: ""},
				},
			},
			actualDestCols:  destColumns("id"),
			expectedKept:    []string{"id"},
			expectedRemoved: []string{"val"},
		},
		{
			name:            "multiple columns dropped",
			schema:          makeSchema("id", "c1", "val1", "val2"),
			actualDestCols:  destColumns("id", "c1"),
			expectedKept:    []string{"id", "c1"},
			expectedRemoved: []string{"val1", "val2"},
		},
		{
			name:            "nothing dropped returns empty removed list",
			schema:          makeSchema("id", "c1", "val"),
			actualDestCols:  destColumns("id", "c1", "val"),
			expectedKept:    []string{"id", "c1", "val"},
			expectedRemoved: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filtered, removed := filterSchemaColumnsByDestination(tc.schema, tc.tableMapping, tc.actualDestCols)
			require.Equal(t, tc.expectedKept, columnNames(filtered))
			require.Equal(t, tc.expectedRemoved, removed)
		})
	}
}

func Test_FilterSchemaColumnsByDestination_PreservesSchemaMetadata(t *testing.T) {
	schema := &protos.TableSchema{
		TableIdentifier:       "public.tbl",
		PrimaryKeyColumns:     []string{"id"},
		IsReplicaIdentityFull: true,
		System:                protos.TypeSystem_PG,
		NullableEnabled:       true,
		TableOid:              42,
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "val", Type: string(types.QValueKindString)},
		},
	}

	filtered, removed := filterSchemaColumnsByDestination(schema, nil, map[string]struct{}{"id": {}})
	require.Equal(t, []string{"val"}, removed)
	require.Equal(t, schema.TableIdentifier, filtered.TableIdentifier)
	require.Equal(t, schema.PrimaryKeyColumns, filtered.PrimaryKeyColumns)
	require.Equal(t, schema.IsReplicaIdentityFull, filtered.IsReplicaIdentityFull)
	require.Equal(t, schema.System, filtered.System)
	require.Equal(t, schema.NullableEnabled, filtered.NullableEnabled)
	require.Equal(t, schema.TableOid, filtered.TableOid)
}

func Test_RebuildQueryWithDestinationColumns(t *testing.T) {
	const dstTableName = "tbl_dst"

	newGenerator := func(schema *protos.TableSchema, tableMappings []*protos.TableMapping) *NormalizeQueryGenerator {
		return NewNormalizeQueryGenerator(
			dstTableName,
			map[string]*protos.TableSchema{dstTableName: schema},
			tableMappings,
			5, // endBatchID
			2, // lastNormBatchID
			false,
			false,
			map[string]string{},
			"raw_tbl",
			nil,
			false,
			"",
			0,
			nil,
		)
	}
	schema := &protos.TableSchema{
		TableIdentifier:   "public.tbl",
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "c1", Type: string(types.QValueKindInt32)},
			{Name: "dropped_col", Type: string(types.QValueKindString)},
		},
	}

	t.Run("dropped column removed from rebuilt query", func(t *testing.T) {
		gen := newGenerator(schema, nil)
		query, removed, err := rebuildQueryWithDestinationColumns(t.Context(), gen, map[string]struct{}{
			"id": {}, "c1": {},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"dropped_col"}, removed)
		require.Contains(t, query, "`id`")
		require.Contains(t, query, "`c1`")
		require.NotContains(t, query, "dropped_col")

		// the generator itself must not be mutated: retries build from a copy
		require.Empty(t, gen.Query)
		require.Len(t, gen.tableNameSchemaMapping[dstTableName].Columns, 3)
	})

	t.Run("renamed column returns source name and drops destination name", func(t *testing.T) {
		gen := newGenerator(schema, []*protos.TableMapping{{
			SourceTableIdentifier:      "public.tbl",
			DestinationTableIdentifier: dstTableName,
			Columns: []*protos.ColumnSetting{
				{SourceName: "dropped_col", DestinationName: "dropped_col_dst"},
			},
		}})
		query, removed, err := rebuildQueryWithDestinationColumns(t.Context(), gen, map[string]struct{}{
			"id": {}, "c1": {},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"dropped_col"}, removed)
		require.NotContains(t, query, "dropped_col")
		require.NotContains(t, query, "dropped_col_dst")
	})

	t.Run("all columns present errors instead of rebuilding", func(t *testing.T) {
		gen := newGenerator(schema, nil)
		_, _, err := rebuildQueryWithDestinationColumns(t.Context(), gen, map[string]struct{}{
			"id": {}, "c1": {}, "dropped_col": {},
		})
		require.ErrorContains(t, err, "no user columns were absent from destination table")
	})
}
