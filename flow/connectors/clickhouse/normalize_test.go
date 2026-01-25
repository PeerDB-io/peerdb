package connclickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestColumnProjector(t *testing.T) {
	ctx := context.Background()
	projector := NewColumnProjector(nil)

	tests := []struct {
		name       string
		srcCol     string
		dstCol     string
		colType    types.QValueKind
		chType     string
		dataSource string
		want       string
	}{
		{
			name:       "Date32 type",
			srcCol:     "created_at",
			dstCol:     "created_at",
			chType:     "Date32",
			dataSource: "_peerdb_data",
			want:       "toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, 'created_at'),6,'UTC')) AS `created_at`",
		},
		{
			name:       "Nullable Date32 type",
			srcCol:     "updated_at",
			dstCol:     "updated_at",
			chType:     "Nullable(Date32)",
			dataSource: "_peerdb_data",
			want:       "toDate32(parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, 'updated_at'),6,'UTC')) AS `updated_at`",
		},
		{
			name:       "DateTime64 type",
			srcCol:     "timestamp",
			dstCol:     "timestamp",
			chType:     "DateTime64(6)",
			dataSource: "_peerdb_data",
			want:       "parseDateTime64BestEffortOrNull(JSONExtractString(_peerdb_data, 'timestamp'),6,'UTC') AS `timestamp`",
		},
		{
			name:       "Time type anchored to epoch",
			srcCol:     "start_time",
			dstCol:     "start_time",
			colType:    types.QValueKindTime,
			chType:     "DateTime64(6)",
			dataSource: "_peerdb_data",
			want:       "parseDateTime64BestEffortOrNull('1970-01-01 ' || JSONExtractString(_peerdb_data, 'start_time'),6,'UTC') AS `start_time`",
		},
		{
			name:       "TimeTZ type anchored to epoch",
			srcCol:     "end_time",
			dstCol:     "end_time",
			colType:    types.QValueKindTimeTZ,
			chType:     "Nullable(DateTime64(6))",
			dataSource: "_peerdb_data",
			want:       "parseDateTime64BestEffortOrNull('1970-01-01 ' || JSONExtractString(_peerdb_data, 'end_time'),6,'UTC') AS `end_time`",
		},
		{
			name:       "Array DateTime64 type",
			srcCol:     "timestamps",
			dstCol:     "timestamps",
			chType:     "Array(DateTime64(6))",
			dataSource: "_peerdb_data",
			want:       "arrayMap(x -> parseDateTime64BestEffortOrNull(x,6,'UTC'),JSONExtract(_peerdb_data,'timestamps','Array(String)')) AS `timestamps`",
		},
		{
			name:       "JSON type",
			srcCol:     "metadata",
			dstCol:     "metadata",
			chType:     "JSON",
			dataSource: "_peerdb_data",
			want:       "JSONExtractString(_peerdb_data, 'metadata')::JSON AS `metadata`",
		},
		{
			name:       "Generic string type",
			srcCol:     "name",
			dstCol:     "name",
			chType:     "String",
			dataSource: "_peerdb_data",
			want:       "JSONExtract(_peerdb_data, 'name', 'String') AS `name`",
		},
		{
			name:       "Generic Int64 type",
			srcCol:     "id",
			dstCol:     "id",
			chType:     "Int64",
			dataSource: "_peerdb_data",
			want:       "JSONExtract(_peerdb_data, 'id', 'Int64') AS `id`",
		},
		{
			name:       "Different data source for update query",
			srcCol:     "id",
			dstCol:     "id",
			chType:     "Int64",
			dataSource: "_peerdb_match_data",
			want:       "JSONExtract(_peerdb_match_data, 'id', 'Int64') AS `id`",
		},
		{
			name:       "Column name mapping",
			srcCol:     "old_name",
			dstCol:     "new_name",
			chType:     "String",
			dataSource: "_peerdb_data",
			want:       "JSONExtract(_peerdb_data, 'old_name', 'String') AS `new_name`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := projector.Project(ctx, tt.srcCol, tt.dstCol, tt.colType, tt.chType, tt.dataSource)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestResolveColumns(t *testing.T) {
	ctx := context.Background()

	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id", Type: string(types.QValueKindInt64)},
			{Name: "name", Type: string(types.QValueKindString)},
			{Name: "data", Type: string(types.QValueKindJSON)},
		},
		NullableEnabled: false,
	}

	tableMapping := &protos.TableMapping{
		SourceTableIdentifier:      "public.test",
		DestinationTableIdentifier: "test",
		Columns: []*protos.ColumnSetting{
			{SourceName: "name", DestinationName: "full_name"},
			{SourceName: "data", DestinationType: "String"}, // Override type
		},
	}

	g := &NormalizeQueryGenerator{
		env: map[string]string{},
	}

	columns, err := g.resolveColumns(ctx, schema, tableMapping)
	require.NoError(t, err)
	require.Len(t, columns, 3)

	// id: no mapping, default type
	require.Equal(t, "id", columns[0].srcName)
	require.Equal(t, "id", columns[0].dstName)
	require.Equal(t, "Int64", columns[0].chType)

	// name: mapped to full_name
	require.Equal(t, "name", columns[1].srcName)
	require.Equal(t, "full_name", columns[1].dstName)

	// data: type overridden to String
	require.Equal(t, "data", columns[2].srcName)
	require.Equal(t, "data", columns[2].dstName)
	require.Equal(t, "String", columns[2].chType)
}

func TestQueryVariants(t *testing.T) {
	// Test that the query variants have the expected values
	require.Equal(t, "_peerdb_data", mainQueryVariant.dataSource)
	require.Equal(t, "intDiv(_peerdb_record_type, 2)", mainQueryVariant.isDeletedExpr)
	require.Equal(t, "_peerdb_timestamp", mainQueryVariant.versionExpr)
	require.Empty(t, mainQueryVariant.extraWhere)

	require.Equal(t, "_peerdb_match_data", updateQueryVariant.dataSource)
	require.Equal(t, "1", updateQueryVariant.isDeletedExpr)
	require.Equal(t, "_peerdb_timestamp - 1", updateQueryVariant.versionExpr)
	require.Contains(t, updateQueryVariant.extraWhere, "_peerdb_match_data != ''")
	require.Contains(t, updateQueryVariant.extraWhere, "_peerdb_record_type = 1")
}

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
