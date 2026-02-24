package connclickhouse

import (
	"testing"

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
