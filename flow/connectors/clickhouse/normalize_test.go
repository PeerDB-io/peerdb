package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func Test_GetOrderByColumns_WithColMap_AndOrdering(t *testing.T) {
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

	expected := []string{"`my id`", "`name`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, colNameMap)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func Test_GetOrderByColumns_NoOrdering_NoColMap(t *testing.T) {
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

	sourcePkeys := []string{"my id"}
	expected := []string{"`my id`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func Test_GetOrderByColumns_WithColMap_NoOrdering(t *testing.T) {
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

	sourcePkeys := []string{"my id", "name"}
	colNameMap := map[string]string{
		"my id": "my id destination",
		"name":  "name",
	}
	expected := []string{"`my id destination`", "`name`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, colNameMap)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func Test_GetOrderByColumns_NoColMap_WithOrdering(t *testing.T) {
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
	expected := []string{"`my id`", "`name`"}
	actual := getOrderedOrderByColumns(tableMappingForTest, sourcePkeys, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Expected %v, got %v", expected, actual)
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func TestBuildQuery_Basic(t *testing.T) {
	ctx := t.Context()
	tableName := "my_table"
	rawTableName := "raw_my_table"
	part := uint64(0)
	numParts := uint64(1)
	syncBatchID := int64(10)
	batchIDToLoadForTable := int64(5)
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
		part,
		tableNameSchemaMapping,
		tableMappings,
		syncBatchID,
		batchIDToLoadForTable,
		numParts,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
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
	part := uint64(0)
	numParts := uint64(1)
	syncBatchID := int64(10)
	batchIDToLoadForTable := int64(5)
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
		part,
		tableNameSchemaMapping,
		tableMappings,
		syncBatchID,
		batchIDToLoadForTable,
		numParts,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
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
	part := uint64(0)
	numParts := uint64(1)
	syncBatchID := int64(10)
	batchIDToLoadForTable := int64(5)
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
		part,
		tableNameSchemaMapping,
		tableMappings,
		syncBatchID,
		batchIDToLoadForTable,
		numParts,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
		nil,
	)

	query, err := g.BuildQuery(ctx)
	require.NoError(t, err)
	require.Contains(t, query, " AS `_peerdb_source_schema`")
}

func TestBuildQuery_WithNumParts(t *testing.T) {
	ctx := t.Context()
	tableName := "my_table"
	rawTableName := "raw_my_table"
	part := uint64(2)
	numParts := uint64(4)
	syncBatchID := int64(10)
	batchIDToLoadForTable := int64(5)
	enablePrimaryUpdate := false
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
		part,
		tableNameSchemaMapping,
		tableMappings,
		syncBatchID,
		batchIDToLoadForTable,
		numParts,
		enablePrimaryUpdate,
		sourceSchemaAsDestinationColumn,
		env,
		rawTableName,
		nil,
	)

	query, err := g.BuildQuery(ctx)
	require.NoError(t, err)
	require.Contains(t, query, "cityHash64(_peerdb_uid) % 4 = 2")
}
