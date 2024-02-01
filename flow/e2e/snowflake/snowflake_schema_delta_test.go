package e2e_snowflake

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

const schemaDeltaTestSchemaName = "PUBLIC"

type SnowflakeSchemaDeltaTestSuite struct {
	t *testing.T

	connector    *connsnowflake.SnowflakeConnector
	sfTestHelper *SnowflakeTestHelper
}

func setupSchemaDeltaSuite(t *testing.T) SnowflakeSchemaDeltaTestSuite {
	t.Helper()

	sfTestHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		t.Fatalf("Error in test: %v", err)
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		context.Background(),
		sfTestHelper.Config,
	)
	if err != nil {
		t.Fatalf("Error in test: %v", err)
	}

	return SnowflakeSchemaDeltaTestSuite{
		t:            t,
		connector:    connector,
		sfTestHelper: sfTestHelper,
	}
}

func (s SnowflakeSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := fmt.Sprintf("%s.SIMPLE_ADD_COLUMN", schemaDeltaTestSchemaName)
	err := s.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	err = s.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "HI",
			ColumnType: string(qvalue.QValueKindJSON),
		}},
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				ColumnName:   "ID",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "HI",
				ColumnType:   string(qvalue.QValueKindJSON),
				TypeModifier: -1,
			},
		},
	}, output.TableNameSchemaMapping[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := fmt.Sprintf("%s.ADD_DROP_ALL_COLUMN_TYPES", schemaDeltaTestSchemaName)
	err := s.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				ColumnName:   "ID",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C1",
				ColumnType:   string(qvalue.QValueKindBoolean),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C2",
				ColumnType:   string(qvalue.QValueKindBytes),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C3",
				ColumnType:   string(qvalue.QValueKindDate),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C4",
				ColumnType:   string(qvalue.QValueKindFloat64),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C5",
				ColumnType:   string(qvalue.QValueKindJSON),
				TypeModifier: -1,
			},
			{
				ColumnName: "C6",
				ColumnType: string(qvalue.QValueKindNumeric),

				TypeModifier: -1,
			},
			{
				ColumnName:   "C7",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C8",
				ColumnType:   string(qvalue.QValueKindTime),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C9",
				ColumnType:   string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C10",
				ColumnType:   string(qvalue.QValueKindTimestampTZ),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.ColumnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.ColumnName,
				ColumnType: column.ColumnType,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_TRICKY_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := s.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(id TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				ColumnName:   "ID",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C1",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C1",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "C 1",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "right",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "select",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "XMIN",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "Cariño",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "±ªþ³§",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "カラム",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.ColumnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.ColumnName,
				ColumnType: column.ColumnType,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (s SnowflakeSchemaDeltaTestSuite) TestAddWhitespaceColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_WHITESPACE_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := s.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(\" \" TEXT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				ColumnName:   " ",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "  ",
				ColumnType:   string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				ColumnName:   "   ",
				ColumnType:   string(qvalue.QValueKindTime),
				TypeModifier: -1,
			},
			{
				ColumnName:   "\t",
				ColumnType:   string(qvalue.QValueKindDate),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.ColumnName != " " {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.ColumnName,
				ColumnType: column.ColumnType,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func TestSnowflakeSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, setupSchemaDeltaSuite, func(s SnowflakeSchemaDeltaTestSuite) {
		require.NoError(s.t, s.sfTestHelper.Cleanup())
		require.NoError(s.t, s.connector.Close())
	})
}
