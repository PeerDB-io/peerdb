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

	err = s.connector.ReplayTableSchemaDeltas(context.Background(), "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "HI",
			ColumnType: string(qvalue.QValueKindJSON),
		}},
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier: tableName,
		Columns: []*protos.FieldDescription{
			{
				Name:         "ID",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "HI",
				Type:         string(qvalue.QValueKindJSON),
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
				Name:         "ID",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C1",
				Type:         string(qvalue.QValueKindBoolean),
				TypeModifier: -1,
			},
			{
				Name:         "C2",
				Type:         string(qvalue.QValueKindBytes),
				TypeModifier: -1,
			},
			{
				Name:         "C3",
				Type:         string(qvalue.QValueKindDate),
				TypeModifier: -1,
			},
			{
				Name:         "C4",
				Type:         string(qvalue.QValueKindFloat64),
				TypeModifier: -1,
			},
			{
				Name:         "C5",
				Type:         string(qvalue.QValueKindJSON),
				TypeModifier: -1,
			},
			{
				Name: "C6",
				Type: string(qvalue.QValueKindNumeric),

				TypeModifier: -1,
			},
			{
				Name:         "C7",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C8",
				Type:         string(qvalue.QValueKindTime),
				TypeModifier: -1,
			},
			{
				Name:         "C9",
				Type:         string(qvalue.QValueKindTimestamp),
				TypeModifier: -1,
			},
			{
				Name:         "C10",
				Type:         string(qvalue.QValueKindTimestampTZ),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.Name,
				ColumnType: column.Type,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(context.Background(), "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
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
				Name:         "ID",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C1",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "C 1",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "RIGHT",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "SELECT",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "XMIN",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "CARIÑO",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "±ªÞ³§",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "カラム",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.Name,
				ColumnType: column.Type,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(context.Background(), "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
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
				Name:         " ",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "  ",
				Type:         string(qvalue.QValueKindString),
				TypeModifier: -1,
			},
			{
				Name:         "   ",
				Type:         string(qvalue.QValueKindTime),
				TypeModifier: -1,
			},
			{
				Name:         "\t",
				Type:         string(qvalue.QValueKindDate),
				TypeModifier: -1,
			},
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != " " {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.Name,
				ColumnType: column.Type,
			})
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(context.Background(), "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
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
