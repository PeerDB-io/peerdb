package e2e_snowflake

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/ysmood/got"
)

const schemaDeltaTestSchemaName = "PUBLIC"

type SnowflakeSchemaDeltaTestSuite struct {
	got.G
	t *testing.T

	connector    *connsnowflake.SnowflakeConnector
	sfTestHelper *SnowflakeTestHelper
}

func (suite SnowflakeSchemaDeltaTestSuite) failTestError(err error) {
	if err != nil {
		slog.Error("Error in test", slog.Any("error", err))
		suite.FailNow()
	}
}

func setupSchemaDeltaSuite(
	t *testing.T,
	g got.G,
) SnowflakeSchemaDeltaTestSuite {
	t.Helper()

	sfTestHelper, err := NewSnowflakeTestHelper()
	if err != nil {
		slog.Error("Error in test", slog.Any("error", err))
		t.FailNow()
	}

	connector, err := connsnowflake.NewSnowflakeConnector(
		context.Background(),
		sfTestHelper.Config,
	)
	if err != nil {
		slog.Error("Error in test", slog.Any("error", err))
		t.FailNow()
	}

	return SnowflakeSchemaDeltaTestSuite{
		G:            g,
		t:            t,
		connector:    connector,
		sfTestHelper: sfTestHelper,
	}
}

func (suite SnowflakeSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := fmt.Sprintf("%s.SIMPLE_ADD_COLUMN", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "HI",
			ColumnType: string(qvalue.QValueKindJSON),
		}},
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames:     []string{"ID", "HI"},
		ColumnTypes:     []string{string(qvalue.QValueKindString), string(qvalue.QValueKindJSON)},
	}, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := fmt.Sprintf("%s.ADD_DROP_ALL_COLUMN_TYPES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(ID TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		// goal is to test all types we're currently mapping to, not all QValue types
		ColumnNames: []string{"ID", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10"},
		ColumnTypes: []string{
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindBoolean),
			string(qvalue.QValueKindBytes),
			string(qvalue.QValueKindDate),
			string(qvalue.QValueKindFloat64),
			string(qvalue.QValueKindJSON),
			string(qvalue.QValueKindNumeric),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindTime),
			string(qvalue.QValueKindTimestamp),
			string(qvalue.QValueKindTimestampTZ),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	utils.IterColumns(expectedTableSchema, func(columnName, columnType string) {
		if columnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	})

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_TRICKY_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(id TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		// strings.ToUpper also does Unicode uppercasing :)
		ColumnNames: []string{
			"ID",
			"C1",
			"C 1",
			"RIGHT",
			"SELECT",
			"XMIN",
			"CARIÑO",
			"±ªÞ³§",
			"カラム",
		},
		ColumnTypes: []string{
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	utils.IterColumns(expectedTableSchema, func(columnName, columnType string) {
		if columnName != "ID" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	})

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func (suite SnowflakeSchemaDeltaTestSuite) TestAddWhitespaceColumnNames() {
	tableName := fmt.Sprintf("%s.ADD_DROP_WHITESPACE_COLUMN_NAMES", schemaDeltaTestSchemaName)
	err := suite.sfTestHelper.RunCommand(fmt.Sprintf("CREATE TABLE %s(\" \" TEXT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames:     []string{" ", "  ", "   ", "\t"},
		ColumnTypes: []string{
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindTime),
			string(qvalue.QValueKindDate),
		},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	utils.IterColumns(expectedTableSchema, func(columnName, columnType string) {
		if columnName != " " {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: columnType,
			})
		}
	})

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(expectedTableSchema, output.TableNameSchemaMapping[tableName])
}

func TestSnowflakeSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.GotSuite(t, setupSchemaDeltaSuite, func(suite SnowflakeSchemaDeltaTestSuite) {
		suite.failTestError(suite.sfTestHelper.Cleanup())
		suite.failTestError(suite.connector.Close())
	})
}
