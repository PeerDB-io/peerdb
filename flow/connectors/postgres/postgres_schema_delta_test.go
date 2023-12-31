package connpostgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

type PostgresSchemaDeltaTestSuite struct {
	suite.Suite
	connector *PostgresConnector
}

const schemaDeltaTestSchemaName = "pgschema_delta_test"

func (suite *PostgresSchemaDeltaTestSuite) failTestError(err error) {
	if err != nil {
		suite.FailNow(err.Error())
	}
}

func (suite *PostgresSchemaDeltaTestSuite) SetupSuite() {
	var err error
	suite.connector, err = NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	}, false)
	suite.failTestError(err)

	setupTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()
	_, err = setupTx.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		schemaDeltaTestSchemaName))
	suite.failTestError(err)
	_, err = setupTx.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schemaDeltaTestSchemaName))
	suite.failTestError(err)
	err = setupTx.Commit(context.Background())
	suite.failTestError(err)
}

func (suite *PostgresSchemaDeltaTestSuite) TearDownSuite() {
	teardownTx, err := suite.connector.pool.Begin(context.Background())
	suite.failTestError(err)
	defer func() {
		err := teardownTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			suite.failTestError(err)
		}
	}()
	_, err = teardownTx.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		schemaDeltaTestSchemaName))
	suite.failTestError(err)
	err = teardownTx.Commit(context.Background())
	suite.failTestError(err)

	suite.True(suite.connector.ConnectionActive() == nil)
	err = suite.connector.Close()
	suite.failTestError(err)
	suite.False(suite.connector.ConnectionActive() == nil)
}

func (suite *PostgresSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := fmt.Sprintf("%s.simple_add_column", schemaDeltaTestSchemaName)
	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	err = suite.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "hi",
			ColumnType: string(qvalue.QValueKindInt64),
		}},
	}})
	suite.failTestError(err)

	output, err := suite.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	suite.failTestError(err)
	suite.Equal(&protos.TableSchema{
		TableIdentifier:   tableName,
		ColumnNames:       []string{"id", "hi"},
		ColumnTypes:       []string{string(qvalue.QValueKindInt32), string(qvalue.QValueKindInt64)},
		PrimaryKeyColumns: []string{"id"},
	}, output.TableNameSchemaMapping[tableName])
}

func (suite *PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := fmt.Sprintf("%s.add_drop_all_column_types", schemaDeltaTestSchemaName)
	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		// goal is to test all types we're currently mapping to, not all QValue types
		ColumnNames: []string{
			"id", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9",
			"c10", "c11", "c12", "c13", "c14", "c15", "c16",
		},
		ColumnTypes: []string{
			string(qvalue.QValueKindInt32),
			string(qvalue.QValueKindBit),
			string(qvalue.QValueKindBoolean),
			string(qvalue.QValueKindBytes),
			string(qvalue.QValueKindDate),
			string(qvalue.QValueKindFloat32),
			string(qvalue.QValueKindFloat64),
			string(qvalue.QValueKindInt16),
			string(qvalue.QValueKindInt32),
			string(qvalue.QValueKindInt64),
			string(qvalue.QValueKindJSON),
			string(qvalue.QValueKindNumeric),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindTime),
			string(qvalue.QValueKindTimestamp),
			string(qvalue.QValueKindTimestampTZ),
			string(qvalue.QValueKindUUID),
		},
		PrimaryKeyColumns: []string{"id"},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	utils.IterColumns(expectedTableSchema, func(columnName, columnType string) {
		if columnName != "id" {
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

func (suite *PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := fmt.Sprintf("%s.add_drop_tricky_column_names", schemaDeltaTestSchemaName)
	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames: []string{
			"id", "c1", "C1", "C 1", "right",
			"select", "XMIN", "Cariño", "±ªþ³§", "カラム",
		},
		ColumnTypes: []string{
			string(qvalue.QValueKindInt32),
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
		PrimaryKeyColumns: []string{"id"},
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	utils.IterColumns(expectedTableSchema, func(columnName, columnType string) {
		if columnName != "id" {
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

func (suite *PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	tableName := fmt.Sprintf("%s.add_drop_whitespace_column_names", schemaDeltaTestSchemaName)
	_, err := suite.connector.pool.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	suite.failTestError(err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames:     []string{" ", "  ", "   ", "\t"},
		ColumnTypes: []string{
			string(qvalue.QValueKindInt32),
			string(qvalue.QValueKindString),
			string(qvalue.QValueKindInt64),
			string(qvalue.QValueKindDate),
		},
		PrimaryKeyColumns: []string{" "},
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

func TestPostgresSchemaDeltaTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresSchemaDeltaTestSuite))
}
