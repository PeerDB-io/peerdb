package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PostgresSchemaDeltaTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupSuite(t *testing.T) PostgresSchemaDeltaTestSuite {
	t.Helper()

	connector, err := NewPostgresConnector(context.Background(), &protos.PostgresConfig{
		Host:     "localhost",
		Port:     7132,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)

	setupTx, err := connector.conn.Begin(context.Background())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()
	schema := "pgdelta_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schema))
	require.NoError(t, err)
	err = setupTx.Commit(context.Background())
	require.NoError(t, err)

	return PostgresSchemaDeltaTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := fmt.Sprintf("%s.simple_add_column", s.schema)
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	err = s.connector.ReplayTableSchemaDeltas("schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "hi",
			ColumnType: string(qvalue.QValueKindInt64),
		}},
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(&protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		ColumnNames:       []string{"id", "hi"},
		ColumnTypes:       []string{string(qvalue.QValueKindInt32), string(qvalue.QValueKindInt64)},
		PrimaryKeyColumns: []string{"id"},
	}, output.TableNameSchemaMapping[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := fmt.Sprintf("%s.add_drop_all_column_types", s.schema)
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

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
	for i, columnName := range expectedTableSchema.ColumnNames {
		if columnName != "id" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: expectedTableSchema.ColumnTypes[i],
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

func (s PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := fmt.Sprintf("%s.add_drop_tricky_column_names", s.schema)
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

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
	for i, columnName := range expectedTableSchema.ColumnNames {
		if columnName != "id" {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: expectedTableSchema.ColumnTypes[i],
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

func (s PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	tableName := fmt.Sprintf("%s.add_drop_whitespace_column_names", s.schema)
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

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
	for i, columnName := range expectedTableSchema.ColumnNames {
		if columnName != " " {
			addedColumns = append(addedColumns, &protos.DeltaAddedColumn{
				ColumnName: columnName,
				ColumnType: expectedTableSchema.ColumnTypes[i],
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

func TestPostgresSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite, func(s PostgresSchemaDeltaTestSuite) {
		teardownTx, err := s.connector.conn.Begin(context.Background())
		require.NoError(s.t, err)
		defer func() {
			err := teardownTx.Rollback(context.Background())
			if err != pgx.ErrTxClosed {
				require.NoError(s.t, err)
			}
		}()
		_, err = teardownTx.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
			s.schema))
		require.NoError(s.t, err)
		err = teardownTx.Commit(context.Background())
		require.NoError(s.t, err)

		require.NoError(s.t, s.connector.ConnectionActive())
		err = s.connector.Close()
		require.NoError(s.t, err)
		require.Error(s.t, s.connector.ConnectionActive())
	})
}
