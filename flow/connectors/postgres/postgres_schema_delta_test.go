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
	_, err = setupTx.Exec(context.Background(), "CREATE SCHEMA "+schema)
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
	tableName := s.schema + ".simple_add_column"
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	err = s.connector.ReplayTableSchemaDeltas(context.Background(), "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.DeltaAddedColumn{{
			ColumnName: "hi",
			ColumnType: string(qvalue.QValueKindInt64),
		}},
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(context.Background(), &protos.GetTableSchemaBatchInput{
		TableIdentifiers: []string{tableName},
	})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(qvalue.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "hi",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
			},
		},
	}, output.TableNameSchemaMapping[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := s.schema + ".add_drop_all_column_types"
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           AddAllColumnTypesFields,
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
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

func (s PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := s.schema + ".add_drop_tricky_column_names"
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           TrickyFields,
	}
	addedColumns := make([]*protos.DeltaAddedColumn, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
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

func (s PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	tableName := s.schema + ".add_drop_whitespace_column_names"
	_, err := s.connector.conn.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{" "},
		Columns:           WhitespaceFields,
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

		require.NoError(s.t, s.connector.ConnectionActive(context.Background()))
		err = s.connector.Close(context.Background())
		require.NoError(s.t, err)
		require.Error(s.t, s.connector.ConnectionActive(context.Background()))
	})
}
