package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PostgresSchemaDeltaTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupSuite(t *testing.T) PostgresSchemaDeltaTestSuite {
	t.Helper()

	connector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)

	setupTx, err := connector.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()
	schema := "pgdelta_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	err = setupTx.Commit(t.Context())
	require.NoError(t, err)

	return PostgresSchemaDeltaTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := s.schema + ".simple_add_column"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.FieldDescription{
			{
				Name:         "hi",
				Type:         string(qvalue.QValueKindInt64),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
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
				Nullable:     true,
			},
		},
	}, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := s.schema + ".add_drop_all_column_types"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           AddAllColumnTypesFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := s.schema + ".add_drop_tricky_column_names"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           TrickyFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	tableName := s.schema + ".add_drop_whitespace_column_names"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{" "},
		Columns:           WhitespaceFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != " " {
			addedColumns = append(addedColumns, column)
		}
	}

	err = s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}})
	require.NoError(s.t, err)

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func TestPostgresSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PostgresSchemaDeltaTestSuite) Teardown(ctx context.Context) {
	teardownTx, err := s.connector.conn.Begin(ctx)
	require.NoError(s.t, err)
	defer func() {
		err := teardownTx.Rollback(ctx)
		if err != pgx.ErrTxClosed {
			require.NoError(s.t, err)
		}
	}()
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE",
		s.schema))
	require.NoError(s.t, err)
	err = teardownTx.Commit(ctx)
	require.NoError(s.t, err)

	require.NoError(s.t, s.connector.ConnectionActive(ctx))
	require.NoError(s.t, s.connector.Close())
	require.Error(s.t, s.connector.ConnectionActive(ctx))
}
