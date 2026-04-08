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
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresSchemaDeltaTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

var typeSystems = []protos.TypeSystem{protos.TypeSystem_Q, protos.TypeSystem_PG}

func (s PostgresSchemaDeltaTestSuite) TestSimpleAddColumn() {
	for _, system := range typeSystems {
		s.t.Run(system.String(), func(t *testing.T) {
			s.testSimpleAddColumn(t, system)
		})
	}
}

func (s PostgresSchemaDeltaTestSuite) testSimpleAddColumn(t *testing.T, system protos.TypeSystem) {
	t.Helper()
	tableName := fmt.Sprintf("%s.simple_add_column_%s", s.schema, strings.ToLower(system.String()))
	_, err := s.connector.conn.Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(t, err)

	addedColumns := fieldsForSystem([]*protos.FieldDescription{
		{
			Name:           "hi",
			Type:           string(types.QValueKindInt64),
			TypeModifier:   -1,
			Nullable:       true,
			TypeSchemaName: "pg_catalog",
		},
	}, system)

	require.NoError(t, s.connector.ReplayTableSchemaDeltas(t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
		System:       system,
	}}, nil))

	output, err := s.connector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, system,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(t, err)
	require.NotEqual(t, 0, output[tableName].TableOid)
	output[tableName].TableOid = 0 // zero out TableOid for comparison

	expectedColumns := fieldsForSystem([]*protos.FieldDescription{
		{
			Name:           "id",
			Type:           string(types.QValueKindInt32),
			TypeModifier:   -1,
			TypeSchemaName: "pg_catalog",
		},
		{
			Name:           "hi",
			Type:           string(types.QValueKindInt64),
			TypeModifier:   -1,
			Nullable:       true,
			TypeSchemaName: "pg_catalog",
		},
	}, system)

	require.Equal(t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            system,
		Columns:           expectedColumns,
	}, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	for _, system := range typeSystems {
		s.t.Run(system.String(), func(t *testing.T) {
			s.testAddAllColumnTypes(t, system)
		})
	}
}

func (s PostgresSchemaDeltaTestSuite) testAddAllColumnTypes(t *testing.T, system protos.TypeSystem) {
	t.Helper()
	tableName := fmt.Sprintf("%s.add_drop_all_column_types_%s", s.schema, strings.ToLower(system.String()))
	_, err := s.connector.conn.Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(t, err)

	fields := fieldsForSystem(AddAllColumnTypesFields, system)
	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           fields,
		System:            system,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range fields {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(t, s.connector.ReplayTableSchemaDeltas(t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
		System:       system,
	}}, nil))

	output, err := s.connector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, system,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(t, err)
	require.NotEqual(t, 0, output[tableName].TableOid)
	output[tableName].TableOid = 0 // zero out TableOid for comparison
	require.Equal(t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	for _, system := range typeSystems {
		s.t.Run(system.String(), func(t *testing.T) {
			s.testAddTrickyColumnNames(t, system)
		})
	}
}

func (s PostgresSchemaDeltaTestSuite) testAddTrickyColumnNames(t *testing.T, system protos.TypeSystem) {
	t.Helper()
	tableName := fmt.Sprintf("%s.add_drop_tricky_column_names_%s", s.schema, strings.ToLower(system.String()))
	_, err := s.connector.conn.Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(t, err)

	fields := fieldsForSystem(TrickyFields, system)
	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           fields,
		System:            system,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range fields {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(t, s.connector.ReplayTableSchemaDeltas(t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
		System:       system,
	}}, nil))

	output, err := s.connector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, system,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(t, err)
	require.NotEqual(t, 0, output[tableName].TableOid)
	output[tableName].TableOid = 0 // zero out TableOid for comparison
	require.Equal(t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	for _, system := range typeSystems {
		s.t.Run(system.String(), func(t *testing.T) {
			s.testAddDropWhitespaceColumnNames(t, system)
		})
	}
}

func (s PostgresSchemaDeltaTestSuite) testAddDropWhitespaceColumnNames(t *testing.T, system protos.TypeSystem) {
	t.Helper()
	tableName := fmt.Sprintf("%s.add_drop_whitespace_column_names_%s", s.schema, strings.ToLower(system.String()))
	_, err := s.connector.conn.Exec(t.Context(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	require.NoError(t, err)

	fields := fieldsForSystem(WhitespaceFields, system)
	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{" "},
		Columns:           fields,
		System:            system,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range fields {
		if column.Name != " " {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(t, s.connector.ReplayTableSchemaDeltas(t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
		System:       system,
	}}, nil))

	output, err := s.connector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, system,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(t, err)
	require.NotEqual(t, 0, output[tableName].TableOid)
	output[tableName].TableOid = 0 // zero out TableOid for comparison
	require.Equal(t, expectedTableSchema, output[tableName])
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
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.schema))
	require.NoError(s.t, err)
	require.NoError(s.t, teardownTx.Commit(ctx))

	require.NoError(s.t, s.connector.ConnectionActive(ctx))
	require.NoError(s.t, s.connector.Close())
	require.Error(s.t, s.connector.ConnectionActive(ctx))
}
