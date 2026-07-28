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
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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
	schema := "pgdelta_" + strings.ToLower(common.RandomString(8))
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

// TestAddedColumnCatalogInfo pins down what a live Postgres hands back for a column DEFAULT, and how
// much of it survives translation. want is empty for the defaults we decline to carry over.
func (s PostgresSchemaDeltaTestSuite) TestAddedColumnCatalogInfo() {
	enumType := s.schema + ".mood"
	_, err := s.connector.conn.Exec(s.t.Context(), "CREATE TYPE "+enumType+" AS ENUM ('sad','ok')")
	require.NoError(s.t, err)

	// the case name doubles as the column name
	for _, tc := range []struct {
		name    string
		colDef  string
		qkind   types.QValueKind
		want    string
		notNull bool
	}{
		// integers, which pg quotes once a sign or enough width is involved
		{name: "c_int", colDef: "int DEFAULT 5", qkind: types.QValueKindInt32, want: "5"},
		{name: "c_int_neg", colDef: "int DEFAULT -1", qkind: types.QValueKindInt32, want: "-1"},
		{name: "c_int_zero", colDef: "int DEFAULT 0", qkind: types.QValueKindInt32, want: "0"},
		{name: "c_int_nn", colDef: "int NOT NULL DEFAULT 7", qkind: types.QValueKindInt32, want: "7", notNull: true},
		{name: "c_smallint", colDef: "smallint DEFAULT -32768", qkind: types.QValueKindInt16, want: "-32768"},
		{
			name: "c_bigint", colDef: "bigint DEFAULT 9223372036854775807", qkind: types.QValueKindInt64,
			want: "9223372036854775807",
		},

		// scale is whatever pg rendered, so numeric(10,2) keeps its trailing zero
		{name: "c_numeric", colDef: "numeric(10,2) DEFAULT 1.50", qkind: types.QValueKindNumeric, want: "1.50"},
		{name: "c_float8", colDef: "double precision DEFAULT 2.5", qkind: types.QValueKindFloat64, want: "2.5"},
		{name: "c_float4", colDef: "real DEFAULT -1.5", qkind: types.QValueKindFloat32, want: "-1.5"},

		{name: "c_bool", colDef: "boolean DEFAULT true", qkind: types.QValueKindBoolean, want: "true"},
		{name: "c_bool_false", colDef: "boolean DEFAULT false", qkind: types.QValueKindBoolean, want: "false"},

		// strings, requoted with SQL doubling so the literal is dialect neutral
		{name: "c_text", colDef: "text DEFAULT 'hello'", qkind: types.QValueKindString, want: "'hello'"},
		{name: "c_text_empty", colDef: "text DEFAULT ''", qkind: types.QValueKindString, want: "''"},
		{name: "c_text_quote", colDef: "text DEFAULT 'it''s'", qkind: types.QValueKindString, want: "'it''s'"},
		{name: "c_varchar", colDef: "varchar(10) DEFAULT 'abc'", qkind: types.QValueKindString, want: "'abc'"},
		{name: "c_char", colDef: "char(3) DEFAULT 'abc'", qkind: types.QValueKindString, want: "'abc'"},
		{name: "c_enum", colDef: enumType + " DEFAULT 'ok'", qkind: types.QValueKindEnum, want: "'ok'"},
		{
			name: "c_uuid", colDef: "uuid DEFAULT '00000000-0000-0000-0000-000000000001'", qkind: types.QValueKindUUID,
			want: "'00000000-0000-0000-0000-000000000001'",
		},
		{name: "c_jsonb", colDef: `jsonb DEFAULT '{"a": 1}'`, qkind: types.QValueKindJSONB, want: `'{"a": 1}'`},
		{name: "c_inet", colDef: "inet DEFAULT '10.0.0.1'", qkind: types.QValueKindINET, want: "'10.0.0.1'"},
		{name: "c_date", colDef: "date DEFAULT '2020-01-02'", qkind: types.QValueKindDate, want: "'2020-01-02'"},
		{
			name: "c_ts", colDef: "timestamp DEFAULT '2020-01-02 03:04:05.678'", qkind: types.QValueKindTimestamp,
			want: "'2020-01-02 03:04:05.678'",
		},
		// our connections run with timezone=UTC, so pg shifts the offset it renders to +00
		{
			name: "c_tstz", colDef: "timestamptz DEFAULT '2020-01-02 03:04:05+02'", qkind: types.QValueKindTimestampTZ,
			want: "'2020-01-02 01:04:05'",
		},

		// declined: not a constant
		{name: "c_none", colDef: "int", qkind: types.QValueKindInt32},
		{name: "c_null", colDef: "int DEFAULT NULL", qkind: types.QValueKindInt32},
		{name: "c_now", colDef: "timestamptz DEFAULT now()", qkind: types.QValueKindTimestampTZ},
		{name: "c_expr", colDef: "int DEFAULT (2 + 3)", qkind: types.QValueKindInt32},
		{name: "c_concat", colDef: "text DEFAULT 'a' || 'b'", qkind: types.QValueKindString},
		{name: "c_serial", colDef: "serial", qkind: types.QValueKindInt32, notNull: true},
		{name: "c_identity", colDef: "int GENERATED BY DEFAULT AS IDENTITY", qkind: types.QValueKindInt32, notNull: true},
		// a generated column keeps its expression in pg_attrdef, where a default would sit
		{name: "c_generated", colDef: "int GENERATED ALWAYS AS (base * 2) STORED", qkind: types.QValueKindInt32},

		// declined: a constant whose text form does not carry over
		{name: "c_array", colDef: "int[] DEFAULT '{1,2}'", qkind: types.QValueKindArrayInt32},
		{name: "c_bytea", colDef: `bytea DEFAULT '\x0001'`, qkind: types.QValueKindBytes},
		{name: "c_interval", colDef: "interval DEFAULT '1 day'", qkind: types.QValueKindInterval},
		{name: "c_time", colDef: "time DEFAULT '13:14:15'", qkind: types.QValueKindTime},
		{name: "c_nan", colDef: "numeric DEFAULT 'NaN'", qkind: types.QValueKindNumeric},
		// backslashes escape differently across dialects
		{name: "c_backslash", colDef: `text DEFAULT E'a\\b'`, qkind: types.QValueKindString},
	} {
		s.t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("%s.added_column_catalog_info_%s", s.schema, tc.name)
			_, err := s.connector.conn.Exec(t.Context(),
				fmt.Sprintf("CREATE TABLE %s(base int, %s %s)", tableName, tc.name, tc.colDef))
			require.NoError(t, err)

			var relID uint32
			require.NoError(t, s.connector.conn.QueryRow(t.Context(), "SELECT $1::regclass::oid", tableName).Scan(&relID))

			catalogInfo, err := s.connector.fetchAddedColumnCatalogInfo(t.Context(), relID, []string{tc.name})
			require.NoError(t, err)
			require.Len(t, catalogInfo, 1)
			require.Equal(t, tc.notNull, catalogInfo[tc.name].notNull)

			var literal string
			if renderedDefault := catalogInfo[tc.name].defaultExpr; renderedDefault != "" {
				literal, _ = defaultExprFromPostgresDefault(renderedDefault, tc.qkind)
			}
			require.Equal(t, tc.want, literal)
		})
	}
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
