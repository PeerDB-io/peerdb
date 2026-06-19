package connmysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func newTestConnector(t *testing.T, ctx context.Context) *MySqlConnector {
	t.Helper()
	flavor := protos.MySqlFlavor_MYSQL_MYSQL
	if internal.MySQLTestVersionIsMaria() {
		flavor = protos.MySqlFlavor_MYSQL_MARIA
	}
	connector, err := NewMySqlConnector(ctx, &protos.MySqlConfig{
		Host:       internal.MySQLTestHost(),
		Port:       internal.MySQLTestPort(),
		User:       "root",
		Password:   internal.MySQLTestRootPasswordWithFallback("cipass"),
		Database:   "mysql",
		DisableTls: true,
		Flavor:     flavor,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := connector.Close(); err != nil {
			t.Logf("connector close failed: %v", err)
		}
	})
	return connector
}

func createTestDB(t *testing.T, ctx context.Context, c *MySqlConnector, dbName string) {
	t.Helper()
	quoted := "`" + dbName + "`"
	_, err := c.Execute(ctx, "DROP DATABASE IF EXISTS "+quoted)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "CREATE DATABASE "+quoted)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := c.Execute(context.Background(), "DROP DATABASE IF EXISTS "+quoted); err != nil {
			t.Logf("drop test db %s failed: %v", dbName, err)
		}
	})
}

func TestMappedTableForUnparsedColumnAlter(t *testing.T) {
	t.Parallel()

	tableNameMapping := map[string]model.NameAndExclude{
		"db.t":        {Name: "dst"},
		"db.mixed":    {Name: "mixed_dst"},
		"db.has`tick": {Name: "tick_dst"},
		"other.t":     {Name: "other_dst"},
		"db.empty":    {},
	}

	testCases := []struct {
		name        string
		query       string
		eventSchema string
		wantTable   string
		wantMatch   bool
	}{
		{
			name:      "schema qualified add column",
			query:     "ALTER TABLE db.t ADD COLUMN c INT",
			wantTable: "db.t",
			wantMatch: true,
		},
		{
			name:      "backtick qualified modify column",
			query:     "ALTER TABLE `db`.`t` MODIFY COLUMN c BIGINT",
			wantTable: "db.t",
			wantMatch: true,
		},
		{
			name:        "implicit schema drop column",
			query:       "ALTER TABLE t DROP COLUMN c",
			eventSchema: "db",
			wantTable:   "db.t",
			wantMatch:   true,
		},
		{
			name:      "change column",
			query:     "ALTER TABLE db.t CHANGE COLUMN c c2 INT",
			wantTable: "db.t",
			wantMatch: true,
		},
		{
			name:      "drop index followed by drop column",
			query:     "ALTER TABLE db.t DROP INDEX c_idx, DROP COLUMN c",
			wantTable: "db.t",
			wantMatch: true,
		},
		{
			name:      "multi statement mapped alter",
			query:     "SET @x = 1; ALTER TABLE `db`.`mixed` ADD COLUMN c INT",
			wantTable: "db.mixed",
			wantMatch: true,
		},
		{
			name:      "escaped backtick in table identifier",
			query:     "ALTER TABLE `db`.`has``tick` ADD COLUMN c INT",
			wantTable: "db.has`tick",
			wantMatch: true,
		},
		{
			name:  "unmapped add column",
			query: "ALTER TABLE db.unmapped ADD COLUMN c INT",
		},
		{
			name:  "mapped entry with empty destination",
			query: "ALTER TABLE db.empty ADD COLUMN c INT",
		},
		{
			name:  "non column table option",
			query: "ALTER TABLE db.t ENGINE=InnoDB",
		},
		{
			name:  "non column add unique index",
			query: "ALTER TABLE db.t ADD UNIQUE INDEX c_idx (c)",
		},
		{
			name:  "non column optimize",
			query: "OPTIMIZE TABLE db.t",
		},
		{
			name:        "implicit schema missing event schema",
			query:       "ALTER TABLE t ADD COLUMN c INT",
			eventSchema: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotTable, gotMatch := mappedTableForUnparsedColumnAlter(tc.query, tc.eventSchema, tableNameMapping)
			require.Equal(t, tc.wantMatch, gotMatch)
			require.Equal(t, tc.wantTable, gotTable)
		})
	}
}

func TestTiDBParserRejectsMariaDBUUIDColumnAlter(t *testing.T) {
	t.Parallel()

	_, _, err := parser.New().ParseSQL("ALTER TABLE db.t ADD COLUMN uuid_col UUID")
	require.Error(t, err)
}

func TestGetTableSchemaCaseSensitiveIdentifiers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	// make sure case-sensitive identifiers are supported server-side by the test db
	rs, err := connector.Execute(ctx, "SELECT @@lower_case_table_names")
	require.NoError(t, err)
	lctn, err := rs.GetInt(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), lctn)

	lowerDB := "cs"
	upperDB := "CS_IDS_TEST"
	createTestDB(t, ctx, connector, lowerDB)
	createTestDB(t, ctx, connector, upperDB)

	llTable := lowerDB + ".cs_t"
	luTable := lowerDB + ".CS_T"
	ulTable := upperDB + ".cs_t"
	uuTable := upperDB + ".CS_T"

	exec := func(sql string) {
		t.Helper()
		_, err := connector.Execute(ctx, sql)
		require.NoError(t, err)
	}

	assertSchema := func(schema *protos.TableSchema, pk []string, cols ...string) {
		t.Helper()
		require.NotNil(t, schema)
		require.Equal(t, pk, schema.PrimaryKeyColumns)
		require.Len(t, schema.Columns, len(cols))
		for i, name := range cols {
			require.Equal(t, name, schema.Columns[i].Name)
		}
	}

	exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, ll TEXT)", llTable))
	exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, lu TEXT)", luTable))
	exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, ul TEXT)", ulTable))
	exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, uu TEXT)", uuTable))

	schemas, err := connector.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{
			{SourceTableIdentifier: llTable},
			{SourceTableIdentifier: luTable},
			{SourceTableIdentifier: ulTable},
			{SourceTableIdentifier: uuTable},
		})
	require.NoError(t, err)
	assertSchema(schemas[llTable], []string{"id"}, "id", "ll")
	assertSchema(schemas[luTable], []string{"id"}, "id", "lu")
	assertSchema(schemas[ulTable], []string{"id"}, "id", "ul")
	assertSchema(schemas[uuTable], []string{"id"}, "id", "uu")
}

func TestGetTableSchemaPrimaryKeyVariants(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	dbName := "pk_variants_test"
	createTestDB(t, ctx, connector, dbName)

	exec := func(sql string) {
		t.Helper()
		_, err := connector.Execute(ctx, sql)
		require.NoError(t, err)
	}

	assertSchema := func(schema *protos.TableSchema, pk []string, cols ...string) {
		t.Helper()
		require.NotNil(t, schema)
		if len(pk) == 0 {
			require.Empty(t, schema.PrimaryKeyColumns)
		} else {
			require.Equal(t, pk, schema.PrimaryKeyColumns)
		}
		require.Len(t, schema.Columns, len(cols))
		for i, name := range cols {
			require.Equal(t, name, schema.Columns[i].Name)
		}
	}

	compositeTable := dbName + ".composite_pk"
	noPKTable := dbName + ".no_pk"
	mixedCaseTable := dbName + ".mixed_case_pk"

	// Composite PK whose key order (b, a) differs from column definition order (a, b, c),
	// exercising the seq_in_index sort.
	exec(fmt.Sprintf("CREATE TABLE %s (a INT, b INT, c INT, PRIMARY KEY (b, a))", compositeTable))
	// Table without a primary key, exercising the LEFT JOIN's NULL seq_in_index path.
	exec(fmt.Sprintf("CREATE TABLE %s (a INT, b TEXT)", noPKTable))
	// PK on a mixed-case column name, exercising the case-preserving column_name join.
	exec(fmt.Sprintf("CREATE TABLE %s (`MyId` INT PRIMARY KEY, val TEXT)", mixedCaseTable))

	schemas, err := connector.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{
			{SourceTableIdentifier: compositeTable},
			{SourceTableIdentifier: noPKTable},
			{SourceTableIdentifier: mixedCaseTable},
		})
	require.NoError(t, err)
	assertSchema(schemas[compositeTable], []string{"b", "a"}, "a", "b", "c")
	assertSchema(schemas[noPKTable], nil, "a", "b")
	assertSchema(schemas[mixedCaseTable], []string{"MyId"}, "MyId", "val")
}
