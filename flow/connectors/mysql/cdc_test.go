package connmysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
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
