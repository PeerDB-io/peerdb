//go:build tilt

package connmysql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

func TestIntegrationSchemaMappingCaseSensitiveIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
		{name: "mariadb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			connector := newTestConnector(t, ctx, tc.name)

			// make sure case-sensitive identifiers are supported server-side by the test db
			rs, err := connector.Execute(ctx, "SELECT @@lower_case_table_names")
			require.NoError(t, err)
			lctn, err := rs.GetInt(0, 0)
			require.NoError(t, err)
			require.Equal(t, int64(0), lctn,
				"case-variant tables cannot coexist unless lower_case_table_names=0, so this test "+
					"cannot verify case sensitivity")

			// Two databases differing only by case, each holding two tables differing only by
			// case. Every table gets distinct column names so any bleed is visible.
			lowerDB := testDBName("cs")
			upperDB := strings.ToUpper(lowerDB)
			createTestDB(t, ctx, connector, lowerDB)
			createTestDB(t, ctx, connector, upperDB)

			exec := func(sql string) {
				t.Helper()
				_, err := connector.Execute(ctx, sql)
				require.NoError(t, err)
			}

			// Only the capitalised table in each database has a primary key, so a bleed between
			// variants shows up in PrimaryKeys as well as Columns.
			exec(fmt.Sprintf("CREATE TABLE `%s`.`widget` (ll_a INT, ll_b INT)", lowerDB))
			exec(fmt.Sprintf("CREATE TABLE `%s`.`Widget` (lu_a INT NOT NULL, lu_b INT, PRIMARY KEY (lu_a))", lowerDB))
			exec(fmt.Sprintf("CREATE TABLE `%s`.`widget` (ul_a INT, ul_b INT)", upperDB))
			exec(fmt.Sprintf("CREATE TABLE `%s`.`Widget` (uu_a INT NOT NULL, uu_b INT, PRIMARY KEY (uu_a))", upperDB))

			conn := connector.Conn()

			lowerLower := common.QualifiedTable{Namespace: lowerDB, Table: "widget"}
			lowerUpper := common.QualifiedTable{Namespace: lowerDB, Table: "Widget"}
			upperLower := common.QualifiedTable{Namespace: upperDB, Table: "widget"}

			// One table at a time: the case variant next to it must not appear.
			got, err := mysql_validation.GetSchemaMapping(conn, lowerDB, []string{"widget"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 1)
			require.Equal(t, []string{"ll_a", "ll_b"}, got.Columns[lowerLower])
			require.NotContains(t, got.PrimaryKeys, lowerLower)

			// The capitalised variant, whose primary key must not appear on the lowercase one.
			got, err = mysql_validation.GetSchemaMapping(conn, lowerDB, []string{"Widget"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 1)
			require.Equal(t, []string{"lu_a", "lu_b"}, got.Columns[lowerUpper])
			require.Equal(t, []string{"lu_a"}, got.PrimaryKeys[lowerUpper])

			// Same table name in the case-variant database must not leak across either.
			got, err = mysql_validation.GetSchemaMapping(conn, upperDB, []string{"widget"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 1)
			require.Equal(t, []string{"ul_a", "ul_b"}, got.Columns[upperLower])

			// Both variants in one call: the IN list in the query must not consolidate them.
			got, err = mysql_validation.GetSchemaMapping(conn, lowerDB, []string{"widget", "Widget"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 2)
			require.Equal(t, []string{"ll_a", "ll_b"}, got.Columns[lowerLower])
			require.Equal(t, []string{"lu_a", "lu_b"}, got.Columns[lowerUpper])
			require.Len(t, got.PrimaryKeys, 1)
			require.Equal(t, []string{"lu_a"}, got.PrimaryKeys[lowerUpper])
		})
	}
}

func TestIntegrationSchemaMappingPrimaryKeyVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "mysql"},
		{name: "mariadb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			connector := newTestConnector(t, ctx, tc.name)

			dbName := testDBName("pk_variants")
			createTestDB(t, ctx, connector, dbName)

			exec := func(sql string) {
				t.Helper()
				_, err := connector.Execute(ctx, sql)
				require.NoError(t, err)
			}

			// Composite PK whose key order (b, a) differs from column definition order
			// (a, b, c), so sorting by ORDINAL_POSITION would silently pass.
			exec(fmt.Sprintf("CREATE TABLE `%s`.composite_pk (a INT, b INT, c INT, PRIMARY KEY (b, a))", dbName))
			exec(fmt.Sprintf("CREATE TABLE `%s`.no_pk (a INT, b TEXT)", dbName))
			// information_schema.columns reports COLUMN_KEY = 'PRI' for a UNIQUE NOT NULL column
			// on a PK-less table, so this fails if the implementation goes back to COLUMN_KEY.
			exec(fmt.Sprintf("CREATE TABLE `%s`.uniq_notnull (a INT NOT NULL UNIQUE, b INT)", dbName))
			exec(fmt.Sprintf("CREATE TABLE `%s`.mixed_case_pk (`MyId` INT PRIMARY KEY, val TEXT)", dbName))

			// "absent" does not exist and must produce no entry, so callers' len(...) > 0
			// checks behave as they did under the old LEFT JOIN.
			got, err := mysql_validation.GetSchemaMapping(connector.Conn(), dbName,
				[]string{"composite_pk", "no_pk", "uniq_notnull", "mixed_case_pk", "absent"})
			require.NoError(t, err)

			key := func(table string) common.QualifiedTable {
				return common.QualifiedTable{Namespace: dbName, Table: table}
			}

			require.Len(t, got.Columns, 4)
			require.Equal(t, []string{"a", "b", "c"}, got.Columns[key("composite_pk")])
			require.Equal(t, []string{"b", "a"}, got.PrimaryKeys[key("composite_pk")], "index order, not [a b]")

			require.Equal(t, []string{"a", "b"}, got.Columns[key("no_pk")])
			require.NotContains(t, got.PrimaryKeys, key("no_pk"))

			require.Equal(t, []string{"a", "b"}, got.Columns[key("uniq_notnull")])
			require.NotContains(t, got.PrimaryKeys, key("uniq_notnull"), "UNIQUE NOT NULL is not a primary key")

			require.Equal(t, []string{"MyId", "val"}, got.Columns[key("mixed_case_pk")])
			require.Equal(t, []string{"MyId"}, got.PrimaryKeys[key("mixed_case_pk")])

			require.NotContains(t, got.Columns, key("absent"))
			require.NotContains(t, got.PrimaryKeys, key("absent"))
		})
	}
}
