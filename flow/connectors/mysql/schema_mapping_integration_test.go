//go:build tilt

package connmysql

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

type schemaMapping = map[common.QualifiedTable][]string

func TestSchemaMappingCaseSensitiveIdentifiers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	// make sure case-sensitive identifiers are supported server-side by the test db
	rs, err := connector.Execute(ctx, "SELECT @@lower_case_table_names")
	require.NoError(t, err)
	lctn, err := rs.GetInt(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), lctn,
		"tables differing only in casing cannot coexist unless lower_case_table_names=0, so this test "+
			"cannot verify case sensitivity")

	// Two databases differing only by case, each holding two tables differing only by
	// case.
	lowerDB := testDBName("cs")
	upperDB := strings.ToUpper(lowerDB)
	createTestDB(t, ctx, connector, lowerDB)
	createTestDB(t, ctx, connector, upperDB)

	exec := func(sql string) {
		t.Helper()
		_, err := connector.Execute(ctx, sql)
		require.NoError(t, err)
	}

	// The four tables share column names, so a bleed cannot be hidden by a future implementation
	// which drops columns that the table does not have. Each primary key differs in column set
	// or order, to catch PK bleeds or when SEQ_IN_INDEX order is not respected.
	exec(fmt.Sprintf("CREATE TABLE `%s`.`widget` (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a))", lowerDB))
	exec(fmt.Sprintf("CREATE TABLE `%s`.`Widget` (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (b, a))", lowerDB))
	exec(fmt.Sprintf("CREATE TABLE `%s`.`widget` (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (b))", upperDB))
	exec(fmt.Sprintf("CREATE TABLE `%s`.`Widget` (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))", upperDB))

	conn := connector.Conn()

	lowerLower := common.QualifiedTable{Namespace: lowerDB, Table: "widget"}
	lowerUpper := common.QualifiedTable{Namespace: lowerDB, Table: "Widget"}
	upperLower := common.QualifiedTable{Namespace: upperDB, Table: "widget"}
	upperUpper := common.QualifiedTable{Namespace: upperDB, Table: "Widget"}

	// Whole maps are compared rather than single keys, so we catch cases with extra values.
	for _, tt := range []struct {
		name      string
		db        string
		tables    []string
		wantCols  schemaMapping
		wantPKeys schemaMapping
	}{
		// One table at a time: the columns of the table differing in casing must not appear.
		{
			name: "lowercase alone", db: lowerDB, tables: []string{"widget"},
			wantCols:  schemaMapping{lowerLower: {"a", "b"}},
			wantPKeys: schemaMapping{lowerLower: {"a"}},
		},
		{
			name: "capitalised alone", db: lowerDB, tables: []string{"Widget"},
			wantCols:  schemaMapping{lowerUpper: {"a", "b"}},
			wantPKeys: schemaMapping{lowerUpper: {"b", "a"}},
		},
		// Same test as the alone test cases, but now on the uppercase database.
		{
			name: "uppercase database", db: upperDB, tables: []string{"widget"},
			wantCols:  schemaMapping{upperLower: {"a", "b"}},
			wantPKeys: schemaMapping{upperLower: {"b"}},
		},
		{
			name: "uppercase database, capitalised table", db: upperDB, tables: []string{"Widget"},
			wantCols:  schemaMapping{upperUpper: {"a", "b"}},
			wantPKeys: schemaMapping{upperUpper: {"a", "b"}},
		},
		// Both table cases in one call: the IN list in the query must not combine their columns.
		{
			name: "both variants", db: lowerDB, tables: []string{"widget", "Widget"},
			wantCols:  schemaMapping{lowerLower: {"a", "b"}, lowerUpper: {"a", "b"}},
			wantPKeys: schemaMapping{lowerLower: {"a"}, lowerUpper: {"b", "a"}},
		},
	} {
		got, err := mysql_validation.GetSchemaMapping(conn, tt.db, tt.tables)
		require.NoError(t, err, tt.name)
		require.Equal(t, tt.wantCols, got.Columns, tt.name)
		require.Equal(t, tt.wantPKeys, got.PrimaryKeys, tt.name)
	}
}

func TestSchemaMappingPrimaryKeyVariants(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

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
}

// TestSchemaMappingDoesNotScanInstance asserts that GetSchemaMapping's work is
// proportional to the tables requested, not to the tables on the server.
func TestSchemaMappingDoesNotScanInstance(t *testing.T) {
	const (
		schemas         = 10
		tablesPerSchema = 20
		tablesRequested = 5
		// A correct implementation opens no tables at all. A joined one opens every table on the
		// instance, so the budget only needs to sit below the 200 seeded here.
		openBudget = 100
	)

	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	seedStart := time.Now()
	var target string
	for i := range schemas {
		db := testDBName(fmt.Sprintf("scan%02d", i))
		createTestDB(t, ctx, connector, db)
		for j := range tablesPerSchema {
			_, err := connector.Execute(ctx, fmt.Sprintf(
				"CREATE TABLE `%s`.t%02d (id INT PRIMARY KEY, payload VARCHAR(32))", db, j))
			require.NoError(t, err)
		}
		if i == 0 {
			target = db
		}
	}
	seeded := schemas * tablesPerSchema
	t.Logf("seeded %d tables across %d schemas in %s", seeded, schemas,
		time.Since(seedStart).Round(time.Millisecond))

	requested := make([]string, 0, tablesRequested)
	for j := range tablesRequested {
		requested = append(requested, fmt.Sprintf("t%02d", j))
	}

	conn := connector.Conn()
	// Session scope keeps other tests from skewing the count, so one connection serves both
	// readings and the measured call; the connector's Execute may retry onto a different one.
	// Opened_table_definitions goes quiet once table_definition_cache exceeds the table count.
	openedTables := func() int64 {
		rs, err := conn.Execute("SHOW SESSION STATUS LIKE 'Opened_tables'")
		require.NoError(t, err)
		require.Len(t, rs.Values, 1)
		raw, err := rs.GetString(0, 1)
		require.NoError(t, err)
		n, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err)
		return n
	}

	before := openedTables()
	start := time.Now()
	got, err := mysql_validation.GetSchemaMapping(conn, target, requested)
	elapsed := time.Since(start)
	require.NoError(t, err)
	delta := openedTables() - before

	require.Len(t, got.Columns, tablesRequested)
	require.Equal(t, []string{"id", "payload"},
		got.Columns[common.QualifiedTable{Namespace: target, Table: "t00"}])

	// Elapsed is logged but never asserted, since it varies with machine load and cache
	// warmth. On a 12,000 table instance requesting 50, this reads 50 definitions and
	// opens 0 tables, against 12,118 and 12,120 for the previous joined form.
	t.Logf("requested %d tables of %d seeded: Opened_tables +%d (budget %d), took %s",
		tablesRequested, seeded, delta, openBudget, elapsed.Round(time.Millisecond))

	require.Lessf(t, delta, int64(openBudget),
		"opened %d tables for %d requested: the instance is being scanned, which means a"+
			" metadata join was reintroduced", delta, tablesRequested)
}
