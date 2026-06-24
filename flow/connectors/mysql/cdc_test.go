package connmysql

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// testDBName namespaces a database per test with a random suffix, mirroring the
// e2e suite convention, so parallel tests and reruns don't collide.
func testDBName(prefix string) string {
	return prefix + "_" + strings.ToLower(common.RandomString(8))
}

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

func startBinlogStream(t *testing.T, ctx context.Context, c *MySqlConnector) *replication.BinlogStreamer {
	t.Helper()

	var syncer *replication.BinlogSyncer
	var stream *replication.BinlogStreamer
	if internal.MySQLTestVersionIsMySQLPos() {
		pos, err := c.GetMasterPos(ctx)
		require.NoError(t, err)
		syncer, stream, _, _, err = c.startCdcStreamingFilePos(ctx, pos, nil)
		require.NoError(t, err)
	} else {
		gset, err := c.GetMasterGTIDSet(ctx)
		require.NoError(t, err)
		syncer, stream, _, _, err = c.startCdcStreamingGtid(ctx, gset, nil)
		require.NoError(t, err)
	}
	t.Cleanup(syncer.Close)
	return stream
}

func waitForQueryEventContaining(
	t *testing.T, ctx context.Context, stream *replication.BinlogStreamer, marker string,
) ([]byte, string) {
	t.Helper()

	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		event, err := stream.GetEvent(streamCtx)
		require.NoError(t, err, "did not observe QueryEvent containing %q in the binlog", marker)
		if qe, ok := event.Event.(*replication.QueryEvent); ok && strings.Contains(string(qe.Query), marker) {
			// status vars alias into the event buffer, which go-mysql may reuse
			return slices.Clone(qe.StatusVars), string(qe.Query)
		}
	}
}

func TestANSIQuotesDDLParsedFromBinlog(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	connector := newTestConnector(t, ctx)

	dbName := testDBName("ansi_quotes_binlog")
	createTestDB(t, ctx, connector, dbName)
	_, err := connector.Execute(ctx, "CREATE TABLE `"+dbName+"`.`t` (id INT PRIMARY KEY)")
	require.NoError(t, err)

	stream := startBinlogStream(t, ctx, connector)

	// ANSI_QUOTES makes "..." an identifier quote, so the ALTER below only parses
	// when the mode is passed to the parser as well
	guid := uuid.NewString()
	_, err = connector.Execute(ctx, `SET SESSION sql_mode='ANSI_QUOTES'`)
	require.NoError(t, err)

	ddl := fmt.Sprintf(`/* %s */ ALTER TABLE "%s"."t" ADD COLUMN "c1" INT`, guid, dbName)
	_, err = connector.Execute(ctx, ddl)
	require.NoError(t, err)

	statusVars, query := waitForQueryEventContaining(t, ctx, stream, guid)

	// Validate extraction
	mode, ok := sqlModeFromStatusVars(statusVars)
	require.True(t, ok)
	require.NotZero(t, mode&uint64(tidbmysql.ModeANSIQuotes), "ANSI_QUOTES missing from binlog status vars")

	// Validate parsing
	mysqlParser := parser.New()
	setParserSQLModeFromStatusVars(mysqlParser, statusVars)
	stmts, _, err := mysqlParser.ParseSQL(query)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.IsType(t, &ast.AlterTableStmt{}, stmts[0])

	// Validate parsing fails without the mode
	_, _, err = parser.New().ParseSQL(query)
	require.Error(t, err)

	// Status vars with codes > 1 are written after sql_mode (code 1). Every
	// QueryEvent already carries catalog (2) and charset (4); force a few more —
	// auto_increment (3), time_zone (5), lc_time_names (7) — to confirm a fuller
	// status-var block still doesn't interfere with extracting sql_mode.
	_, err = connector.Execute(ctx,
		`SET SESSION auto_increment_increment=7, auto_increment_offset=3, time_zone='+05:00', lc_time_names='fr_FR'`)
	require.NoError(t, err)

	guid = uuid.NewString()
	ddl = fmt.Sprintf(`/* %s */ ALTER TABLE "%s"."t" ADD COLUMN "c2" INT`, guid, dbName)
	_, err = connector.Execute(ctx, ddl)
	require.NoError(t, err)

	statusVars, _ = waitForQueryEventContaining(t, ctx, stream, guid)

	mode, ok = sqlModeFromStatusVars(statusVars)
	require.True(t, ok)
	require.NotZero(t, mode&uint64(tidbmysql.ModeANSIQuotes), "ANSI_QUOTES missing from binlog status vars with extra status vars present")
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

	lowerDB := testDBName("cs")
	upperDB := testDBName("CS_IDS_TEST")
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

	dbName := testDBName("pk_variants")
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
