package connmysql

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
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

func TestParseSQLParsesTrailingNull(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  []byte
		assert func(*testing.T, ast.StmtNode)
	}{
		{
			name:  "alter table",
			query: []byte("ALTER TABLE t ADD COLUMN c INT\x00"),
			assert: func(t *testing.T, stmt ast.StmtNode) {
				t.Helper()
				require.IsType(t, &ast.AlterTableStmt{}, stmt)
			},
		},
		{
			name:  "commit",
			query: []byte("COMMIT\x00"),
			assert: func(t *testing.T, stmt ast.StmtNode) {
				t.Helper()
				require.IsType(t, &ast.CommitStmt{}, stmt)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mysqlParser := parser.New()
			_, _, err := mysqlParser.ParseSQL(string(tc.query))
			require.Error(t, err)

			stmts, warns, err := parseSQL(mysqlParser, tc.query)
			require.NoError(t, err)
			require.Empty(t, warns)
			require.Len(t, stmts, 1)
			tc.assert(t, stmts[0])
		})
	}
}

func TestClassifyOnlineSchemaMigrationTool(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oldTable string
		newTable string
		want     string
	}{
		{"gh-ost", "_users_gho", "users", otel_metrics.OnlineSchemaMigrationToolGhOst},
		{"pt-osc", "_users_new", "users", otel_metrics.OnlineSchemaMigrationToolPtOsc},
		{"plain rename", "users_backup", "users", otel_metrics.OnlineSchemaMigrationToolOther},
		{"gh-ost del table is not the target", "_users_del", "users", otel_metrics.OnlineSchemaMigrationToolOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyOnlineSchemaMigrationTool(tc.oldTable, tc.newTable))
		})
	}
}

func TestParseSQLParsesGhOstRename(t *testing.T) {
	mysqlParser := parser.New()
	// the atomic swap gh-ost issues at the end of a migration
	query := []byte("RENAME TABLE `mydb`.`users` TO `mydb`.`_users_del`, `mydb`.`_users_gho` TO `mydb`.`users`")
	stmts, warns, err := parseSQL(mysqlParser, query)
	require.NoError(t, err)
	require.Empty(t, warns)
	require.Len(t, stmts, 1)

	rename, ok := stmts[0].(*ast.RenameTableStmt)
	require.True(t, ok)
	require.Len(t, rename.TableToTables, 2)

	// the rename that lands on the tracked table is the ghost table swap
	swap := rename.TableToTables[1]
	require.Equal(t, "users", swap.NewTable.Name.String())
	require.Equal(t, "_users_gho", swap.OldTable.Name.String())
	require.Equal(t, otel_metrics.OnlineSchemaMigrationToolGhOst,
		classifyOnlineSchemaMigrationTool(swap.OldTable.Name.String(), swap.NewTable.Name.String()))
}

// newMetricsTestOtelManager builds an OtelManager backed by a ManualReader so tests
// can collect and assert recorded metric values without a real exporter.
func newMetricsTestOtelManager(t *testing.T) (*otel_metrics.OtelManager, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	om := &otel_metrics.OtelManager{
		MetricsProvider:    provider,
		Meter:              provider.Meter("test"),
		Int64CountersCache: make(map[string]metric.Int64Counter),
	}
	counter, err := om.GetOrInitInt64Counter(otel_metrics.BuildMetricName(otel_metrics.OnlineSchemaMigrationsName))
	require.NoError(t, err)
	om.Metrics.OnlineSchemaMigrationsCounter = counter
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return om, reader
}

// collectOnlineSchemaMigrationCounts collects the online_schema_migrations counter and
// returns its value bucketed by the `tool` attribute.
func collectOnlineSchemaMigrationCounts(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	wantName := otel_metrics.BuildMetricName(otel_metrics.OnlineSchemaMigrationsName)
	counts := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != wantName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "expected Int64 sum for %s", wantName)
			for _, dp := range sum.DataPoints {
				tool, _ := dp.Attributes.Value(otel_metrics.OnlineSchemaMigrationTool)
				source, _ := dp.Attributes.Value(otel_metrics.SourcePeerType)
				require.Equal(t, "mysql", source.AsString())
				counts[tool.AsString()] += dp.Value
			}
		}
	}
	return counts
}

func TestProcessRenameTableQueryMetric(t *testing.T) {
	ctx := t.Context()
	c := &MySqlConnector{logger: log.NewStructuredLogger(slog.Default())}

	parseRename := func(query string) *ast.RenameTableStmt {
		t.Helper()
		stmts, _, err := parseSQL(parser.New(), []byte(query))
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		rename, ok := stmts[0].(*ast.RenameTableStmt)
		require.True(t, ok)
		return rename
	}

	t.Run("gh-ost cutover on tracked table increments once", func(t *testing.T) {
		om, reader := newMetricsTestOtelManager(t)
		req := &model.PullRecordsRequest[model.RecordItems]{
			TableNameMapping: map[string]model.NameAndExclude{"mydb.users": {Name: "users_dst"}},
		}
		// full gh-ost atomic swap: only the `_users_gho -> users` pair lands on a tracked table
		rename := parseRename("RENAME TABLE `mydb`.`users` TO `mydb`.`_users_del`, `mydb`.`_users_gho` TO `mydb`.`users`")
		c.processRenameTableQuery(ctx, om, req, rename, "mydb")

		require.Equal(t, map[string]int64{otel_metrics.OnlineSchemaMigrationToolGhOst: 1},
			collectOnlineSchemaMigrationCounts(t, ctx, reader))
	})

	t.Run("pt-online-schema-change cutover", func(t *testing.T) {
		om, reader := newMetricsTestOtelManager(t)
		req := &model.PullRecordsRequest[model.RecordItems]{
			TableNameMapping: map[string]model.NameAndExclude{"mydb.users": {Name: "users_dst"}},
		}
		rename := parseRename("RENAME TABLE `mydb`.`users` TO `mydb`.`_users_old`, `mydb`.`_users_new` TO `mydb`.`users`")
		c.processRenameTableQuery(ctx, om, req, rename, "mydb")

		require.Equal(t, map[string]int64{otel_metrics.OnlineSchemaMigrationToolPtOsc: 1},
			collectOnlineSchemaMigrationCounts(t, ctx, reader))
	})

	t.Run("schema falls back to event schema", func(t *testing.T) {
		om, reader := newMetricsTestOtelManager(t)
		req := &model.PullRecordsRequest[model.RecordItems]{
			TableNameMapping: map[string]model.NameAndExclude{"mydb.users": {Name: "users_dst"}},
		}
		// no schema qualifier on the statement; resolved from the event's schema
		rename := parseRename("RENAME TABLE `users` TO `_users_del`, `_users_gho` TO `users`")
		c.processRenameTableQuery(ctx, om, req, rename, "mydb")

		require.Equal(t, map[string]int64{otel_metrics.OnlineSchemaMigrationToolGhOst: 1},
			collectOnlineSchemaMigrationCounts(t, ctx, reader))
	})

	t.Run("rename into untracked table is ignored", func(t *testing.T) {
		om, reader := newMetricsTestOtelManager(t)
		req := &model.PullRecordsRequest[model.RecordItems]{
			TableNameMapping: map[string]model.NameAndExclude{"mydb.users": {Name: "users_dst"}},
		}
		// neither target is a table we replicate
		rename := parseRename("RENAME TABLE `mydb`.`orders` TO `mydb`.`orders_archive`")
		c.processRenameTableQuery(ctx, om, req, rename, "mydb")

		require.Empty(t, collectOnlineSchemaMigrationCounts(t, ctx, reader))
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
