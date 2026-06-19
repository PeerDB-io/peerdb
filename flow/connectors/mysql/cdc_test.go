package connmysql

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	temporalLog "go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type recordingInt64Counter struct {
	noop.Int64Counter

	mu             sync.Mutex
	value          int64
	lastAttributes []attribute.KeyValue
}

func (c *recordingInt64Counter) Add(_ context.Context, incr int64, opts ...metric.AddOption) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.value += incr
	attrs := metric.NewAddConfig(opts).Attributes()
	c.lastAttributes = attrs.ToSlice()
}

func installRecordingCodeNotificationCounter(t *testing.T) *recordingInt64Counter {
	t.Helper()

	counter := &recordingInt64Counter{}
	previousCounter := otel_metrics.CodeNotificationCounter
	otel_metrics.CodeNotificationCounter = counter
	t.Cleanup(func() {
		otel_metrics.CodeNotificationCounter = previousCounter
	})
	return counter
}

func newRenameObserverTestConnector() (*MySqlConnector, *bytes.Buffer) {
	var logBuffer bytes.Buffer
	logger := temporalLog.NewStructuredLogger(slog.New(slog.NewTextHandler(&logBuffer, nil)))
	return &MySqlConnector{logger: logger}, &logBuffer
}

func parseSingleDDLStmt(t *testing.T, query string) ast.StmtNode {
	t.Helper()

	stmts, warns, err := parser.New().ParseSQL(query)
	require.NoError(t, err)
	require.Empty(t, warns)
	require.Len(t, stmts, 1)
	return stmts[0]
}

func TestProcessRenameTableQueryObservesAllRenamePairs(t *testing.T) {
	counter := installRecordingCodeNotificationCounter(t)
	connector, logBuffer := newRenameObserverTestConnector()

	stmt, ok := parseSingleDDLStmt(t,
		"RENAME TABLE original TO _original_del, _original_gho TO original").(*ast.RenameTableStmt)
	require.True(t, ok)

	connector.processRenameTableQuery(t.Context(), stmt, "app")

	counter.mu.Lock()
	counterValue := counter.value
	counterAttributes := counter.lastAttributes
	counter.mu.Unlock()
	require.Equal(t, int64(2), counterValue)
	require.Contains(t, counterAttributes, attribute.String("message", mysqlTableRenameObservedMessage))

	logs := logBuffer.String()
	require.Contains(t, logs, `msg="table rename observed"`)
	require.Contains(t, logs, "oldTable=app.original")
	require.Contains(t, logs, "newTable=app._original_del")
	require.Contains(t, logs, "oldTable=app._original_gho")
	require.Contains(t, logs, "newTable=app.original")
}

func TestProcessAlterTableRenameObservesRename(t *testing.T) {
	counter := installRecordingCodeNotificationCounter(t)
	connector, logBuffer := newRenameObserverTestConnector()

	stmt, ok := parseSingleDDLStmt(t, "ALTER TABLE original RENAME TO renamed").(*ast.AlterTableStmt)
	require.True(t, ok)

	req := &model.PullRecordsRequest[model.RecordItems]{
		TableNameMapping: map[string]model.NameAndExclude{
			"app.original": {Name: "dst"},
		},
		TableNameSchemaMapping: map[string]*protos.TableSchema{
			"dst": {},
		},
	}
	require.NoError(t, connector.processAlterTableQuery(
		t.Context(), shared.CatalogPool{}, req, stmt, "app", true, shared.InternalVersion_Latest))

	counter.mu.Lock()
	counterValue := counter.value
	counterAttributes := counter.lastAttributes
	counter.mu.Unlock()
	require.Equal(t, int64(1), counterValue)
	require.Contains(t, counterAttributes, attribute.String("message", mysqlTableRenameObservedMessage))

	logs := logBuffer.String()
	require.Contains(t, logs, `msg="table rename observed"`)
	require.Contains(t, logs, "oldTable=app.original")
	require.Contains(t, logs, "newTable=app.renamed")
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
