package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
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

func TestClassifyUnparsedQueryEvent(t *testing.T) {
	tableNameMapping := map[string]model.NameAndExclude{
		"db.mapped_t":      model.NewNameAndExclude("dst_mapped_t", nil),
		"db.MixedCase":     model.NewNameAndExclude("dst_mixed_case", nil),
		"other.qualified":  model.NewNameAndExclude("dst_qualified", nil),
		"db.empty_mapping": {},
	}

	tests := []struct {
		name       string
		query      string
		schema     string
		wantAction unparsedQueryEventAction
		wantTable  string
	}{
		{
			name:       "non alter table query is not interesting",
			query:      "CREATE TABLE t (id INT) WITH VENDOR OPTION",
			schema:     "db",
			wantAction: unparsedQueryEventDontCare,
		},
		{
			name:       "unmapped column alter is not interesting",
			query:      "ALTER TABLE unmapped_t ADD COLUMN c INT \x00",
			schema:     "db",
			wantAction: unparsedQueryEventDontCare,
		},
		{
			name:       "mapped add column pages",
			query:      "ALTER TABLE mapped_t ADD COLUMN c INT \x00",
			schema:     "db",
			wantAction: unparsedQueryEventColumnAlter,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "mapped online add column pages",
			query:      "ALTER ONLINE TABLE mapped_t ADD COLUMN c INT",
			schema:     "db",
			wantAction: unparsedQueryEventColumnAlter,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "qualified backtick modify column pages",
			query:      "ALTER TABLE `db`.`mapped_t` MODIFY COLUMN `c` BIGINT /* unsupported */",
			wantAction: unparsedQueryEventColumnAlter,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "qualified table uses captured schema",
			query:      "ALTER TABLE other.qualified DROP COLUMN c",
			schema:     "db",
			wantAction: unparsedQueryEventColumnAlter,
			wantTable:  "other.qualified",
		},
		{
			name:       "mapped non column alter is significant but non paging",
			query:      "ALTER TABLE mapped_t ENGINE=InnoDB VENDOR_OPTION",
			schema:     "db",
			wantAction: unparsedQueryEventNotice,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "unqualified alter table without event schema is significant",
			query:      "ALTER TABLE mapped_t ENGINE=InnoDB VENDOR_OPTION",
			wantAction: unparsedQueryEventNotice,
		},
		{
			name:       "alter table phrase with unrecognized identifier is significant",
			query:      "ALTER TABLE @@@ ADD COLUMN c INT",
			schema:     "db",
			wantAction: unparsedQueryEventNotice,
		},
		{
			name:       "empty mapping value is treated as unmapped",
			query:      "ALTER TABLE empty_mapping ADD COLUMN c INT",
			schema:     "db",
			wantAction: unparsedQueryEventDontCare,
		},
		{
			name:       "index alter on unmapped table is not interesting",
			query:      "ALTER TABLE unmapped_t ADD INDEX idx_c (c) VENDOR_OPTION",
			schema:     "db",
			wantAction: unparsedQueryEventDontCare,
		},
		{
			name:       "mapped vector index alter is significant but non paging",
			query:      "ALTER TABLE mapped_t ADD VECTOR INDEX idx_c (c) VENDOR_OPTION",
			schema:     "db",
			wantAction: unparsedQueryEventNotice,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "mapped columnar index alter is significant but non paging",
			query:      "ALTER TABLE mapped_t ADD COLUMNAR INDEX idx_c (c) VENDOR_OPTION",
			schema:     "db",
			wantAction: unparsedQueryEventNotice,
			wantTable:  "db.mapped_t",
		},
		{
			name:       "online column alter on unmapped table is not interesting",
			query:      "ALTER ONLINE TABLE unmapped_t ADD COLUMN c INT",
			schema:     "db",
			wantAction: unparsedQueryEventDontCare,
		},
		{
			name:       "rename column pages",
			query:      "ALTER TABLE MixedCase RENAME COLUMN old_name TO new_name",
			schema:     "db",
			wantAction: unparsedQueryEventColumnAlter,
			wantTable:  "db.MixedCase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyUnparsedQueryEvent(tt.query, tt.schema, tableNameMapping)
			require.Equal(t, tt.wantAction, got.action)
			require.Equal(t, tt.wantTable, got.tableName)
		})
	}
}

func TestHandleUnparsedQueryEventSideEffects(t *testing.T) {
	parseErr := errors.New("syntax error")
	tableNameMapping := map[string]model.NameAndExclude{
		"db.mapped_t": model.NewNameAndExclude("dst_mapped_t", nil),
	}

	oldCounter := otel_metrics.CodeNotificationCounter
	counter := &countingInt64Counter{}
	otel_metrics.CodeNotificationCounter = counter
	t.Cleanup(func() {
		otel_metrics.CodeNotificationCounter = oldCounter
	})

	handler := &recordSlogHandler{}
	logger := log.NewStructuredLogger(slog.New(handler))

	err := handleUnparsedQueryEvent(t.Context(), logger,
		"CREATE TABLE t (id INT) WITH VENDOR OPTION", "db", "flow", tableNameMapping, parseErr)
	require.NoError(t, err)
	require.Zero(t, counter.calls)
	handler.requireLast(t, slog.LevelDebug, "ignored uninteresting QueryEvent parse failure")

	err = handleUnparsedQueryEvent(t.Context(), logger,
		"ALTER TABLE mapped_t ENGINE=InnoDB VENDOR_OPTION", "db", "flow", tableNameMapping, parseErr)
	require.NoError(t, err)
	require.Equal(t, 1, counter.calls)
	require.Equal(t, int64(1), counter.total)
	handler.requireLast(t, slog.LevelError, "failed to parse QueryEvent")

	err = handleUnparsedQueryEvent(t.Context(), logger,
		"ALTER TABLE mapped_t ADD COLUMN c INT \x00", "db", "flow", tableNameMapping, parseErr)
	require.Error(t, err)
	var alterErr *exceptions.MySQLUnparsedColumnAlterError
	require.ErrorAs(t, err, &alterErr)
	require.Equal(t, "db.mapped_t", alterErr.TableName)
	require.Equal(t, 1, counter.calls)
}

type countingInt64Counter struct {
	noop.Int64Counter
	calls int
	total int64
}

func (c *countingInt64Counter) Add(_ context.Context, incr int64, _ ...metric.AddOption) {
	c.calls++
	c.total += incr
}

func (c *countingInt64Counter) Enabled(context.Context) bool {
	return true
}

type slogRecord struct {
	level   slog.Level
	message string
}

type recordSlogHandler struct {
	mu      sync.Mutex
	records []slogRecord
}

func (h *recordSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordSlogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, slogRecord{level: record.Level, message: record.Message})
	return nil
}

func (h *recordSlogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordSlogHandler) requireLast(t *testing.T, level slog.Level, message string) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.NotEmpty(t, h.records)
	last := h.records[len(h.records)-1]
	require.Equal(t, level, last.level)
	require.Equal(t, message, last.message)
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
