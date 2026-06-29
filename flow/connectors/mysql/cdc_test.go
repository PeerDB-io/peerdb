package connmysql

import (
	"context"
	"fmt"
	"log/slog"
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
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

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

func TestParseSQLAlterTableAddColumnTypes(t *testing.T) {
	// addColumnType parses `ALTER TABLE t ADD COLUMN c <colDef>` and returns the
	// column's InfoSchemaStr, the same value processAlterTableQuery feeds to
	// QkindFromMysqlColumnType.
	addColumnType := func(t *testing.T, colDef string) string {
		t.Helper()
		stmts, warns, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c "+colDef))
		require.NoError(t, err)
		require.Empty(t, warns)
		require.Len(t, stmts, 1)
		alter, ok := stmts[0].(*ast.AlterTableStmt)
		require.True(t, ok)
		require.Len(t, alter.Specs, 1)
		require.Len(t, alter.Specs[0].NewColumns, 1)
		col := alter.Specs[0].NewColumns[0]
		require.NotNil(t, col.Tp)
		return col.Tp.InfoSchemaStr()
	}

	for _, tc := range []struct {
		name   string
		colDef string
		want   types.QValueKind
	}{
		// integers
		{"tinyint(1) is boolean", "TINYINT(1)", types.QValueKindBoolean},
		{"tinyint", "TINYINT", types.QValueKindInt8},
		{"tinyint unsigned", "TINYINT UNSIGNED", types.QValueKindUInt8},
		{"smallint", "SMALLINT", types.QValueKindInt16},
		{"smallint unsigned", "SMALLINT UNSIGNED", types.QValueKindUInt16},
		{"year", "YEAR", types.QValueKindInt16},
		{"mediumint", "MEDIUMINT", types.QValueKindInt32},
		{"mediumint unsigned", "MEDIUMINT UNSIGNED", types.QValueKindUInt32},
		{"int", "INT", types.QValueKindInt32},
		{"int unsigned", "INT UNSIGNED", types.QValueKindUInt32},
		{"bigint", "BIGINT", types.QValueKindInt64},
		{"bigint unsigned", "BIGINT UNSIGNED", types.QValueKindUInt64},
		{"bit", "BIT(8)", types.QValueKindUInt64},

		// floating point and exact numeric
		{"float", "FLOAT", types.QValueKindFloat32},
		{"double", "DOUBLE", types.QValueKindFloat64},
		{"decimal", "DECIMAL(10,2)", types.QValueKindNumeric},
		{"numeric", "NUMERIC(10,2)", types.QValueKindNumeric},

		// strings
		{"char", "CHAR(10)", types.QValueKindString},
		{"varchar", "VARCHAR(255)", types.QValueKindString},
		{"tinytext", "TINYTEXT", types.QValueKindString},
		{"text", "TEXT", types.QValueKindString},
		{"mediumtext", "MEDIUMTEXT", types.QValueKindString},
		{"longtext", "LONGTEXT", types.QValueKindString},
		{"set", "SET('a','b','c')", types.QValueKindString},

		// binary
		{"binary", "BINARY(16)", types.QValueKindBytes},
		{"varbinary", "VARBINARY(255)", types.QValueKindBytes},
		{"tinyblob", "TINYBLOB", types.QValueKindBytes},
		{"blob", "BLOB", types.QValueKindBytes},
		{"mediumblob", "MEDIUMBLOB", types.QValueKindBytes},
		{"longblob", "LONGBLOB", types.QValueKindBytes},

		// date and time
		{"date", "DATE", types.QValueKindDate},
		{"datetime", "DATETIME", types.QValueKindTimestamp},
		{"timestamp", "TIMESTAMP", types.QValueKindTimestamp},
		{"time", "TIME", types.QValueKindTime},

		// json
		{"json", "JSON", types.QValueKindJSON},

		// enum (binlog row metadata available -> Enum, not the Uint16 fallback)
		{"enum", "ENUM('a','b','c')", types.QValueKindEnum},

		// vector
		{"vector", "VECTOR(3)", types.QValueKindArrayFloat32},

		// Type synonyms: spellings MySQL and MariaDB accept as aliases.
		// QkindFromMysqlColumnType has no case for any of these names — it works because
		// the TiDB parser normalizes each to its canonical InfoSchemaStr
		// (BOOLEAN->tinyint(1), INTEGER->int, REAL->double, SERIAL->bigint unsigned,
		// NVARCHAR->varchar, MariaDB INT1..INT8->the sized int, ...).

		// boolean spellings normalize to tinyint(1)
		{"bool", "BOOL", types.QValueKindBoolean},
		{"boolean", "BOOLEAN", types.QValueKindBoolean},

		// integer spellings
		{"integer", "INTEGER", types.QValueKindInt32},
		{"integer unsigned", "INTEGER UNSIGNED", types.QValueKindUInt32},
		// MariaDB int synonyms INT1..INT8 / MIDDLEINT map to the sized integer
		{"int1 -> tinyint", "INT1", types.QValueKindInt8},
		{"int2 -> smallint", "INT2", types.QValueKindInt16},
		{"int3 -> mediumint", "INT3", types.QValueKindInt32},
		{"int4 -> int", "INT4", types.QValueKindInt32},
		{"int8 -> bigint", "INT8", types.QValueKindInt64},
		{"middleint -> mediumint", "MIDDLEINT", types.QValueKindInt32},
		// SERIAL is BIGINT UNSIGNED ... AUTO_INCREMENT UNIQUE
		{"serial -> bigint unsigned", "SERIAL", types.QValueKindUInt64},

		// exact numeric spellings normalize to decimal
		{"dec", "DEC(10,2)", types.QValueKindNumeric},
		{"fixed", "FIXED(10,2)", types.QValueKindNumeric},

		// approximate numeric spellings normalize to double
		{"double precision", "DOUBLE PRECISION", types.QValueKindFloat64},
		{"real", "REAL", types.QValueKindFloat64},

		// char spellings normalize to char
		{"character", "CHARACTER(10)", types.QValueKindString},
		{"nchar", "NCHAR(10)", types.QValueKindString},
		{"national char", "NATIONAL CHAR(10)", types.QValueKindString},

		// varchar spellings normalize to varchar
		{"character varying", "CHARACTER VARYING(255)", types.QValueKindString},
		{"nvarchar", "NVARCHAR(255)", types.QValueKindString},
		{"national varchar", "NATIONAL VARCHAR(255)", types.QValueKindString},

		// MariaDB LONG / LONG VARCHAR normalize to mediumtext
		{"long", "LONG", types.QValueKindString},
		{"long varchar", "LONG VARCHAR", types.QValueKindString},

		// Display widths: the (M) on integers is parsed away (we cut at "(").
		{"tinyint width", "TINYINT(4)", types.QValueKindInt8},
		{"smallint width", "SMALLINT(5)", types.QValueKindInt16},
		{"mediumint width", "MEDIUMINT(8)", types.QValueKindInt32},
		{"int width", "INT(11)", types.QValueKindInt32},
		{"integer width", "INTEGER(11)", types.QValueKindInt32},
		{"bigint width", "BIGINT(20)", types.QValueKindInt64},

		// ZEROFILL implies UNSIGNED; QkindFromMysqlColumnType strips both suffixes.
		{"int zerofill", "INT ZEROFILL", types.QValueKindUInt32},
		{"int width zerofill", "INT(11) ZEROFILL", types.QValueKindUInt32},
		{"bigint zerofill", "BIGINT ZEROFILL", types.QValueKindUInt64},

		// YEAR width.
		{"year(4)", "YEAR(4)", types.QValueKindInt16},

		// Floating point with precision/scale and unsigned modifiers.
		{"float(m,d)", "FLOAT(10,2)", types.QValueKindFloat32},
		{"float unsigned", "FLOAT UNSIGNED", types.QValueKindFloat32},
		{"double(m,d)", "DOUBLE(10,2)", types.QValueKindFloat64},
		{"double unsigned", "DOUBLE UNSIGNED", types.QValueKindFloat64},
		{"double precision(m,d)", "DOUBLE PRECISION(10,2)", types.QValueKindFloat64},

		// Character types without an explicit length normalize to length 1.
		{"char without length", "CHAR", types.QValueKindString},
		{"nchar without length", "NCHAR", types.QValueKindString},
		{"national char without length", "NATIONAL CHAR", types.QValueKindString},

		// Binary without length normalizes to binary(1); blob with length stays bytes.
		{"binary without length", "BINARY", types.QValueKindBytes},
		{"blob(M)", "BLOB(100)", types.QValueKindBytes},

		// charset / collate / (deprecated) binary modifiers on string-ish types don't
		// change the qkind — they only affect collation, which we don't key off here.
		{"varchar charset", "VARCHAR(10) CHARACTER SET utf8mb4", types.QValueKindString},
		{"varchar collate", "VARCHAR(10) COLLATE utf8mb4_bin", types.QValueKindString},
		{"varchar charset collate", "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", types.QValueKindString},
		{"text charset", "TEXT CHARACTER SET latin1", types.QValueKindString},
		{"enum charset", "ENUM('a','b') CHARACTER SET utf8mb4", types.QValueKindEnum},
		{"set collate", "SET('a','b') COLLATE utf8mb4_bin", types.QValueKindString},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType(addColumnType(t, tc.colDef), true, shared.InternalVersion_Latest)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
		})
	}
}

func TestParseSQLAlterTableAddColumnEnumWithoutRowMetadata(t *testing.T) {
	stmts, _, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c ENUM('a','b','c')"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	col := stmts[0].(*ast.AlterTableStmt).Specs[0].NewColumns[0]

	qkind, err := QkindFromMysqlColumnType(col.Tp.InfoSchemaStr(), false, shared.InternalVersion_MySQL5ConvertEnumsToInts)
	require.NoError(t, err)
	require.Equal(t, types.QValueKindUint16Enum, qkind)
}

// TestParseSQLAlterTableAddColumnUnsupportedTypes covers types the TiDB parser rejects
// in ADD COLUMN: spatial types, and MariaDB's network/UUID types.
func TestParseSQLAlterTableAddColumnUnsupportedTypes(t *testing.T) {
	for _, colType := range []string{
		// spatial types
		"GEOMETRY", "POINT", "POLYGON", "LINESTRING", "MULTIPOINT",
		"MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION",
		// MariaDB-only network/UUID types
		"INET4", "INET6", "UUID",
	} {
		t.Run(colType, func(t *testing.T) {
			_, _, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c "+colType))
			require.Error(t, err)
		})
	}
}

func TestShouldReportColumnTypeChange(t *testing.T) {
	for _, tc := range []struct {
		name       string
		schemaKind types.QValueKind
		wireKind   types.QValueKind
		flavor     protos.MySqlFlavor
		want       bool
	}{
		// Same kind is never a change, regardless of flavor.
		{"same kind", types.QValueKindInt32, types.QValueKindInt32, protos.MySqlFlavor_MYSQL_MARIA, false},
		// MariaDB uuid/inet arrive as bytes on the wire: benign, suppressed.
		{"maria uuid as bytes", types.QValueKindUUID, types.QValueKindBytes, protos.MySqlFlavor_MYSQL_MARIA, false},
		{"maria inet as bytes", types.QValueKindINET, types.QValueKindBytes, protos.MySqlFlavor_MYSQL_MARIA, false},
		// Same shape but on MySQL: not the known MariaDB behavior, so report it.
		{"mysql uuid as bytes", types.QValueKindUUID, types.QValueKindBytes, protos.MySqlFlavor_MYSQL_MYSQL, true},
		// A genuine change to/from uuid/inet must still be reported even on MariaDB.
		{"maria uuid to integer", types.QValueKindUUID, types.QValueKindInt32, protos.MySqlFlavor_MYSQL_MARIA, true},
		{"maria inet to string", types.QValueKindINET, types.QValueKindString, protos.MySqlFlavor_MYSQL_MARIA, true},
		// Bytes on the wire for a non-uuid/inet schema kind is a real change.
		{"maria string to bytes", types.QValueKindString, types.QValueKindBytes, protos.MySqlFlavor_MYSQL_MARIA, true},
		// Ordinary type change.
		{"int to bigint", types.QValueKindInt32, types.QValueKindInt64, protos.MySqlFlavor_MYSQL_MYSQL, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldReportColumnTypeChange(tc.schemaKind, tc.wireKind, tc.flavor))
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
		{"gh-ost", "_users_gho", "users", OnlineSchemaMigrationToolGhOst},
		{"pt-osc", "_users_new", "users", OnlineSchemaMigrationToolPtOsc},
		{"plain rename", "users_backup", "users", OnlineSchemaMigrationToolOther},
		{"gh-ost del table is not the target", "_users_del", "users", OnlineSchemaMigrationToolOther},
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
	require.Equal(t, OnlineSchemaMigrationToolGhOst,
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

	for _, tc := range []struct {
		name  string
		query string
		want  map[string]int64
	}{
		{
			name: "gh-ost cutover on tracked table increments once",
			// full gh-ost atomic swap: only the `_users_gho -> users` pair lands on a tracked table
			query: "RENAME TABLE `mydb`.`users` TO `mydb`.`_users_del`, `mydb`.`_users_gho` TO `mydb`.`users`",
			want:  map[string]int64{OnlineSchemaMigrationToolGhOst: 1},
		},
		{
			name:  "pt-online-schema-change cutover",
			query: "RENAME TABLE `mydb`.`users` TO `mydb`.`_users_old`, `mydb`.`_users_new` TO `mydb`.`users`",
			want:  map[string]int64{OnlineSchemaMigrationToolPtOsc: 1},
		},
		{
			name: "schema falls back to event schema",
			// no schema qualifier on the statement; resolved from the event's schema
			query: "RENAME TABLE `users` TO `_users_del`, `_users_gho` TO `users`",
			want:  map[string]int64{OnlineSchemaMigrationToolGhOst: 1},
		},
		{
			name: "rename into untracked table is ignored",
			// neither target is a table we replicate
			query: "RENAME TABLE `mydb`.`orders` TO `mydb`.`orders_archive`",
			want:  map[string]int64{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			om, reader := newMetricsTestOtelManager(t)
			req := &model.PullRecordsRequest[model.RecordItems]{
				TableNameMapping: map[string]model.NameAndExclude{"mydb.users": {Name: "users_dst"}},
			}
			c.processRenameTableQuery(ctx, om, req, parseRename(tc.query), "mydb")

			require.Equal(t, tc.want, collectOnlineSchemaMigrationCounts(t, ctx, reader))
		})
	}
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
