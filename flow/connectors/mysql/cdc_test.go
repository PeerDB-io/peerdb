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

func TestParseSQLAlterTableAddColumnSpatialTypesUnsupported(t *testing.T) {
	for _, colType := range []string{
		"GEOMETRY", "POINT", "POLYGON", "LINESTRING", "MULTIPOINT",
		"MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION",
	} {
		t.Run(colType, func(t *testing.T) {
			_, _, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c "+colType))
			require.Error(t, err)
		})
	}
}

// TestParseSQLAlterTableAddColumnMariaDBOnlyTypesNotParseable documents that MariaDB's
// network/UUID types cannot be parsed in ADD COLUMN: the TiDB parser rejects them.
func TestParseSQLAlterTableAddColumnMariaDBOnlyTypesNotParseable(t *testing.T) {
	for _, colType := range []string{"INET4", "INET6", "UUID"} {
		t.Run(colType, func(t *testing.T) {
			_, _, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c "+colType))
			require.Error(t, err)
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
