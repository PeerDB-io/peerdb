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
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func startBinlogStream(t *testing.T, ctx context.Context, c *MySqlConnector) *replication.BinlogStreamer {
	t.Helper()

	var syncer *replication.BinlogSyncer
	var stream *replication.BinlogStreamer
	if c.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_FILEPOS {
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

func TestIntegrationANSIQuotesDDLParsedFromBinlog(t *testing.T) {
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
			// QueryEvent already carries catalog (2) and charset (4); force a few more -
			// auto_increment (3), time_zone (5), lc_time_names (7) - to confirm a fuller
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
			require.NotZero(t,
				mode&uint64(tidbmysql.ModeANSIQuotes),
				"ANSI_QUOTES missing from binlog status vars with extra status vars present",
			)
		})
	}
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

func TestAlterTableTypes(t *testing.T) {
	// addColumnFieldDescription parses `ALTER TABLE t ADD COLUMN c <colDef>`
	// and builds the same FieldDescription processAlterTableQuery emits.
	addColumnFieldDescription := func(t *testing.T, colDef string) *protos.FieldDescription {
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
		fd, err := fieldDescriptionFromMysqlColumn(col, true, shared.InternalVersion_Latest)
		require.NoError(t, err)
		return fd
	}

	for _, tc := range []struct {
		name         string
		colDef       string
		want         types.QValueKind
		typeModifier int32
		notNull      bool
	}{
		// integers
		{name: "tinyint(1) is boolean", colDef: "TINYINT(1)", want: types.QValueKindBoolean},
		{name: "tinyint", colDef: "TINYINT", want: types.QValueKindInt8},
		{name: "tinyint unsigned", colDef: "TINYINT UNSIGNED", want: types.QValueKindUInt8},
		{name: "smallint", colDef: "SMALLINT", want: types.QValueKindInt16},
		{name: "smallint unsigned", colDef: "SMALLINT UNSIGNED", want: types.QValueKindUInt16},
		{name: "year", colDef: "YEAR", want: types.QValueKindInt16},
		{name: "mediumint", colDef: "MEDIUMINT", want: types.QValueKindInt32},
		{name: "mediumint unsigned", colDef: "MEDIUMINT UNSIGNED", want: types.QValueKindUInt32},
		{name: "int", colDef: "INT", want: types.QValueKindInt32},
		{name: "int unsigned", colDef: "INT UNSIGNED", want: types.QValueKindUInt32},
		{name: "bigint", colDef: "BIGINT", want: types.QValueKindInt64},
		{name: "bigint unsigned", colDef: "BIGINT UNSIGNED", want: types.QValueKindUInt64},
		{name: "bit", colDef: "BIT(8)", want: types.QValueKindUInt64},
		{name: "bit no args", colDef: "BIT", want: types.QValueKindUInt64},

		// floating point and exact numeric
		{name: "float", colDef: "FLOAT", want: types.QValueKindFloat32},
		{name: "double", colDef: "DOUBLE", want: types.QValueKindFloat64},
		{name: "decimal", colDef: "DECIMAL(10,2)", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 2)},
		{name: "numeric", colDef: "NUMERIC(10,2)", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 2)},

		// strings
		{name: "char(10)", colDef: "CHAR(10)", want: types.QValueKindString},
		{name: "char", colDef: "CHAR", want: types.QValueKindString},
		{name: "character", colDef: "CHARACTER", want: types.QValueKindString},
		{name: "character(10)", colDef: "CHARACTER(10)", want: types.QValueKindString},
		{name: "varchar", colDef: "VARCHAR(255)", want: types.QValueKindString},
		{name: "tinytext", colDef: "TINYTEXT", want: types.QValueKindString},
		{name: "text", colDef: "TEXT", want: types.QValueKindString},
		{name: "mediumtext", colDef: "MEDIUMTEXT", want: types.QValueKindString},
		{name: "longtext", colDef: "LONGTEXT", want: types.QValueKindString},
		{name: "set", colDef: "SET('a','b','c')", want: types.QValueKindString},

		// binary
		{name: "binary", colDef: "BINARY(16)", want: types.QValueKindBytes},
		{name: "varbinary", colDef: "VARBINARY(255)", want: types.QValueKindBytes},
		{name: "tinyblob", colDef: "TINYBLOB", want: types.QValueKindBytes},
		{name: "blob", colDef: "BLOB", want: types.QValueKindBytes},
		{name: "mediumblob", colDef: "MEDIUMBLOB", want: types.QValueKindBytes},
		{name: "longblob", colDef: "LONGBLOB", want: types.QValueKindBytes},

		// date and time
		{name: "date", colDef: "DATE", want: types.QValueKindDate},
		{name: "datetime", colDef: "DATETIME", want: types.QValueKindTimestamp},
		{name: "timestamp", colDef: "TIMESTAMP", want: types.QValueKindTimestamp},
		{name: "time", colDef: "TIME", want: types.QValueKindTime},

		// json
		{name: "json", colDef: "JSON", want: types.QValueKindJSON},

		// enum (binlog row metadata available -> Enum, not the Uint16 fallback)
		{name: "enum", colDef: "ENUM('a','b','c')", want: types.QValueKindEnum},

		// vector
		{name: "vector", colDef: "VECTOR(3)", want: types.QValueKindArrayFloat32},

		// Type synonyms: spellings MySQL and MariaDB accept as aliases.
		// QkindFromMysqlColumnType has no case for any of these names — it works because
		// the TiDB parser normalizes each to its canonical InfoSchemaStr
		// (BOOLEAN->tinyint(1), INTEGER->int, REAL->double, SERIAL->bigint unsigned,
		// NVARCHAR->varchar, MariaDB INT1..INT8->the sized int, ...).

		// boolean spellings normalize to tinyint(1)
		{name: "bool", colDef: "BOOL", want: types.QValueKindBoolean},
		{name: "boolean", colDef: "BOOLEAN", want: types.QValueKindBoolean},

		// integer spellings
		{name: "integer", colDef: "INTEGER", want: types.QValueKindInt32},
		{name: "integer unsigned", colDef: "INTEGER UNSIGNED", want: types.QValueKindUInt32},
		// MariaDB int synonyms INT1..INT8 / MIDDLEINT map to the sized integer
		{name: "int1 -> tinyint", colDef: "INT1", want: types.QValueKindInt8},
		{name: "int2 -> smallint", colDef: "INT2", want: types.QValueKindInt16},
		{name: "int3 -> mediumint", colDef: "INT3", want: types.QValueKindInt32},
		{name: "int4 -> int", colDef: "INT4", want: types.QValueKindInt32},
		{name: "int8 -> bigint", colDef: "INT8", want: types.QValueKindInt64},
		{name: "middleint -> mediumint", colDef: "MIDDLEINT", want: types.QValueKindInt32},
		// SERIAL is BIGINT UNSIGNED ... AUTO_INCREMENT UNIQUE
		{name: "serial -> bigint unsigned", colDef: "SERIAL", want: types.QValueKindUInt64, notNull: true},

		// exact numeric spellings normalize to decimal
		{name: "dec", colDef: "DEC(10,2)", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 2)},
		{name: "fixed", colDef: "FIXED(10,2)", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 2)},
		// Bracketless decimal family defaults to decimal(10,0) (the (M,D) is cut away anyway).
		{name: "decimal no args", colDef: "DECIMAL", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 0)},
		{name: "numeric no args", colDef: "NUMERIC", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 0)},
		{name: "dec no args", colDef: "DEC", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 0)},
		{name: "fixed no args", colDef: "FIXED", want: types.QValueKindNumeric, typeModifier: datatypes.MakeNumericTypmod(10, 0)},

		// approximate numeric spellings normalize to double
		{name: "double precision", colDef: "DOUBLE PRECISION", want: types.QValueKindFloat64},
		{name: "real", colDef: "REAL", want: types.QValueKindFloat64},
		// MariaDB FLOAT4/FLOAT8 synonyms normalize to float/double.
		{name: "float4 -> float", colDef: "FLOAT4", want: types.QValueKindFloat32},
		{name: "float8 -> double", colDef: "FLOAT8", want: types.QValueKindFloat64},

		// char spellings normalize to char
		{name: "character(10)", colDef: "CHARACTER(10)", want: types.QValueKindString},
		{name: "character", colDef: "CHARACTER", want: types.QValueKindString},
		{name: "nchar", colDef: "NCHAR(10)", want: types.QValueKindString},
		{name: "national char", colDef: "NATIONAL CHAR(10)", want: types.QValueKindString},

		// varchar spellings normalize to varchar
		{name: "character varying", colDef: "CHARACTER VARYING(255)", want: types.QValueKindString},
		{name: "nvarchar", colDef: "NVARCHAR(255)", want: types.QValueKindString},
		{name: "national varchar", colDef: "NATIONAL VARCHAR(255)", want: types.QValueKindString},
		// more MariaDB varchar spellings (all normalize to varchar)
		{name: "char varying", colDef: "CHAR VARYING(255)", want: types.QValueKindString},
		{name: "varcharacter", colDef: "VARCHARACTER(255)", want: types.QValueKindString},
		{name: "national char varying", colDef: "NATIONAL CHAR VARYING(255)", want: types.QValueKindString},
		{name: "national character varying", colDef: "NATIONAL CHARACTER VARYING(255)", want: types.QValueKindString},
		{name: "national varcharacter", colDef: "NATIONAL VARCHARACTER(255)", want: types.QValueKindString},
		{name: "nchar varchar", colDef: "NCHAR VARCHAR(255)", want: types.QValueKindString},
		{name: "nchar varying", colDef: "NCHAR VARYING(255)", want: types.QValueKindString},
		{name: "nchar varcharacter", colDef: "NCHAR VARCHARACTER(255)", want: types.QValueKindString},

		// MariaDB NATIONAL CHARACTER normalizes to char
		{name: "national character", colDef: "NATIONAL CHARACTER(10)", want: types.QValueKindString},

		// MariaDB LONG / LONG VARCHAR and friends normalize to mediumtext
		{name: "long", colDef: "LONG", want: types.QValueKindString},
		{name: "long varchar", colDef: "LONG VARCHAR", want: types.QValueKindString},
		{name: "long char varying", colDef: "LONG CHAR VARYING", want: types.QValueKindString},
		{name: "long character varying", colDef: "LONG CHARACTER VARYING", want: types.QValueKindString},
		{name: "long varcharacter", colDef: "LONG VARCHARACTER", want: types.QValueKindString},
		// LONG VARBINARY normalizes to mediumblob
		{name: "long varbinary", colDef: "LONG VARBINARY", want: types.QValueKindBytes},

		// Display widths: the (M) on integers is parsed away (we cut at "(").
		{name: "tinyint width", colDef: "TINYINT(4)", want: types.QValueKindInt8},
		{name: "smallint width", colDef: "SMALLINT(5)", want: types.QValueKindInt16},
		{name: "mediumint width", colDef: "MEDIUMINT(8)", want: types.QValueKindInt32},
		{name: "int width", colDef: "INT(11)", want: types.QValueKindInt32},
		{name: "integer width", colDef: "INTEGER(11)", want: types.QValueKindInt32},
		{name: "bigint width", colDef: "BIGINT(20)", want: types.QValueKindInt64},

		// ZEROFILL implies UNSIGNED; QkindFromMysqlColumnType strips both suffixes.
		{name: "int zerofill", colDef: "INT ZEROFILL", want: types.QValueKindUInt32},
		{name: "int width zerofill", colDef: "INT(11) ZEROFILL", want: types.QValueKindUInt32},
		{name: "bigint zerofill", colDef: "BIGINT ZEROFILL", want: types.QValueKindUInt64},

		// YEAR width.
		{name: "year(4)", colDef: "YEAR(4)", want: types.QValueKindInt16},

		// Floating point with precision/scale and unsigned modifiers.
		{name: "float(m,d)", colDef: "FLOAT(10,2)", want: types.QValueKindFloat32},
		{name: "float unsigned", colDef: "FLOAT UNSIGNED", want: types.QValueKindFloat32},
		{name: "double(m,d)", colDef: "DOUBLE(10,2)", want: types.QValueKindFloat64},
		{name: "double unsigned", colDef: "DOUBLE UNSIGNED", want: types.QValueKindFloat64},
		{name: "double precision(m,d)", colDef: "DOUBLE PRECISION(10,2)", want: types.QValueKindFloat64},

		// FLOAT(p) single-precision form: p<=24 stays float, p>=25 normalizes to double.
		{name: "float(p) p<=24 is float", colDef: "FLOAT(24)", want: types.QValueKindFloat32},
		{name: "float(p) p>=25 is double", colDef: "FLOAT(25)", want: types.QValueKindFloat64},

		// Character types without an explicit length normalize to length 1.
		{name: "char without length", colDef: "CHAR CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", want: types.QValueKindString},
		{name: "nchar without length", colDef: "NCHAR", want: types.QValueKindString},
		{name: "national char without length", colDef: "NATIONAL CHAR", want: types.QValueKindString},

		// Binary without length normalizes to binary(1); blob with length stays bytes.
		{name: "binary without length", colDef: "BINARY", want: types.QValueKindBytes},
		{name: "blob(M)", colDef: "BLOB(100)", want: types.QValueKindBytes},

		// CHARACTER SET binary changes char/varchar/text into binary/blob types.
		{name: "char charset binary", colDef: "CHAR(10) CHARACTER SET binary", want: types.QValueKindBytes},
		{name: "varchar charset binary", colDef: "VARCHAR(10) CHARACTER SET binary", want: types.QValueKindBytes},
		{name: "text charset binary", colDef: "TEXT CHARACTER SET binary", want: types.QValueKindBytes},

		// Non-binary charset / collate / (deprecated) binary modifiers on string-ish types
		// don't change the qkind — they only affect collation, which we don't key off here.
		{name: "varchar charset", colDef: "VARCHAR(10) CHARACTER SET utf8mb4", want: types.QValueKindString},
		{name: "varchar collate", colDef: "VARCHAR(10) COLLATE utf8mb4_bin", want: types.QValueKindString},
		{name: "varchar charset collate", colDef: "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", want: types.QValueKindString},
		{name: "text charset", colDef: "TEXT CHARACTER SET latin1", want: types.QValueKindString},
		{name: "enum charset", colDef: "ENUM('a','b') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", want: types.QValueKindEnum},
		{name: "set collate", colDef: "SET('a','b') COLLATE utf8mb4_bin", want: types.QValueKindString},
		// The deprecated BINARY modifier only forces binary collation; the parser strips it,
		// so the qkind is unaffected (it is NOT the BINARY column type).
		{name: "varchar binary modifier", colDef: "VARCHAR(10) BINARY", want: types.QValueKindString},
		{name: "text binary modifier", colDef: "TEXT BINARY", want: types.QValueKindString},
		{name: "enum binary modifier", colDef: "ENUM('a','b') BINARY", want: types.QValueKindEnum},
		{name: "set binary modifier", colDef: "SET('a','b') BINARY", want: types.QValueKindString},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typeModifier := int32(-1)
			if tc.typeModifier != 0 {
				typeModifier = tc.typeModifier
			}

			want := &protos.FieldDescription{
				Name:         "c",
				Type:         string(tc.want),
				TypeModifier: typeModifier,
				Nullable:     !tc.notNull,
			}
			require.Equal(t, want, addColumnFieldDescription(t, tc.colDef))

			wantNotNull := *want
			wantNotNull.Nullable = false
			require.Equal(t, &wantNotNull, addColumnFieldDescription(t, tc.colDef+" NOT NULL"))
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

func TestUnsupportedDDLTypes(t *testing.T) {
	for _, colType := range []string{
		// spatial types
		"GEOMETRY", "POINT", "POLYGON", "LINESTRING", "MULTIPOINT",
		"MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION",
		// MariaDB-only network/UUID types
		"INET4", "INET6", "UUID",
		// MariaDB/Oracle-mode aliases the TiDB parser doesn't accept. The ones with a scalar
		// mapping (CHAR BYTE/RAW->bytes, CLOB->text, VARCHAR2->varchar, XMLTYPE->text,
		// NUMBER->numeric) are still handled directly by QkindFromMysqlColumnType, see
		// TestQkindFromMysSQLTypeForUnsupportedDDLTypes.
		"CHAR BYTE", "RAW", "CLOB", "VARCHAR2", "XMLTYPE", "NUMBER",
	} {
		t.Run(colType, func(t *testing.T) {
			_, _, err := parseSQL(parser.New(), []byte("ALTER TABLE t ADD COLUMN c "+colType))
			require.Error(t, err)
		})
	}
}

func TestQkindFromMysSQLTypeForUnsupportedDDLTypes(t *testing.T) {
	for _, tc := range []struct {
		ct   string
		want types.QValueKind
	}{
		{"char byte", types.QValueKindBytes}, // BINARY
		{"raw", types.QValueKindBytes},       // VARBINARY (Oracle mode)
		{"clob", types.QValueKindString},     // LONGTEXT (Oracle mode)
		{"varchar2", types.QValueKindString}, // VARCHAR (Oracle mode)
		{"xmltype", types.QValueKindString},  // XML stored as text (MariaDB 12.3+)
		{"number", types.QValueKindNumeric},  // DECIMAL (Oracle mode)
		{"number(10,2)", types.QValueKindNumeric},
		{"inet4", types.QValueKindINET},
		{"inet6", types.QValueKindINET},
		{"uuid", types.QValueKindUUID},
	} {
		t.Run(tc.ct, func(t *testing.T) {
			qkind, err := QkindFromMysqlColumnType(tc.ct, true, shared.InternalVersion_Latest)
			require.NoError(t, err)
			require.Equal(t, tc.want, qkind)
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

func TestIntegrationGetTableSchemaCaseSensitiveIdentifiers(t *testing.T) {
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
		})
	}
}

func TestIntegrationGetTableSchemaPrimaryKeyVariants(t *testing.T) {
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
			decimalDefaultsTable := dbName + ".decimal_defaults"

			// Composite PK whose key order (b, a) differs from column definition order (a, b, c),
			// exercising the seq_in_index sort.
			exec(fmt.Sprintf("CREATE TABLE %s (a INT, b INT, c INT, PRIMARY KEY (b, a))", compositeTable))
			// Table without a primary key, exercising the LEFT JOIN's NULL seq_in_index path.
			exec(fmt.Sprintf("CREATE TABLE %s (a INT, b TEXT)", noPKTable))
			// PK on a mixed-case column name, exercising the case-preserving column_name join.
			exec(fmt.Sprintf("CREATE TABLE %s (`MyId` INT PRIMARY KEY, val TEXT)", mixedCaseTable))
			// Bare DECIMAL defaults to DECIMAL(10,0) in MySQL/MariaDB; verify GetTableSchema preserves that typmod.
			exec(fmt.Sprintf("CREATE TABLE %s (bare DECIMAL, explicit DECIMAL(10,2))", decimalDefaultsTable))

			schemas, err := connector.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
				[]*protos.TableMapping{
					{SourceTableIdentifier: compositeTable},
					{SourceTableIdentifier: noPKTable},
					{SourceTableIdentifier: mixedCaseTable},
					{SourceTableIdentifier: decimalDefaultsTable},
				})
			require.NoError(t, err)
			assertSchema(schemas[compositeTable], []string{"b", "a"}, "a", "b", "c")
			assertSchema(schemas[noPKTable], nil, "a", "b")
			assertSchema(schemas[mixedCaseTable], []string{"MyId"}, "MyId", "val")

			decimalSchema := schemas[decimalDefaultsTable]
			assertSchema(decimalSchema, nil, "bare", "explicit")
			require.Equal(t, string(types.QValueKindNumeric), decimalSchema.Columns[0].Type)
			require.Equal(t, datatypes.MakeNumericTypmod(10, 0), decimalSchema.Columns[0].TypeModifier)
			require.Equal(t, string(types.QValueKindNumeric), decimalSchema.Columns[1].Type)
			require.Equal(t, datatypes.MakeNumericTypmod(10, 2), decimalSchema.Columns[1].TypeModifier)
		})
	}
}
