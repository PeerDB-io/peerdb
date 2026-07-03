package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ddlTypeCase drives the type-matrix tests: typ is everything written after the
// column name in "ALTER TABLE t ADD COLUMN c <typ>"; want is compared against
// the single parsed column. Ground truth for expectations is engine behavior and
// type-alias documentation, not the parser's own output.
type ddlTypeCase struct {
	typ     string
	want    ddlColumnDef
	sqlMode uint64
	maria   bool
	hasPos  bool
	noQkind bool // MariaDB UDT passthroughs have no QkindFromMysqlColumnType entry
}

func ddlTypesCol(typeStr string, precision, scale int, notNull bool) ddlColumnDef {
	return ddlColumnDef{Name: "c", TypeStr: typeStr, Precision: precision, Scale: scale, NotNull: notNull}
}

func ddlTypesRun(t *testing.T, cases []ddlTypeCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			alter := ddlParseAlter(t, "ALTER TABLE t ADD COLUMN c "+tc.typ, tc.sqlMode, tc.maria)
			require.Len(t, alter.Specs, 1)
			require.Equal(t, tc.hasPos, alter.Specs[0].HasPosition)
			require.Equal(t, []ddlColumnDef{tc.want}, alter.Specs[0].NewColumns)
			if !tc.noQkind {
				// guards the load-bearing TypeStr format contract: every server-valid
				// type must land on a base name the downstream converter knows
				_, qerr := QkindFromMysqlColumnType(tc.want.TypeStr, true, 0)
				require.NoError(t, qerr, "QkindFromMysqlColumnType(%q)", tc.want.TypeStr)
			}
		})
	}
}

func TestDDLTypesIntegers(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "TINYINT", want: ddlTypesCol("tinyint", -1, -1, false)},
		{typ: "TINYINT(1)", want: ddlTypesCol("tinyint(1)", 1, -1, false)},
		{typ: "TINYINT(3) UNSIGNED", want: ddlTypesCol("tinyint(3) unsigned", 3, -1, false)},
		{typ: "INT1", want: ddlTypesCol("tinyint", -1, -1, false)},
		// INT1 is an alias for TINYINT, so INT1(1) must keep bool detection
		{typ: "INT1(1)", want: ddlTypesCol("tinyint(1)", 1, -1, false)},
		{typ: "SMALLINT(6)", want: ddlTypesCol("smallint(6)", 6, -1, false)},
		{typ: "INT2", want: ddlTypesCol("smallint", -1, -1, false)},
		{typ: "MEDIUMINT(9)", want: ddlTypesCol("mediumint(9)", 9, -1, false)},
		{typ: "INT3", want: ddlTypesCol("mediumint", -1, -1, false)},
		{typ: "MIDDLEINT", want: ddlTypesCol("mediumint", -1, -1, false)},
		{typ: "INT(11)", want: ddlTypesCol("int(11)", 11, -1, false)},
		{typ: "INTEGER(11)", want: ddlTypesCol("int(11)", 11, -1, false)},
		{typ: "INT4", want: ddlTypesCol("int", -1, -1, false)},
		{typ: "BIGINT(20) UNSIGNED", want: ddlTypesCol("bigint(20) unsigned", 20, -1, false)},
		{typ: "INT8", want: ddlTypesCol("bigint", -1, -1, false)},
		{typ: "INT SIGNED", want: ddlTypesCol("int", -1, -1, false)},
		{typ: "INT ZEROFILL", want: ddlTypesCol("int unsigned", -1, -1, false)},
		{typ: "INT UNSIGNED ZEROFILL", want: ddlTypesCol("int unsigned", -1, -1, false)},
		{typ: "INT(10) ZEROFILL UNSIGNED", want: ddlTypesCol("int(10) unsigned", 10, -1, false)},
		{typ: "BOOL", want: ddlTypesCol("tinyint(1)", -1, -1, false)},
		{typ: "BOOLEAN NOT NULL", want: ddlTypesCol("tinyint(1)", -1, -1, true)},
		{typ: "SERIAL", want: ddlTypesCol("bigint unsigned", -1, -1, true)},
		{typ: "BIT", want: ddlTypesCol("bit", -1, -1, false)},
		// bit params never feed numeric typmods
		{typ: "BIT(64)", want: ddlTypesCol("bit(64)", -1, -1, false)},
	})
}

func TestDDLTypesDecimalAndFloat(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "DECIMAL", want: ddlTypesCol("decimal", -1, -1, false)},
		{typ: "DECIMAL(10)", want: ddlTypesCol("decimal(10)", 10, -1, false)},
		{typ: "DECIMAL(10,2)", want: ddlTypesCol("decimal(10,2)", 10, 2, false)},
		// whitespace between params does not survive into TypeStr
		{typ: "DECIMAL(10, 2)", want: ddlTypesCol("decimal(10,2)", 10, 2, false)},
		{typ: "DECIMAL(65,30) UNSIGNED", want: ddlTypesCol("decimal(65,30) unsigned", 65, 30, false)},
		{typ: "DEC(8,3)", want: ddlTypesCol("decimal(8,3)", 8, 3, false)},
		{typ: "FIXED", want: ddlTypesCol("decimal", -1, -1, false)},
		{typ: "FIXED(6,2)", want: ddlTypesCol("decimal(6,2)", 6, 2, false)},
		{typ: "NUMERIC", want: ddlTypesCol("numeric", -1, -1, false)},
		{typ: "NUMERIC(5)", want: ddlTypesCol("numeric(5)", 5, -1, false)},
		// an explicit scale of 0 must survive as 0, not collapse to -1
		{typ: "NUMERIC(10,0)", want: ddlTypesCol("numeric(10,0)", 10, 0, false)},
		{typ: "FLOAT", want: ddlTypesCol("float", -1, -1, false)},
		{typ: "FLOAT4", want: ddlTypesCol("float", -1, -1, false)},
		// float params are written through but never feed typmods
		{typ: "FLOAT(10)", want: ddlTypesCol("float(10)", -1, -1, false)},
		{typ: "FLOAT(7,4)", want: ddlTypesCol("float(7,4)", -1, -1, false)},
		{typ: "FLOAT UNSIGNED", want: ddlTypesCol("float unsigned", -1, -1, false)},
		{typ: "DOUBLE", want: ddlTypesCol("double", -1, -1, false)},
		{typ: "DOUBLE(8,3)", want: ddlTypesCol("double(8,3)", -1, -1, false)},
		{typ: "DOUBLE PRECISION", want: ddlTypesCol("double", -1, -1, false)},
		{typ: "DOUBLE PRECISION(10,2)", want: ddlTypesCol("double(10,2)", -1, -1, false)},
		{typ: "REAL", want: ddlTypesCol("double", -1, -1, false)},
		{typ: "FLOAT8", want: ddlTypesCol("double", -1, -1, false)},
	})
}

func TestDDLTypesCharFamilies(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "CHAR", want: ddlTypesCol("char", -1, -1, false)},
		{typ: "CHAR(5)", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "CHARACTER(5)", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "VARCHAR(255)", want: ddlTypesCol("varchar(255)", -1, -1, false)},
		{typ: "VARCHARACTER(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "CHAR VARYING(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "CHARACTER VARYING(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NCHAR(5)", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "NCHAR VARCHAR(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NCHAR VARYING(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NATIONAL CHAR(5)", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "NATIONAL CHARACTER(5)", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "NATIONAL VARCHAR(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NATIONAL CHARACTER VARYING(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NVARCHAR(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		// VARCHARACTER is an alias of VARCHAR, so it is valid after NATIONAL and NCHAR too
		{typ: "NATIONAL VARCHARACTER(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "NCHAR VARCHARACTER(5)", want: ddlTypesCol("varchar(5)", -1, -1, false)},
		{typ: "BINARY", want: ddlTypesCol("binary", -1, -1, false)},
		{typ: "BINARY(16)", want: ddlTypesCol("binary(16)", -1, -1, false)},
		{typ: "VARBINARY(16)", want: ddlTypesCol("varbinary(16)", -1, -1, false)},
	})
}

func TestDDLTypesBlobTextLong(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "TINYTEXT", want: ddlTypesCol("tinytext", -1, -1, false)},
		{typ: "TEXT", want: ddlTypesCol("text", -1, -1, false)},
		{typ: "TEXT(100)", want: ddlTypesCol("text(100)", -1, -1, false)},
		{typ: "MEDIUMTEXT", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONGTEXT", want: ddlTypesCol("longtext", -1, -1, false)},
		{typ: "TINYBLOB", want: ddlTypesCol("tinyblob", -1, -1, false)},
		{typ: "BLOB", want: ddlTypesCol("blob", -1, -1, false)},
		{typ: "BLOB(255)", want: ddlTypesCol("blob(255)", -1, -1, false)},
		{typ: "MEDIUMBLOB", want: ddlTypesCol("mediumblob", -1, -1, false)},
		{typ: "LONGBLOB", want: ddlTypesCol("longblob", -1, -1, false)},
		{typ: "LONG", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONG VARCHAR", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONG VARCHARACTER", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONG CHAR VARYING", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONG CHARACTER VARYING", want: ddlTypesCol("mediumtext", -1, -1, false)},
		{typ: "LONG VARBINARY", want: ddlTypesCol("mediumblob", -1, -1, false)},
		// CLOB is a longtext synonym on both engines even outside Oracle mode
		{typ: "CLOB", want: ddlTypesCol("longtext", -1, -1, false)},
	})
}

func TestDDLTypesEnumSetTemporalSpatial(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "ENUM('a','b')", want: ddlTypesCol("enum('a','b')", -1, -1, false)},
		// the element list passes through exactly as written, escapes included
		{typ: `ENUM('a''b','c\'d')`, want: ddlTypesCol(`enum('a''b','c\'d')`, -1, -1, false)},
		{typ: "ENUM('a', 'b')", want: ddlTypesCol("enum('a', 'b')", -1, -1, false)},
		{typ: "SET('x,y','z')", want: ddlTypesCol("set('x,y','z')", -1, -1, false)},
		// the binary-charset remap only applies to the char/text family
		{typ: "ENUM('a','b') CHARACTER SET binary", want: ddlTypesCol("enum('a','b')", -1, -1, false)},
		{typ: "DATE", want: ddlTypesCol("date", -1, -1, false)},
		{typ: "TIME", want: ddlTypesCol("time", -1, -1, false)},
		{typ: "TIME(3)", want: ddlTypesCol("time(3)", -1, -1, false)},
		{typ: "DATETIME", want: ddlTypesCol("datetime", -1, -1, false)},
		{typ: "DATETIME(6)", want: ddlTypesCol("datetime(6)", -1, -1, false)},
		{typ: "TIMESTAMP", want: ddlTypesCol("timestamp", -1, -1, false)},
		{typ: "TIMESTAMP(6)", want: ddlTypesCol("timestamp(6)", -1, -1, false)},
		{typ: "YEAR", want: ddlTypesCol("year", -1, -1, false)},
		{typ: "YEAR(4)", want: ddlTypesCol("year(4)", -1, -1, false)},
		{typ: "JSON", want: ddlTypesCol("json", -1, -1, false)},
		{typ: "GEOMETRY", want: ddlTypesCol("geometry", -1, -1, false)},
		{typ: "POINT", want: ddlTypesCol("point", -1, -1, false)},
		{typ: "LINESTRING", want: ddlTypesCol("linestring", -1, -1, false)},
		{typ: "POLYGON", want: ddlTypesCol("polygon", -1, -1, false)},
		{typ: "MULTIPOINT", want: ddlTypesCol("multipoint", -1, -1, false)},
		{typ: "MULTILINESTRING", want: ddlTypesCol("multilinestring", -1, -1, false)},
		{typ: "MULTIPOLYGON", want: ddlTypesCol("multipolygon", -1, -1, false)},
		{typ: "GEOMETRYCOLLECTION", want: ddlTypesCol("geometrycollection", -1, -1, false)},
		{typ: "GEOMCOLLECTION", want: ddlTypesCol("geomcollection", -1, -1, false)},
		{typ: "VECTOR", want: ddlTypesCol("vector", -1, -1, false)},
		{typ: "VECTOR(4)", want: ddlTypesCol("vector(4)", -1, -1, false)},
		{typ: "GEOMETRY", want: ddlTypesCol("geometry", -1, -1, false), maria: true},
		{typ: "VECTOR(4)", want: ddlTypesCol("vector(4)", -1, -1, false), maria: true},
	})
}

func TestDDLTypesCharsetSuffixes(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "CHAR(5) CHARACTER SET binary", want: ddlTypesCol("binary(5)", -1, -1, false)},
		{typ: "VARCHAR(10) CHARACTER SET binary", want: ddlTypesCol("varbinary(10)", -1, -1, false)},
		{typ: "TINYTEXT CHARACTER SET binary", want: ddlTypesCol("tinyblob", -1, -1, false)},
		{typ: "TEXT CHARSET binary", want: ddlTypesCol("blob", -1, -1, false)},
		{typ: "MEDIUMTEXT CHARACTER SET binary", want: ddlTypesCol("mediumblob", -1, -1, false)},
		{typ: "LONGTEXT CHARACTER SET binary", want: ddlTypesCol("longblob", -1, -1, false)},
		// charset_name may be a quoted string literal
		{typ: "VARCHAR(10) CHARACTER SET 'binary'", want: ddlTypesCol("varbinary(10)", -1, -1, false)},
		// the bare BINARY attribute is a collation marker, never a type change
		{typ: "VARCHAR(10) BINARY", want: ddlTypesCol("varchar(10)", -1, -1, false)},
		{typ: "VARCHAR(10) BINARY CHARACTER SET utf8mb4", want: ddlTypesCol("varchar(10)", -1, -1, false)},
		// ASCII/UNICODE shorthands pick latin1/ucs2, not binary
		{typ: "CHAR(5) ASCII", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "CHAR(5) UNICODE", want: ddlTypesCol("char(5)", -1, -1, false)},
		{typ: "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", want: ddlTypesCol("varchar(10)", -1, -1, true)},
		{typ: "CHAR(3) BYTE", want: ddlTypesCol("binary(3)", -1, -1, false), maria: true},
		{typ: "VARCHAR(10) BYTE", want: ddlTypesCol("varbinary(10)", -1, -1, false), maria: true},
		{typ: "TEXT BYTE", want: ddlTypesCol("blob", -1, -1, false), maria: true},
		// BYTE is valid on both engines and remaps through the binary charset.
		{typ: "CHAR(3) BYTE", want: ddlTypesCol("binary(3)", -1, -1, false)},
	})
}

func TestDDLTypesMariaDBUDTsAndOracle(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "UUID", want: ddlTypesCol("uuid", -1, -1, false), maria: true, noQkind: true},
		{typ: "INET4", want: ddlTypesCol("inet4", -1, -1, false), maria: true, noQkind: true},
		{typ: "INET6", want: ddlTypesCol("inet6", -1, -1, false), maria: true, noQkind: true},
		// any identifier in type position is a candidate plugin UDT, lowercased
		{typ: "PriceType", want: ddlTypesCol("pricetype", -1, -1, false), maria: true, noQkind: true},
		// schema-qualified type names resolve through mariadb_schema
		{typ: "mariadb_schema.DATE", want: ddlTypesCol("date", -1, -1, false), maria: true},
		// without sql_mode=ORACLE these tokens are UDT passthroughs, with no typmod
		{typ: "NUMBER(10,2)", want: ddlTypesCol("number(10,2)", -1, -1, false), maria: true, noQkind: true},
		{typ: "VARCHAR2(30)", want: ddlTypesCol("varchar2(30)", -1, -1, false), maria: true, noQkind: true},
		{typ: "RAW(16)", want: ddlTypesCol("raw(16)", -1, -1, false), maria: true, noQkind: true},
		{typ: "NUMBER(10,2)", want: ddlTypesCol("decimal(10,2)", 10, 2, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "NUMBER(10)", want: ddlTypesCol("decimal(10)", 10, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "NUMBER", want: ddlTypesCol("double", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "VARCHAR2(30)", want: ddlTypesCol("varchar(30)", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "RAW(16)", want: ddlTypesCol("varbinary(16)", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "CLOB", want: ddlTypesCol("longtext", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		// Oracle-mode lengthless BLOB is LONGBLOB; with a length it stays BLOB
		// with a length it stays BLOB
		{typ: "BLOB", want: ddlTypesCol("longblob", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "BLOB(10)", want: ddlTypesCol("blob(10)", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
		{typ: "INT", want: ddlTypesCol("int", -1, -1, false), maria: true, sqlMode: sqlModeOracle},
	})
}

// TestDDLTypesMySQLUnknownTypeErrors: MySQL's type keyword set is closed, so a
// word it does not know in type position is a parse error — reported, never a
// silently mis-typed column.
func TestDDLTypesMySQLUnknownTypeErrors(t *testing.T) {
	for _, typ := range []string{"UUID", "INET4", "INET6", "VARCHAR2(30)", "RAW(16)", "NUMBER(10,2)", "GEOGRAPHY"} {
		t.Run(typ, func(t *testing.T) {
			_, err := parseQueryEvent([]byte("ALTER TABLE t ADD COLUMN c "+typ), 0, false)
			require.Error(t, err)
		})
	}
}

func TestDDLTypesAttributeSkipping(t *testing.T) {
	ddlTypesRun(t, []ddlTypeCase{
		{typ: "INT NULL", want: ddlTypesCol("int", -1, -1, false)},
		{typ: "INT DEFAULT (1+(2*3)) NOT NULL", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "VARCHAR(20) DEFAULT (concat('a)b', ')')) NOT NULL", want: ddlTypesCol("varchar(20)", -1, -1, true)},
		{typ: "INT DEFAULT (/* ) */ 1) NOT NULL", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "VARCHAR(10) DEFAULT 'it''s' NOT NULL", want: ddlTypesCol("varchar(10)", -1, -1, true)},
		{typ: `VARCHAR(10) DEFAULT 'a\'b' NOT NULL`, want: ddlTypesCol("varchar(10)", -1, -1, true)},
		{typ: "INT GENERATED ALWAYS AS (a + b) STORED NOT NULL", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "DOUBLE AS (sqrt(x)) VIRTUAL", want: ddlTypesCol("double", -1, -1, false)},
		{typ: "INT AS (a + 1) PERSISTENT", want: ddlTypesCol("int", -1, -1, false), maria: true},
		{
			typ:  "TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)",
			want: ddlTypesCol("timestamp(6)", -1, -1, true),
		},
		{typ: "POINT NOT NULL SRID 4326", want: ddlTypesCol("point", -1, -1, true)},
		{typ: "INT NOT NULL AUTO_INCREMENT UNIQUE KEY COMMENT 'id, pk' AFTER b", want: ddlTypesCol("int", -1, -1, true), hasPos: true},
		{typ: "INT FIRST", want: ddlTypesCol("int", -1, -1, false), hasPos: true},
		{typ: "INT AFTER after", want: ddlTypesCol("int", -1, -1, false), hasPos: true},
		{typ: "INT NOT NULL FIRST", want: ddlTypesCol("int", -1, -1, true), hasPos: true},
		{typ: `VARCHAR(10) ENGINE_ATTRIBUTE='{"k": "v"}' NOT NULL`, want: ddlTypesCol("varchar(10)", -1, -1, true)},
		{typ: "INT NOT NULL REFERENCES other (id) ON DELETE CASCADE", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "INT CONSTRAINT ck CHECK (c > 0 AND c < 10) NOT ENFORCED NOT NULL", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "INT INVISIBLE NOT NULL", want: ddlTypesCol("int", -1, -1, true)},
		{typ: "VARCHAR(100) COMPRESSED NOT NULL", want: ddlTypesCol("varchar(100)", -1, -1, true), maria: true},
		{typ: "VARCHAR(100) COMPRESSED=zlib NOT NULL", want: ddlTypesCol("varchar(100)", -1, -1, true), maria: true},
		{typ: "INT NOT NULL ENABLE", want: ddlTypesCol("int", -1, -1, true), maria: true},
	})
}
