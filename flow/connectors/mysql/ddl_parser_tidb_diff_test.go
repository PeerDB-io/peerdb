package connmysql

// Differential tests: the hand-rolled DDL parser vs the TiDB parser it replaced.
// The corpus at the bottom covers production binlog noise and locally authored
// DDL edge cases. For every statement TiDB parses, both parsers' outputs are
// reduced to a signature over exactly what cdc.go consumes: classification,
// schema/table, per-spec column names, QkindFromMysqlColumnType of the type
// string, NOT NULL, FIRST/AFTER presence, and (precision,scale) for
// decimal/numeric. Where TiDB cannot parse a dialect feature, the expected
// signature is hand-annotated from engine behavior.

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type tidbDiffCase struct {
	sql    string
	engine string // "mysql", "mariadb" or "both"
	note   string
}

// ddlDiffColSig renders one column the way cdc.go consumes it: qkind (or ERR
// when QkindFromMysqlColumnType rejects the type — cdc.go logs and skips such
// columns), (precision,scale) only for the numeric kind, and the NOT NULL flag.
func ddlDiffColSig(c ddlColumnDef) string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteByte('=')
	kind, err := QkindFromMysqlColumnType(c.TypeStr, true, 0)
	if err != nil {
		sb.WriteString("ERR")
	} else {
		sb.WriteString(string(kind))
	}
	if kind == types.QValueKindNumeric {
		fmt.Fprintf(&sb, "(%d,%d)", c.Precision, c.Scale)
	}
	if c.NotNull {
		sb.WriteString(" nn")
	}
	return sb.String()
}

func ddlDiffSpecSig(sp ddlAlterSpec) string {
	var sb strings.Builder
	switch {
	case sp.RenameColumn:
		sb.WriteString("ren " + sp.OldColumnName + ">" + sp.NewColumnName)
	case len(sp.NewColumns) > 0:
		if sp.OldColumnName != "" {
			sb.WriteString("chg " + sp.OldColumnName + " ")
		} else {
			sb.WriteString("col ")
		}
		for i, c := range sp.NewColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ddlDiffColSig(c))
		}
	default:
		sb.WriteString("drop " + sp.OldColumnName)
	}
	if sp.HasPosition {
		sb.WriteString(" @pos")
	}
	return sb.String()
}

func ddlDiffQual(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// ddlDiffSignature reduces parser output to the comparison signature. Benign
// output (no statements, or alters with no column-relevant specs) still renders
// the alter head so both sides must also agree on classification.
func ddlDiffSignature(stmts []ddlStatement) string {
	var parts []string
	for _, s := range stmts {
		switch st := s.(type) {
		case *ddlAlterTable:
			specs := make([]string, len(st.Specs))
			for i, sp := range st.Specs {
				specs[i] = ddlDiffSpecSig(sp)
			}
			parts = append(parts, "alter "+ddlDiffQual(st.Schema, st.Table)+"{"+strings.Join(specs, "; ")+"}")
		case *ddlRenameTable:
			prs := make([]string, len(st.Pairs))
			for i, p := range st.Pairs {
				prs[i] = ddlDiffQual(p.OldSchema, p.OldTable) + ">" + ddlDiffQual(p.NewSchema, p.NewTable)
			}
			parts = append(parts, "rename "+strings.Join(prs, ", "))
		}
	}
	return strings.Join(parts, " | ")
}

// ddlDiffOurSig runs the hand-rolled parser and renders its signature; a parse
// error renders as "ERROR".
func ddlDiffOurSig(query string, sqlMode uint64, isMariaDB bool) string {
	stmts, err := parseQueryEvent([]byte(query), sqlMode, isMariaDB)
	if err != nil {
		return "ERROR"
	}
	return ddlDiffSignature(stmts)
}

// tidbToDDLStatements maps a TiDB AST onto the hand-rolled parser's output
// shape, keeping only what cdc.go consumed on the TiDB path: column-relevant
// alter specs (columns with a type — ALTER COLUMN SET DEFAULT specs carry a nil
// Tp and were skipped there too) and rename pairs.
func tidbToDDLStatements(nodes []ast.StmtNode) []ddlStatement {
	var out []ddlStatement
	for _, node := range nodes {
		switch st := node.(type) {
		case *ast.AlterTableStmt:
			alter := &ddlAlterTable{Schema: st.Table.Schema.O, Table: st.Table.Name.O}
			for _, sp := range st.Specs {
				if conv, ok := tidbSpecToDDL(sp); ok {
					alter.Specs = append(alter.Specs, conv)
				}
			}
			out = append(out, alter)
		case *ast.RenameTableStmt:
			rename := &ddlRenameTable{}
			for _, tt := range st.TableToTables {
				rename.Pairs = append(rename.Pairs, ddlRenamePair{
					OldSchema: tt.OldTable.Schema.O, OldTable: tt.OldTable.Name.O,
					NewSchema: tt.NewTable.Schema.O, NewTable: tt.NewTable.Name.O,
				})
			}
			out = append(out, rename)
		}
	}
	return out
}

func tidbSpecToDDL(sp *ast.AlterTableSpec) (ddlAlterSpec, bool) {
	var out ddlAlterSpec
	switch sp.Tp {
	case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
		if sp.OldColumnName != nil {
			out.OldColumnName = sp.OldColumnName.Name.O
		}
		for _, col := range sp.NewColumns {
			if col.Tp == nil {
				continue
			}
			def := ddlColumnDef{
				Name:      col.Name.Name.O,
				TypeStr:   col.Tp.InfoSchemaStr(),
				Precision: col.Tp.GetFlen(),
				Scale:     col.Tp.GetDecimal(),
			}
			for _, opt := range col.Options {
				if opt.Tp == ast.ColumnOptionNotNull {
					def.NotNull = true
				}
			}
			out.NewColumns = append(out.NewColumns, def)
		}
		if sp.Position != nil && sp.Position.Tp != ast.ColumnPositionNone {
			out.HasPosition = true
		}
		if len(out.NewColumns) == 0 && out.OldColumnName == "" {
			return out, false
		}
	case ast.AlterTableDropColumn:
		out.OldColumnName = sp.OldColumnName.Name.O
	case ast.AlterTableRenameColumn:
		out.OldColumnName = sp.OldColumnName.Name.O
		out.NewColumnName = sp.NewColumnName.Name.O
		out.RenameColumn = true
	default:
		return out, false
	}
	return out, true
}

// TestDDLTidbDiffCorpus is the main differential run. Every corpus statement is
// parsed by both parsers ("both" statements once per engine flag). Signatures
// must match wherever TiDB parses, except tidbDiffOverrides (statements where
// TiDB itself diverges from real server behavior). Where TiDB fails to parse,
// the hand-annotated tidbDiffFailWant entry is authoritative — a missing
// annotation fails the test.
func TestDDLTidbDiffCorpus(t *testing.T) {
	for i, tc := range tidbDiffCorpus {
		var runs []bool
		switch tc.engine {
		case "mysql":
			runs = []bool{false}
		case "mariadb":
			runs = []bool{true}
		default:
			runs = []bool{false, true}
		}
		nodes, _, tidbErr := parser.New().ParseSQL(tc.sql)
		for _, isMaria := range runs {
			t.Run(strconv.Itoa(i)+"/maria="+strconv.FormatBool(isMaria), func(t *testing.T) {
				ourSig := ddlDiffOurSig(tc.sql, 0, isMaria)
				if want, ok := tidbDiffOverride(tc.sql, isMaria); ok {
					require.Equal(t, want, ourSig, "override mismatch (%s): %q", tc.note, tc.sql)
					return
				}
				if tidbErr != nil {
					want, ok := tidbDiffFailWant[tc.sql]
					require.True(t, ok, "TiDB failed to parse and no expectation is annotated (%s): %q", tc.note, tc.sql)
					require.Equal(t, want, ourSig, "annotated expectation mismatch (%s): %q", tc.note, tc.sql)
					return
				}
				require.Equal(t, ddlDiffSignature(tidbToDDLStatements(nodes)), ourSig, "TiDB diff (%s): %q", tc.note, tc.sql)
			})
		}
	}
}

// tidbDiffOverrides lists statements TiDB parses but diverges on from engine
// behavior; the value is the expected signature.
var tidbDiffOverrides = map[string]string{
	// TiDB treats /*M! as a plain comment; MariaDB executes the body when the
	// version gate passes.
	"ALTER TABLE t /*M! MODIFY c INT */":           "alter t{col c=int32}",
	"ALTER TABLE t /*M!100500 ADD COLUMN c INT */": "alter t{col c=int32}",
	// ALTER TABLE ... RENAME is a table rename; the TiDB path ignored it as an
	// empty ALTER TABLE.
	"ALTER TABLE t RENAME = t2": "rename t>t2",
}

func tidbDiffOverride(sql string, isMariaDB bool) (string, bool) {
	if !isMariaDB && sql == "ALTER TABLE t ADD c TINYINT(1) UNSIGNED" {
		return "alter t{col c=uint8}", true
	}
	want, ok := tidbDiffOverrides[sql]
	return want, ok
}

// tidbDiffFailWant holds the expected signature for every corpus statement the
// TiDB parser rejects.
var tidbDiffFailWant = map[string]string{
	"SET STATEMENT max_statement_time=60 FOR INSERT INTO mysql.rds_heartbeat2(id, " +
		"value) values (1,1782476058116) ON DUPLICATE KEY UPDATE value = 1782476058116": "",
	"SET STATEMENT max_statement_time=60 FOR DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'": "",
	"SET STATEMENT max_statement_time=60 FOR flush table":                                                 "",
	"SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD INDEX idx (a)":                             "alter t{}",
	"SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD COLUMN c INT":                              "alter t{col c=int32}",
	"SET STATEMENT max_statement_time=60, sort_buffer_size=1000000 FOR ALTER TABLE t MODIFY c INT":        "alter t{col c=int32}",
	"SET STATEMENT max_statement_time=60 FOR RENAME TABLE a TO b":                                         "rename a>b",
	"SET STATEMENT sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES') FOR ALTER TABLE t ADD c INT":               "alter t{col c=int32}",
	"SET STATEMENT lc_messages='en FOR us' FOR ALTER TABLE t DROP COLUMN c":                               "alter t{drop c}",
	"ALTER TABLE \"db\".\"t\" ADD COLUMN \"c\" INT":                                                       "ERROR",
	"ALTER TABLE [db].[t] DROP COLUMN [c]":                                                                "ERROR",
	"ALTER TABLE t DROP COLUMN c\x00\x00":                                                                 "alter t{drop c}",
	"ALTER TABLE t ADD c INT DEFAULT (1+1)":                                                               "alter t{col c=int32}",
	"ALTER TABLE t ADD c TEXT COMMENT $$migration$$":                                                      "alter t{col c=string}",
	"ALTER TABLE t ADD c CHAR(8) BYTE":                                                                    "alter t{col c=bytes}",
	"ALTER TABLE t ADD c CLOB":                                                                            "alter t{col c=string}",
	"ALTER TABLE t ADD g GEOMCOLLECTION":                                                                  "alter t{col g=geometry}",
	"ALTER TABLE t ADD g GEOMETRYCOLLECTION":                                                              "alter t{col g=geometry}",
	"ALTER TABLE t ADD g GEOMETRY REF_SYSTEM_ID=4326":                                                     "alter t{col g=geometry}",
	"ALTER TABLE t1 MODIFY a NUMBER(10,2)":                                                                "alter t1{col a=ERR}",
	"ALTER TABLE t1 MODIFY a NUMBER":                                                                      "alter t1{col a=ERR}",
	"ALTER TABLE t1 ADD c VARCHAR2(10)":                                                                   "alter t1{col c=ERR}",
	"ALTER TABLE t1 ADD c RAW(16)":                                                                        "alter t1{col c=ERR}",
	"ALTER TABLE t ADD vector INT":                                                                        "alter t{col vector=int32}",
	"RENAME TABLE a WAIT 5 TO b, c NOWAIT TO d":                                                           "rename a>b, c>d",
	"ALTER TABLE t ADD c INT INVISIBLE":                                                                   "alter t{col c=int32}",
	"ALTER TABLE t ADD c INT VISIBLE":                                                                     "alter t{col c=int32}",
	"ALTER TABLE t ADD c INT ENGINE_ATTRIBUTE='{\"k\":1}' SECONDARY_ENGINE_ATTRIBUTE='{}'":                "alter t{col c=int32}",
	"ALTER TABLE t ADD c BLOB COMPRESSED":                                                                 "alter t{col c=bytes}",
	"ALTER TABLE t ADD c TEXT COMPRESSED=zlib":                                                            "alter t{col c=string}",
	"ALTER TABLE t ADD b INT AS (a*2) PERSISTENT":                                                         "alter t{col b=int32}",
	"ALTER ONLINE TABLE t ADD COLUMN c INT":                                                               "alter t{col c=int32}",
	"ALTER ONLINE IGNORE TABLE t ADD COLUMN c INT":                                                        "alter t{col c=int32}",
	"XA START X'30623263663564642d616630322d34'":                                                          "",
	"XA END X'30623263663564642d616630322d34'":                                                            "",
	"XA COMMIT X'30623263663564642d616630322d34'":                                                         "",
	"CREATE DEFINER=`admin`@`%` PROCEDURE `check_daily_stats_date_contin" +
		"uity`()\nBEGIN\n  DECLARE done INT;\n  CREATE TABLE tmp(x INT);\nEND": "",
	"CREATE DEFINER=`root`@`%` TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @x=1": "",
	"CREATE EVENT nexon_program ON SCHEDULE EVERY 1 DAY DO BEGIN END":                "",
	"ALTER EVENT nexon_program\nDO\nBEGIN\n  UPDATE nexon.employee SET x=1;\nEND":    "",
	"ALTER ALGORITHM=MERGE VIEW v AS SELECT 1":                                       "",
	"ALTER DEFINER=`u`@`%` VIEW v AS SELECT 1":                                       "",
	"ALTER USER 'ipsadmin'@'%' DISCARD OLD PASSWORD":                                 "",
	"ALTER TABLESPACE ts ADD DATAFILE 'f'":                                           "",
	"CREATE PROCEDURE p() BEGIN CREATE TABLE z(x INT); END":                          "",
	"CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1":                         "",
	"COMMIT\x00":                 "",
	"REPAIR TABLE t1":            "",
	"DROP TRIGGER IF EXISTS trg": "",
}

type tidbDiffModeCase struct {
	sql       string
	want      string // expected signature; "ERROR" when parseQueryEvent must error
	note      string
	sqlMode   uint64
	isMariaDB bool
}

// TestDDLTidbDiffHandPicked covers locally authored statements whose lexing
// depends on per-event sql_mode bits or sits on grammar corners TiDB cannot
// represent.
func TestDDLTidbDiffHandPicked(t *testing.T) {
	for i, tc := range tidbDiffModeCases {
		t.Run(strconv.Itoa(i)+"/"+tc.note, func(t *testing.T) {
			require.Equal(t, tc.want, ddlDiffOurSig(tc.sql, tc.sqlMode, tc.isMariaDB), "%s: %q", tc.note, tc.sql)
		})
	}
}

var tidbDiffModeCases = []tidbDiffModeCase{
	// --- ANSI_QUOTES: "..." is a delimited identifier only with the mode bit ---
	{
		sql: `ALTER TABLE "db"."t" ADD COLUMN "c" INT NOT NULL`, sqlMode: sqlModeANSIQuotes,
		want: "alter db.t{col c=int32 nn}", note: "ansi quotes idents",
	},
	{
		sql: `ALTER TABLE "db"."t" ADD COLUMN "K" INT KEY`, sqlMode: sqlModeANSIQuotes,
		want: "alter db.t{col K=int32 nn}", note: "primary-key shorthand implies not null",
	},
	{
		sql: `ALTER TABLE "t" DROP COLUMN "we""ird"`, sqlMode: sqlModeANSIQuotes,
		want: `alter t{drop we"ird}`, note: "ansi quotes doubled delimiter",
	},
	{
		sql:  `ALTER TABLE "db"."t" ADD COLUMN "c" INT`,
		want: "ERROR", note: "without ANSI_QUOTES a double-quoted table ident is a string: parse error, as on the server",
	},
	{
		sql:  `ALTER TABLE t ADD c VARCHAR(10) DEFAULT "a,b", ADD d INT`,
		want: "alter t{col c=string; col d=int32}", note: "comma inside default double-quoted string",
	},
	// --- NO_BACKSLASH_ESCAPES flips which of these parses ---
	{
		sql: `ALTER TABLE t ADD c VARCHAR(10) DEFAULT 'a\', ADD d INT`, sqlMode: sqlModeNoBackslashEscapes,
		want: "alter t{col c=string; col d=int32}", note: "backslash is a literal byte under NO_BACKSLASH_ESCAPES",
	},
	{
		sql:  `ALTER TABLE t ADD c VARCHAR(10) DEFAULT 'a\', ADD d INT`,
		want: "ERROR", note: "backslash escapes the quote by default: unterminated string",
	},
	{
		sql:  `ALTER TABLE t ADD c VARCHAR(10) DEFAULT 'it\'s', ADD d INT`,
		want: "alter t{col c=string; col d=int32}", note: "escaped quote inside string by default",
	},
	{
		sql: `ALTER TABLE t ADD c VARCHAR(10) DEFAULT 'it\'s', ADD d INT`, sqlMode: sqlModeNoBackslashEscapes,
		want: "ERROR", note: "same text is an unterminated string under NO_BACKSLASH_ESCAPES",
	},
	// --- MariaDB MODE_MSSQL bracket identifiers ---
	{
		sql: "ALTER TABLE [db].[t] ADD COLUMN [c] INT NOT NULL", sqlMode: sqlModeMSSQL, isMariaDB: true,
		want: "alter db.t{col c=int32 nn}", note: "mssql bracket idents",
	},
	{
		sql: "ALTER TABLE [db].[t] DROP COLUMN [c]", sqlMode: sqlModeMSSQL,
		want: "ERROR", note: "MODE_MSSQL lexing is MariaDB-only; on MySQL '[' stays punctuation",
	},
	{
		sql: "ALTER TABLE  .t DROP COLUMN a", sqlMode: sqlModeOracle | sqlModeANSIQuotes, isMariaDB: true,
		want: "alter t{drop a}", note: "leading-dot table name uses the current database",
	},
	// --- MariaDB sql_mode=ORACLE types ---
	{
		sql: "ALTER TABLE t MODIFY a NUMBER(10,2)", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col a=numeric(10,2)}", note: "oracle NUMBER(p,s) is decimal",
	},
	{
		sql: "ALTER TABLE t MODIFY a NUMBER(10)", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col a=numeric(10,-1)}", note: "oracle NUMBER(p) keeps scale unwritten",
	},
	{
		sql: "ALTER TABLE t MODIFY a NUMBER", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col a=float64}", note: "bare oracle NUMBER is double",
	},
	{
		sql: "ALTER TABLE t ADD c VARCHAR2(30) NOT NULL", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col c=string nn}", note: "oracle VARCHAR2",
	},
	{
		sql: "ALTER TABLE t ADD c RAW(16)", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col c=bytes}", note: "oracle RAW is varbinary",
	},
	{
		sql: "ALTER TABLE t ADD c CLOB", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col c=string}", note: "oracle CLOB is longtext",
	},
	{
		sql: "ALTER TABLE t ADD c BLOB", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{col c=bytes}", note: "lengthless oracle BLOB is longblob",
	},
	// --- executable comments ---
	{
		sql: "/*!!100500 ALTER TABLE t DROP COLUMN c */", isMariaDB: true,
		want: "", note: "reversed comment with surviving digits means the source skipped it",
	},
	{
		sql: "/*!! ALTER TABLE t DROP COLUMN c */", isMariaDB: true,
		want: "alter t{drop c}", note: "digitless reversed comment body was executed (digits patched to spaces at source)",
	},
	{
		sql: "/*M!100500 ALTER TABLE t DROP COLUMN c */", isMariaDB: true,
		want: "alter t{drop c}", note: "/*M! executes on MariaDB",
	},
	{
		sql: "/*M!!ALTER TABLE t DROP COLUMN c /* trailing */ */", isMariaDB: true,
		want: "alter t{drop c}", note: "digitless MariaDB reversed comment body was executed",
	},
	{
		sql:  "/*M!100500 ALTER TABLE t DROP COLUMN c */",
		want: "", note: "/*M! is a plain comment on MySQL",
	},
	{
		sql: "/*!40000 ALTER TABLE `t` DISABLE KEYS */", isMariaDB: true,
		want: "alter t{}", note: "mysqldump header executes on MariaDB too (40000 is below its skip range)",
	},
	{
		sql: "ALTER TABLE fixture /*!80000 ADD COLUMN n1 INT */", isMariaDB: true,
		want: "alter fixture{}", note: "MariaDB skips MySQL-style executable comments in the 50700..99999 range",
	},
	{
		sql:  "ALTER TABLE t /*!80000 ADD COLUMN c INT, */ DROP COLUMN d",
		want: "alter t{col c=int32; drop d}", note: "versioned comment body lexes as code mid-statement",
	},
	{
		sql:  "ALTER /*! TABLE t MODIFY c INT */",
		want: "alter t{col c=int32}", note: "digitless /*! body is code immediately",
	},
	// --- line comments terminate at \n or NUL only ---
	{
		sql:  "-- c\rALTER TABLE t ADD c INT",
		want: "", note: "bare CR does not end a line comment",
	},
	{
		sql:  "--\nALTER TABLE t DROP COLUMN c",
		want: "alter t{drop c}", note: "minus-minus followed by newline is a comment",
	},
	{
		sql:  "--x\nALTER TABLE t ADD c INT",
		want: "", note: "minus-minus without space is two operators: not a statement head, whole input ignored",
	},
	{
		sql:  "ALTER TABLE t ADD c INT -- done",
		want: "alter t{col c=int32}", note: "trailing line comment",
	},
	{
		sql:  "ALTER TABLE t/*c*/ADD c INT",
		want: "alter t{col c=int32}", note: "block comment as token separator",
	},
	// --- literal lexing inside skipped attributes ---
	{
		sql:  "ALTER TABLE t ADD c VARCHAR(10) DEFAULT _utf8mb4 X'4D', ADD d INT",
		want: "alter t{col c=string; col d=int32}", note: "charset introducer plus hex literal",
	},
	{
		sql:  "ALTER TABLE t ADD c INT DEFAULT 0b101, ADD d INT",
		want: "alter t{col c=int32; col d=int32}", note: "binary literal",
	},
	{
		sql:  "ALTER TABLE t ADD c DOUBLE DEFAULT 1.e5, ADD d INT",
		want: "alter t{col c=float64; col d=int32}", note: "float literal with dot-e",
	},
	{
		sql:  "ALTER TABLE t ADD $c INT",
		want: "alter t{col $c=int32}", note: "unclosed dollar tag falls back to an ordinary identifier",
	},
	{
		sql:  "ALTER TABLE t ADD c VARCHAR(20) DEFAULT $q$a'b,c$q$ NOT NULL",
		want: "alter t{col c=string nn}", note: "MySQL 9 dollar-quoted string swallows quotes and commas",
	},
	// --- benign spec surface that must be consumed correctly ---
	{
		sql:  "ALTER TABLE t ADD COLUMN c INT, WITH VALIDATION",
		want: "alter t{col c=int32}", note: "alter_commands_modifier WITH VALIDATION",
	},
	{
		sql:  "ALTER TABLE t REMOVE PARTITIONING",
		want: "alter t{}", note: "REMOVE PARTITIONING",
	},
	{
		sql:  "ALTER TABLE t PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
		want: "alter t{}", note: "PARTITION BY with embedded expressions",
	},
	{
		sql:  "ALTER TABLE t DISCARD TABLESPACE",
		want: "alter t{}", note: "standalone tablespace action",
	},
	{
		sql:  "ALTER TABLE t SECONDARY_LOAD",
		want: "alter t{}", note: "secondary engine action",
	},
	{
		sql:  "ALTER TABLE t ALTER CHECK ck NOT ENFORCED",
		want: "alter t{}", note: "ALTER CHECK",
	},
	{
		sql:  "ALTER TABLE t ALTER COLUMN c SET VISIBLE",
		want: "alter t{}", note: "ALTER COLUMN visibility",
	},
	{
		sql:  "ALTER TABLE t CONVERT TO CHARACTER SET DEFAULT",
		want: "alter t{}", note: "CONVERT TO CHARACTER SET DEFAULT",
	},
	{
		sql:  "ALTER TABLE t ADD SPATIAL (g)",
		want: "alter t{}", note: "ADD SPATIAL with optional INDEX keyword omitted",
	},
	// --- column definitions and attributes ---
	{
		sql:  "ALTER TABLE t ADD c YEAR NOT NULL",
		want: "alter t{col c=int16 nn}", note: "year maps to int16 qkind",
	},
	{
		sql:  "ALTER TABLE t ADD b BIT(8) NOT NULL",
		want: "alter t{col b=uint64 nn}", note: "bit",
	},
	{
		sql: "ALTER TABLE t ADD c INT NOT NULL ENABLE", isMariaDB: true,
		want: "alter t{col c=int32 nn}", note: "oracle-ism NOT NULL ENABLE",
	},
	{
		sql: "ALTER TABLE t WAIT 10 ADD COLUMN c INT", isMariaDB: true,
		want: "alter t{col c=int32}", note: "lock wait timeout after table name",
	},
	{
		sql: "ALTER TABLE t ADD period date, ADD PERIOD FOR p(s, e)", isMariaDB: true,
		want: "alter t{col period=date}", note: "period is a column name unless followed by FOR",
	},
	{
		sql: "ALTER TABLE t DROP period, DROP PERIOD FOR p", isMariaDB: true,
		want: "alter t{drop period}", note: "same on the DROP side",
	},
	{
		sql: "ALTER TABLE t ADD system INT, ADD SYSTEM VERSIONING", isMariaDB: true,
		want: "alter t{col system=int32}", note: "system is a column name unless followed by VERSIONING",
	},
	{
		sql:  "ALTER TABLE t ADD vector VECTOR(4) NOT NULL",
		want: "alter t{col vector=array_float32 nn}", note: "vector both as column name and type",
	},
	{
		sql: "ALTER TABLE t ADD VECTOR INDEX v (emb)", isMariaDB: true,
		want: "alter t{}", note: "vector index add is benign",
	},
	{
		sql: "/*!ALTER TABLE t ADD VECTOR  v (emb) */", isMariaDB: true,
		want: "alter t{}", note: "vector index with optional name inside executable comment",
	},
	{
		sql: "ALTER TABLE t ADD VECTOR RAW(N6)", sqlMode: sqlModeOracle, isMariaDB: true,
		want: "alter t{}", note: "vector index with optional INDEX keyword omitted",
	},
	{
		sql: "SET STATEMENT a=CAST(1 AS CHAR) FOR ALTER TABLE t DROP COLUMN c", isMariaDB: true,
		want: "alter t{drop c}", note: "SET STATEMENT value is a full expression",
	},
	{
		sql:  "ALTER TABLE t DROP COLUMN a; RENAME TABLE x TO y",
		want: "alter t{drop a} | rename x>y", note: "statements after a parsed actionable one keep being classified",
	},
	// CHAR/text + binary charset remaps; LONG VARCHAR is mediumtext, binary
	// charset then makes it mediumblob; MariaDB CHAR BYTE is BINARY.
	{
		sql:  "ALTER TABLE t ADD c LONG VARCHAR CHARACTER SET binary",
		want: "alter t{col c=bytes}", note: "LONG VARCHAR with binary charset",
	},
	{
		sql: "ALTER TABLE t ADD c CHAR BYTE", isMariaDB: true,
		want: "alter t{col c=bytes}", note: "CHAR BYTE is BINARY",
	},
	{
		sql:  "ALTER TABLE t MODIFY c VARCHAR(10) CHARSET binary",
		want: "alter t{col c=bytes}", note: "CHARSET spelling of the binary remap",
	},
	{
		sql:  "ALTER TABLE db.t CHANGE COLUMN a b DECIMAL(6,3) NOT NULL FIRST",
		want: "alter db.t{chg a b=numeric(6,3) nn @pos}", note: "qualified change with precision and position",
	},
	{
		sql:  "ALTER TABLE t ADD c SET('a','b') NOT NULL",
		want: "alter t{col c=string nn}", note: "set type",
	},
	{
		sql:  "ALTER TABLE t ADD c ENUM('x') NOT NULL DEFAULT 'x'",
		want: "alter t{col c=enum nn}", note: "enum type",
	},
	{
		sql:  "ALTER TABLE t ADD c JSON NOT NULL",
		want: "alter t{col c=json nn}", note: "json type",
	},
	{
		sql: "ALTER TABLE t ADD c JSON NOT NULL", isMariaDB: true,
		want: "alter t{col c=string nn}",
		note: "MariaDB JSON is a LONGTEXT alias (MariaDB KB, JSON Data Type)",
	},
}

var tidbDiffCorpus = []tidbDiffCase{
	{
		sql: "SET STATEMENT max_statement_time=60 FOR INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1782" +
			"476058116) ON DUPLICATE KEY UPDATE value = 1782476058116",
		engine: "mariadb", note: "query_parser_test.go -- RDS heartbeat, dominant prod noise, benign",
	},
	{
		sql: "SET STATEMENT max_statement_time=60 FOR DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key" +
			"'",
		engine: "mariadb", note: "query_parser_test.go -- RDS sysinfo delete, benign",
	},
	{
		sql:    "SET STATEMENT max_statement_time=60 FOR flush table",
		engine: "mariadb", note: "query_parser_test.go -- SET STATEMENT for flush, benign",
	},
	{
		sql:    "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD INDEX idx (a)",
		engine: "mariadb", note: "query_parser_test.go -- wrapped index-only alter, benign",
	},
	{
		sql:    "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD COLUMN c INT",
		engine: "mariadb", note: "query_parser_test.go -- wrapped column alter, actionable",
	},
	{
		sql:    "SET STATEMENT max_statement_time=60, sort_buffer_size=1000000 FOR ALTER TABLE t MODIFY c INT",
		engine: "mariadb", note: "query_parser_test.go -- multiple vars before FOR, actionable",
	},
	{
		sql:    "SET STATEMENT max_statement_time=60 FOR RENAME TABLE a TO b",
		engine: "mariadb", note: "query_parser_test.go -- wrapped rename, actionable",
	},
	{
		sql:    "SET STATEMENT sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES') FOR ALTER TABLE t ADD c INT",
		engine: "mariadb", note: "mariadb-grammar.md -- full expression value with parens+comma before FOR",
	},
	{
		sql:    "SET STATEMENT lc_messages='en FOR us' FOR ALTER TABLE t DROP COLUMN c",
		engine: "mariadb", note: "string value containing the word FOR; FOR keyword must be found quote-aware at depth 0",
	},
	{
		sql:    "RENAME TABLE `users` TO `_users_del`, `_users_gho` TO `users`",
		engine: "both", note: "gh-ost cut-over rename (query_parser_test.go)",
	},
	{
		sql:    "RENAME TABLE `mydb`.`mytable` TO `mydb`.`_mytable_old`, `mydb`.`_mytable_new` TO `mydb`.`mytable`",
		engine: "both", note: "pt-online-schema-change cut-over rename, schema-qualified",
	},
	{
		sql:    "ALTER TABLE `mydb`.`_mytable_gho` ADD COLUMN new_col INT NOT NULL DEFAULT 0",
		engine: "both", note: "gh-ost applying user alter on ghost table",
	},
	{
		sql:    "/*!40000 ALTER TABLE `t1` DISABLE KEYS */",
		engine: "both", note: "mysqldump wrapper: executable comment around index-only ALTER, benign after unwrap",
	},
	{
		sql:    "/*!40000 ALTER TABLE `t1` ENABLE KEYS */",
		engine: "both", note: "mysqldump wrapper, benign",
	},
	{
		sql:    "/*!40101 SET NAMES utf8mb4 */",
		engine: "both", note: "mysqldump SET wrapper, benign",
	},
	{
		sql:    "/*!50503 SET NAMES utf8mb4 */",
		engine: "both", note: "mysqldump SET wrapper, benign",
	},
	{
		sql:    "ALTER TABLE db.t\n  /*! ADD COLUMN mysql_only INT, */\n  DROP COLUMN old_col",
		engine: "both", note: "query_parser_test.go -- digitless /*! body is code on both engines",
	},
	{
		sql:    "ALTER /*! TABLE db.t MODIFY c INT */",
		engine: "both", note: "query_parser_test.go -- executable comment hides the TABLE keyword",
	},
	{
		sql:    "ALTER TABLE t /*!80000 ADD COLUMN c INT */",
		engine: "mysql", note: "query_parser_test.go -- 5-digit versioned executable comment, body is code",
	},
	{
		sql:    "ALTER TABLE t /*M! MODIFY c INT */",
		engine: "mariadb", note: "query_parser_test.go -- /*M! body is code on MariaDB",
	},
	{
		sql:    "ALTER TABLE t /*M! MODIFY c INT */ ADD INDEX idx (a)",
		engine: "mysql", note: "query_parser_test.go -- /*M! is a plain comment on MySQL, index-only => benign",
	},
	{
		sql:    "ALTER TABLE t /*M!100500 ADD COLUMN c INT */",
		engine: "mariadb", note: "6-digit versioned /*M! comment, body is code",
	},
	{
		sql:    "ALTER TABLE t /*!80000 ADD c VARCHAR(10) DEFAULT 'x*/y' */",
		engine: "mysql", note: "string containing */ inside an executable comment body must not close the comment",
	},
	{
		sql:    "ALTER TABLE t ADD c INT /*!80000 , ADD d INT */",
		engine: "mysql", note: "trailing spec hidden in versioned executable comment",
	},
	{
		sql:    "/*+ looks like a hint */ ALTER TABLE t ADD c INT",
		engine: "both", note: "lexer-and-binlog.md -- /*+ is a plain comment outside hint position",
	},
	{
		sql:    "/* abc-123 */ ALTER TABLE `db`.`t` ADD COLUMN c INT",
		engine: "both", note: "query_parser_test.go -- block comment prefix",
	},
	{
		sql:    "-- migration\nALTER TABLE t ADD COLUMN c INT",
		engine: "both", note: "query_parser_test.go -- line comment prefix",
	},
	{
		sql:    "# note\nALTER TABLE t ADD COLUMN c INT",
		engine: "both", note: "query_parser_test.go -- hash comment prefix",
	},
	{
		sql:    "-- migration\rALTER TABLE t ADD COLUMN c INT",
		engine: "both", note: "bare \\r does NOT terminate a line comment (server-exact); " +
			"whole input is comment => benign; expectation deliberately flipped vs old classifier",
	},
	{
		sql:    "\n\t\f\u000b  ALTER TABLE t ADD COLUMN c INT",
		engine: "both", note: "query_parser_test.go -- every whitespace kind",
	},
	{
		sql:    "ALTER TABLE t\n-- middle\nADD c INT",
		engine: "both", note: "line comment between specs",
	},
	{
		sql:    "ALTER TABLE t ADD c INT -- trailing comment",
		engine: "both", note: "trailing line comment without newline",
	},
	{
		sql:    "--\nALTER TABLE t ADD c INT",
		engine: "both", note: "'--' followed directly by newline is a line comment",
	},
	{
		sql:    "ALTER TABLE `we``ird` ADD `co``l` INT",
		engine: "both", note: "doubled backticks inside quoted idents",
	},
	{
		sql:    "ALTER TABLE \"db\".\"t\" ADD COLUMN \"c\" INT",
		engine: "both", note: "requires sql_mode ANSI_QUOTES: double quotes are identifiers",
	},
	{
		sql:    "ALTER TABLE [db].[t] DROP COLUMN [c]",
		engine: "mariadb", note: "requires sql_mode MSSQL (MariaDB-only): bracket-delimited identifiers",
	},
	{
		sql:    "ALTER TABLE 1ea10 ADD c INT",
		engine: "both", note: "identifier starting with digits ('1e' lexing trap)",
	},
	{
		sql:    "ALTER TABLE db.1234 ADD c INT",
		engine: "both", note: "all-digit identifier is legal after '.'",
	},
	{
		sql:    "ALTER TABLE t\u00ebst ADD c\u00f6lumn_\u00f1 INT",
		engine: "both", note: "bytes >= 0x80 pass through as identifier chars",
	},
	{
		sql:    "",
		engine: "both", note: "empty input, benign",
	},
	{
		sql:    "/* nothing here */",
		engine: "both", note: "query_parser_test.go -- comment-only input, benign",
	},
	{
		sql:    "ALTER TABLE t ADD c INT;",
		engine: "both", note: "trailing semicolon after actionable statement",
	},
	{
		sql:    "ALTER TABLE t ADD c INT ; ALTER TABLE t2 DROP COLUMN d",
		engine: "both", note: "two actionable statements in one QueryEvent; remainder must be classified",
	},
	{
		sql:    "ALTER TABLE t DROP COLUMN c\u0000\u0000",
		engine: "both", note: "trailing NUL bytes are trimmed",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(20) DEFAULT 'it''s'",
		engine: "both", note: "doubled-quote escape in string",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(20) DEFAULT 'a\\'b'",
		engine: "both", note: "backslash escape; only valid when NO_BACKSLASH_ESCAPES is off",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(20) DEFAULT _utf8mb4'h\u00e9llo'",
		engine: "both", note: "charset introducer before string",
	},
	{
		sql:    "ALTER TABLE t ADD c VARBINARY(16) DEFAULT X'DEADBEEF'",
		engine: "both", note: "hex string literal default",
	},
	{
		sql:    "ALTER TABLE t ADD c BIT(8) DEFAULT b'1010'",
		engine: "both", note: "bit string literal default",
	},
	{
		sql:    "ALTER TABLE t ADD c INT DEFAULT 0x41",
		engine: "both", note: "0x hex number default",
	},
	{
		sql:    "ALTER TABLE t ADD c DOUBLE DEFAULT 1e5",
		engine: "both", note: "exponent float literal",
	},
	{
		sql:    "ALTER TABLE t ADD c DOUBLE DEFAULT .5",
		engine: "both", note: "leading-dot float literal",
	},
	{
		sql:    "ALTER TABLE t ADD c DOUBLE DEFAULT -1.5e-10",
		engine: "both", note: "signed float with exponent",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(10) COMMENT 'contains */ and -- inside'",
		engine: "both", note: "comment markers inside a string literal",
	},
	{
		sql: "ALTER TABLE t ADD c TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP" +
			"(6)",
		engine: "both", note: "now(n) default + ON UPDATE now(n)",
	},
	{
		sql:    "ALTER TABLE t ADD c INT DEFAULT (1+1)",
		engine: "both", note: "parenthesized expression default",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(36) DEFAULT (uuid())",
		engine: "both", note: "function call expression default",
	},
	{
		sql:    "ALTER TABLE t ADD n NVARCHAR(10) DEFAULT N'abc'",
		engine: "both", note: "N'...' national string literal",
	},
	{
		sql:    "ALTER TABLE t ADD c TEXT COMMENT $$migration$$",
		engine: "mysql", note: "MySQL 9.0+ dollar-quoted string; not valid on MariaDB",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT1",
		engine: "both", note: "int1 -> tinyint",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT2 UNSIGNED ZEROFILL",
		engine: "both", note: "int2 -> smallint; zerofill implies unsigned",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT3",
		engine: "both", note: "int3 -> mediumint",
	},
	{
		sql:    "ALTER TABLE t MODIFY c MIDDLEINT",
		engine: "both", note: "middleint -> mediumint",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT4 SIGNED",
		engine: "both", note: "int4 -> int; SIGNED attribute",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT8 ZEROFILL",
		engine: "both", note: "int8 -> bigint",
	},
	{
		sql:    "ALTER TABLE t ADD c DEC(10,2)",
		engine: "both", note: "dec -> decimal, precision+scale",
	},
	{
		sql:    "ALTER TABLE t ADD c FIXED(10,2)",
		engine: "both", note: "fixed -> decimal",
	},
	{
		sql:    "ALTER TABLE t ADD c NUMERIC(10)",
		engine: "both", note: "numeric with precision only",
	},
	{
		sql:    "ALTER TABLE t ADD c DECIMAL UNSIGNED",
		engine: "both", note: "decimal without params + unsigned",
	},
	{
		sql:    "ALTER TABLE t ADD c DECIMAL(65,30)",
		engine: "both", note: "max decimal params",
	},
	{
		sql:    "ALTER TABLE t ADD c FLOAT4",
		engine: "both", note: "float4 -> float",
	},
	{
		sql:    "ALTER TABLE t ADD c FLOAT8",
		engine: "both", note: "float8 -> double",
	},
	{
		sql:    "ALTER TABLE t ADD c DOUBLE PRECISION",
		engine: "both", note: "two-word type, no params",
	},
	{
		sql:    "ALTER TABLE t ADD flag BOOLEAN NOT NULL DEFAULT TRUE",
		engine: "both", note: "boolean -> tinyint(1)",
	},
	{
		sql:    "ALTER TABLE t ADD flag BOOL",
		engine: "both", note: "bool -> tinyint(1)",
	},
	{
		sql:    "ALTER TABLE t ADD id SERIAL",
		engine: "both", note: "serial -> bigint unsigned + NotNull",
	},
	{
		sql:    "ALTER TABLE t ADD c NCHAR(5)",
		engine: "both", note: "nchar -> char",
	},
	{
		sql:    "ALTER TABLE t ADD c NVARCHAR(10)",
		engine: "both", note: "nvarchar -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c NATIONAL CHAR(5)",
		engine: "both", note: "national char -> char",
	},
	{
		sql:    "ALTER TABLE t ADD c NATIONAL CHAR VARYING(10)",
		engine: "both", note: "three-word type -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c NATIONAL VARCHAR(10)",
		engine: "both", note: "national varchar -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c NCHAR VARYING(10)",
		engine: "both", note: "nchar varying -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c CHAR VARYING(10)",
		engine: "both", note: "char varying -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c CHARACTER(5)",
		engine: "both", note: "character -> char",
	},
	{
		sql:    "ALTER TABLE t ADD c CHARACTER VARYING(10)",
		engine: "both", note: "character varying -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHARACTER(10)",
		engine: "both", note: "varcharacter -> varchar",
	},
	{
		sql:    "ALTER TABLE t ADD c CHAR(8) BYTE",
		engine: "both", note: "char byte -> binary",
	},
	{
		sql:    "ALTER TABLE t ADD c LONG",
		engine: "both", note: "bare LONG -> mediumtext",
	},
	{
		sql:    "ALTER TABLE t ADD c LONG VARCHAR",
		engine: "both", note: "long varchar -> mediumtext",
	},
	{
		sql:    "ALTER TABLE t ADD c LONG VARBINARY",
		engine: "both", note: "long varbinary -> mediumblob",
	},
	{
		sql:    "ALTER TABLE t ADD c CLOB",
		engine: "mariadb", note: "clob -> longtext (MariaDB keyword)",
	},
	{
		sql:    "ALTER TABLE t ADD c TINYINT(1)",
		engine: "both", note: "tinyint(1) must survive exactly (bool detection)",
	},
	{
		sql:    "ALTER TABLE t ADD c TINYINT(1) UNSIGNED",
		engine: "both", note: "tinyint(1) unsigned",
	},
	{
		sql:    "ALTER TABLE t ADD y YEAR(4)",
		engine: "both", note: "year with display width",
	},
	{
		sql:    "ALTER TABLE t ADD j JSON",
		engine: "mysql", note: "json type; MariaDB is covered by the LONGTEXT-alias mode case",
	},
	{
		sql:    "ALTER TABLE t ADD g GEOMCOLLECTION",
		engine: "mysql", note: "geomcollection (MySQL-only spelling)",
	},
	{
		sql:    "ALTER TABLE t ADD g GEOMETRYCOLLECTION",
		engine: "both", note: "geometrycollection",
	},
	{
		sql:    "ALTER TABLE t ADD g GEOMETRY REF_SYSTEM_ID=4326",
		engine: "mariadb", note: "REF_SYSTEM_ID type suffix",
	},
	{
		sql:    "ALTER TABLE t1 MODIFY a NUMBER(10,2)",
		engine: "mariadb", note: "sql_mode=ORACLE: number(p,s) -> decimal",
	},
	{
		sql:    "ALTER TABLE t1 MODIFY a NUMBER",
		engine: "mariadb", note: "sql_mode=ORACLE: bare number -> double",
	},
	{
		sql:    "ALTER TABLE t1 ADD c VARCHAR2(10)",
		engine: "mariadb", note: "sql_mode=ORACLE: varchar2 -> varchar",
	},
	{
		sql:    "ALTER TABLE t1 ADD c RAW(16)",
		engine: "mariadb", note: "sql_mode=ORACLE: raw -> varbinary",
	},
	{
		sql:    "ALTER TABLE t1 ADD c BLOB",
		engine: "both", note: "lengthless blob (-> longblob under ORACLE mode)",
	},
	{
		sql:    "ALTER TABLE t ADD c ENUM('a','b') CHARACTER SET binary",
		engine: "both", note: "enum with binary charset",
	},
	{
		sql:    "ALTER TABLE t MODIFY c TEXT CHARACTER SET binary",
		engine: "both", note: "CHARACTER SET binary remaps text -> blob",
	},
	{
		sql:    "ALTER TABLE t ADD period INT",
		engine: "both", note: "column named period (PERIOD FOR needs 2nd-word peek)",
	},
	{
		sql:    "ALTER TABLE t DROP period",
		engine: "both", note: "drop column named period",
	},
	{
		sql:    "ALTER TABLE t ADD vector INT",
		engine: "both", note: "column named vector (VECTOR INDEX needs 2nd-word peek)",
	},
	{
		sql:    "ALTER TABLE t DROP vector",
		engine: "both", note: "drop column named vector",
	},
	{
		sql:    "ALTER TABLE t ADD system INT",
		engine: "both", note: "column named system (SYSTEM VERSIONING needs 2nd-word peek)",
	},
	{
		sql:    "ALTER TABLE t DROP system",
		engine: "both", note: "drop column named system",
	},
	{
		sql:    "ALTER TABLE t ADD c INT AFTER first",
		engine: "both", note: "AFTER's argument may be a keyword-ish word",
	},
	{
		sql:    "ALTER TABLE t ADD `first` INT AFTER `after`",
		engine: "both", note: "quoted keyword idents in placement",
	},
	{
		sql:    "ALTER TABLE t RENAME = t2",
		engine: "both", note: "opt_to accepts bare '='",
	},
	{
		sql:    "RENAME TABLE a WAIT 5 TO b, c NOWAIT TO d",
		engine: "mariadb", note: "mariadb-grammar.md -- WAIT/NOWAIT is per rename pair",
	},
	{
		sql:    "ALTER TABLE t ADD c INT COLUMN_FORMAT DYNAMIC STORAGE DISK",
		engine: "mysql", note: "NDB column attributes, parsed by mainline server",
	},
	{
		sql:    "ALTER TABLE t ADD c INT INVISIBLE",
		engine: "both", note: "invisible column",
	},
	{
		sql:    "ALTER TABLE t ADD c INT VISIBLE",
		engine: "mysql", note: "VISIBLE column attribute (MySQL spelling)",
	},
	{
		sql:    "ALTER TABLE t ADD c INT ENGINE_ATTRIBUTE='{\"k\":1}' SECONDARY_ENGINE_ATTRIBUTE='{}'",
		engine: "mysql", note: "engine attribute JSON strings",
	},
	{
		sql:    "ALTER TABLE t ADD c INT CHECK (c > 0) NOT ENFORCED",
		engine: "mysql", note: "column check + NOT ENFORCED",
	},
	{
		sql:    "ALTER TABLE t ADD c VARCHAR(10) COLLATE utf8mb4_bin UNIQUE KEY COMMENT 'x'",
		engine: "both", note: "collate + unique key + comment attribute run",
	},
	{
		sql:    "ALTER TABLE t MODIFY c INT SERIAL DEFAULT VALUE",
		engine: "both", note: "SERIAL DEFAULT VALUE attribute",
	},
	{
		sql:    "ALTER TABLE t ADD c BLOB COMPRESSED",
		engine: "mariadb", note: "compressed column",
	},
	{
		sql:    "ALTER TABLE t ADD c TEXT COMPRESSED=zlib",
		engine: "mariadb", note: "compressed=method column",
	},
	{
		sql:    "ALTER TABLE t ADD b INT AS (a*2) PERSISTENT",
		engine: "mariadb", note: "PERSISTENT gencol (maria spelling of STORED)",
	},
	{
		sql:    "ALTER TABLE t ADD r INT REFERENCES other (id) MATCH FULL ON DELETE CASCADE ON UPDATE RESTRICT",
		engine: "both", note: "column-level REFERENCES with match + referential actions",
	},
	{
		sql:    "ALTER ONLINE TABLE t ADD COLUMN c INT",
		engine: "mariadb", note: "ALTER ONLINE TABLE head",
	},
	{
		sql:    "ALTER ONLINE IGNORE TABLE t ADD COLUMN c INT",
		engine: "mariadb", note: "both head options together",
	},
	{
		sql:    "XA START X'30623263663564642d616630322d34'",
		engine: "both", note: "query_parser_test.go -- XA control",
	},
	{
		sql:    "XA END X'30623263663564642d616630322d34'",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "XA COMMIT X'30623263663564642d616630322d34'",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql: "CREATE DEFINER=`admin`@`%` PROCEDURE `check_daily_stats_date_continuity`()\nBEGIN\n  DECLARE done " +
			"INT;\n  CREATE TABLE tmp(x INT);\nEND",
		engine: "both", note: "query_parser_test.go -- routine body with internal semicolons; must not split on ';'",
	},
	{
		sql:    "CREATE PROCEDURE sp_x() BEGIN END",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "DROP PROCEDURE IF EXISTS sp_x",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "CREATE DEFINER=`root`@`%` TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @x=1",
		engine: "both", note: "query_parser_test.go -- trigger with @var",
	},
	{
		sql:    "CREATE EVENT nexon_program ON SCHEDULE EVERY 1 DAY DO BEGIN END",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "ALTER EVENT nexon_program\nDO\nBEGIN\n  UPDATE nexon.employee SET x=1;\nEND",
		engine: "both", note: "query_parser_test.go -- ALTER head that is not ALTER TABLE, with body semicolons",
	},
	{
		sql:    "CREATE ALGORITHM=UNDEFINED DEFINER=`API`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT 1",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "ALTER ALGORITHM=MERGE VIEW v AS SELECT 1",
		engine: "both", note: "ALTER second-token dispatch: ALGORITHM= is a view",
	},
	{
		sql:    "ALTER DEFINER=`u`@`%` VIEW v AS SELECT 1",
		engine: "both", note: "ALTER DEFINER= head is a view",
	},
	{
		sql:    "ALTER USER 'ipsadmin'@'%' DISCARD OLD PASSWORD",
		engine: "mysql", note: "query_parser_test.go",
	},
	{
		sql:    "RENAME USER 'old'@'%' TO 'new'@'%'",
		engine: "both", note: "query_parser_test.go -- RENAME non-table head",
	},
	{
		sql:    "GRANT SELECT ON db.* TO 'u'@'%'",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "REVOKE SELECT ON db.* FROM 'u'@'%'",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "CREATE TABLE `formiik`.`t` (\n  `form_id` varchar(60) DEFAULT NULL\n)",
		engine: "both", note: "query_parser_test.go -- CREATE TABLE is not handled",
	},
	{
		sql:    "DROP TABLE IF EXISTS `t`",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "ALTER DATABASE db CHARACTER SET utf8mb4",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "ALTER SCHEMA db DEFAULT COLLATE utf8mb4_bin",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "CREATE INDEX idx ON t (a)",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "DROP INDEX idx ON t",
		engine: "both", note: "query_parser_test.go",
	},
	{
		sql:    "ALTER TABLESPACE ts ADD DATAFILE 'f'",
		engine: "mysql", note: "query_parser_test.go",
	},
	{
		sql:    "CREATE PROCEDURE p() BEGIN CREATE TABLE z(x INT); END",
		engine: "both", note: "query_parser_test.go -- CREATE TABLE inside routine body cannot fool head classification",
	},
	{
		sql:    "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1",
		engine: "both", note: "stored function",
	},
	{
		sql:    "CREATE SEQUENCE s START WITH 1",
		engine: "mariadb", note: "sequence DDL",
	},
	{
		sql:    "BEGIN",
		engine: "both", note: "transaction control QueryEvent",
	},
	{
		sql:    "COMMIT\u0000",
		engine: "both", note: "cdc_test.go -- trailing NUL on benign statement",
	},
	{
		sql:    "SET autocommit=1",
		engine: "both", note: "query_parser_test.go -- plain SET",
	},
	{
		sql:    "TRUNCATE TABLE t1",
		engine: "both", note: "table maintenance, not handled",
	},
	{
		sql:    "ANALYZE TABLE t1",
		engine: "both", note: "not handled",
	},
	{
		sql:    "OPTIMIZE TABLE t1",
		engine: "both", note: "not handled",
	},
	{
		sql:    "REPAIR TABLE t1",
		engine: "both", note: "not handled",
	},
	{
		sql:    "FLUSH TABLES",
		engine: "both", note: "not handled",
	},
	{
		sql:    "DROP TRIGGER IF EXISTS trg",
		engine: "both", note: "not handled",
	},
	{
		sql:    "DROP DATABASE olddb",
		engine: "both", note: "not handled",
	},
	{
		sql:    "SAVEPOINT sp1",
		engine: "both", note: "not handled",
	},
	{
		sql:    "INSERT INTO t VALUES (1, 'ALTER TABLE fake ADD c INT')",
		engine: "both", note: "actionable-looking text inside a string literal of a benign statement",
	},
	{
		sql:    "UPDATE t SET note = 'RENAME TABLE a TO b' WHERE id = 1",
		engine: "both", note: "rename-looking text inside a string literal",
	},
}
