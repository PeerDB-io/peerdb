package gen

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
)

type entry struct {
	ID         string
	W          int
	MariaOnly  bool
	MySQLOnly  bool
	ModeReq    uint64
	ModeForbid uint64
	Emit       func(*Ctx) string
}

func pick(c *Ctx, table []entry) (entry, bool) {
	total := 0
	for _, e := range table {
		if entryOK(c, e) {
			w := e.W
			if w <= 0 {
				w = 1
			}
			total += w
		}
	}
	if total == 0 {
		return entry{}, false
	}
	n := c.R.IntN(total)
	for _, e := range table {
		if !entryOK(c, e) {
			continue
		}
		w := e.W
		if w <= 0 {
			w = 1
		}
		if n < w {
			return e, true
		}
		n -= w
	}
	return entry{}, false
}

func entryOK(c *Ctx, e entry) bool {
	if e.MariaOnly && !c.IsMariaDB || e.MySQLOnly && c.IsMariaDB {
		return false
	}
	return c.Mode&e.ModeReq == e.ModeReq && c.Mode&e.ModeForbid == 0
}

func allEntries() []entry {
	var out []entry
	for _, table := range [][]entry{alterHeads, alterSpecs, typeEntries, columnAttrs, renameForms, benignHeads, lexEntries} {
		out = append(out, table...)
	}
	return out
}

var alterHeads = []entry{
	{"head.alter_table", 20, false, false, 0, 0, func(*Ctx) string { return "ALTER TABLE" }},
	{"head.alter_online", 2, true, false, 0, 0, func(*Ctx) string { return "ALTER ONLINE TABLE" }},
	{"head.alter_ignore", 2, false, false, 0, 0, func(*Ctx) string { return "ALTER IGNORE TABLE" }},
	{"head.alter_online_ignore", 1, true, false, 0, 0, func(*Ctx) string { return "ALTER ONLINE IGNORE TABLE" }},
	{"head.if_exists", 2, true, false, 0, 0, func(*Ctx) string { return "ALTER TABLE IF EXISTS" }},
	{"head.wait_nowait", 1, true, false, 0, 0, func(c *Ctx) string {
		c.tableSuffix = " " + pickString(c.R, []string{"WAIT 5", "NOWAIT"})
		return "ALTER TABLE"
	}},
	{"head.schema_qualified", 2, false, false, 0, 0, func(c *Ctx) string {
		c.V.Table = "db." + pickString(c.R, []string{"t", "1234", "orders"})
		return "ALTER TABLE"
	}},
}

var alterSpecs = []entry{
	{"spec.add_column", 18, false, false, 0, 0, func(c *Ctx) string {
		pos := pickString(c.R, []string{"", " FIRST", " AFTER " + quoteMaybe(c, pickString(c.R, c.V.Columns))})
		return "ADD " + maybe(c, "COLUMN ") + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + genType(c) + genAttrs(c) + pos
	}},
	{"spec.add_column_if_not_exists", 4, true, false, 0, 0, func(c *Ctx) string {
		return "ADD COLUMN IF NOT EXISTS " + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + genType(c) + genAttrs(c)
	}},
	{"spec.add_column_list", 6, false, false, 0, 0, func(c *Ctx) string {
		return "ADD (" + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + genType(c) + ", " + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + genType(c) + ", INDEX (" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + "))"
	}},
	{"spec.modify_column", 10, false, false, 0, 0, func(c *Ctx) string {
		return "MODIFY " + maybe(c, "COLUMN ") + maybeMaria(c, "IF EXISTS ") + quoteMaybe(c, pickString(c.R, c.V.Columns)) + " " + genType(c) + genAttrs(c) + position(c)
	}},
	{"spec.change_column", 10, false, false, 0, 0, func(c *Ctx) string {
		return "CHANGE " + maybe(c, "COLUMN ") + maybeMaria(c, "IF EXISTS ") + quoteMaybe(c, pickString(c.R, c.V.Columns)) + " " + quoteMaybe(c, pickString(c.R, c.V.FreshNames)) + " " + genType(c) + genAttrs(c) + position(c)
	}},
	{"spec.drop_column", 6, false, false, 0, 0, func(c *Ctx) string {
		return "DROP " + maybe(c, "COLUMN ") + maybeMaria(c, "IF EXISTS ") + quoteMaybe(c, pickString(c.R, c.V.Columns)) + pickString(c.R, []string{"", " RESTRICT", " CASCADE"})
	}},
	{"spec.rename_column", 4, false, false, 0, 0, func(c *Ctx) string {
		return "RENAME COLUMN " + maybeMaria(c, "IF EXISTS ") + quoteMaybe(c, pickString(c.R, c.V.Columns)) + " TO " + quoteMaybe(c, pickString(c.R, c.V.FreshNames))
	}},
	{"spec.add_index", 7, false, false, 0, 0, func(c *Ctx) string {
		return "ADD " + pickString(c.R, []string{"INDEX", "KEY"}) + " " + maybeMaria(c, "IF NOT EXISTS ") + quoteMaybe(c, "idx_"+bareIdentifier(pickString(c.R, c.V.Columns))) + " " + pickString(c.R, []string{"", "BTREE ", "HASH "}) + "(" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ")" + indexOption(c)
	}},
	{"spec.add_fulltext_spatial", 3, false, false, 0, 0, func(c *Ctx) string {
		return "ADD " + pickString(c.R, []string{"FULLTEXT", "SPATIAL"}) + " " + pickString(c.R, []string{"INDEX", "KEY"}) + " ft (" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ")"
	}},
	{"spec.add_vector_index", 2, true, false, 0, 0, func(c *Ctx) string {
		return "ADD VECTOR INDEX vi (" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ")"
	}},
	{"spec.add_primary", 3, false, false, 0, 0, func(c *Ctx) string {
		return "ADD " + maybeConstraint(c) + "PRIMARY KEY (" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ")"
	}},
	{"spec.add_unique", 3, false, false, 0, 0, func(c *Ctx) string {
		return "ADD " + maybeConstraint(c) + "UNIQUE KEY uk (" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ")"
	}},
	{"spec.add_foreign_key", 5, false, false, 0, 0, func(c *Ctx) string {
		col := quoteMaybe(c, pickString(c.R, c.V.Columns))
		return "ADD " + maybeConstraint(c) + "FOREIGN KEY fk (" + col + ") REFERENCES " + quoteMaybe(c, "parent") + "(" + col + ") MATCH FULL ON DELETE CASCADE ON UPDATE SET NULL"
	}},
	{"spec.add_check", 5, false, false, 0, 0, func(c *Ctx) string {
		return "ADD " + maybeConstraint(c) + "CHECK (" + genExpr(c, 0) + ") " + pickString(c.R, []string{"", "ENFORCED", "NOT ENFORCED"})
	}},
	{"spec.add_period", 3, true, false, 0, 0, func(c *Ctx) string {
		return "ADD PERIOD FOR " + pickString(c.R, []string{"SYSTEM_TIME", "p"}) + " (s,e)"
	}},
	{"spec.system_versioning", 2, true, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"ADD SYSTEM VERSIONING", "DROP SYSTEM VERSIONING"})
	}},
	{"spec.alter_default", 6, false, false, 0, 0, func(c *Ctx) string {
		return "ALTER COLUMN " + quoteMaybe(c, pickString(c.R, c.V.Columns)) + " " + pickString(c.R, []string{"SET DEFAULT " + literal(c), "SET DEFAULT (" + genExpr(c, 0) + ")", "DROP DEFAULT"})
	}},
	{"spec.alter_visibility", 3, false, false, 0, 0, func(c *Ctx) string {
		return "ALTER COLUMN " + quoteMaybe(c, pickString(c.R, c.V.Columns)) + " SET " + pickString(c.R, []string{"VISIBLE", "INVISIBLE"})
	}},
	{"spec.alter_index_visibility", 3, false, false, 0, 0, func(c *Ctx) string {
		return "ALTER INDEX idx " + pickString(c.R, []string{"VISIBLE", "INVISIBLE", "NOT IGNORED", "IGNORED"})
	}},
	{"spec.alter_check", 2, false, false, 0, 0, func(c *Ctx) string {
		return "ALTER " + pickString(c.R, []string{"CHECK", "CONSTRAINT"}) + " chk " + pickString(c.R, []string{"ENFORCED", "NOT ENFORCED"})
	}},
	{"spec.drop_constraint", 3, false, false, 0, 0, func(c *Ctx) string {
		return "DROP " + pickString(c.R, []string{"CHECK chk", "CONSTRAINT chk", "CONSTRAINT IF EXISTS chk"})
	}},
	{"spec.drop_index", 4, false, false, 0, 0, func(c *Ctx) string {
		return "DROP " + pickString(c.R, []string{"INDEX idx", "KEY idx", "PRIMARY KEY", "FOREIGN KEY fk", "FOREIGN KEY IF EXISTS fk"})
	}},
	{"spec.keys_toggle", 2, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"DISABLE KEYS", "ENABLE KEYS"}) }},
	{"spec.rename_table", 3, false, false, 0, 0, func(c *Ctx) string {
		return "RENAME " + pickString(c.R, []string{"TO ", "AS ", "= "}) + quoteMaybe(c, pickString(c.R, c.V.FreshNames))
	}},
	{"spec.rename_index", 2, false, false, 0, 0, func(*Ctx) string { return "RENAME INDEX old_idx TO new_idx" }},
	{"spec.order_by", 2, false, false, 0, 0, func(c *Ctx) string {
		return "ORDER BY " + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ", " + quoteMaybe(c, pickString(c.R, c.V.Columns))
	}},
	{"spec.convert_charset", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "CONVERT TO CHARACTER SET DEFAULT"})
	}},
	{"spec.default_charset", 2, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"DEFAULT CHARACTER SET = utf8mb4", "CHARACTER SET utf8mb4", "DEFAULT COLLATE = utf8mb4_bin"})
	}},
	{"spec.tablespace", 2, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"DISCARD TABLESPACE", "IMPORT TABLESPACE"}) }},
	{"spec.algorithm", 2, false, false, 0, 0, func(c *Ctx) string {
		return "ALGORITHM=" + pickString(c.R, []string{"DEFAULT", "INSTANT", "INPLACE", "NOCOPY", "COPY"})
	}},
	{"spec.lock", 2, false, false, 0, 0, func(c *Ctx) string {
		return "LOCK=" + pickString(c.R, []string{"DEFAULT", "NONE", "SHARED", "EXCLUSIVE"})
	}},
	{"spec.force", 1, false, false, 0, 0, func(*Ctx) string { return "FORCE" }},
	{"spec.validation", 1, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"WITH VALIDATION", "WITHOUT VALIDATION"}) }},
	{"spec.secondary_load", 1, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"SECONDARY_LOAD", "SECONDARY_UNLOAD"}) }},
	{"spec.table_options", 5, false, false, 0, 0, func(c *Ctx) string { return tableOptions(c) }},
	{"spec.partition_options", 5, false, false, 0, 0, func(c *Ctx) string { return partitionOp(c) }},
}

var typeEntries = []entry{
	{"type.integer", 8, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"TINYINT", "SMALLINT", "MEDIUMINT", "MIDDLEINT", "INT", "INTEGER", "INT4", "BIGINT", "INT8"}) + pickString(c.R, []string{"", " UNSIGNED", "(11)"})
	}},
	{"type.fixed", 5, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"DECIMAL", "NUMERIC", "DEC", "FIXED"}) + pickString(c.R, []string{"", "(10)", "(10,2)", " UNSIGNED"})
	}},
	{"type.float", 4, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT(10,2)"})
	}},
	{"type.bit_bool", 3, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"BIT", "BIT(8)", "BOOL", "BOOLEAN"}) }},
	{"type.serial", 2, false, false, 0, 0, func(*Ctx) string { return "SERIAL" }},
	{"type.datetime", 4, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"DATE", "TIME", "TIME(6)", "DATETIME", "DATETIME(6)", "TIMESTAMP", "TIMESTAMP(6)", "YEAR", "YEAR(4)"})
	}},
	{"type.char_text", 8, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"CHAR", "CHAR(8)", "VARCHAR(32)", "CHARACTER", "CHARACTER VARYING(12)", "VARCHARACTER(12)", "NCHAR(8)", "NVARCHAR(8)", "LONG VARCHAR", "CHAR BYTE", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"})
	}},
	{"type.binary_blob", 4, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"BINARY", "BINARY(8)", "VARBINARY(16)", "TINYBLOB", "BLOB", "BLOB(64)", "MEDIUMBLOB", "LONGBLOB"})
	}},
	{"type.enum_set", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"ENUM('a','b,c','quote''s','*/','FOR')", "SET('x','y,z','-- inside')"})
	}},
	{"type.json", 3, false, false, 0, 0, func(*Ctx) string { return "JSON" }},
	{"type.spatial", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"})
	}},
	{"type.vector", 3, false, false, 0, 0, func(c *Ctx) string { return fmt.Sprintf("VECTOR(%d)", 1+c.R.IntN(16)) }},
	{"type.mariadb_udt", 3, true, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"UUID", "INET4", "INET6"}) }},
	{"type.oracle", 3, true, false, ModeOracle, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"NUMBER", "NUMBER(10)", "NUMBER(10,2)", "VARCHAR2(30)", "RAW(16)", "CLOB"})
	}},
}

var columnAttrs = []entry{
	{"attr.nullability", 7, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"NOT NULL", "NULL", "NOT NULL ENABLE"}) }},
	{"attr.default", 7, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"DEFAULT " + literal(c), "DEFAULT (" + genExpr(c, 0) + ")", "DEFAULT CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP(6)"})
	}},
	{"attr.auto_key", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"AUTO_INCREMENT", "UNIQUE", "UNIQUE KEY", "PRIMARY KEY", "KEY"})
	}},
	{"attr.comment", 5, false, false, 0, 0, func(c *Ctx) string {
		forms := []string{"'ddlfuzz'", "'comma,value'", "'has */ marker'", "'-- line'"}
		if !c.IsMariaDB {
			forms = append(forms, "$$mysql9$$")
		}
		return "COMMENT " + pickString(c.R, forms)
	}},
	{"attr.charset_collate", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"COLLATE utf8mb4_bin", "CHARACTER SET utf8mb4", "CHARSET utf8mb4"})
	}},
	{"attr.column_format_storage", 2, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"COLUMN_FORMAT FIXED", "COLUMN_FORMAT DYNAMIC", "COLUMN_FORMAT DEFAULT", "STORAGE DISK", "STORAGE MEMORY"})
	}},
	{"attr.engine_attribute", 2, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"ENGINE_ATTRIBUTE='{\"k\":1}'", "SECONDARY_ENGINE_ATTRIBUTE='{\"k\":2}'"})
	}},
	{"attr.visibility", 3, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"VISIBLE", "INVISIBLE"}) }},
	{"attr.generated", 4, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"GENERATED ALWAYS AS (" + genExpr(c, 0) + ") VIRTUAL", "GENERATED ALWAYS AS (" + genExpr(c, 0) + ") STORED", "AS (" + genExpr(c, 0) + ")", "GENERATED ALWAYS AS (" + genExpr(c, 0) + ") PERSISTENT"})
	}},
	{"attr.check", 4, false, false, 0, 0, func(c *Ctx) string {
		return "CHECK (" + genExpr(c, 0) + ") " + pickString(c.R, []string{"", "NOT ENFORCED"})
	}},
	{"attr.references", 3, false, false, 0, 0, func(c *Ctx) string {
		return "REFERENCES other(" + quoteMaybe(c, pickString(c.R, c.V.Columns)) + ") MATCH FULL ON DELETE CASCADE ON UPDATE RESTRICT"
	}},
	{"attr.serial_default", 2, false, false, 0, 0, func(*Ctx) string { return "SERIAL DEFAULT VALUE" }},
	{"attr.compressed", 2, true, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{"COMPRESSED", "COMPRESSED=zlib"}) }},
	{"attr.ref_system_id", 1, true, false, 0, 0, func(c *Ctx) string { return fmt.Sprintf("REF_SYSTEM_ID=%d", c.R.IntN(10)) }},
	{"attr.system_versioning", 1, true, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"WITH SYSTEM VERSIONING", "WITHOUT SYSTEM VERSIONING"})
	}},
}

var renameForms = []entry{
	{"rename.table", 8, false, false, 0, 0, func(c *Ctx) string {
		return "RENAME TABLE " + quoteMaybe(c, c.V.Table) + " TO " + quoteMaybe(c, pickString(c.R, c.V.FreshNames))
	}},
	{"rename.tables", 3, false, false, 0, 0, func(c *Ctx) string {
		return "RENAME TABLES " + quoteMaybe(c, c.V.Table) + " TO " + quoteMaybe(c, pickString(c.R, c.V.FreshNames))
	}},
	{"rename.multi_pair", 4, false, false, 0, 0, func(c *Ctx) string { return "RENAME TABLE a TO b, c TO d" }},
	{"rename.schema_qualified", 3, false, false, 0, 0, func(c *Ctx) string { return "RENAME TABLE db.a TO db.b" }},
	{"rename.wait_nowait", 2, true, false, 0, 0, func(c *Ctx) string {
		return "RENAME TABLE a " + pickString(c.R, []string{"WAIT 5", "NOWAIT"}) + " TO b, c NOWAIT TO d"
	}},
}

var benignHeads = []entry{
	{"benign.create", 4, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"CREATE TABLE x (c INT)", "CREATE INDEX i ON t(c)", "CREATE VIEW v AS SELECT 1", "CREATE PROCEDURE p() SELECT 1", "CREATE FUNCTION f() RETURNS INT RETURN 1", "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET @a=1", "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SELECT 1", "CREATE SEQUENCE s"})
	}},
	{"benign.drop", 3, false, false, 0, 0, func(c *Ctx) string {
		return pickString(c.R, []string{"DROP TABLE IF EXISTS x", "DROP INDEX i ON t", "DROP VIEW IF EXISTS v", "DROP PROCEDURE IF EXISTS p", "DROP FUNCTION IF EXISTS f", "DROP TRIGGER IF EXISTS tr", "DROP EVENT IF EXISTS ev", "DROP SEQUENCE IF EXISTS s"})
	}},
	{"benign.insert", 2, false, false, 0, 0, func(*Ctx) string { return "INSERT INTO t(c) VALUES (1)" }},
	{"benign.set_statement", 2, true, false, 0, 0, func(c *Ctx) string { return genSetStatement(c, "ALTER TABLE t ADD c INT") }},
}

var lexEntries = []entry{
	{"lex.identifiers", 1, false, false, 0, 0, func(c *Ctx) string {
		return quoteMaybe(c, pickString(c.R, []string{"first", "after", "period", "system", "vector", "column", "table", "select", "db.1234", "1ea10", "tëst", "世界"}))
	}},
	{"lex.comments", 1, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, commentForms(c)) }},
	{"lex.strings", 1, false, false, 0, 0, func(c *Ctx) string { return literal(c) }},
	{"lex.whitespace", 1, false, false, 0, 0, func(c *Ctx) string { return pickString(c.R, []string{" ", "\t", "\n", "\r", "\v", "\f"}) }},
	{"lex.nul_semicolon_chain", 1, false, false, 0, 0, func(*Ctx) string { return ";\x00;" }},
}

func genType(c *Ctx) string {
	if e, ok := pick(c, typeEntries); ok {
		return e.Emit(c)
	}
	return "INT"
}

var (
	defaultTypesMySQL = sync.OnceValue(func() []string { return buildDefaultTypes(false) })
	defaultTypesMaria = sync.OnceValue(func() []string { return buildDefaultTypes(true) })
)

func defaultTypes(isMariaDB bool) []string {
	if isMariaDB {
		return defaultTypesMaria()
	}
	return defaultTypesMySQL()
}

func buildDefaultTypes(isMariaDB bool) []string {
	c := &Ctx{R: rand.New(rand.NewPCG(9, 10)), IsMariaDB: isMariaDB}
	var out []string
	for _, e := range typeEntries {
		if entryOK(c, e) && e.ModeReq == 0 {
			out = append(out, e.Emit(c))
		}
	}
	if len(out) == 0 {
		return []string{"INT"}
	}
	return out
}

func tableOptions(c *Ctx) string {
	opts := []string{
		"ENGINE=InnoDB",
		"AUTO_INCREMENT=42",
		"ROW_FORMAT=DYNAMIC",
		"COMMENT='table,comment */'",
		"COMPRESSION='zlib'",
		"STATS_PERSISTENT=DEFAULT",
		"TABLESPACE ts STORAGE DISK",
	}
	c.R.Shuffle(len(opts), func(i, j int) { opts[i], opts[j] = opts[j], opts[i] })
	return strings.Join(opts[:1+c.R.IntN(3)], " ")
}

func partitionOp(c *Ctx) string {
	return pickString(c.R, []string{
		"ADD PARTITION (PARTITION p" + fmt.Sprint(c.R.IntN(10)) + " VALUES LESS THAN (" + genExpr(c, 0) + "))",
		"DROP PARTITION p0",
		"TRUNCATE PARTITION p0",
		"ANALYZE PARTITION p0",
		"CHECK PARTITION p0",
		"OPTIMIZE PARTITION p0",
		"REBUILD PARTITION p0",
		"REORGANIZE PARTITION p0 INTO (PARTITION p1 VALUES LESS THAN (" + genExpr(c, 0) + "))",
		"REMOVE PARTITIONING",
	})
}

func indexOption(c *Ctx) string {
	return pickString(c.R, []string{"", " KEY_BLOCK_SIZE=4", " COMMENT 'idx,comment'", " VISIBLE", " INVISIBLE", " WITH PARSER ngram", " ALGORITHM=DEFAULT", " LOCK=NONE"})
}

func position(c *Ctx) string {
	return pickString(c.R, []string{"", " FIRST", " AFTER " + quoteMaybe(c, pickString(c.R, c.V.Columns))})
}

func maybe(c *Ctx, s string) string {
	if c.R.IntN(2) == 0 {
		return s
	}
	return ""
}

func maybeMaria(c *Ctx, s string) string {
	if c.IsMariaDB && c.R.IntN(2) == 0 {
		return s
	}
	return ""
}

func maybeConstraint(c *Ctx) string {
	if c.R.IntN(2) == 0 {
		return "CONSTRAINT " + quoteMaybe(c, "sym") + " "
	}
	return ""
}

func genSetStatement(c *Ctx, sql string) string {
	values := []string{"a=1", "b=" + genExpr(c, 0), "sql_mode=CONCAT(@@sql_mode,',ANSI_QUOTES')", "c=CAST(1 AS CHAR)", "`FOR`='value FOR token'"}
	n := 1 + c.R.IntN(3)
	c.R.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
	return "SET STATEMENT " + strings.Join(values[:n], ", ") + " FOR " + sql
}
