package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ddlAlterSpecsOf parses a query expected to yield exactly one *ddlAlterTable
// and returns its Specs (nil when every spec was consumed and dropped).
func ddlAlterSpecsOf(t *testing.T, query string, isMariaDB bool) []ddlAlterSpec {
	t.Helper()
	return ddlParseAlter(t, query, 0, isMariaDB).Specs
}

func ddlAltCol(name, typ string) ddlColumnDef {
	return ddlColumnDef{Name: name, TypeStr: typ, Precision: -1, Scale: -1}
}

func ddlAltColNN(name, typ string) ddlColumnDef {
	col := ddlAltCol(name, typ)
	col.NotNull = true
	return col
}

func ddlAltAddSpec(cols ...ddlColumnDef) ddlAlterSpec { return ddlAlterSpec{NewColumns: cols} }

func ddlAltAdd(cols ...ddlColumnDef) []ddlAlterSpec { return []ddlAlterSpec{ddlAltAddSpec(cols...)} }

func ddlAltAddIfNotExistsSpec(cols ...ddlColumnDef) ddlAlterSpec {
	return ddlAlterSpec{NewColumns: cols, AddIfNotExists: true}
}

func ddlAltAddIfNotExists(cols ...ddlColumnDef) []ddlAlterSpec {
	return []ddlAlterSpec{ddlAltAddIfNotExistsSpec(cols...)}
}

func ddlAltChange(oldName string, col ddlColumnDef) ddlAlterSpec {
	return ddlAlterSpec{OldColumnName: oldName, NewColumns: []ddlColumnDef{col}, ChangeColumn: true}
}

func ddlAltChangeIfExists(oldName string, col ddlColumnDef) ddlAlterSpec {
	return ddlAlterSpec{
		OldColumnName:  oldName,
		NewColumns:     []ddlColumnDef{col},
		ChangeColumn:   true,
		ChangeIfExists: true,
	}
}

func ddlAltModifyIfExists(cols ...ddlColumnDef) []ddlAlterSpec {
	return []ddlAlterSpec{{NewColumns: cols, ModifyIfExists: true}}
}

func ddlAltDrop(name string) []ddlAlterSpec { return []ddlAlterSpec{{OldColumnName: name}} }

func ddlAltDropIfExists(name string) []ddlAlterSpec {
	return []ddlAlterSpec{{OldColumnName: name, DropIfExists: true}}
}

func ddlAltRename(oldName, newName string) ddlAlterSpec {
	return ddlAlterSpec{OldColumnName: oldName, NewColumnName: newName, RenameColumn: true}
}

func ddlAltRenameIfExists(oldName, newName string) ddlAlterSpec {
	return ddlAlterSpec{
		OldColumnName:  oldName,
		NewColumnName:  newName,
		RenameColumn:   true,
		RenameIfExists: true,
	}
}

func ddlAltPos(spec ddlAlterSpec) ddlAlterSpec {
	spec.HasPosition = true
	return spec
}

// TestDDLAlterSpecBuckets walks the alter-list surface, asserting each
// alternative lands in the right bucket: want == nil means consumed-and-dropped
// (benign), otherwise the exact column-relevant specs. Every row of the
// keyword-ambiguity table is pinned here with its per-engine reading.
func TestDDLAlterSpecBuckets(t *testing.T) {
	for _, tc := range []struct {
		alter string
		maria bool
		want  []ddlAlterSpec
	}{
		// ADD index/constraint forms
		{alter: "ADD KEY k (a, b)"},
		{alter: "ADD CHECK (a > 0)"},
		{alter: "ADD CONSTRAINT ck CHECK (a > 0 AND b < 2)"},
		{alter: "ADD CONSTRAINT uq UNIQUE (a)"},
		{alter: "ADD SPATIAL INDEX sp (g)"},
		{alter: "ADD FULLTEXT KEY ft (b) WITH PARSER ngram"},
		{alter: "ADD INDEX i ((LOWER(a)), b DESC) KEY_BLOCK_SIZE=8 COMMENT 'idx' INVISIBLE"},
		{alter: "ADD (INDEX i (a))"},
		{alter: "ADD INDEX IF NOT EXISTS i (a)", maria: true},
		{alter: "ADD CONSTRAINT IF NOT EXISTS ck CHECK (a > 0)", maria: true},
		// DROP of non-columns
		{alter: "DROP FOREIGN KEY fk"},
		{alter: "DROP CHECK ck"},
		{alter: "DROP CONSTRAINT ck"},
		{alter: "DROP KEY k"},
		{alter: "DROP INDEX IF EXISTS i", maria: true},
		{alter: "DROP FOREIGN KEY IF EXISTS fk", maria: true},
		// ALTER [COLUMN|INDEX|CHECK|CONSTRAINT|KEY] items
		{alter: "ALTER COLUMN c SET DEFAULT 5"},
		{alter: "ALTER c SET DEFAULT (RAND() * 10)"},
		{alter: "ALTER COLUMN c DROP DEFAULT"},
		{alter: "ALTER COLUMN c SET VISIBLE"},
		{alter: "ALTER CHECK ck NOT ENFORCED"},
		{alter: "ALTER CONSTRAINT ck ENFORCED"},
		{alter: "ALTER KEY IF EXISTS k IGNORED", maria: true},
		{alter: "ALTER INDEX i NOT IGNORED", maria: true},
		// ambiguity #8: a non-keyword after ALTER is a column name; still benign (no type change)
		{alter: "ALTER period SET DEFAULT 5", maria: true},
		// RENAME inside the list
		{alter: "RENAME INDEX ix TO iy"},
		{alter: "RENAME KEY kx TO ky"},
		{alter: "RENAME INDEX IF EXISTS ix TO iy", maria: true},
		{alter: "RENAME COLUMN a TO b", want: []ddlAlterSpec{ddlAltRename("a", "b")}},
		{alter: "RENAME COLUMN IF EXISTS a TO b", maria: true, want: []ddlAlterSpec{ddlAltRenameIfExists("a", "b")}},
		// CONVERT TO / FORCE / ORDER BY / key toggles (#26-29, #12-13)
		{alter: "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
		{alter: "CONVERT TO CHARACTER SET DEFAULT"},
		{alter: "CHECK PARTITION p0, MODIFY  , SECONDARY_LOAD"},
		{alter: "CHECK PARTITION REPLICA, MODIFY CHANGED", maria: true},
		{alter: "REBUILD PARTITION p0, MODIFY # DECIMAL DECIMAL WITH (10,2) " +
			"GENERATED ALWAYS AS (NOW()) VIRTUAL DEFAULT 'x' COMMENT $$mysql9$$ FIRST"},
		{alter: "FORCE"},
		{alter: "ORDER BY a, b"},
		{alter: "ORDER BY after, modify"},
		{alter: "DISABLE KEYS"},
		{alter: "ENABLE KEYS"},
		// standalone_alter_commands incl. partition expressions (VALUES LESS THAN needs balanced parens)
		{alter: "DISCARD TABLESPACE"},
		{alter: "IMPORT TABLESPACE"},
		{alter: "ADD PARTITION (PARTITION p1 VALUES LESS THAN (100))"},
		{alter: "DROP PARTITION p0"},
		{alter: "TRUNCATE PARTITION ALL"},
		{alter: "COALESCE PARTITION 2"},
		{alter: "REORGANIZE PARTITION p0 INTO (PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE)"},
		{alter: "EXCHANGE PARTITION p WITH TABLE t2 WITH VALIDATION"},
		{alter: "REMOVE PARTITIONING"},
		{alter: "PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))"},
		{alter: "CONVERT PARTITION p TO TABLE t2", maria: true},
		// table options: bare, =, space-separated runs, MariaDB ident=value (IGNORE_BAD_TABLE_OPTIONS)
		{alter: "ENGINE=InnoDB ROW_FORMAT=DYNAMIC AUTO_INCREMENT=100"},
		{alter: "AUTO_INCREMENT 100"},
		{alter: "COMMENT='x, y'"},
		{alter: "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
		{alter: "UNION=(t1,t2)"},
		{alter: "transactional=1", maria: true},
		{alter: "ALGORITHM=INPLACE, LOCK=NONE"},
		// MariaDB period / system versioning / vector index specs (8115-8310, 7395)
		{alter: "ADD PERIOD FOR SYSTEM_TIME(rs, re)", maria: true},
		{alter: "ADD PERIOD IF NOT EXISTS FOR app_time(s, e)", maria: true},
		{alter: "DROP PERIOD FOR SYSTEM_TIME", maria: true},
		{alter: "DROP PERIOD IF EXISTS FOR app_time", maria: true},
		{alter: "ADD SYSTEM VERSIONING", maria: true},
		{alter: "DROP SYSTEM VERSIONING", maria: true},
		{alter: "ADD VECTOR INDEX v (emb) M=6 DISTANCE=cosine", maria: true},
		{alter: "ADD VECTOR KEY vk (emb)", maria: true},
		{alter: "ADD VECTOR v (emb)", maria: true},
		{alter: "ADD VECTOR RAW(N6)", maria: true},
		{alter: "ADD (VECTOR INDEX COLLATION (emb) )", maria: true},
		// ambiguity #3/#5: PERIOD/VECTOR after bare ADD are column names unless
		// they start a period or vector-index form.
		{alter: "ADD period date", want: ddlAltAdd(ddlAltCol("period", "date"))},
		{alter: "ADD period TIMESTAMP NOT NULL", want: ddlAltAdd(ddlAltColNN("period", "timestamp"))},
		{alter: "ADD vector INT", want: ddlAltAdd(ddlAltCol("vector", "int"))},
		{alter: "ADD vector INT", maria: true, want: ddlAltAdd(ddlAltCol("vector", "int"))},
		{alter: "ADD vector VECTOR(4)", want: ddlAltAdd(ddlAltCol("vector", "vector(4)"))},
		// ambiguity #4 note (safety doc): explicit COLUMN / IF NOT EXISTS commits to the column branch
		{alter: "ADD COLUMN period date", maria: true, want: ddlAltAdd(ddlAltCol("period", "date"))},
		{alter: "ADD IF NOT EXISTS period date", maria: true, want: ddlAltAddIfNotExists(ddlAltCol("period", "date"))},
		// column-relevant items land with types, typmods and positions intact
		{alter: "DROP COLUMN c", want: ddlAltDrop("c")},
		{alter: "DROP COLUMN IF EXISTS c RESTRICT", maria: true, want: ddlAltDropIfExists("c")},
		{
			alter: "MODIFY COLUMN c DECIMAL(10,2) NOT NULL",
			want:  ddlAltAdd(ddlColumnDef{Name: "c", TypeStr: "decimal(10,2)", Precision: 10, Scale: 2, NotNull: true}),
		},
		{
			alter: "CHANGE COLUMN old_c new_c TINYINT(1) AFTER z",
			want: []ddlAlterSpec{
				ddlAltPos(ddlAltChange("old_c", ddlColumnDef{Name: "new_c", TypeStr: "tinyint(1)", Precision: 1, Scale: -1})),
			},
		},
		{
			alter: "ADD COLUMN c VARCHAR(255) STORAGE DEFAULT AFTER b",
			want:  []ddlAlterSpec{ddlAltPos(ddlAltAddSpec(ddlAltCol("c", "varchar(255)")))},
		},
		// ambiguity #6/#7: any non-branch word after DROP is a column drop
		{alter: "DROP period", want: ddlAltDrop("period")},
		{alter: "DROP vector", want: ddlAltDrop("vector")},
		{alter: "DROP first", want: ddlAltDrop("first")},
		{alter: "DROP after", want: ddlAltDrop("after")},
		{alter: "DROP modify", want: ddlAltDrop("modify")},
		{alter: "DROP algorithm", want: ddlAltDrop("algorithm")},
		// quoted reserved words are always plain column names
		{alter: "ADD `key` INT", want: ddlAltAdd(ddlAltCol("key", "int"))},
		{alter: "DROP `index`", want: ddlAltDrop("index")},
		{
			alter: "ADD COLUMN (`(` INT UNSIGNED NOT NULL DEFAULT 0, `B` INT UNSIGNED NOT NULL DEFAULT 0)",
			want:  ddlAltAdd(ddlAltColNN("(", "int unsigned"), ddlAltColNN("B", "int unsigned")),
		},
		// MariaDB per-spec IF [NOT] EXISTS on the column-relevant items
		{
			alter: "ADD COLUMN IF NOT EXISTS (a INT, b INT)",
			maria: true,
			want:  ddlAltAddIfNotExists(ddlAltCol("a", "int"), ddlAltCol("b", "int")),
		},
		{alter: "MODIFY IF EXISTS c INT", maria: true, want: ddlAltModifyIfExists(ddlAltCol("c", "int"))},
		{alter: "MODIFY COLUMN IF EXISTS c INT", maria: true, want: ddlAltModifyIfExists(ddlAltCol("c", "int"))},
		{alter: "DROP IF EXISTS c", maria: true, want: ddlAltDropIfExists("c")},
		{
			alter: "CHANGE IF EXISTS a b INT", maria: true,
			want: []ddlAlterSpec{ddlAltChangeIfExists("a", ddlAltCol("b", "int"))},
		},
		// ADD (...) mixing columns with index/constraint/period elements keeps only the columns
		{
			alter: "ADD (a INT, KEY (a), b TINYINT, FOREIGN KEY (b) REFERENCES x (id), CONSTRAINT ck CHECK (b > 0))",
			want:  ddlAltAdd(ddlAltCol("a", "int"), ddlAltCol("b", "tinyint")),
		},
		{
			alter: "ADD COLUMN (a INT, b INT) REMOVE PARTITIONING",
			want:  ddlAltAdd(ddlAltCol("a", "int"), ddlAltCol("b", "int")),
		},
		{
			alter: "RENAME COLUMN c TO d REMOVE PARTITIONING",
			want:  []ddlAlterSpec{ddlAltRename("c", "d")},
		},
		{alter: "ADD (c INT, PERIOD FOR SYSTEM_TIME(rs, re))", maria: true, want: ddlAltAdd(ddlAltCol("c", "int"))},
	} {
		t.Run(tc.alter, func(t *testing.T) {
			specs := ddlAlterSpecsOf(t, "ALTER TABLE t "+tc.alter, tc.maria)
			if tc.want == nil {
				require.Empty(t, specs)
			} else {
				require.Equal(t, tc.want, specs)
			}
		})
	}
}

// TestDDLAlterMixedSpecOrdering pins multi-spec statements: benign specs with
// gnarly bodies (strings, nested parens, engine options) must be consumed
// exactly so the neighboring column specs survive, in source order.
func TestDDLAlterMixedSpecOrdering(t *testing.T) {
	for _, tc := range []struct {
		alter string
		maria bool
		want  []ddlAlterSpec
	}{
		{alter: "ADD INDEX i (a), ADD COLUMN c INT", want: ddlAltAdd(ddlAltCol("c", "int"))},
		{alter: "ENGINE=InnoDB, ADD COLUMN c INT", want: ddlAltAdd(ddlAltCol("c", "int"))},
		{alter: "foo=bar, ADD COLUMN c INT", maria: true, want: ddlAltAdd(ddlAltCol("c", "int"))},
		{alter: "UNION=(t1,t2), ADD COLUMN c INT", want: ddlAltAdd(ddlAltCol("c", "int"))},
		{
			alter: "ADD VECTOR INDEX v (emb) M=6 DISTANCE=cosine, ADD COLUMN c INT", maria: true,
			want: ddlAltAdd(ddlAltCol("c", "int")),
		},
		{
			alter: "ADD VECTOR v (emb), ADD COLUMN c INT", maria: true,
			want: ddlAltAdd(ddlAltCol("c", "int")),
		},
		{
			alter: "DROP a, ADD b INT, RENAME COLUMN c TO d",
			want: []ddlAlterSpec{
				{OldColumnName: "a"}, ddlAltAddSpec(ddlAltCol("b", "int")), ddlAltRename("c", "d"),
			},
		},
		{
			alter: "CHANGE `conversion` c2 VECTOR (.5) AFTER a, ALTER COLUMN `system` SET INVISIBLE",
			want:  []ddlAlterSpec{ddlAltPos(ddlAltChange("conversion", ddlAltCol("c2", "vector")))},
		},
		{
			alter: "ADD COLUMN n6 inet6 NOT NULL, RENAME COLUMN n6 TO vector2", maria: true,
			want: []ddlAlterSpec{ddlAltAddSpec(ddlAltColNN("n6", "inet6")), ddlAltRename("n6", "vector2")},
		},
		{
			alter: "ADD (`n1` MIDDLEINT, n2 BINARY, INDEX (after)), ADD COLUMN IF NOT EXISTS `n1` LINESTRING",
			maria: true,
			want: []ddlAlterSpec{
				ddlAltAddSpec(ddlAltCol("n1", "mediumint"), ddlAltCol("n2", "binary")),
				ddlAltAddIfNotExistsSpec(ddlAltCol("n1", "linestring")),
			},
		},
		{
			alter: "ADD (`世界_col` INET6, `table` FLOAT4, INDEX (after)), " +
				"ADD COLUMN IF NOT EXISTS `table` BIT(8) NOT NULL ENABLE COLLATE utf8mb4_bin " +
				"DEFAULT CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
			maria: true,
			want: []ddlAlterSpec{
				ddlAltAddSpec(ddlAltCol("世界_col", "inet6"), ddlAltCol("table", "float")),
				ddlAltAddIfNotExistsSpec(ddlAltColNN("table", "bit(8)")),
			},
		},
		{
			alter: "ADD c ENUM('a,b','c)d'), DROP f",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "enum('a,b','c)d')")), {OldColumnName: "f"}},
		},
		{
			alter: "ADD c INT DEFAULT (1 + 2), ADD d INT",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "int")), ddlAltAddSpec(ddlAltCol("d", "int"))},
		},
		{
			alter: "ADD c INT REFERENCES x (id) ON DELETE CASCADE, DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "int")), {OldColumnName: "d"}},
		},
		// NOT ENFORCED after a check must not read as NOT NULL
		{
			alter: "ADD c INT CHECK (c > 0) NOT ENFORCED, DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "int")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD c TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "timestamp(6)")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD COLUMN `table` BIT UNIQUE KEY FIRST, " +
				"ADD CONSTRAINT `sym` FOREIGN KEY fk (period) REFERENCES `parent`(period) MATCH FULL " +
				"ON DELETE CASCADE ON UPDATE SET NULL, CHANGE COLUMN after c2 DEC( 3.14159)",
			maria: true,
			want: []ddlAlterSpec{
				ddlAltPos(ddlAltAddSpec(ddlAltCol("table", "bit"))),
				ddlAltChange("after", ddlColumnDef{Name: "c2", TypeStr: "decimal(3)", Precision: 3, Scale: -1}),
			},
		},
		{
			alter: "ADD COLUMN c INT, PARTITION BY RANGE (c) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			want:  ddlAltAdd(ddlAltCol("c", "int")),
		},
		{
			alter: "MODIFY `key` INT COMMENT 'comma,value' COMMENT '-- line' NOT NULL FIRST, " +
				"RENAME COLUMN `世界` TO `世界_col`, CHANGE `key` `parameters` INTEGER " +
				"ENGINE_ATTRIBUTE='{\"k\":1}' PARTITION BY KEY ()",
			want: []ddlAlterSpec{
				ddlAltPos(ddlAltAddSpec(ddlAltColNN("key", "int"))),
				ddlAltRename("世界", "世界_col"),
				ddlAltChange("key", ddlAltCol("parameters", "int")),
			},
		},
		// MariaDB engine-defined ident=value column options
		{
			alter: "ADD c9 INT NOT NULL COMMENT 'x' foo=bar baz=2, DROP d", maria: true,
			want: []ddlAlterSpec{ddlAltAddSpec(ddlAltColNN("c9", "int")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD g GEOMETRY SRID 4326, DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("g", "geometry")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD g GEOMETRY REF_SYSTEM_ID=4326 NOT NULL, DROP d", maria: true,
			want: []ddlAlterSpec{ddlAltAddSpec(ddlAltColNN("g", "geometry")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD c TEXT COMPRESSED=zlib, DROP d", maria: true,
			want: []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "text")), {OldColumnName: "d"}},
		},
		// generated expression with nested calls and a string containing ')'
		{
			alter: "ADD c INT GENERATED ALWAYS AS (f(a, ')', b)) STORED, DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "int")), {OldColumnName: "d"}},
		},
		{
			alter: "ADD c VARCHAR(10) DEFAULT 'it''s', DROP d",
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "varchar(10)")), {OldColumnName: "d"}},
		},
		{
			alter: `ADD c VARCHAR(10) DEFAULT 'a\'b', DROP d`,
			want:  []ddlAlterSpec{ddlAltAddSpec(ddlAltCol("c", "varchar(10)")), {OldColumnName: "d"}},
		},
		// FIRST/AFTER placement is tracked per spec, not per statement
		{
			alter: "ADD a INT AFTER b, ADD COLUMN c INT FIRST, ADD d INT",
			want: []ddlAlterSpec{
				ddlAltPos(ddlAltAddSpec(ddlAltCol("a", "int"))),
				ddlAltPos(ddlAltAddSpec(ddlAltCol("c", "int"))),
				ddlAltAddSpec(ddlAltCol("d", "int")),
			},
		},
	} {
		t.Run(tc.alter, func(t *testing.T) {
			require.Equal(t, tc.want, ddlAlterSpecsOf(t, "ALTER TABLE t "+tc.alter, tc.maria))
		})
	}
}

func TestDDLAlterRenameColumnToEmptyIdentifier(t *testing.T) {
	sql := "ALTER TABLE db.orders RENAME COLUMN old_col TO \"\", " +
		"RENAME COLUMN \"system\" TO \"select\", DROP b RESTRICT, " +
		"MODIFY COLUMN \"system\" VARCHAR(32) DEFAULT 'quote''s', ADD after_col YEAR FIRST"
	stmts, err := parseQueryEvent(
		[]byte(sql),
		sqlModeANSIQuotes,
		false,
	)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	alter, ok := stmts[0].(*ddlAlterTable)
	require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
	require.Equal(t, "db", alter.Schema)
	require.Equal(t, "orders", alter.Table)
	require.Equal(t, []ddlAlterSpec{
		ddlAltRename("old_col", ""),
		ddlAltRename("system", "select"),
		{OldColumnName: "b"},
		ddlAltAddSpec(ddlAltCol("system", "varchar(32)")),
		ddlAltPos(ddlAltAddSpec(ddlAltCol("after_col", "year"))),
	}, alter.Specs)

	got, err := FuzzDDLSignature(
		[]byte(sql),
		sqlModeANSIQuotes,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, "alter db.orders{ren old_col>``; ren system>select; drop b; col system=string; col after_col=int16 @pos}", got)
}

func TestDDLAlterChangeFromEmptyIdentifier(t *testing.T) {
	sql := "ALTER TABLE db.`1234` ALTER COLUMN NEVER DROP DEFAULT, LOCK=NONE, " +
		"CHANGE COLUMN `` `table` TIMESTAMP(6), " +
		"ADD CONSTRAINT sym FOREIGN KEY fk (`system`) REFERENCES parent(`system`) MATCH FULL ON DELETE CASCADE ON UPDATE SET NULL; " +
		"RENAME TABLE db.a TO db.b; " +
		"ALTER TABLE db.`1234` RENAME COLUMN first TO `1ea10`, DISABLE KEYS, ADD CONSTRAINT sym PRIMARY KEY (b)"
	stmts, err := parseQueryEvent([]byte(sql), 1049088, false)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	alter, ok := stmts[0].(*ddlAlterTable)
	require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
	require.Equal(t, []ddlAlterSpec{
		ddlAltChange("", ddlAltCol("table", "timestamp(6)")),
	}, alter.Specs)

	got, err := FuzzDDLSignature([]byte(sql), 1049088, false)
	require.NoError(t, err)
	require.Equal(t, "alter db.1234{chg `` table=timestamp} | rename db.a>db.b | alter db.1234{ren first>1ea10}", got)
}

func TestDDLAlterSpecNameReuseImpliesPositionShift(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "rename then add old name with embedded backtick",
			sql:  "ALTER TABLE fixture RENAME COLUMN `new``tick` TO `имя2`, ADD COLUMN `new``tick` enum('a','b') NOT NULL",
			want: true,
		},
		{
			name: "drop then add same name",
			sql:  "ALTER TABLE fixture DROP COLUMN period2, ADD COLUMN period2 datetime(3) COMMENT 'ddlfuzz'",
			want: true,
		},
		{
			name: "change away then add old name",
			sql:  "ALTER TABLE fixture CHANGE COLUMN n4 first2 json NOT NULL, ADD COLUMN n4 blob NOT NULL",
			want: true,
		},
		{
			name: "ordinary change keeps name",
			sql:  "ALTER TABLE fixture CHANGE COLUMN n2 n2 int NOT NULL",
		},
		{
			name: "independent drop and add",
			sql:  "ALTER TABLE fixture DROP COLUMN old_c, ADD COLUMN new_c int",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alter := ddlParseAlter(t, tc.sql, 0, false)
			require.Equal(t, tc.want, ddlAlterSpecsHaveImplicitPositionShift(alter.Specs))
		})
	}
}

func TestDDLAlterHeadModifiers(t *testing.T) {
	// MariaDB head: ALTER alter_options TABLE opt_if_exists table_ident opt_lock_wait_timeout
	stmts, err := parseQueryEvent([]byte("ALTER ONLINE IGNORE TABLE IF EXISTS db.t WAIT 10 DROP COLUMN c"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	alter := stmts[0].(*ddlAlterTable)
	require.Equal(t, "db", alter.Schema)
	require.Equal(t, "t", alter.Table)
	require.Equal(t, ddlAltDrop("c"), alter.Specs)

	require.Equal(t, ddlAltDrop("c"), ddlAlterSpecsOf(t, "ALTER TABLE t NOWAIT DROP COLUMN c", true))

	// quoted schema.table qualification with a full column definition
	alter = ddlParseAlter(t, "ALTER TABLE `db`.`t` ADD COLUMN `c` VARCHAR(255) NOT NULL DEFAULT 'x' AFTER `b`", 0, false)
	require.Equal(t, "db", alter.Schema)
	require.Equal(t, "t", alter.Table)
	require.Equal(t, []ddlAlterSpec{ddlAltPos(ddlAltAddSpec(ddlAltColNN("c", "varchar(255)")))}, alter.Specs)

	alter = ddlParseAlter(t, "ALTER TABLE `db`.`t` ADD COLUMN `c` VARCHAR(255) NOT NULL NULL DEFAULT 'x' AFTER `b`", 0, true)
	require.Equal(t, "db", alter.Schema)
	require.Equal(t, "t", alter.Table)
	require.Equal(t, []ddlAlterSpec{ddlAltPos(ddlAltAddSpec(ddlAltCol("c", "varchar(255)")))}, alter.Specs)
}

func TestDDLAlterRenameTableForms(t *testing.T) {
	for _, tc := range []struct {
		query   string
		sqlMode uint64
		maria   bool
		want    ddlRenamePair
	}{
		{
			query: "ALTER TABLE t RENAME TO t2",
			want:  ddlRenamePair{OldTable: "t", NewTable: "t2"},
		},
		{
			query: "ALTER TABLE db.t RENAME AS db2.t2",
			want:  ddlRenamePair{OldSchema: "db", OldTable: "t", NewSchema: "db2", NewTable: "t2"},
		},
		{
			query: "ALTER TABLE t RENAME = t2",
			want:  ddlRenamePair{OldTable: "t", NewTable: "t2"},
		},
		{
			query: "ALTER TABLE t RENAME t2",
			want:  ddlRenamePair{OldTable: "t", NewTable: "t2"},
		},
		{
			query: "ALTER TABLE t RENAME period",
			want:  ddlRenamePair{OldTable: "t", NewTable: "period"},
		},
		{
			query: "ALTER TABLE t RENAME REMOVE REMOVE PARTITIONING",
			want:  ddlRenamePair{OldTable: "t", NewTable: "REMOVE"},
		},
		{
			query:   "ALTER TABLE t RENAME CLOB",
			sqlMode: sqlModeOracle,
			maria:   true,
			want:    ddlRenamePair{OldTable: "t", NewTable: "CLOB"},
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(tc.query), tc.sqlMode, tc.maria)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			rename, ok := stmts[0].(*ddlRenameTable)
			require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[0])
			require.Equal(t, []ddlRenamePair{tc.want}, rename.Pairs)
		})
	}

	for _, query := range []string{
		"ALTER TABLE fixture RENAME TO fixture",
		"ALTER TABLE fixture RENAME = fixture",
		"ALTER TABLE fixture RENAME fixture",
	} {
		t.Run(query, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(query), 0, true)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			alter, ok := stmts[0].(*ddlAlterTable)
			require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
			require.Equal(t, "fixture", alter.Table)
			require.Empty(t, alter.Specs)
			require.False(t, ddlIsActionable(stmts))
		})
	}

	stmts, err := parseQueryEvent([]byte("ALTER TABLE fixture ADD COLUMN added INT, RENAME TO fixture"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	alter, ok := stmts[0].(*ddlAlterTable)
	require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
	require.Equal(t, ddlAltAdd(ddlAltCol("added", "int")), alter.Specs)

	stmts, err = parseQueryEvent([]byte("RENAME TABLE fixture TO fixture"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename, ok := stmts[0].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[0])
	require.Equal(t, []ddlRenamePair{{OldTable: "fixture", NewTable: "fixture"}}, rename.Pairs)

	stmts, err = parseQueryEvent([]byte("RENAME TABLE  `fixture` TO `bP`"), sqlModeNoBackslashEscapes, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename, ok = stmts[0].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[0])
	require.Equal(t, []ddlRenamePair{{OldTable: "fixture", NewTable: "bP"}}, rename.Pairs)

	stmts, err = parseQueryEvent([]byte("RENAME TABLE fixture TO FAST"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename, ok = stmts[0].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[0])
	require.Equal(t, []ddlRenamePair{{OldTable: "fixture", NewTable: "FAST"}}, rename.Pairs)

	stmts, err = parseQueryEvent(
		[]byte("/*!RENAME TABLE `)ydb`.`mytable` TO SLAVE_POS, `mydb`.`_mytable_new` TO `mydb`.`mytable` */"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename, ok = stmts[0].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[0])
	require.Equal(t, []ddlRenamePair{
		{OldSchema: ")ydb", OldTable: "mytable", NewTable: "SLAVE_POS"},
		{OldSchema: "mydb", OldTable: "_mytable_new", NewSchema: "mydb", NewTable: "mytable"},
	}, rename.Pairs)

	stmts, err = parseQueryEvent([]byte("ALTER TABLE t ADD COLUMN c INT, RENAME TO t2"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	alter, ok = stmts[0].(*ddlAlterTable)
	require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
	require.Equal(t, ddlAltAdd(ddlAltCol("c", "int")), alter.Specs)
	rename, ok = stmts[1].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[1])
	require.Equal(t, []ddlRenamePair{{OldTable: "t", NewTable: "t2"}}, rename.Pairs)

	stmts, err = parseQueryEvent(
		[]byte("ALTER TABLE orders ADD COLUMN json_col JSON VISIBLE, "+
			"MODIFY COLUMN `a` VARCHAR(32) DEFAULT NULL, DROP COLUMN c, "+
			"RENAME TO `世界_col`, RENAME TO new_col"),
		sqlModeANSIQuotes, false)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	alter, ok = stmts[0].(*ddlAlterTable)
	require.True(t, ok, "expected *ddlAlterTable, got %T", stmts[0])
	require.Equal(t, "orders", alter.Table)
	require.Equal(t, []ddlAlterSpec{
		ddlAltAddSpec(ddlAltCol("json_col", "json")),
		ddlAltAddSpec(ddlAltCol("a", "varchar(32)")),
		{OldColumnName: "c"},
	}, alter.Specs)
	rename, ok = stmts[1].(*ddlRenameTable)
	require.True(t, ok, "expected *ddlRenameTable, got %T", stmts[1])
	require.Equal(t, []ddlRenamePair{{OldTable: "orders", NewTable: "new_col"}}, rename.Pairs)

	// RENAME TABLES is valid on MySQL too.
	stmts, err = parseQueryEvent([]byte("RENAME TABLES a TO b, c TO d"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{{OldTable: "a", NewTable: "b"}, {OldTable: "c", NewTable: "d"}}, rename.Pairs)

	stmts, err = parseQueryEvent([]byte("RENAME TABLES a TO  a, c TO d"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{{OldTable: "a", NewTable: "a"}, {OldTable: "c", NewTable: "d"}}, rename.Pairs)

	// MariaDB: statement-level IF EXISTS plus per-pair WAIT/NOWAIT after the old name
	stmts, err = parseQueryEvent([]byte("RENAME TABLE IF EXISTS db1.old WAIT 3 TO db2.new, x NOWAIT TO y"), 0, true)
	require.NoError(t, err)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{
		{OldSchema: "db1", OldTable: "old", NewSchema: "db2", NewTable: "new"},
		{OldTable: "x", NewTable: "y"},
	}, rename.Pairs)

	stmts, err = parseQueryEvent([]byte("RENAME TABLE a WAIT 5e5 TO b, c NOWAIT TO d"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{{OldTable: "a", NewTable: "b"}, {OldTable: "c", NewTable: "d"}}, rename.Pairs)

	// ddlfuzz 7f5636632ad9: unquoted keyword-shaped source name mixed with
	// schema-qualified pairs (MariaDB)
	stmts, err = parseQueryEvent(
		[]byte("RENAME TABLE ROLLUP TO `mydb`.`_mytable_old`, `mydb`.`_mytable_new` TO `mydb`.`mytable`"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{
		{OldTable: "ROLLUP", NewSchema: "mydb", NewTable: "_mytable_old"},
		{OldSchema: "mydb", OldTable: "_mytable_new", NewSchema: "mydb", NewTable: "mytable"},
	}, rename.Pairs)

	// MariaDB twin of the 8ba763557d03 self-pair case: pairs renaming a table
	// to itself must be retained, not silently dropped
	stmts, err = parseQueryEvent([]byte("RENAME TABLE ak TO b, d TO d"), 0, true)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	rename = stmts[0].(*ddlRenameTable)
	require.Equal(t, []ddlRenamePair{{OldTable: "ak", NewTable: "b"}, {OldTable: "d", NewTable: "d"}}, rename.Pairs)

	// RENAME USER is the only other RENAME head; ignored
	stmts, err = parseQueryEvent([]byte("RENAME USER 'o'@'%' TO 'n'@'%'"), 0, false)
	require.NoError(t, err)
	require.Empty(t, stmts)
}

func TestDDLAlterSetStatement(t *testing.T) {
	// values are full expressions: commas inside calls and FOR inside strings
	// must not end the var list; only FOR at paren depth 0 does
	specs := ddlAlterSpecsOf(t,
		"SET STATEMENT sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES'), max_statement_time=60 FOR ALTER TABLE t ADD COLUMN c INT", true)
	require.Equal(t, ddlAltAdd(ddlAltCol("c", "int")), specs)

	specs = ddlAlterSpecsOf(t,
		"SET STATEMENT a='wait FOR it', b=CAST('FOR' AS CHAR(3)) FOR ALTER TABLE t DROP COLUMN c", true)
	require.Equal(t, ddlAltDrop("c"), specs)

	specs = ddlAlterSpecsOf(t,
		"SET STATEMENT max_statement_time=60, sort_buffer_size=100E00009FOR ALTER TABLE t MODIFY c INT", true)
	require.Equal(t, ddlAltAdd(ddlAltCol("c", "int")), specs)

	// an index-only inner ALTER TABLE still parses, into an empty-spec (benign) alter
	specs = ddlAlterSpecsOf(t, "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD INDEX i (a)", true)
	require.Empty(t, specs)

	// no FOR at depth 0: nothing to classify
	stmts, err := parseQueryEvent([]byte("SET STATEMENT a=1"), 0, true)
	require.NoError(t, err)
	require.Empty(t, stmts)
}

// TestDDLAlterSafetyNoSilentDrop is the safety backstop: statements carrying a
// real column op in an awkward position must classify as actionable or error —
// a silently benign parse is the one forbidden outcome.
func TestDDLAlterSafetyNoSilentDrop(t *testing.T) {
	for _, tc := range []struct {
		query string
		maria bool
	}{
		{query: "ALTER TABLE t ADD INDEX i (a), /*!80000 ADD COLUMN c INT */"},
		{query: "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t DROP COLUMN c", maria: true},
		{query: "ALTER TABLE t ADD c uuid", maria: true},
		{query: "ALTER TABLE t ADD c SOMEUDT(4)"}, // unknown MySQL type: error is the safe outcome
		{query: "ALTER TABLE t ENGINE=InnoDB, ADD COLUMN c INT, transactional=1", maria: true},
		{query: "ALTER ONLINE TABLE t DROP COLUMN c", maria: true},
		{query: "ALTER TABLE `t` ADD COLUMN `c` INT COMMENT ', ADD INDEX -- */ ;'"},
		{query: "ALTER TABLE t ADD period date"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(tc.query), 0, tc.maria)
			require.True(t, err != nil || ddlIsActionable(stmts), "silently benign: %s", tc.query)
		})
	}
}

func TestDDLAlterParseErrors(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE t ADD d INT, ADD e", // truncated column spec: must error, never a silent partial parse
		"ALTER TABLE t MODIFY",
		"ALTER TABLE t CHANGE a",
		"ALTER TABLE t ADD COLUMN (a INT",
		"ALTER TABLE t ADD INDEX i (a))",
		"RENAME TABLE a TO",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := parseQueryEvent([]byte(query), 0, false)
			require.Error(t, err)
		})
	}
}
