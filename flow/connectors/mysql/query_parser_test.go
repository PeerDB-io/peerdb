package connmysql

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestIsBenignUnparsedStatement(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		isMariaDb bool
		want      bool
	}{
		// --- benign: MariaDB/RDS "SET STATEMENT ... FOR ..." (the dominant prod noise) ---
		{
			name: "set statement for rds_heartbeat insert",
			query: "SET STATEMENT max_statement_time=60 FOR INSERT INTO mysql.rds_heartbeat2(id, value) " +
				"values (1,1782476058116) ON DUPLICATE KEY UPDATE value = 1782476058116",
			want: true,
		},
		{
			name:  "set statement for rds_sysinfo delete",
			query: "SET STATEMENT max_statement_time=60 FOR DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'",
			want:  true,
		},
		{
			name:  "set statement for flush table",
			query: "SET STATEMENT max_statement_time=60 FOR flush table",
			want:  true,
		},
		{name: "plain set", query: "SET autocommit=1", want: true},
		{
			name:  "set statement for index-only alter table",
			query: "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD INDEX idx (a)",
			want:  true,
		},
		// --- NOT benign: a real ALTER/RENAME TABLE wrapped in SET STATEMENT must still be reported ---
		{
			name:  "set statement for alter table column op is reported",
			query: "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t ADD COLUMN c INT",
			want:  false,
		},
		{
			name:  "set statement multiple vars for alter table modify is reported",
			query: "SET STATEMENT max_statement_time=60, sort_buffer_size=1000000 FOR ALTER TABLE t MODIFY c INT",
			want:  false,
		},
		{
			name:  "set statement for rename table is reported",
			query: "SET STATEMENT max_statement_time=60 FOR RENAME TABLE a TO b",
			want:  false,
		},
		// --- benign: XA distributed transaction control ---
		{name: "xa start", query: `XA START X'30623263663564642d616630322d34'`, want: true},
		{name: "xa end", query: `XA END X'30623263663564642d616630322d34'`, want: true},
		{name: "xa commit", query: `XA COMMIT X'30623263663564642d616630322d34'`, want: true},
		// --- benign: stored routines / triggers / events / views ---
		{
			name: "create definer procedure",
			query: "CREATE DEFINER=`admin`@`%` PROCEDURE `check_daily_stats_date_continuity`()\n" +
				"BEGIN\n  DECLARE done INT;\n  CREATE TABLE tmp(x INT);\nEND",
			want: true,
		},
		{name: "create procedure no definer", query: "CREATE PROCEDURE sp_x() BEGIN END", want: true},
		{name: "drop procedure", query: "DROP PROCEDURE IF EXISTS sp_x", want: true},
		{name: "create trigger", query: "CREATE DEFINER=`root`@`%` TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @x=1", want: true},
		{name: "create event", query: "CREATE EVENT nexon_program ON SCHEDULE EVERY 1 DAY DO BEGIN END", want: true},
		{name: "alter event", query: "ALTER EVENT nexon_program\nDO\nBEGIN\n  UPDATE nexon.employee SET x=1;\nEND", want: true},
		{
			name:  "create view with algorithm and definer",
			query: "CREATE ALGORITHM=UNDEFINED DEFINER=`API`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT 1",
			want:  true,
		},
		// --- benign: user / privilege DDL ---
		{name: "alter user discard old password", query: "ALTER USER 'ipsadmin'@'%' DISCARD OLD PASSWORD", want: true},
		{name: "rename user", query: "RENAME USER 'old'@'%' TO 'new'@'%'", want: true},
		{name: "grant", query: "GRANT SELECT ON db.* TO 'u'@'%'", want: true},
		{name: "revoke", query: "REVOKE SELECT ON db.* FROM 'u'@'%'", want: true},
		// --- benign: object kinds the handler never acts on ---
		{name: "create table not handled", query: "CREATE TABLE `formiik`.`t` (\n  `form_id` varchar(60) DEFAULT NULL\n)", want: true},
		{name: "drop table not handled", query: "DROP TABLE IF EXISTS `t`", want: true},
		{name: "alter database not handled", query: "ALTER DATABASE db CHARACTER SET utf8mb4", want: true},
		{name: "alter schema not handled", query: "ALTER SCHEMA db DEFAULT COLLATE utf8mb4_bin", want: true},
		{name: "create index not handled", query: "CREATE INDEX idx ON t (a)", want: true},
		{name: "drop index not handled", query: "DROP INDEX idx ON t", want: true},
		{name: "alter tablespace not handled", query: "ALTER TABLESPACE ts ADD DATAFILE 'f'", want: true},
		// --- benign: index/key/constraint-only ALTER TABLE (handler ignores these) ---
		{name: "alter table add unique index", query: "ALTER TABLE `t` ADD UNIQUE INDEX `idx` (`a`, `b`)", want: true},
		{name: "alter table add index", query: "ALTER TABLE t ADD INDEX idx (a)", want: true},
		{name: "alter table add fulltext key", query: "ALTER TABLE t ADD FULLTEXT KEY ft (body)", want: true},
		{name: "alter table add primary key", query: "ALTER TABLE t ADD PRIMARY KEY (id)", want: true},
		{name: "alter table drop index", query: "ALTER TABLE t DROP INDEX idx", want: true},
		{name: "alter table drop primary key", query: "ALTER TABLE t DROP PRIMARY KEY", want: true},
		{
			name:  "alter table add constraint foreign key",
			query: "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES x (id)",
			want:  true,
		},
		{name: "alter table add foreign key", query: "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES x (id)", want: true},
		{name: "alter table drop and add index", query: "ALTER TABLE t DROP INDEX i, ADD UNIQUE INDEX j (c)", want: true},
		{name: "alter ignore table add index", query: "ALTER IGNORE TABLE t ADD UNIQUE INDEX idx (a)", want: true},
		{name: "alter table add index with algorithm", query: "ALTER TABLE t ADD INDEX idx (a), ALGORITHM=INPLACE, LOCK=NONE", want: true},
		{name: "alter table rename index", query: "ALTER TABLE t RENAME INDEX old TO new", want: true},
		{name: "alter table rename key", query: "ALTER TABLE t RENAME KEY old TO new", want: true},
		{name: "alter table alter index invisible", query: "ALTER TABLE t ALTER INDEX idx INVISIBLE", want: true},
		{name: "alter table disable keys", query: "ALTER TABLE t DISABLE KEYS", want: true},
		{name: "alter table enable keys", query: "ALTER TABLE t ENABLE KEYS", want: true},
		// --- NOT benign: mixing an index op with a column op must still be reported ---
		{name: "alter table add column and index is reported", query: "ALTER TABLE t ADD c INT, ADD INDEX i (c)", want: false},
		{name: "alter table drop column and add index is reported", query: "ALTER TABLE t DROP COLUMN a, ADD INDEX i (b)", want: false},
		{
			name:  "alter table rename index and modify column is reported",
			query: "ALTER TABLE t RENAME INDEX old TO new, MODIFY c INT",
			want:  false,
		},
		{name: "alter table rename column is reported", query: "ALTER TABLE t RENAME COLUMN a TO b", want: false},
		{name: "alter table rename to table is reported", query: "ALTER TABLE t RENAME TO t2", want: false},
		// --- NOT benign: a column whose quoted name matches an index keyword must still be reported ---
		{name: "alter table add backtick key column is reported", query: "ALTER TABLE t ADD `key` VARCHAR(20)", want: false},
		{name: "alter table add backtick index column is reported", query: "ALTER TABLE t ADD COLUMN `index` INT", want: false},
		{name: "alter table drop backtick constraint column is reported", query: "ALTER TABLE t DROP `constraint`", want: false},
		// --- benign: an index op naming a column that matches a keyword stays index-only ---
		{name: "alter table add index on keyword-named column", query: "ALTER TABLE t ADD INDEX idx (`key`)", want: true},
		// --- NOT benign: the only statements the handler acts on must still be reported ---
		{
			name:  "alter table modify is reported",
			query: "ALTER TABLE `mt5_holidays` MODIFY `Description` VARCHAR(128) COLLATE utf8_bin NOT NULL DEFAULT '' ",
			want:  false,
		},
		{
			name:  "alter table add parenthesized columns is reported",
			query: "ALTER TABLE `mt5_managers` ADD COLUMN (`A` INT UNSIGNED NOT NULL DEFAULT 0,`B` INT UNSIGNED NOT NULL DEFAULT 0)",
			want:  false,
		},
		{name: "alter ignore table is reported", query: "ALTER IGNORE TABLE t ADD COLUMN c INT", want: false},
		{name: "alter online table is reported", query: "ALTER ONLINE TABLE t ADD COLUMN c INT", want: false},
		{name: "rename table is reported", query: "RENAME TABLE `a` TO `b`", want: false},
		{name: "rename table multi is reported", query: "RENAME TABLE `users` TO `_users_del`, `_users_gho` TO `users`", want: false},
		{name: "rename tables mariadb is reported on maria", query: "RENAME TABLES `a` TO `b`, `c` TO `d`", want: false, isMariaDb: true},
		{name: "rename tables mariadb is not reported on mysql", query: "RENAME TABLES `a` TO `b`, `c` TO `d`", want: true, isMariaDb: false},
		// --- executable comments (/*! */, /*M! */) are lexed as code, not skipped ---
		{
			name:  "alter table with executable comment column op is reported",
			query: "ALTER TABLE db.t\n  /*! ADD COLUMN mysql_only INT, */\n  DROP COLUMN old_col",
			want:  false,
		},
		{
			name:  "executable comment hiding alter table keyword is reported",
			query: "ALTER /*! TABLE db.t MODIFY c INT */",
			want:  false,
		},
		{
			name:  "versioned executable comment column op is reported",
			query: "ALTER TABLE t /*!80000 ADD COLUMN c INT */",
			want:  false,
		},
		{
			name:  "mariadb executable comment lexed on maria",
			query: "ALTER TABLE t /*M! MODIFY c INT */", want: false, isMariaDb: true,
		},
		{
			name:  "mariadb executable comment is a plain comment on mysql",
			query: "ALTER TABLE t /*M! MODIFY c INT */ ADD INDEX idx (a)", want: true, isMariaDb: false,
		},
		// --- comments / whitespace must not hide a real ALTER TABLE ---
		{name: "alter table behind block comment", query: "/* abc-123 */ ALTER TABLE `db`.`t` ADD COLUMN c INT", want: false},
		{name: "alter table behind line comment", query: "-- migration\nALTER TABLE t ADD COLUMN c INT", want: false},
		{name: "alter table behind hash comment", query: "# note\nALTER TABLE t ADD COLUMN c INT", want: false},
		{name: "alter table behind cr terminated line comment", query: "-- migration\rALTER TABLE t ADD COLUMN c INT", want: false},
		{name: "alter table with leading whitespace", query: "\n\t\f\v  ALTER TABLE t ADD COLUMN c INT", want: false},
		// --- edge cases: nothing actionable recognized => benign ---
		{name: "empty", query: "", want: true},
		{name: "only comment", query: "/* nothing here */", want: true},
		{
			name:  "procedure body mentioning table before object keyword cannot fool it",
			query: "CREATE PROCEDURE p() BEGIN CREATE TABLE z(x INT); END",
			want:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// want == true means the statement is benign noise, i.e. classified as ddlKindIgnored.
			require.Equal(t, tc.want, classifyUnparsedStatement(tc.query, tc.isMariaDb) == ddlKindIgnored)
		})
	}
}

func TestClassifyParsedStatementTxControl(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  ddlKind
	}{
		{name: "commit", query: "COMMIT", want: ddlKindCommit},
		{name: "rollback", query: "ROLLBACK", want: ddlKindRollback},
		{name: "begin", query: "BEGIN", want: ddlKindIgnored},
		{name: "alter table add column", query: "ALTER TABLE t ADD COLUMN c INT", want: ddlKindAlterTable},
		{name: "rename table", query: "RENAME TABLE a TO b", want: ddlKindRenameTable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, _, err := parseSQL(parser.New(), []byte(tc.query))
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			kind, _, _ := classifyParsedStatement(stmts[0])
			require.Equal(t, tc.want, kind)
		})
	}
}
