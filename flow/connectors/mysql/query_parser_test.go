package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsBenignUnparsedStatement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  bool
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
		// --- comments / whitespace must not hide a real ALTER TABLE ---
		{name: "alter table behind block comment", query: "/* abc-123 */ ALTER TABLE `db`.`t` ADD COLUMN c INT", want: false},
		{name: "alter table behind line comment", query: "-- migration\nALTER TABLE t ADD COLUMN c INT", want: false},
		{name: "alter table behind hash comment", query: "# note\nALTER TABLE t ADD COLUMN c INT", want: false},
		{name: "alter table with leading whitespace", query: "\n\t  ALTER TABLE t ADD COLUMN c INT", want: false},
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
			require.Equal(t, tc.want, isBenignUnparsedStatement(tc.query))
		})
	}
}
