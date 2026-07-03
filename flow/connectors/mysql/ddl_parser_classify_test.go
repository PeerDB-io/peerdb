package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDDLClassifyIgnoredHeads walks the ignored statement-head universe
// (mysql-grammar.md / mariadb-grammar.md section 6): every case must yield zero
// statements and nil error — benign statements never error, and an ignored head
// swallows the entire remaining input including routine bodies with internal ';'.
func TestDDLClassifyIgnoredHeads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		query   string
		sqlMode uint64
		maria   bool
	}{
		// --- CREATE/DROP/ALTER of objects the handler never acts on ---
		{name: "create or replace table", query: "CREATE OR REPLACE TABLE t (a INT PRIMARY KEY)", maria: true},
		{name: "create database", query: "CREATE DATABASE IF NOT EXISTS db DEFAULT CHARACTER SET utf8mb4"},
		{name: "drop database", query: "DROP DATABASE IF EXISTS db"},
		{name: "create unique index", query: "CREATE UNIQUE INDEX idx ON t (a, b)"},
		{name: "create or replace index", query: "CREATE OR REPLACE INDEX idx ON t (a)", maria: true},
		{
			name:  "create or replace view full prefix",
			query: "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`u`@`%` SQL SECURITY DEFINER VIEW v AS SELECT 1",
			maria: true,
		},
		{name: "alter view with algorithm", query: "ALTER ALGORITHM=UNDEFINED VIEW v AS SELECT 1"},
		{name: "alter definer view", query: "ALTER DEFINER=`admin`@`%` SQL SECURITY INVOKER VIEW v AS SELECT * FROM t"},
		{name: "drop view", query: "DROP VIEW IF EXISTS v"},
		// --- routines / triggers / events / sequences / servers / accounts ---
		{name: "create function", query: "CREATE FUNCTION f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1"},
		{name: "alter function", query: "ALTER FUNCTION f COMMENT 'recalculated'"},
		{name: "drop function", query: "DROP FUNCTION IF EXISTS f"},
		{name: "alter procedure", query: "ALTER PROCEDURE p SQL SECURITY INVOKER"},
		{name: "drop trigger", query: "DROP TRIGGER IF EXISTS trg"},
		{name: "drop event", query: "DROP EVENT IF EXISTS ev"},
		{name: "create sequence", query: "CREATE SEQUENCE s START WITH 1 INCREMENT BY 1", maria: true},
		{name: "alter sequence", query: "ALTER SEQUENCE s MAXVALUE 1000", maria: true},
		{name: "drop sequence", query: "DROP SEQUENCE IF EXISTS s", maria: true},
		{name: "create server", query: "CREATE SERVER srv FOREIGN DATA WRAPPER mysql OPTIONS (HOST 'h', DATABASE 'd')"},
		{name: "alter server", query: "ALTER SERVER srv OPTIONS (USER 'u')"},
		{name: "drop server", query: "DROP SERVER IF EXISTS srv"},
		{name: "create user", query: "CREATE USER 'u'@'%' IDENTIFIED BY 'secret'"},
		{name: "drop user", query: "DROP USER IF EXISTS 'u'@'%'"},
		{name: "create role", query: "CREATE ROLE r"},
		{name: "drop role", query: "DROP ROLE IF EXISTS r"},
		{name: "create tablespace", query: "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd' ENGINE=InnoDB"},
		{name: "drop tablespace", query: "DROP TABLESPACE ts"},
		// --- privileges ---
		{name: "grant all with grant option", query: "GRANT ALL PRIVILEGES ON *.* TO 'u'@'%' WITH GRANT OPTION"},
		{name: "revoke all", query: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'u'@'%'"},
		{name: "set password", query: "SET PASSWORD FOR 'u'@'localhost' = 'ciphertext'"},
		// --- XA / transaction control ---
		{name: "xa prepare", query: "XA PREPARE X'6772316471'"},
		{name: "xa rollback", query: "XA ROLLBACK X'6772316471'"},
		{name: "xa commit one phase", query: "XA COMMIT X'6772316471' ONE PHASE"},
		{name: "begin", query: "BEGIN"},
		{name: "commit", query: "COMMIT"},
		{name: "savepoint", query: "SAVEPOINT sp1"},
		{name: "rollback to savepoint", query: "ROLLBACK TO SAVEPOINT sp1"},
		{name: "start transaction", query: "START TRANSACTION"},
		// --- maintenance ---
		{name: "truncate table", query: "TRUNCATE TABLE t"},
		{name: "truncate bare", query: "TRUNCATE t"},
		{name: "analyze table", query: "ANALYZE TABLE t"},
		{name: "optimize table", query: "OPTIMIZE LOCAL TABLE t"},
		{name: "repair table", query: "REPAIR TABLE t QUICK"},
		{name: "flush tables with read lock", query: "FLUSH TABLES WITH READ LOCK"},
		{name: "flush logs", query: "FLUSH LOGS"},
		// --- DML and session settings (RDS heartbeat shapes) ---
		{
			name: "rds heartbeat insert",
			query: "INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1782476058116) " +
				"ON DUPLICATE KEY UPDATE value = 1782476058116",
		},
		{name: "rds sysinfo delete", query: "DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'"},
		{name: "delete history", query: "DELETE HISTORY FROM t BEFORE SYSTEM_TIME '2026-01-01 00:00:00'", maria: true},
		{name: "set names", query: "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"},
		{name: "set global", query: "SET GLOBAL max_connections=500"},
		// --- routine bodies containing ALTER TABLE and internal ';' — head-based
		// classification never splits on ';' after an ignored head ---
		{
			name:  "procedure body with alter table statements",
			query: "CREATE PROCEDURE migrate() BEGIN ALTER TABLE t ADD COLUMN c INT; ALTER TABLE t DROP COLUMN d; END",
		},
		{
			name:  "definer function body with alter table",
			query: "CREATE DEFINER=`u`@`%` FUNCTION f() RETURNS INT BEGIN ALTER TABLE t ADD COLUMN c INT; RETURN 1; END",
		},
		{
			name:  "create event body with alter table",
			query: "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO BEGIN ALTER TABLE t ADD COLUMN c INT; DELETE FROM t; END",
		},
		{name: "alter event body with alter table", query: "ALTER EVENT ev DO BEGIN ALTER TABLE t ADD COLUMN c INT; END"},
		{
			name:  "trigger body with statements",
			query: "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1); UPDATE agg SET n = n + 1; END",
		},
		// --- PL/SQL-ish bodies under sql_mode=ORACLE (':=' and '%TYPE' tokens) ---
		{
			name:    "oracle procedure body",
			query:   "CREATE OR REPLACE PROCEDURE p AS v t.c%TYPE; BEGIN v := 1; UPDATE t SET c = v; END;",
			sqlMode: sqlModeOracle, maria: true,
		},
		{
			name:    "oracle function body",
			query:   "CREATE OR REPLACE FUNCTION f RETURN NUMBER AS BEGIN RETURN 1; END;",
			sqlMode: sqlModeOracle, maria: true,
		},
		// --- MySQL 9 dollar-quoted routine bodies ---
		{name: "dollar quoted body", query: "CREATE PROCEDURE p() BEGIN SET @s = $q$ALTER TABLE t ADD COLUMN c INT;$q$; END"},
		// --- statements hidden inside comment forms stay hidden ---
		{name: "alter inside block comment", query: "/* ALTER TABLE t ADD COLUMN c INT */"},
		{name: "rename inside multiline block comment", query: "/*\nRENAME TABLE a TO b;\n*/"},
		{name: "alter inside hash comment", query: "# ALTER TABLE t ADD COLUMN c INT"},
		{name: "alter inside dash comment", query: "-- ALTER TABLE t ADD COLUMN c INT"},
		{name: "maria executable comment is plain on mysql", query: "/*M!100500 ALTER TABLE t ADD COLUMN c INT */"},
		{name: "reversed comment with digits was skipped at source", query: "/*!!110500 RENAME TABLE a TO b */", maria: true},
		// an executable comment body with an ignored head is still ignored
		{name: "executable comment around create table", query: "/*!40000 CREATE TABLE t (a INT) */"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(tc.query), tc.sqlMode, tc.maria)
			require.NoError(t, err)
			require.Empty(t, stmts)
		})
	}
}

// TestDDLClassifyActionableWrapped: a real ALTER TABLE column op or RENAME TABLE
// stays actionable behind SET STATEMENT wrappers, executable comments, and any
// plain comment form preceding it.
func TestDDLClassifyActionableWrapped(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		maria bool
	}{
		{
			name:  "set statement for change column",
			query: "SET STATEMENT max_statement_time=60 FOR ALTER TABLE t CHANGE COLUMN a b BIGINT",
			maria: true,
		},
		{name: "set statement for drop column", query: "SET STATEMENT lock_wait_timeout=10 FOR ALTER TABLE t DROP COLUMN c", maria: true},
		{
			name:  "set statement for executable comment alter",
			query: "SET STATEMENT max_statement_time=60 FOR /*!100000 ALTER TABLE t ADD COLUMN c INT */",
			maria: true,
		},
		{
			name:  "executable comment around set statement alter",
			query: "/*!100000 SET STATEMENT max_statement_time=60 FOR ALTER TABLE t DROP COLUMN c */",
			maria: true,
		},
		{name: "versioned executable comment alter", query: "/*!50600 ALTER TABLE t ADD COLUMN c INT */"},
		{name: "versioned executable comment rename", query: "/*!12345 RENAME TABLE a TO b */"},
		{name: "maria executable comment rename on maria", query: "/*M! RENAME TABLE a TO b */", maria: true},
		{name: "hint comment before alter", query: "/*+ MAX_EXECUTION_TIME(1000) */ ALTER TABLE t ADD COLUMN c INT"},
		// block comments do not nest: the first */ closes, the ALTER is live
		{name: "non-nesting block comment before alter", query: "/* outer /* inner */ ALTER TABLE t ADD COLUMN c INT"},
		// '--' directly followed by \n (a control char) still opens a line comment
		{name: "dashes then newline before alter", query: "--\nALTER TABLE t ADD COLUMN c INT"},
		{name: "stacked comment forms before rename", query: "# a\n-- b\n/* c */ RENAME TABLE a TO b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parseQueryEvent([]byte(tc.query), 0, tc.maria)
			require.NoError(t, err)
			require.True(t, ddlIsActionable(stmts))
		})
	}
}

// TestDDLClassifyMultiStatementContinuation: after a successfully parsed
// actionable statement the remainder keeps being classified, but the first
// ignored head swallows everything after it — no splitting on ';'.
func TestDDLClassifyMultiStatementContinuation(t *testing.T) {
	stmts, err := parseQueryEvent([]byte("ALTER TABLE t ADD c INT; DROP TABLE x"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.True(t, ddlIsActionable(stmts))

	// the ALTER after the ignored DROP head is swallowed with it
	stmts, err = parseQueryEvent([]byte("ALTER TABLE t ADD c INT; DROP TABLE x; ALTER TABLE t DROP COLUMN c"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmts, err = parseQueryEvent([]byte("RENAME TABLE a TO b; ALTER TABLE t ADD c INT"), 0, false)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
}
