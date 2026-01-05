package pgwire_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	testHost     = "127.0.0.1"
	testPort     = "5732"
	testUser     = "peerdb"
	testPassword = "peerdb"
	testPeer     = "catalog" // peer name to use for tests
)

// basePsqlArgs returns common psql arguments
func basePsqlArgs() []string {
	return []string{"-h", testHost, "-p", testPort, "-d", testPeer, "-U", testUser}
}

// psqlEnv returns environment with password set
func psqlEnv() []string {
	return append(os.Environ(), "PGPASSWORD="+testPassword)
}

// runPsql executes psql with base args plus additional args
func runPsql(t *testing.T, extraArgs ...string) (string, error) {
	t.Helper()
	args := append(basePsqlArgs(), extraArgs...)
	cmd := exec.Command("psql", args...)
	cmd.Env = psqlEnv()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("psql failed: %s\nOutput: %s", err, string(output))
	}
	return strings.TrimSpace(string(output)), err
}

// query executes a SQL query and returns tuples-only output
func query(t *testing.T, sql string) (string, error) {
	t.Helper()
	return runPsql(t, "-tA", "-c", sql)
}

// psqlExec executes a SQL statement (no output expected)
func psqlExec(t *testing.T, sql string) error {
	t.Helper()
	_, err := runPsql(t, "-c", sql)
	return err
}

// buildDSN builds a connection string with optional peerdb options
func buildDSN(options map[string]string) string {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		testUser, testPassword, testHost, testPort, testPeer)

	if len(options) > 0 {
		var optParts []string
		for k, v := range options {
			optParts = append(optParts, fmt.Sprintf("-c peerdb.%s=%s", k, v))
		}
		dsn += "&options=" + strings.Join(optParts, " ")
	}

	return dsn
}

// TestMain sets up test environment
func TestMain(m *testing.M) {
	if _, err := exec.LookPath("psql"); err != nil {
		fmt.Fprintf(os.Stderr, "psql not found in PATH, skipping integration tests\n")
		os.Exit(0)
	}

	// Check if pgwire proxy is available
	cmd := exec.Command("psql", append(basePsqlArgs(), "-c", "SELECT 1")...) //nolint:gosec
	cmd.Env = psqlEnv()
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "PgWire proxy not available at %s:%s (peer: %s), skipping integration tests\n",
			testHost, testPort, testPeer)
		os.Exit(0)
	}

	os.Exit(m.Run())
}

// ========================================
// Test Cases
// ========================================

func TestBasicConnectivity(t *testing.T) {
	t.Run("SimpleSelect", func(t *testing.T) {
		output, err := query(t, "SELECT 1 AS test")
		require.NoError(t, err)
		require.Equal(t, "1", output)
	})

	t.Run("SelectVersion", func(t *testing.T) {
		output, err := query(t, "SELECT version()")
		require.NoError(t, err)
		require.Contains(t, output, "PostgreSQL")
	})

	t.Run("CurrentDatabase", func(t *testing.T) {
		output, err := query(t, "SELECT current_database()")
		require.NoError(t, err)
		require.NotEmpty(t, output)
	})
}

func TestDataTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"Integer", "SELECT 42::integer", "42"},
		{"BigInt", "SELECT 9223372036854775807::bigint", "9223372036854775807"},
		{"Text", "SELECT 'hello'::text", "hello"},
		{"Boolean", "SELECT true::boolean", "t"},
		{"Numeric", "SELECT 123.456::numeric", "123.456"},
		{"Timestamp", "SELECT '2024-01-01 12:00:00'::timestamp", "2024-01-01 12:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := query(t, tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func TestMultipleRows(t *testing.T) {
	t.Run("GenerateSeries", func(t *testing.T) {
		output, err := query(t, "SELECT * FROM generate_series(1, 5)")
		require.NoError(t, err)
		require.Equal(t, "1\n2\n3\n4\n5", output)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		output, err := query(t, "SELECT 1 AS a, 2 AS b, 3 AS c")
		require.NoError(t, err)
		require.Equal(t, "1|2|3", output)
	})
}

func TestTransactions(t *testing.T) {
	t.Run("CommitTransaction", func(t *testing.T) {
		output, err := query(t, `
			BEGIN;
			SELECT 'in_transaction';
			COMMIT;
			SELECT 'after_commit';
		`)
		require.NoError(t, err)
		require.Contains(t, output, "in_transaction")
		require.Contains(t, output, "after_commit")
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		output, err := query(t, `
			BEGIN;
			SELECT 'before_rollback';
			ROLLBACK;
			SELECT 'after_rollback';
		`)
		require.NoError(t, err)
		require.Contains(t, output, "after_rollback")
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("SyntaxError", func(t *testing.T) {
		_, err := query(t, "SELCT 1") // Typo
		require.Error(t, err)
	})

	t.Run("TableDoesNotExist", func(t *testing.T) {
		_, err := query(t, "SELECT * FROM nonexistent_table_xyz")
		require.Error(t, err)
	})

	t.Run("ColumnDoesNotExist", func(t *testing.T) {
		_, err := query(t, "SELECT nonexistent_column FROM pg_class LIMIT 1")
		require.Error(t, err)
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		_, err := query(t, "SELECT 1 WHERE 1 = 'not_a_number'")
		require.Error(t, err)
	})
}

func TestCatalogQueries(t *testing.T) {
	t.Run("DescribeCommand", func(t *testing.T) {
		err := psqlExec(t, "\\d")
		require.NoError(t, err)
	})

	t.Run("QuerySystemCatalog", func(t *testing.T) {
		output, err := query(t, "SELECT COUNT(*) > 0 FROM pg_class")
		require.NoError(t, err)
		require.Equal(t, "t", output)
	})
}

func TestConcurrentConnections(t *testing.T) {
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := query(t, fmt.Sprintf("SELECT %d AS conn_id, pg_sleep(0.1)", id))
			if err != nil {
				errors <- fmt.Errorf("connection %d failed: %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}
}

func TestLargeResults(t *testing.T) {
	t.Run("ManyRows", func(t *testing.T) {
		output, err := query(t, "SELECT * FROM generate_series(1, 1000)")
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Len(t, lines, 1000)
		require.Equal(t, "1", lines[0])
		require.Equal(t, "1000", lines[999])
	})

	t.Run("LargeText", func(t *testing.T) {
		output, err := query(t, "SELECT repeat('x', 1000000)")
		require.NoError(t, err)
		require.Len(t, output, 1000000)
	})
}

func TestJoinQueries(t *testing.T) {
	t.Run("SelfJoin", func(t *testing.T) {
		output, err := query(t, `
			SELECT a.i, b.i
			FROM generate_series(1, 3) AS a(i)
			CROSS JOIN generate_series(1, 2) AS b(i)
			ORDER BY a.i, b.i
		`)
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Len(t, lines, 6) // 3 * 2 = 6 rows
		require.Equal(t, "1|1", lines[0])
		require.Equal(t, "3|2", lines[5])
	})
}

func TestAggregates(t *testing.T) {
	tests := []struct {
		name, sql, expected string
	}{
		{"Count", "SELECT COUNT(*) FROM generate_series(1, 10)", "10"},
		{"Sum", "SELECT SUM(i) FROM generate_series(1, 5) AS t(i)", "15"},
		{"Avg", "SELECT AVG(i)::integer FROM generate_series(1, 5) AS t(i)", "3"},
		{"Min", "SELECT MIN(i) FROM generate_series(1, 10) AS t(i)", "1"},
		{"Max", "SELECT MAX(i) FROM generate_series(1, 10) AS t(i)", "10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := query(t, tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func TestSubqueries(t *testing.T) {
	t.Run("SimpleSubquery", func(t *testing.T) {
		output, err := query(t, `
			SELECT * FROM (
				SELECT i * 2 AS doubled
				FROM generate_series(1, 3) AS t(i)
			) sub
			WHERE doubled > 2
		`)
		require.NoError(t, err)
		require.Equal(t, "4\n6", output)
	})

	t.Run("SubqueryInWhere", func(t *testing.T) {
		output, err := query(t, `
			SELECT i FROM generate_series(1, 5) AS t(i)
			WHERE i IN (SELECT 2 UNION SELECT 4)
		`)
		require.NoError(t, err)
		require.Equal(t, "2\n4", output)
	})
}

func TestNullHandling(t *testing.T) {
	t.Run("SelectNull", func(t *testing.T) {
		output, err := query(t, "SELECT NULL")
		require.NoError(t, err)
		require.Empty(t, output) // NULL displays as empty
	})

	t.Run("NullCoalesce", func(t *testing.T) {
		output, err := query(t, "SELECT COALESCE(NULL, 'default')")
		require.NoError(t, err)
		require.Equal(t, "default", output)
	})

	t.Run("IsNull", func(t *testing.T) {
		output, err := query(t, "SELECT NULL IS NULL")
		require.NoError(t, err)
		require.Equal(t, "t", output)
	})
}

func TestWindowFunctions(t *testing.T) {
	t.Run("RowNumber", func(t *testing.T) {
		output, err := query(t, `
			SELECT i, ROW_NUMBER() OVER (ORDER BY i) AS rn
			FROM generate_series(1, 3) AS t(i)
		`)
		require.NoError(t, err)
		require.Equal(t, "1|1\n2|2\n3|3", output)
	})

	t.Run("Lag", func(t *testing.T) {
		output, err := query(t, `
			SELECT i, LAG(i, 1) OVER (ORDER BY i) AS prev
			FROM generate_series(1, 3) AS t(i)
		`)
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Equal(t, "1|", lines[0])
		require.Equal(t, "2|1", lines[1])
		require.Equal(t, "3|2", lines[2])
	})
}

func TestCTEQueries(t *testing.T) {
	t.Run("SimpleCTE", func(t *testing.T) {
		output, err := query(t, `
			WITH doubled AS (
				SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)
			)
			SELECT val FROM doubled WHERE val > 2
		`)
		require.NoError(t, err)
		require.Equal(t, "4\n6", output)
	})

	t.Run("MultipleCTEs", func(t *testing.T) {
		output, err := query(t, `
			WITH
				doubled AS (SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)),
				tripled AS (SELECT i * 3 AS val FROM generate_series(1, 3) AS t(i))
			SELECT * FROM doubled UNION ALL SELECT * FROM tripled ORDER BY val
		`)
		require.NoError(t, err)
		require.Contains(t, output, "2")
		require.Contains(t, output, "9")
	})
}

func TestConnectionRecovery(t *testing.T) {
	t.Run("MultipleSequentialConnections", func(t *testing.T) {
		for i := range 10 {
			output, err := query(t, fmt.Sprintf("SELECT %d", i))
			require.NoError(t, err)
			require.Equal(t, strconv.Itoa(i), output)
		}
	})
}

func TestQueryTimeout(t *testing.T) {
	t.Run("ShortQuery", func(t *testing.T) {
		output, err := query(t, "SELECT pg_sleep(0.1), 1")
		require.NoError(t, err)
		require.Contains(t, output, "1")
	})
}

func TestSpecialCharacters(t *testing.T) {
	tests := []struct {
		name, value string
	}{
		{"SingleQuote", "it's"},
		{"DoubleQuote", `he said "hello"`},
		{"Backslash", `path\to\file`},
		{"Newline", "line1\nline2"},
		{"Tab", "col1\tcol2"},
		{"Unicode", "Hello 世界 🌍"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := query(t, fmt.Sprintf("SELECT $$%s$$", tt.value))
			require.NoError(t, err)
			require.Equal(t, tt.value, output)
		})
	}
}

func TestEmptyResults(t *testing.T) {
	t.Run("NoRows", func(t *testing.T) {
		output, err := query(t, "SELECT 1 WHERE FALSE")
		require.NoError(t, err)
		require.Empty(t, output)
	})

	t.Run("EmptyResultSet", func(t *testing.T) {
		output, err := query(t, "SELECT COUNT(*) FROM (SELECT 1 WHERE FALSE) sq")
		require.NoError(t, err)
		require.Contains(t, output, "0")
	})
}

func TestBooleanLogic(t *testing.T) {
	tests := []struct {
		name, sql, expected string
	}{
		{"And", "SELECT TRUE AND TRUE", "t"},
		{"Or", "SELECT FALSE OR TRUE", "t"},
		{"Not", "SELECT NOT FALSE", "t"},
		{"Comparison", "SELECT 5 > 3", "t"},
		{"Between", "SELECT 5 BETWEEN 1 AND 10", "t"},
		{"In", "SELECT 3 IN (1, 2, 3)", "t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := query(t, tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

// ========================================
// Critical Protocol Tests
// ========================================

func TestMultiStatementQueries(t *testing.T) {
	t.Run("TwoSelectStatements", func(t *testing.T) {
		output, err := query(t, "SELECT 1 AS first; SELECT 2 AS second;")
		require.NoError(t, err)
		require.Contains(t, output, "1")
		require.Contains(t, output, "2")
	})

	t.Run("TransactionWithMultipleSelects", func(t *testing.T) {
		output, err := query(t, "BEGIN; SELECT 1 AS a; SELECT 2 AS b; COMMIT;")
		require.NoError(t, err)
		require.Contains(t, output, "1")
		require.Contains(t, output, "2")
	})

	t.Run("MixedStatementsWithResults", func(t *testing.T) {
		output, err := query(t, `
			SELECT 'setup' AS step;
			SELECT val FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, val) ORDER BY id;
		`)
		require.NoError(t, err)
		require.Contains(t, output, "a")
		require.Contains(t, output, "b")
	})

	t.Run("ThreeSelectStatements", func(t *testing.T) {
		output, err := query(t, "SELECT 'first'; SELECT 'second'; SELECT 'third';")
		require.NoError(t, err)
		require.Contains(t, output, "first")
		require.Contains(t, output, "second")
		require.Contains(t, output, "third")
	})
}

func TestReadOnlyEnforcement(t *testing.T) {
	t.Run("WriteBlocked", func(t *testing.T) {
		output, err := query(t, "CREATE TEMP TABLE readonly_test(id int)")
		require.Error(t, err)
		require.Contains(t, output, "read-only")
	})

	t.Run("DropBlocked", func(t *testing.T) {
		output, err := query(t, "DROP TABLE IF EXISTS nonexistent")
		require.Error(t, err)
		require.Contains(t, output, "read-only")
	})

	t.Run("BypassAttemptBlocked", func(t *testing.T) {
		output, err := query(t, "SET default_transaction_read_only = off")
		require.Error(t, err)
		require.Contains(t, output, "read-only mode")
	})

	t.Run("SetConfigBlocked", func(t *testing.T) {
		// Note: this also contains 'default_transaction_read_only' so triggers that check
		output, err := query(t, "SELECT set_config('default_transaction_read_only', 'off', false)")
		require.Error(t, err)
		require.Contains(t, output, "read-only mode")
	})

	t.Run("SetConfigOnOtherParamBlocked", func(t *testing.T) {
		// set_config on any param is blocked to prevent bypass via string concat
		output, err := query(t, "SELECT set_config('work_mem', '64MB', false)")
		require.Error(t, err)
		require.Contains(t, output, "set_config")
	})

	t.Run("ObfuscatedSetConfigBlocked", func(t *testing.T) {
		// Attempt to bypass by obfuscating the setting name via string concatenation
		// Still blocked because set_config itself is denied
		output, err := query(t, "SELECT set_config('default_transaction' || '_read_only', 'off', false)")
		require.Error(t, err)
		require.Contains(t, output, "set_config")
	})

	t.Run("BeginReadWriteBlocked", func(t *testing.T) {
		output, err := query(t, "BEGIN READ WRITE")
		require.Error(t, err)
		require.Contains(t, output, "READ WRITE")
	})

	t.Run("StartTransactionReadWriteBlocked", func(t *testing.T) {
		output, err := query(t, "START TRANSACTION READ WRITE")
		require.Error(t, err)
		require.Contains(t, output, "READ WRITE")
	})

	t.Run("SetTransactionReadWriteBlocked", func(t *testing.T) {
		output, err := query(t, "SET TRANSACTION READ WRITE")
		require.Error(t, err)
		require.Contains(t, output, "READ WRITE")
	})
}

func TestBlockedCommands(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		blocked string
	}{
		{"COPY", "COPY (SELECT 1) TO STDOUT", "COPY"},
		{"VACUUM", "VACUUM", "VACUUM"},
		{"ANALYZE", "ANALYZE", "ANALYZE"},
		{"CLUSTER", "CLUSTER", "CLUSTER"},
		{"REINDEX", "REINDEX DATABASE postgres", "REINDEX"},
		{"REFRESH", "REFRESH MATERIALIZED VIEW nonexistent", "REFRESH"},
		{"LISTEN", "LISTEN test_channel", "LISTEN"},
		{"NOTIFY", "NOTIFY test_channel", "NOTIFY"},
		{"UNLISTEN", "UNLISTEN test_channel", "UNLISTEN"},
		{"DO", "DO $$ BEGIN NULL; END $$", "DO"},
		{"LOCK", "LOCK TABLE pg_class IN ACCESS SHARE MODE", "LOCK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := query(t, tt.sql)
			require.Error(t, err, "%s should be blocked", tt.name)
			require.Contains(t, output, "denied", "Should mention denied")
			require.Contains(t, output, tt.blocked, "Should mention %s", tt.blocked)
		})
	}
}

func TestGuardrailsRowLimit(t *testing.T) {
	t.Run("RowLimitExceeded", func(t *testing.T) {
		// Use session-level config via connection options
		dsn := buildDSN(map[string]string{"max_rows": "3"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Try to fetch 10 rows with a limit of 3
		_, err = conn.Exec(context.Background(), "SELECT * FROM generate_series(1, 10)")
		require.Error(t, err, "Query should fail due to row limit")
		require.Contains(t, err.Error(), "row limit", "Error message should mention row limit")
	})

	t.Run("WithinRowLimit", func(t *testing.T) {
		dsn := buildDSN(map[string]string{"max_rows": "3"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Fetch 2 rows, should succeed
		rows, err := conn.Query(context.Background(), "SELECT * FROM generate_series(1, 2)")
		require.NoError(t, err, "Query should succeed within row limit")
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}
		require.Equal(t, 2, count)
	})
}

// TestGuardrailsByteLimit tests byte limit enforcement
func TestGuardrailsByteLimit(t *testing.T) {
	t.Run("ByteLimitExceeded", func(t *testing.T) {
		dsn := buildDSN(map[string]string{"max_bytes": "32"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Generate wide rows that will exceed byte limit
		_, err = conn.Exec(context.Background(), "SELECT repeat('x', 20) FROM generate_series(1, 5)")
		require.Error(t, err, "Query should fail due to byte limit")
		require.Contains(t, err.Error(), "byte limit", "Error message should mention byte limit")
	})
}

// TestGuardrailsNoOOM tests that large result sets don't cause OOM
func TestGuardrailsNoOOM(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Run("LargeResultSet", func(t *testing.T) {
		dsn := buildDSN(map[string]string{"max_rows": "100"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// This would generate 2 million rows, but we limit to 100
		_, err = conn.Exec(context.Background(), "SELECT * FROM generate_series(1, 2000000)")
		require.Error(t, err, "Query should fail due to row limit")
		require.Contains(t, err.Error(), "row limit")
	})
}

func TestEmptyQuery(t *testing.T) {
	t.Run("EmptySemicolon", func(t *testing.T) {
		err := psqlExec(t, ";")
		require.NoError(t, err, "Empty query should succeed")
	})

	t.Run("MultipleSemicolons", func(t *testing.T) {
		err := psqlExec(t, ";;;")
		require.NoError(t, err, "Multiple empty queries should succeed")
	})

	t.Run("EmptyWithRealQuery", func(t *testing.T) {
		output, err := query(t, "; SELECT 1; ;")
		require.NoError(t, err, "Mixed empty and real queries should succeed")
		require.Contains(t, output, "1")
	})
}

func TestMidBatchError(t *testing.T) {
	t.Run("ErrorStopsExecution", func(t *testing.T) {
		output, err := query(t, "SELECT 111 + 222; SELCT 2; SELECT 777 + 222")
		require.Error(t, err, "Query batch should fail on syntax error")
		errorText := output
		if err != nil {
			errorText += err.Error()
		}
		require.Contains(t, strings.ToLower(errorText), "syntax", "Should report syntax error")
		require.NotContains(t, output, "333", "First query result should not appear")
		require.NotContains(t, output, "999", "Third query result should not appear")
	})

	t.Run("ErrorInTransaction", func(t *testing.T) {
		output, err := query(t, "BEGIN; SELECT 1; SELCT 2; SELECT 3; COMMIT")
		require.Error(t, err, "Transaction should fail on syntax error")
		errorText := output
		if err != nil {
			errorText += err.Error()
		}
		require.Contains(t, strings.ToLower(errorText), "syntax", "Should report syntax error")
	})
}

// TestExtendedProtocolRejection tests that extended protocol is rejected
func TestExtendedProtocolRejection(t *testing.T) {
	t.Run("PrepareStatementRejected", func(t *testing.T) {
		dsn := buildDSN(nil)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)

		// Force extended protocol by using CacheStatement mode
		cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Try to prepare a statement - this sends Parse message
		_, err = conn.Prepare(ctx, "test_stmt", "SELECT $1::int")

		require.Error(t, err, "Extended protocol (Parse) should be rejected")
		require.Contains(t, err.Error(), "extended protocol not supported", "Should return extended protocol error")
	})

	t.Run("ParameterizedQueryRejected", func(t *testing.T) {
		// Try with explicit extended protocol via QueryExecModeExec
		dsn := buildDSN(nil)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)

		// QueryExecModeExec uses extended protocol
		cfg.DefaultQueryExecMode = pgx.QueryExecModeExec

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Try a simple query with this mode - should fail
		var result int
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)

		// Should reject extended protocol
		require.Error(t, err, "Extended protocol should be rejected")
		require.Contains(t, err.Error(), "extended protocol not supported", "Should return extended protocol error")
		t.Logf("Expected extended protocol rejection: %v", err)
	})

	t.Run("SimpleProtocolStillWorks", func(t *testing.T) {
		dsn := buildDSN(nil)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		var result int
		err = conn.QueryRow(ctx, "SELECT 42").Scan(&result)
		require.NoError(t, err, "Simple protocol should work")
		require.Equal(t, 42, result)
	})
}

// TestCancelRequest tests query cancellation
func TestCancelRequest(t *testing.T) {
	t.Run("CancelLongRunningQuery", func(t *testing.T) {
		dsn := buildDSN(nil)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Start a long-running query
		ctx := context.Background()
		errCh := make(chan error, 1)
		go func() {
			var result int
			err := conn.QueryRow(ctx, "SELECT pg_sleep(10)").Scan(&result)
			errCh <- err
		}()

		// Wait a bit for query to start
		time.Sleep(100 * time.Millisecond)

		// Send cancel request
		err = conn.PgConn().CancelRequest(ctx)
		require.NoError(t, err, "Cancel request should be sent")

		// Query should fail with cancellation error
		select {
		case queryErr := <-errCh:
			require.Error(t, queryErr, "Query should be canceled")
			t.Logf("Query error (expected cancellation): %v", queryErr)
		case <-time.After(5 * time.Second):
			t.Fatal("Query should have been canceled within 5 seconds")
		}

		// Connection may or may not be usable after cancel in simple protocol mode
		// This is a known limitation - simple protocol doesn't handle cancel cleanly
		var result int
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		if err != nil {
			// Accept "conn busy" error which happens with simple protocol after cancel
			t.Logf("Connection state after cancel (may be expected with simple protocol): %v", err)
		} else {
			// If it worked, verify the result
			require.Equal(t, 1, result)
		}
	})

	t.Run("SpuriousCancelRequest", func(t *testing.T) {
		dsn := buildDSN(nil)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		cfg2, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg2.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn2, err := pgx.ConnectConfig(context.Background(), cfg2)
		require.NoError(t, err)
		defer conn2.Close(context.Background())

		// Send cancel from conn2 (wrong connection) - should have no effect
		err = conn2.PgConn().CancelRequest(context.Background())
		require.NoError(t, err)

		// Original connection should still work fine
		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)
	})
}

func TestSSLRejection(t *testing.T) {
	t.Run("SSLRejectionWithFallback", func(t *testing.T) {
		// sslmode=prefer will try SSL first, then fall back
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=prefer",
			testUser, testPassword, testHost, testPort, testPeer)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err, "Should connect with sslmode=prefer and fall back to plaintext")
		defer conn.Close(context.Background())

		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)
	})

	t.Run("SSLRequiredFails", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
			testUser, testPassword, testHost, testPort, testPeer)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		_, err = pgx.ConnectConfig(context.Background(), cfg)
		require.Error(t, err, "Should fail with sslmode=require")
		errorLower := strings.ToLower(err.Error())
		require.True(t, strings.Contains(errorLower, "ssl") || strings.Contains(errorLower, "tls"), "Should mention SSL or TLS in error")
	})
}

// TestByteLimitWithNulls tests byte limit enforcement with NULL values
func TestByteLimitWithNulls(t *testing.T) {
	t.Run("NullsCountOverhead", func(t *testing.T) {
		dsn := buildDSN(map[string]string{"max_bytes": "100"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		_, err = conn.Exec(context.Background(), "SELECT NULL, NULL, NULL FROM generate_series(1, 20)")
		require.Error(t, err, "Should hit byte limit even with NULLs")
		require.Contains(t, err.Error(), "byte limit", "Should mention byte limit")
	})

	t.Run("LongTextExceedsLimit", func(t *testing.T) {
		dsn := buildDSN(map[string]string{"max_bytes": "100"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		_, err = conn.Exec(context.Background(), "SELECT repeat('a', 50) FROM generate_series(1, 5)")
		require.Error(t, err, "Should hit byte limit with long text")
		require.Contains(t, err.Error(), "byte limit", "Should mention byte limit")
	})
}

// TestParameterStatusFidelity tests that ParameterStatus messages are complete
func TestParameterStatusFidelity(t *testing.T) {
	dsn := buildDSN(nil)
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	essentialParams := []string{
		"server_version",
		"server_encoding",
		"client_encoding",
		"DateStyle",
		"TimeZone",
		"integer_datetimes",
		"standard_conforming_strings",
	}

	ctx := context.Background()
	for _, param := range essentialParams {
		var value string
		query := "SHOW " + param
		err := conn.QueryRow(ctx, query).Scan(&value)
		require.NoError(t, err, "Should be able to query %s", param)
		require.NotEmpty(t, value, "Parameter %s should have a value", param)
		t.Logf("Parameter %s = %s", param, value)
	}

	var clientEncoding string
	err = conn.QueryRow(ctx, "SHOW client_encoding").Scan(&clientEncoding)
	require.NoError(t, err)
	require.Equal(t, "UTF8", clientEncoding, "client_encoding should be UTF8")

	var standardConforming string
	err = conn.QueryRow(ctx, "SHOW standard_conforming_strings").Scan(&standardConforming)
	require.NoError(t, err)
	require.Equal(t, "on", standardConforming, "standard_conforming_strings should be on")

	var integerDatetimes string
	err = conn.QueryRow(ctx, "SHOW integer_datetimes").Scan(&integerDatetimes)
	require.NoError(t, err)
	require.Equal(t, "on", integerDatetimes, "integer_datetimes should be on")
}

// TestCancelTimingRaces tests cancel request timing edge cases
func TestCancelTimingRaces(t *testing.T) {
	t.Run("ImmediateCancelDuringSleep", func(t *testing.T) {
		dsn := buildDSN(nil)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		errCh := make(chan error, 1)
		go func() {
			var result int
			err := conn.QueryRow(ctx, "SELECT pg_sleep(5)").Scan(&result)
			errCh <- err
		}()

		time.Sleep(10 * time.Millisecond)
		err = conn.PgConn().CancelRequest(ctx)
		require.NoError(t, err, "Cancel request should be sent")

		select {
		case queryErr := <-errCh:
			require.Error(t, queryErr, "Query should be canceled")
			t.Logf("Query canceled as expected: %v", queryErr)
		case <-time.After(2 * time.Second):
			t.Fatal("Query should have been canceled within 2 seconds")
		}

		t.Log("Cancel request was processed successfully without hang or crash")
	})

	t.Run("CancelDuringRowStreaming", func(t *testing.T) {
		dsn := buildDSN(nil)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		errCh := make(chan error, 1)
		go func() {
			rows, err := conn.Query(ctx, "SELECT i, pg_sleep(0.1) FROM generate_series(1, 50) i")
			if err != nil {
				errCh <- err
				return
			}
			defer rows.Close()

			for rows.Next() {
				var i int
				var sleep interface{}
				if err := rows.Scan(&i, &sleep); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- rows.Err()
		}()

		time.Sleep(200 * time.Millisecond)
		err = conn.PgConn().CancelRequest(ctx)
		require.NoError(t, err, "Cancel should be sent")

		select {
		case queryErr := <-errCh:
			t.Logf("Query result: %v", queryErr)
		case <-time.After(3 * time.Second):
			t.Fatal("Query should complete or be canceled within 3 seconds")
		}

		t.Log("Cancel request during row streaming processed successfully")
	})
}

// TestIdleTimeoutConfigurable tests that idle timeout can be configured per-session
func TestIdleTimeoutConfigurable(t *testing.T) {
	t.Run("ShortIdleTimeout", func(t *testing.T) {
		// Set a very short idle timeout (1 second is minimum since it's parsed as int)
		dsn := buildDSN(map[string]string{"idle_timeout": "1"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// First query should succeed
		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err, "First query should succeed")
		require.Equal(t, 1, result)

		// Wait longer than idle timeout
		time.Sleep(1500 * time.Millisecond)

		// Next query should fail with idle timeout error
		err = conn.QueryRow(context.Background(), "SELECT 2").Scan(&result)
		require.Error(t, err, "Query should fail after idle timeout")
		t.Logf("Expected idle timeout error: %v", err)
	})

	t.Run("DefaultIdleTimeoutWorks", func(t *testing.T) {
		// Without custom idle_timeout, default is 30 minutes
		dsn := buildDSN(nil)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Query should succeed
		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err, "Query should succeed with default timeout")
		require.Equal(t, 1, result)

		// Small wait (nowhere near 30 min default)
		time.Sleep(100 * time.Millisecond)

		// Next query should still succeed
		err = conn.QueryRow(context.Background(), "SELECT 2").Scan(&result)
		require.NoError(t, err, "Query should still succeed after short wait")
		require.Equal(t, 2, result)
	})
}

// TestWriteDeadlineTimeout tests write deadline behavior
// This test is skipped because write deadline testing is complex:
// - Write deadlines are OS-level TCP buffer deadlines
// - They only trigger when the TCP send buffer is full
// - This requires a client that reads slower than the server writes
// - Hard to reliably reproduce in a unit test
func TestWriteDeadlineTimeout(t *testing.T) {
	t.Skip("Write deadline testing requires specialized client that reads slowly - hard to test reliably")

	// If we wanted to test this, we'd need to:
	// 1. Connect with a very short query timeout
	// 2. Execute a query that produces a lot of output
	// 3. Have a client that deliberately reads slowly
	// 4. Expect the server to timeout on write
	//
	// The query timeout (peerdb.query_timeout) affects statement_timeout on the
	// upstream connection, not write deadlines. Write deadlines are handled
	// by the OS TCP stack.
}

func BenchmarkSimpleQuery(b *testing.B) {
	cmd := exec.Command("psql", append(basePsqlArgs(), "-c", "SELECT 1")...) //nolint:gosec
	cmd.Env = psqlEnv()
	if err := cmd.Run(); err != nil {
		b.Skip("Test server not available for benchmark")
	}

	b.ResetTimer()
	for range b.N {
		cmd := exec.Command("psql", append(basePsqlArgs(), "-tA", "-c", "SELECT 1")...) //nolint:gosec
		cmd.Env = psqlEnv()
		_, _ = cmd.Output()
	}
}
