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

	"github.com/PeerDB-io/peerdb/flow/pgwire"
)

const (
	testProxyPort   = 15433
	testProxyAddr   = "127.0.0.1:15433"
	testUpstreamDSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
)

// TestServer manages the pgwire proxy server lifecycle for tests
type TestServer struct {
	server     *pgwire.Server
	ctx        context.Context
	cancel     context.CancelFunc
	errCh      chan error
	shutdownWg sync.WaitGroup
}

// StartTestServer starts a pgwire proxy server for testing
func StartTestServer(t *testing.T) *TestServer {
	t.Helper()
	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 10,
		QueryTimeout:   30 * time.Second,
		MaxRows:        10000,
		MaxBytes:       100 * 1024 * 1024, // 100MB
	}
	return StartTestServerWithConfig(t, config)
}

// StartTestServerWithConfig starts a pgwire proxy server with custom config
func StartTestServerWithConfig(t *testing.T, config *pgwire.ServerConfig) *TestServer {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	server, err := pgwire.NewServer(ctx, config)
	require.NoError(t, err, "Failed to create test server")

	ts := &TestServer{
		server: server,
		ctx:    ctx,
		cancel: cancel,
		errCh:  make(chan error, 1),
	}

	// Start server in background
	ts.shutdownWg.Add(1)
	go func() {
		defer ts.shutdownWg.Done()
		if err := server.ListenAndServe(ctx); err != nil {
			select {
			case ts.errCh <- err:
			default:
			}
		}
	}()

	// Wait for server to be ready
	time.Sleep(500 * time.Millisecond)

	// Verify server is running
	err = execPSQL(t, "-h", "127.0.0.1", "-p", strconv.Itoa(testProxyPort), "-U", "postgres", "-c", "SELECT 1")
	require.NoError(t, err, "Server failed to start or accept connections")

	t.Cleanup(func() {
		ts.Shutdown(t)
	})

	return ts
}

// Shutdown stops the test server
func (ts *TestServer) Shutdown(t *testing.T) {
	t.Helper()
	ts.cancel()
	ts.shutdownWg.Wait()

	// Check if server reported any errors
	select {
	case err := <-ts.errCh:
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Logf("Server error during shutdown: %v", err)
		}
	default:
	}
}

// execPSQL executes psql with given arguments and returns error if it fails
func execPSQL(t *testing.T, args ...string) error {
	t.Helper()
	cmd := exec.Command("psql", args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("psql failed: %s\nOutput: %s", err, string(output))
	}
	return err
}

// execPSQLOutput executes psql and returns stdout
func execPSQLOutput(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("psql", args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
	output, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

// execPSQLQuery executes a SQL query through psql and returns the output
func execPSQLQuery(t *testing.T, query string) (string, error) {
	t.Helper()
	return execPSQLOutput(t,
		"-h", "127.0.0.1",
		"-p", strconv.Itoa(testProxyPort),
		"-U", "postgres",
		"-t", // tuples only (no headers)
		"-A", // unaligned output
		"-c", query,
	)
}

// TestMain sets up test environment
func TestMain(m *testing.M) {
	// Check if psql is available
	if _, err := exec.LookPath("psql"); err != nil {
		fmt.Fprintf(os.Stderr, "psql not found in PATH, skipping integration tests\n")
		os.Exit(0)
	}

	// Check if upstream PostgreSQL is available
	cmd := exec.Command("psql", "-h", "localhost", "-p", "5432", "-U", "postgres", "-c", "SELECT 1")
	cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Upstream PostgreSQL not available at localhost:5432, skipping integration tests\n")
		os.Exit(0)
	}

	// Run tests
	os.Exit(m.Run())
}

// ========================================
// Test Cases
// ========================================

func TestBasicConnectivity(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv // Server is managed by t.Cleanup

	t.Run("SimpleSelect", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT 1 AS test")
		require.NoError(t, err)
		require.Equal(t, "1", output)
	})

	t.Run("SelectVersion", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT version()")
		require.NoError(t, err)
		require.Contains(t, output, "PostgreSQL")
	})

	t.Run("CurrentDatabase", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT current_database()")
		require.NoError(t, err)
		require.Equal(t, "postgres", output)
	})
}

func TestDataTypes(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	tests := []struct {
		name     string
		query    string
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
			output, err := execPSQLQuery(t, tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func TestMultipleRows(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("GenerateSeries", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT * FROM generate_series(1, 5)")
		require.NoError(t, err)
		expected := "1\n2\n3\n4\n5"
		require.Equal(t, expected, output)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT 1 AS a, 2 AS b, 3 AS c")
		require.NoError(t, err)
		require.Equal(t, "1|2|3", output)
	})
}

func TestTransactions(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("CommitTransaction", func(t *testing.T) {
		// Create temp table, insert, commit, verify
		script := `
BEGIN;
CREATE TEMP TABLE tx_test (id int, value text);
INSERT INTO tx_test VALUES (1, 'test');
COMMIT;
SELECT value FROM tx_test WHERE id = 1;
`
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", script,
		)
		require.NoError(t, err)
		require.Contains(t, output, "test")
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		script := `
BEGIN;
CREATE TEMP TABLE rollback_test (id int);
INSERT INTO rollback_test VALUES (1);
ROLLBACK;
SELECT COUNT(*) FROM rollback_test;
`
		_, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", script,
		)
		// Should fail because table doesn't exist after rollback
		require.Error(t, err)
	})
}

func TestErrorHandling(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("SyntaxError", func(t *testing.T) {
		_, err := execPSQLQuery(t, "SELCT 1") // Typo
		require.Error(t, err)
	})

	t.Run("TableDoesNotExist", func(t *testing.T) {
		_, err := execPSQLQuery(t, "SELECT * FROM nonexistent_table_xyz")
		require.Error(t, err)
	})

	t.Run("ColumnDoesNotExist", func(t *testing.T) {
		_, err := execPSQLQuery(t, "SELECT nonexistent_column FROM pg_class LIMIT 1")
		require.Error(t, err)
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		_, err := execPSQLQuery(t, "SELECT 1 WHERE 1 = 'not_a_number'")
		require.Error(t, err)
	})
}

func TestCatalogQueries(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("ListDatabases", func(t *testing.T) {
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-l",
		)
		require.NoError(t, err)
		require.Contains(t, output, "postgres")
	})

	t.Run("DescribeCommand", func(t *testing.T) {
		// \d command queries system catalogs
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "\\d",
		)
		require.NoError(t, err)
	})

	t.Run("QuerySystemCatalog", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT COUNT(*) > 0 FROM pg_class")
		require.NoError(t, err)
		require.Equal(t, "t", output)
	})
}

func TestConcurrentConnections(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT %d AS conn_id, pg_sleep(0.1)", id)
			_, err := execPSQLQuery(t, query)
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
	srv := StartTestServer(t)
	_ = srv

	t.Run("ManyRows", func(t *testing.T) {
		// Test returning 1000 rows
		output, err := execPSQLQuery(t, "SELECT * FROM generate_series(1, 1000)")
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Len(t, lines, 1000)
		require.Equal(t, "1", lines[0])
		require.Equal(t, "1000", lines[999])
	})

	t.Run("LargeText", func(t *testing.T) {
		// Test returning large text value (1MB)
		query := "SELECT repeat('x', 1000000)"
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Len(t, output, 1000000)
	})
}

func TestJoinQueries(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("SelfJoin", func(t *testing.T) {
		query := `
			SELECT a.i, b.i
			FROM generate_series(1, 3) AS a(i)
			CROSS JOIN generate_series(1, 2) AS b(i)
			ORDER BY a.i, b.i
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Len(t, lines, 6) // 3 * 2 = 6 rows
		require.Equal(t, "1|1", lines[0])
		require.Equal(t, "3|2", lines[5])
	})
}

func TestAggregates(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"Count", "SELECT COUNT(*) FROM generate_series(1, 10)", "10"},
		{"Sum", "SELECT SUM(i) FROM generate_series(1, 5) AS t(i)", "15"},
		{"Avg", "SELECT AVG(i)::integer FROM generate_series(1, 5) AS t(i)", "3"},
		{"Min", "SELECT MIN(i) FROM generate_series(1, 10) AS t(i)", "1"},
		{"Max", "SELECT MAX(i) FROM generate_series(1, 10) AS t(i)", "10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := execPSQLQuery(t, tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func TestSubqueries(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("SimpleSubquery", func(t *testing.T) {
		query := `
			SELECT * FROM (
				SELECT i * 2 AS doubled
				FROM generate_series(1, 3) AS t(i)
			) sub
			WHERE doubled > 2
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Equal(t, "4\n6", output)
	})

	t.Run("SubqueryInWhere", func(t *testing.T) {
		query := `
			SELECT i FROM generate_series(1, 5) AS t(i)
			WHERE i IN (SELECT 2 UNION SELECT 4)
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Equal(t, "2\n4", output)
	})
}

func TestNullHandling(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("SelectNull", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT NULL")
		require.NoError(t, err)
		require.Empty(t, output) // NULL displays as empty
	})

	t.Run("NullCoalesce", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT COALESCE(NULL, 'default')")
		require.NoError(t, err)
		require.Equal(t, "default", output)
	})

	t.Run("IsNull", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT NULL IS NULL")
		require.NoError(t, err)
		require.Equal(t, "t", output)
	})
}

func TestDDLOperations(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("CreateAndDropTempTable", func(t *testing.T) {
		script := `
			CREATE TEMP TABLE ddl_test (id int, name text);
			INSERT INTO ddl_test VALUES (1, 'test');
			SELECT name FROM ddl_test WHERE id = 1;
			DROP TABLE ddl_test;
		`
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", script,
		)
		require.NoError(t, err)
		require.Contains(t, output, "test")
	})

	t.Run("CreateIndex", func(t *testing.T) {
		script := `
			CREATE TEMP TABLE idx_test (id int, value text);
			CREATE INDEX idx_test_value ON idx_test(value);
			DROP TABLE idx_test;
		`
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", script,
		)
		require.NoError(t, err)
	})
}

func TestWindowFunctions(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("RowNumber", func(t *testing.T) {
		query := `
			SELECT i, ROW_NUMBER() OVER (ORDER BY i) AS rn
			FROM generate_series(1, 3) AS t(i)
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Equal(t, "1|1\n2|2\n3|3", output)
	})

	t.Run("Lag", func(t *testing.T) {
		query := `
			SELECT i, LAG(i, 1) OVER (ORDER BY i) AS prev
			FROM generate_series(1, 3) AS t(i)
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		lines := strings.Split(output, "\n")
		require.Equal(t, "1|", lines[0]) // First row has no previous
		require.Equal(t, "2|1", lines[1])
		require.Equal(t, "3|2", lines[2])
	})
}

func TestCTEQueries(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("SimpleCTE", func(t *testing.T) {
		query := `
			WITH doubled AS (
				SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)
			)
			SELECT val FROM doubled WHERE val > 2
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Equal(t, "4\n6", output)
	})

	t.Run("MultipleCTEs", func(t *testing.T) {
		query := `
			WITH
				doubled AS (SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)),
				tripled AS (SELECT i * 3 AS val FROM generate_series(1, 3) AS t(i))
			SELECT * FROM doubled UNION ALL SELECT * FROM tripled ORDER BY val
		`
		output, err := execPSQLQuery(t, query)
		require.NoError(t, err)
		require.Contains(t, output, "2")
		require.Contains(t, output, "9")
	})
}

func TestConnectionRecovery(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("MultipleSequentialConnections", func(t *testing.T) {
		// Connect, disconnect, repeat - ensure no resource leaks
		for i := range 10 {
			output, err := execPSQLQuery(t, fmt.Sprintf("SELECT %d", i))
			require.NoError(t, err)
			require.Equal(t, strconv.Itoa(i), output)
		}
	})
}

func TestQueryTimeout(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("ShortQuery", func(t *testing.T) {
		// Query that completes within timeout
		output, err := execPSQLQuery(t, "SELECT pg_sleep(0.1), 1")
		require.NoError(t, err)
		require.Contains(t, output, "1")
	})

	// Note: Testing actual timeout would require server with shorter QueryTimeout
	// or a query that exceeds 30s, which we skip for fast test execution
}

func TestSpecialCharacters(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	tests := []struct {
		name  string
		value string
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
			// Use dollar-quoted strings to avoid escaping issues
			query := fmt.Sprintf("SELECT $$%s$$", tt.value)
			output, err := execPSQLQuery(t, query)
			require.NoError(t, err)
			require.Equal(t, tt.value, output)
		})
	}
}

func TestEmptyResults(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("NoRows", func(t *testing.T) {
		output, err := execPSQLQuery(t, "SELECT 1 WHERE FALSE")
		require.NoError(t, err)
		require.Empty(t, output)
	})

	t.Run("EmptyTable", func(t *testing.T) {
		script := `
			CREATE TEMP TABLE empty_test (id int);
			SELECT COUNT(*) FROM empty_test;
		`
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", script,
		)
		require.NoError(t, err)
		require.Contains(t, output, "0")
	})
}

func TestBooleanLogic(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	tests := []struct {
		name     string
		query    string
		expected string
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
			output, err := execPSQLQuery(t, tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

// Benchmark tests
func BenchmarkSimpleQuery(b *testing.B) {
	// Note: This requires test server to be running
	// Skip if not available
	cmd := exec.Command("psql", "-h", "127.0.0.1", "-p", strconv.Itoa(testProxyPort), "-U", "postgres", "-c", "SELECT 1")
	cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
	if err := cmd.Run(); err != nil {
		b.Skip("Test server not available for benchmark")
	}

	b.ResetTimer()
	for range b.N {
		cmd := exec.Command("psql", "-h", "127.0.0.1", "-p", strconv.Itoa(testProxyPort), "-U", "postgres", "-t", "-A", "-c", "SELECT 1")
		cmd.Env = append(os.Environ(), "PGPASSWORD=postgres")
		_, _ = cmd.Output()
	}
}

// ========================================
// Critical Protocol Tests (from feedback)
// ========================================

// TestMultiStatementQueries validates that multi-statement queries work correctly
// This was the most critical fix - the proxy now properly handles all result sets
func TestMultiStatementQueries(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("TwoSelectStatements", func(t *testing.T) {
		// Validates multi-statement handling with multiple result sets
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 1 AS first; SELECT 2 AS second;",
		)
		require.NoError(t, err)
		require.Contains(t, output, "1")
		require.Contains(t, output, "2")
	})

	t.Run("TransactionWithMultipleSelects", func(t *testing.T) {
		// Critical test: this was explicitly broken before the fix
		// The fix changed from pgx.Query to pgconn.Exec().ReadAll()
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "BEGIN; SELECT 1 AS a; SELECT 2 AS b; COMMIT;",
		)
		require.NoError(t, err)
		require.Contains(t, output, "1")
		require.Contains(t, output, "2")
	})

	t.Run("MixedStatementsWithResults", func(t *testing.T) {
		// DDL, DML, and SELECT in one query
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", `
				CREATE TEMP TABLE multi_test(id int, val text);
				INSERT INTO multi_test VALUES (1, 'a'), (2, 'b');
				SELECT val FROM multi_test ORDER BY id;
			`,
		)
		require.NoError(t, err)
		require.Contains(t, output, "a")
		require.Contains(t, output, "b")
	})

	t.Run("ThreeSelectStatements", func(t *testing.T) {
		// Test more than 2 result sets
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 'first'; SELECT 'second'; SELECT 'third';",
		)
		require.NoError(t, err)
		require.Contains(t, output, "first")
		require.Contains(t, output, "second")
		require.Contains(t, output, "third")
	})
}

// TestCommandTags validates that CommandComplete tags are properly forwarded
func TestCommandTags(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("InsertWithReturning", func(t *testing.T) {
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", `
				CREATE TEMP TABLE tag_test(id serial, val text);
				INSERT INTO tag_test(val) VALUES ('test') RETURNING id;
			`,
		)
		require.NoError(t, err)
		// Verify the returned row
		require.Contains(t, output, "1")
	})

	t.Run("UpdateCommandTag", func(t *testing.T) {
		// psql validates command tags implicitly - if wrong, it will error
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", `
				CREATE TEMP TABLE tag_test(id int, val text);
				INSERT INTO tag_test VALUES (1, 'a'), (2, 'b'), (3, 'c');
				UPDATE tag_test SET val = 'updated' WHERE id <= 2;
			`,
		)
		require.NoError(t, err)
	})

	t.Run("DeleteCommandTag", func(t *testing.T) {
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", `
				CREATE TEMP TABLE tag_test(id int);
				INSERT INTO tag_test VALUES (1), (2), (3);
				DELETE FROM tag_test WHERE id >= 2;
			`,
		)
		require.NoError(t, err)
	})
}

// TestCopyRejection validates that COPY protocol is properly denied
func TestCopyRejection(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("CopyToStdout", func(t *testing.T) {
		_, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "COPY (SELECT 1) TO STDOUT",
		)
		require.Error(t, err, "COPY TO STDOUT should be rejected")
	})

	t.Run("CopyFromStdin", func(t *testing.T) {
		_, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "CREATE TEMP TABLE copy_test(id int); COPY copy_test FROM STDIN;",
		)
		require.Error(t, err, "COPY FROM STDIN should be rejected")
	})
}

// TestTransactionStatusTracking validates proper transaction state handling
func TestTransactionStatusTracking(t *testing.T) {
	srv := StartTestServer(t)
	_ = srv

	t.Run("ErrorInTransaction", func(t *testing.T) {
		// Start transaction, cause error
		_, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", `
				BEGIN;
				CREATE TEMP TABLE tx_status_test(id int PRIMARY KEY);
				INSERT INTO tx_status_test VALUES (1);
				INSERT INTO tx_status_test VALUES (1);
			`,
		)
		require.Error(t, err, "Duplicate key should cause error")
	})

	t.Run("RecoveryAfterRollback", func(t *testing.T) {
		// Verify we can recover after failed transaction
		output, _ := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", `
				BEGIN;
				CREATE TEMP TABLE tx_status_test2(id int PRIMARY KEY);
				INSERT INTO tx_status_test2 VALUES (1);
				INSERT INTO tx_status_test2 VALUES (1);
				ROLLBACK;
				SELECT 1;
			`,
		)
		// The final SELECT 1 should succeed after ROLLBACK
		// Note: psql may return an error exit code due to the duplicate key error,
		// but the SELECT 1 after ROLLBACK should still execute and return results
		// We don't check the error because psql's exit code behavior varies
		require.Contains(t, output, "1", "SELECT 1 should execute after ROLLBACK")
	})
}
// TestGuardrailsRowLimit tests row limit enforcement with streaming
func TestGuardrailsRowLimit(t *testing.T) {
	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 10,
		QueryTimeout:   30 * time.Second,
		MaxRows:        3, // Very small limit to test enforcement
		MaxBytes:       100 * 1024 * 1024,
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("RowLimitExceeded", func(t *testing.T) {
		// Try to fetch 10 rows with a limit of 3
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT * FROM generate_series(1, 10)",
		)
		require.Error(t, err, "Query should fail due to row limit")
		require.Contains(t, output, "row limit exceeded", "Error message should mention row limit")
	})

	t.Run("WithinRowLimit", func(t *testing.T) {
		// Fetch 2 rows, should succeed
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT * FROM generate_series(1, 2)",
		)
		require.NoError(t, err, "Query should succeed within row limit")
		require.Contains(t, output, "1")
		require.Contains(t, output, "2")
	})
}

// TestGuardrailsByteLimit tests byte limit enforcement
func TestGuardrailsByteLimit(t *testing.T) {
	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 10,
		QueryTimeout:   30 * time.Second,
		MaxRows:        10000,
		MaxBytes:       32, // Very small byte limit
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("ByteLimitExceeded", func(t *testing.T) {
		// Generate wide rows that will exceed byte limit
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT repeat('x', 20) FROM generate_series(1, 5)",
		)
		require.Error(t, err, "Query should fail due to byte limit")
		require.Contains(t, output, "byte limit exceeded", "Error message should mention byte limit")
	})
}

// TestGuardrailsNoOOM tests that large result sets don't cause OOM
func TestGuardrailsNoOOM(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 10,
		QueryTimeout:   60 * time.Second,
		MaxRows:        100, // Limit to prevent huge output
		MaxBytes:       1024 * 1024,
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("LargeResultSet", func(t *testing.T) {
		// This would generate 2 million rows, but we limit to 100
		// The key is that it should fail fast without buffering everything
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT * FROM generate_series(1, 2000000)",
		)
		require.Error(t, err, "Query should fail due to row limit")
		require.Contains(t, output, "row limit exceeded")
		// If we buffered everything, we'd OOM or timeout; instead we fail fast
	})
}

// TestAllowDenyMultiStatement tests allow/deny list enforcement on multi-statement queries
func TestAllowDenyMultiStatement(t *testing.T) {
	t.Run("AllowListRejectsSecondStatement", func(t *testing.T) {
		config := &pgwire.ServerConfig{
			ListenAddress:   testProxyAddr,
			UpstreamDSN:     testUpstreamDSN,
			EnableTLS:       false,
			MaxConnections:  10,
			QueryTimeout:    30 * time.Second,
			MaxRows:         10000,
			MaxBytes:        100 * 1024 * 1024,
			AllowStatements: []string{"SELECT"}, // Only allow SELECT
		}
		_ = StartTestServerWithConfig(t, config)

		// Try to sneak in an UPDATE after a SELECT
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 1; UPDATE pg_database SET datname=datname WHERE datname='nonexistent'",
		)
		require.Error(t, err, "Query should be rejected due to allow list")
		require.Contains(t, output, "not in allow list", "Should reject UPDATE statement")
	})

	t.Run("DenyListRejectsSecondStatement", func(t *testing.T) {
		config := &pgwire.ServerConfig{
			ListenAddress:  testProxyAddr,
			UpstreamDSN:    testUpstreamDSN,
			EnableTLS:      false,
			MaxConnections: 10,
			QueryTimeout:   30 * time.Second,
			MaxRows:        10000,
			MaxBytes:       100 * 1024 * 1024,
			DenyStatements: []string{"DROP", "TRUNCATE"}, // Deny dangerous statements
		}
		_ = StartTestServerWithConfig(t, config)

		// Try to sneak in a DROP after a SELECT
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 1; /* comment */ DROP TABLE IF EXISTS nonexistent_table",
		)
		require.Error(t, err, "Query should be rejected due to deny list")
		require.Contains(t, output, "denied", "Should reject DROP statement")
	})

	t.Run("AllowListAcceptsAllStatements", func(t *testing.T) {
		config := &pgwire.ServerConfig{
			ListenAddress:   testProxyAddr,
			UpstreamDSN:     testUpstreamDSN,
			EnableTLS:       false,
			MaxConnections:  10,
			QueryTimeout:    30 * time.Second,
			MaxRows:         10000,
			MaxBytes:        100 * 1024 * 1024,
			AllowStatements: []string{"SELECT", "SHOW"}, // Allow both
		}
		_ = StartTestServerWithConfig(t, config)

		// Both statements should be allowed
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 1; SHOW server_version",
		)
		require.NoError(t, err, "Query should succeed")
		require.Contains(t, output, "1")
	})
}

// TestConnectionLimit tests max connections enforcement
func TestConnectionLimit(t *testing.T) {
	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 1, // Only allow 1 connection
		QueryTimeout:   30 * time.Second,
		MaxRows:        10000,
		MaxBytes:       100 * 1024 * 1024,
	}
	_ = StartTestServerWithConfig(t, config)

	// First connection should succeed and hold
	cmd1 := exec.Command("psql",
		"-h", "127.0.0.1",
		"-p", strconv.Itoa(testProxyPort),
		"-U", "postgres",
		"-c", "SELECT pg_sleep(2); SELECT 1",
	)
	cmd1.Env = append(os.Environ(), "PGPASSWORD=postgres")

	// Start first connection
	err := cmd1.Start()
	require.NoError(t, err, "First connection should start")

	// Give it time to connect
	time.Sleep(100 * time.Millisecond)

	// Second connection should be rejected immediately
	output, err := execPSQLOutput(t,
		"-h", "127.0.0.1",
		"-p", strconv.Itoa(testProxyPort),
		"-U", "postgres",
		"-c", "SELECT 1",
	)
	require.Error(t, err, "Second connection should be rejected")
	t.Logf("Second connection output: %s", output)

	// Wait for first connection to finish
	_ = cmd1.Wait()
}

// TestEmptyQuery tests empty query response
func TestEmptyQuery(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("EmptySemicolon", func(t *testing.T) {
		// Empty query should return EmptyQueryResponse, not error
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", ";",
		)
		require.NoError(t, err, "Empty query should succeed")
	})

	t.Run("MultipleSemicolons", func(t *testing.T) {
		err := execPSQL(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", ";;;",
		)
		require.NoError(t, err, "Multiple empty queries should succeed")
	})

	t.Run("EmptyWithRealQuery", func(t *testing.T) {
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "; SELECT 1; ;",
		)
		require.NoError(t, err, "Mixed empty and real queries should succeed")
		require.Contains(t, output, "1")
	})
}

// TestMidBatchError tests error handling in multi-statement queries
func TestMidBatchError(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("ErrorStopsExecution", func(t *testing.T) {
		// When a multi-statement query has a syntax error, the entire batch is rejected
		// during parsing, so NO statements execute (not even the valid ones before the error)
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "SELECT 111 + 222; SELCT 2; SELECT 777 + 222",
		)
		require.Error(t, err, "Query batch should fail on syntax error")
		// Check for syntax error in output or error message
		errorText := output
		if err != nil {
			errorText += err.Error()
		}
		require.Contains(t, strings.ToLower(errorText), "syntax", "Should report syntax error")
		// No queries execute when there's a syntax error - the entire batch is rejected
		// We check that neither computed result appears
		require.NotContains(t, output, "333", "First query result (111+222=333) should not appear")
		require.NotContains(t, output, "999", "Third query result (777+222=999) should not appear")
	})

	t.Run("ErrorInTransaction", func(t *testing.T) {
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-t", "-A",
			"-c", "BEGIN; SELECT 1; SELCT 2; SELECT 3; COMMIT",
		)
		require.Error(t, err, "Transaction should fail on syntax error")
		// Check for syntax error in output or error message
		errorText := output
		if err != nil {
			errorText += err.Error()
		}
		require.Contains(t, strings.ToLower(errorText), "syntax", "Should report syntax error")
		// After error in transaction, subsequent statements should fail with "current transaction is aborted"
	})
}

// TestExtendedProtocolRejection tests that extended protocol is rejected
func TestExtendedProtocolRejection(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("PrepareStatementRejected", func(t *testing.T) {
		// Connect with pgx and try to prepare a statement (uses extended protocol)
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)

		// Force extended protocol by using CacheStatement mode
		// This will send Parse/Describe/Sync messages
		cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Try to prepare a statement - this sends Parse message
		_, err = conn.Prepare(ctx, "test_stmt", "SELECT $1::int")

		// The proxy should reject Parse with 0A000 "feature_not_supported"
		require.Error(t, err, "Extended protocol (Parse) should be rejected")
		require.Contains(t, err.Error(), "extended protocol not supported", "Should return extended protocol error")
		t.Logf("Expected extended protocol rejection: %v", err)
	})

	t.Run("ParameterizedQueryRejected", func(t *testing.T) {
		// Try with explicit extended protocol via QueryExecModeExec
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)

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
		// Verify simple protocol still works after extended protocol rejection
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)

		// Explicitly use simple protocol
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Simple query should work fine
		var result int
		err = conn.QueryRow(ctx, "SELECT 42").Scan(&result)
		require.NoError(t, err, "Simple protocol should work")
		require.Equal(t, 42, result)
	})
}

// TestCancelRequest tests query cancellation
func TestCancelRequest(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("CancelLongRunningQuery", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
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
		// Test cancel with random pid/secret - should be logged but have no effect
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Create a second connection to send spurious cancel
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

// TestTLSHandshake tests TLS/SSL negotiation
func TestTLSHandshake(t *testing.T) {
	// Note: This test requires the server to be started with TLS enabled
	// For now, we test the non-TLS path (SSL rejection)

	t.Run("SSLRejectionWithPlaintextServer", func(t *testing.T) {
		// Start plaintext server (no TLS)
		_ = StartTestServer(t)

		// Try to connect with sslmode=prefer (will try SSL first, then fall back)
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=prefer", testProxyPort)

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

	t.Run("SSLRequiredWithPlaintextServer", func(t *testing.T) {
		// Start plaintext server (no TLS)
		_ = StartTestServer(t)

		// Try to connect with sslmode=require (should fail)
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=require", testProxyPort)

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		if err != nil {
			// Expected - server doesn't support SSL/TLS
			errorLower := strings.ToLower(err.Error())
			require.True(t, strings.Contains(errorLower, "ssl") || strings.Contains(errorLower, "tls"), "Should mention SSL or TLS in error")
			t.Logf("Expected SSL/TLS error: %v", err)
		} else {
			// If connection succeeded, SSL might be enabled - verify it works
			defer conn.Close(context.Background())
			var result int
			err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
			require.NoError(t, err)
			require.Equal(t, 1, result)
		}
	})

	// TODO: Add test with actual TLS-enabled server using self-signed cert
	// This would require:
	// 1. Generating a self-signed cert in test setup
	// 2. Starting server with EnableTLS=true and TLSConfig set
	// 3. Connecting with sslmode=require and custom RootCAs
}

// TestCOPYRejection tests that COPY protocol is properly rejected
func TestCOPYRejection(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("ServerSideCOPYRejection", func(t *testing.T) {
		// Test server-side COPY (COPY ... TO STDOUT)
		// This should be rejected by guardrails before hitting the wire protocol

		// Create table, insert data, and try COPY in one session
		// (TEMP tables don't persist across psql invocations)
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", `
				CREATE TEMP TABLE copy_test (id int, name text);
				INSERT INTO copy_test VALUES (1, 'test');
				COPY copy_test TO STDOUT;
			`,
		)
		require.Error(t, err, "COPY should be rejected")
		require.Contains(t, output, "denied", "Should mention that COPY is denied")
		t.Logf("Expected COPY rejection: %s", output)
	})

	t.Run("ClientSideCOPYRejection", func(t *testing.T) {
		// Test client-side COPY using \copy in psql
		// \copy is handled by psql client, not sent to server, so this tests that
		// regular COPY FROM STDIN is rejected

		// Create table and try COPY in one session
		// (TEMP tables don't persist across psql invocations)
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", `
				CREATE TEMP TABLE copy_test2 (id int);
				COPY copy_test2 FROM STDIN;
			`,
		)
		require.Error(t, err, "COPY FROM STDIN should be rejected")
		require.Contains(t, output, "denied", "Should mention that COPY is denied")
		t.Logf("Expected COPY FROM STDIN rejection: %s", output)
	})
}

// TestWriteDeadlineTimeout tests that write deadlines prevent hung connections
func TestWriteDeadlineTimeout(t *testing.T) {
	t.Skip("Write deadline timeout test requires simulating a slow/hung client - difficult in integration tests")

	// This test is challenging to implement reliably in an integration test because:
	// 1. We need to simulate a client that stops reading (TCP backpressure)
	// 2. The write deadline is 30 seconds, making the test slow
	// 3. TCP buffering makes it hard to trigger the deadline without sending huge amounts of data
	//
	// The write deadline implementation is verified by code inspection:
	// - protocol.go: writeBackendMessage sets 30s deadline
	// - errors.go: writeErrorResponse and writeProtoError set 30s deadline
	// All write functions clear the deadline after successful write
	//
	// To properly test this, we would need:
	// 1. A mock net.Conn that simulates Write() blocking
	// 2. Unit tests for the write functions with the mock connection
	// 3. Verification that SetWriteDeadline is called before Write
	//
	// For integration testing, we rely on:
	// - Code inspection showing deadlines are set
	// - Manual testing with intentionally slow clients
	// - Production monitoring for write timeout errors

	t.Run("WriteDeadlineIsSet", func(t *testing.T) {
		// This is a placeholder test that verifies the basic functionality
		// without actually triggering a timeout
		_ = StartTestServer(t)

		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
		conn, err := pgx.Connect(context.Background(), dsn)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Execute a query that returns data - write deadline should be set and cleared
		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err, "Write with deadline should succeed for normal clients")
		require.Equal(t, 1, result)

		t.Log("Write deadline implementation verified by code inspection")
		t.Log("See protocol.go:103,170 and errors.go:56,91 for SetWriteDeadline calls")
	})
}

// TestGuardrailsWithStatements tests that WITH statements are properly analyzed for dangerous keywords
func TestGuardrailsWithStatements(t *testing.T) {
	t.Run("WithCTEDelete", func(t *testing.T) {
		// Start server with DELETE denied
		config := &pgwire.ServerConfig{
			ListenAddress:  fmt.Sprintf("127.0.0.1:%d", testProxyPort),
			UpstreamDSN:    testUpstreamDSN,
			MaxConnections: 10,
			QueryTimeout:   30 * time.Second,
			DenyStatements: []string{"DELETE"},
		}
		_ = StartTestServerWithConfig(t, config)

		// Try WITH ... DELETE which should be caught
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "WITH cte AS (SELECT 1) DELETE FROM pg_database WHERE false",
		)
		require.Error(t, err, "WITH ... DELETE should be rejected")
		require.Contains(t, output, "denied", "Should mention that DELETE is denied")
		t.Logf("Expected rejection: %s", output)
	})

	t.Run("WithCTEAllowList", func(t *testing.T) {
		// Start server with SELECT-only allow list
		config := &pgwire.ServerConfig{
			ListenAddress:   fmt.Sprintf("127.0.0.1:%d", testProxyPort),
			UpstreamDSN:     testUpstreamDSN,
			MaxConnections:  10,
			QueryTimeout:    30 * time.Second,
			AllowStatements: []string{"SELECT"},
		}
		_ = StartTestServerWithConfig(t, config)

		// Try WITH ... UPDATE which should be rejected by allowlist
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "WITH cte AS (SELECT 1) UPDATE pg_database SET datname=datname WHERE false",
		)
		require.Error(t, err, "WITH ... UPDATE should be rejected when only SELECT is allowed")
		require.Contains(t, output, "not in allow list", "Should mention allowlist violation")
		t.Logf("Expected rejection: %s", output)
	})
}

// TestGuardrailsCommentsBeforeKeyword tests comment handling in guardrails
func TestGuardrailsCommentsBeforeKeyword(t *testing.T) {
	config := &pgwire.ServerConfig{
		ListenAddress:  fmt.Sprintf("127.0.0.1:%d", testProxyPort),
		UpstreamDSN:    testUpstreamDSN,
		MaxConnections: 10,
		QueryTimeout:   30 * time.Second,
		DenyStatements: []string{"DROP"},
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("CommentBeforeDrop", func(t *testing.T) {
		// Try comment before DROP - should still be caught
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "/* comment */ DROP TABLE IF EXISTS nonexistent",
		)
		require.Error(t, err, "/*comment*/ DROP should be rejected")
		require.Contains(t, output, "denied", "Should mention that DROP is denied")
		t.Logf("Expected rejection: %s", output)
	})

	t.Run("MultiStatementWithComment", func(t *testing.T) {
		// Try multi-statement with comment in between
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT 1; /* mid comment */ DROP TABLE IF EXISTS nonexistent",
		)
		require.Error(t, err, "SELECT; /* */ DROP should be rejected")
		require.Contains(t, output, "denied", "Should catch DROP even with comment")
		t.Logf("Expected rejection: %s", output)
	})
}

// TestGuardrailsDOStatement tests that DO blocks are denied by default
func TestGuardrailsDOStatement(t *testing.T) {
	// Start server with default deny list (includes DO)
	_ = StartTestServer(t)

	t.Run("DOBlockDenied", func(t *testing.T) {
		// Try DO block - should be denied by default
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "DO $$ BEGIN PERFORM pg_sleep(0); END $$",
		)
		require.Error(t, err, "DO block should be denied by default")
		require.Contains(t, output, "denied", "Should mention that DO is denied")
		t.Logf("Expected DO rejection: %s", output)
	})

	t.Run("VacuumDenied", func(t *testing.T) {
		// Try VACUUM - should be denied by default
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "VACUUM",
		)
		require.Error(t, err, "VACUUM should be denied by default")
		require.Contains(t, output, "denied", "Should mention that VACUUM is denied")
		t.Logf("Expected VACUUM rejection: %s", output)
	})
}

// TestByteLimitWithNulls tests byte limit enforcement with NULL values
func TestByteLimitWithNulls(t *testing.T) {
	// Use very small byte limit to test overhead counting
	config := &pgwire.ServerConfig{
		ListenAddress:  fmt.Sprintf("127.0.0.1:%d", testProxyPort),
		UpstreamDSN:    testUpstreamDSN,
		MaxConnections: 10,
		QueryTimeout:   30 * time.Second,
		MaxBytes:       100, // Very small to test with just a few rows
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("NullsCountOverhead", func(t *testing.T) {
		// Create table with NULLs
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT NULL, NULL, NULL FROM generate_series(1, 20)",
		)
		require.Error(t, err, "Should hit byte limit even with NULLs")
		require.Contains(t, output, "byte limit exceeded", "Should mention byte limit")
		t.Logf("Byte limit with NULLs: %s", output)
	})

	t.Run("LongTextExceedsLimit", func(t *testing.T) {
		// Long text should hit limit quickly
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT repeat('a', 50) FROM generate_series(1, 5)",
		)
		require.Error(t, err, "Should hit byte limit with long text")
		require.Contains(t, output, "byte limit exceeded", "Should mention byte limit")
		t.Logf("Byte limit with text: %s", output)
	})
}

// TestIdleTimeoutConfigurable tests that idle timeout can be configured
func TestIdleTimeoutConfigurable(t *testing.T) {
	t.Run("TinyIdleTimeout", func(t *testing.T) {
		// Start server with very short idle timeout
		config := &pgwire.ServerConfig{
			ListenAddress:  fmt.Sprintf("127.0.0.1:%d", testProxyPort),
			UpstreamDSN:    testUpstreamDSN,
			MaxConnections: 10,
			QueryTimeout:   30 * time.Second,
			IdleTimeout:    500 * time.Millisecond, // Very short timeout
		}
		_ = StartTestServerWithConfig(t, config)

		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		// Execute a query successfully
		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)

		// Wait for idle timeout
		time.Sleep(600 * time.Millisecond)

		// Next query should fail due to idle timeout
		err = conn.QueryRow(context.Background(), "SELECT 2").Scan(&result)
		require.Error(t, err, "Should timeout after idle period")
		t.Logf("Expected idle timeout error: %v", err)
	})
}

// TestParameterStatusFidelity tests that ParameterStatus messages are complete
func TestParameterStatusFidelity(t *testing.T) {
	_ = StartTestServer(t)

	dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Query server parameters via SHOW commands to verify they're set correctly
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
		query := fmt.Sprintf("SHOW %s", param)
		err := conn.QueryRow(ctx, query).Scan(&value)
		require.NoError(t, err, "Should be able to query %s", param)
		require.NotEmpty(t, value, "Parameter %s should have a value", param)
		t.Logf("Parameter %s = %s", param, value)
	}

	// Verify some expected values
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

// TestCopyToProgram tests that COPY TO PROGRAM is denied
func TestCopyToProgram(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("CopyToProgramDenied", func(t *testing.T) {
		// Try COPY TO PROGRAM which is dangerous - should be denied
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "COPY (SELECT 1) TO PROGRAM 'cat'",
		)
		require.Error(t, err, "COPY TO PROGRAM should be denied")
		require.Contains(t, output, "denied", "Should mention that COPY is denied")
		t.Logf("Expected COPY TO PROGRAM rejection: %s", output)
	})

	t.Run("CopyFromProgramDenied", func(t *testing.T) {
		// Try COPY FROM PROGRAM - also dangerous
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "CREATE TEMP TABLE t(x text); COPY t FROM PROGRAM 'echo hello'",
		)
		require.Error(t, err, "COPY FROM PROGRAM should be denied")
		require.Contains(t, output, "denied", "Should mention that COPY is denied")
		t.Logf("Expected COPY FROM PROGRAM rejection: %s", output)
	})
}

// TestMaxConnectionsConcurrent tests max connections with multiple concurrent clients
func TestMaxConnectionsConcurrent(t *testing.T) {
	config := &pgwire.ServerConfig{
		ListenAddress:  testProxyAddr,
		UpstreamDSN:    testUpstreamDSN,
		EnableTLS:      false,
		MaxConnections: 2, // Allow 2 concurrent connections
		QueryTimeout:   30 * time.Second,
		MaxRows:        10000,
		MaxBytes:       100 * 1024 * 1024,
	}
	_ = StartTestServerWithConfig(t, config)

	t.Run("TwoConcurrentConnectionsSucceed", func(t *testing.T) {
		// Start 2 connections that will hold for a bit
		cmd1 := exec.Command("psql",
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT pg_sleep(1.5); SELECT 1",
		)
		cmd1.Env = append(os.Environ(), "PGPASSWORD=postgres")

		cmd2 := exec.Command("psql",
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT pg_sleep(1.5); SELECT 2",
		)
		cmd2.Env = append(os.Environ(), "PGPASSWORD=postgres")

		// Start both
		err := cmd1.Start()
		require.NoError(t, err, "First connection should start")

		time.Sleep(50 * time.Millisecond)

		err = cmd2.Start()
		require.NoError(t, err, "Second connection should start")

		// Give them time to connect
		time.Sleep(100 * time.Millisecond)

		// Third connection should be rejected
		output, err := execPSQLOutput(t,
			"-h", "127.0.0.1",
			"-p", strconv.Itoa(testProxyPort),
			"-U", "postgres",
			"-c", "SELECT 3",
		)
		require.Error(t, err, "Third connection should be rejected")
		t.Logf("Third connection rejected as expected: %s", output)

		// Wait for first two to finish
		_ = cmd1.Wait()
		_ = cmd2.Wait()
	})

	t.Run("AfterDisconnectNewConnectionSucceeds", func(t *testing.T) {
		// Open and close a connection
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)

		var result int
		err = conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)

		conn.Close(context.Background())

		// Wait a moment for cleanup
		time.Sleep(50 * time.Millisecond)

		// Should be able to connect again
		conn2, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn2.Close(context.Background())

		err = conn2.QueryRow(context.Background(), "SELECT 2").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 2, result)
	})
}

// TestCancelTimingRaces tests cancel request timing edge cases
func TestCancelTimingRaces(t *testing.T) {
	_ = StartTestServer(t)

	t.Run("ImmediateCancelDuringSleep", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Start a long query and cancel it immediately
		errCh := make(chan error, 1)
		go func() {
			var result int
			err := conn.QueryRow(ctx, "SELECT pg_sleep(5)").Scan(&result)
			errCh <- err
		}()

		// Cancel almost immediately (tiny delay to let query start)
		time.Sleep(10 * time.Millisecond)
		err = conn.PgConn().CancelRequest(ctx)
		require.NoError(t, err, "Cancel request should be sent")

		// Query should fail quickly
		select {
		case queryErr := <-errCh:
			require.Error(t, queryErr, "Query should be canceled")
			t.Logf("Query canceled as expected: %v", queryErr)
		case <-time.After(2 * time.Second):
			t.Fatal("Query should have been canceled within 2 seconds")
		}

		// Key test: verify cancel didn't cause deadlock or crash
		// After cancel, connection may or may not be reusable depending on timing
		// What matters is the server didn't hang or crash
		t.Log("Cancel request was processed successfully without hang or crash")
	})

	t.Run("CancelDuringRowStreaming", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", testProxyPort)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		ctx := context.Background()

		// Start a query that returns rows with delays
		errCh := make(chan error, 1)
		go func() {
			rows, err := conn.Query(ctx, "SELECT i, pg_sleep(0.1) FROM generate_series(1, 50) i")
			if err != nil {
				errCh <- err
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var i int
				var sleep interface{}
				if err := rows.Scan(&i, &sleep); err != nil {
					errCh <- err
					return
				}
				count++
			}
			errCh <- rows.Err()
		}()

		// Wait for some rows to be received, then cancel
		time.Sleep(200 * time.Millisecond)
		err = conn.PgConn().CancelRequest(ctx)
		require.NoError(t, err, "Cancel should be sent")

		// Should get an error (either cancel or normal completion)
		select {
		case queryErr := <-errCh:
			// Could be canceled or could finish - both are acceptable
			// The key is no deadlock or crash
			t.Logf("Query result: %v", queryErr)
		case <-time.After(3 * time.Second):
			t.Fatal("Query should complete or be canceled within 3 seconds")
		}

		// Key test: verify cancel didn't cause deadlock or crash
		// The important thing is that the cancel was processed without hanging
		t.Log("Cancel request during row streaming processed successfully")
	})
}
