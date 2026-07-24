package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type SwitchboardPostgresSuite struct {
	t      *testing.T
	peer   *protos.Peer
	suffix string
}

func (s SwitchboardPostgresSuite) T() *testing.T {
	return s.t
}

func (s SwitchboardPostgresSuite) Teardown(context.Context) {
	// No cleanup needed - catalog peer is shared
}

func SetupSwitchboardPostgresSuite(t *testing.T) SwitchboardPostgresSuite {
	t.Helper()

	suffix := "switchboard_pg_" + strings.ToLower(common.RandomString(8))
	peer := GeneratePostgresPeer(t)

	// Verify Switchboard is available
	conn, err := pgx.Connect(t.Context(), switchboardDSN(peer.Name, nil))
	if err != nil {
		t.Fatalf("Switchboard not available: %v", err)
	}
	conn.Close(t.Context())

	return SwitchboardPostgresSuite{
		t:      t,
		peer:   peer,
		suffix: suffix,
	}
}

func TestSwitchboardPostgres(t *testing.T) {
	e2eshared.RunSuite(t, SetupSwitchboardPostgresSuite)
}

// psql executes a SQL query via psql against catalog_ro (read-only) and returns tuples-only output
func (s SwitchboardPostgresSuite) psql(sql string) (string, error) {
	return runPsql(s.t, "catalog_ro", "-tA", "-c", sql)
}

// psqlExec executes a SQL statement via psql against catalog_ro (read-only, no output expected)
func (s SwitchboardPostgresSuite) psqlExec(sql string) error {
	_, err := runPsql(s.t, "catalog_ro", "-c", sql)
	return err
}

// psqlRW executes a SQL query via psql against catalog (writable) and returns tuples-only output
func (s SwitchboardPostgresSuite) psqlRW(sql string) (string, error) {
	return runPsql(s.t, s.peer.Name, "-tA", "-c", sql)
}

// psqlExecRW executes a SQL statement via psql against catalog (writable, no output expected)
func (s SwitchboardPostgresSuite) psqlExecRW(sql string) error {
	_, err := runPsql(s.t, s.peer.Name, "-c", sql)
	return err
}

// ========================================
// Basic Connectivity
// ========================================

func (s SwitchboardPostgresSuite) Test_BasicConnectivity_SimpleSelect() {
	output, err := s.psql("SELECT 1 AS test")
	require.NoError(s.t, err)
	require.Equal(s.t, "1", output)
}

func (s SwitchboardPostgresSuite) Test_BasicConnectivity_SelectVersion() {
	output, err := s.psql("SELECT version()")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "PostgreSQL")
}

func (s SwitchboardPostgresSuite) Test_BasicConnectivity_CurrentDatabase() {
	output, err := s.psql("SELECT current_database()")
	require.NoError(s.t, err)
	require.NotEmpty(s.t, output)
}

// ========================================
// Data Types
// ========================================

func (s SwitchboardPostgresSuite) Test_DataTypes() {
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
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

// ========================================
// Query Complexity
// ========================================

func (s SwitchboardPostgresSuite) Test_QueryComplexity_GenerateSeries() {
	output, err := s.psql("SELECT * FROM generate_series(1, 5)")
	require.NoError(s.t, err)
	require.Equal(s.t, "1\n2\n3\n4\n5", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_MultipleColumns() {
	output, err := s.psql("SELECT 1 AS a, 2 AS b, 3 AS c")
	require.NoError(s.t, err)
	require.Equal(s.t, "1|2|3", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_Aggregates() {
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
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_Subquery() {
	output, err := s.psql(`
		SELECT * FROM (
			SELECT i * 2 AS doubled
			FROM generate_series(1, 3) AS t(i)
		) sub
		WHERE doubled > 2
	`)
	require.NoError(s.t, err)
	require.Equal(s.t, "4\n6", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_SubqueryInWhere() {
	output, err := s.psql(`
		SELECT i FROM generate_series(1, 5) AS t(i)
		WHERE i IN (SELECT 2 UNION SELECT 4)
	`)
	require.NoError(s.t, err)
	require.Equal(s.t, "2\n4", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_CTE() {
	output, err := s.psql(`
		WITH doubled AS (
			SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)
		)
		SELECT val FROM doubled WHERE val > 2
	`)
	require.NoError(s.t, err)
	require.Equal(s.t, "4\n6", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_SelfJoin() {
	output, err := s.psql(`
		SELECT a.i, b.i
		FROM generate_series(1, 3) AS a(i)
		CROSS JOIN generate_series(1, 2) AS b(i)
		ORDER BY a.i, b.i
	`)
	require.NoError(s.t, err)
	lines := strings.Split(output, "\n")
	require.Len(s.t, lines, 6)
	require.Equal(s.t, "1|1", lines[0])
	require.Equal(s.t, "3|2", lines[5])
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_WindowFunction() {
	output, err := s.psql(`
		SELECT i, ROW_NUMBER() OVER (ORDER BY i) AS rn
		FROM generate_series(1, 3) AS t(i)
	`)
	require.NoError(s.t, err)
	require.Equal(s.t, "1|1\n2|2\n3|3", output)
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_WindowFunctionLag() {
	output, err := s.psql(`
		SELECT i, LAG(i, 1) OVER (ORDER BY i) AS prev
		FROM generate_series(1, 3) AS t(i)
	`)
	require.NoError(s.t, err)
	lines := strings.Split(output, "\n")
	require.Equal(s.t, "1|", lines[0])
	require.Equal(s.t, "2|1", lines[1])
	require.Equal(s.t, "3|2", lines[2])
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_MultipleCTEs() {
	output, err := s.psql(`
		WITH
			doubled AS (SELECT i * 2 AS val FROM generate_series(1, 3) AS t(i)),
			tripled AS (SELECT i * 3 AS val FROM generate_series(1, 3) AS t(i))
		SELECT * FROM doubled UNION ALL SELECT * FROM tripled ORDER BY val
	`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "2")
	require.Contains(s.t, output, "9")
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_BooleanLogic() {
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
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func (s SwitchboardPostgresSuite) Test_QueryComplexity_EmptyResults() {
	s.t.Run("NoRows", func(t *testing.T) {
		output, err := s.psql("SELECT 1 WHERE FALSE")
		require.NoError(t, err)
		require.Empty(t, output)
	})

	s.t.Run("EmptyResultSet", func(t *testing.T) {
		output, err := s.psql("SELECT COUNT(*) FROM (SELECT 1 WHERE FALSE) sq")
		require.NoError(t, err)
		require.Equal(t, "0", output)
	})
}

// ========================================
// Transactions
// ========================================

func (s SwitchboardPostgresSuite) Test_Transaction_CommitTransaction() {
	output, err := s.psql(`
		BEGIN;
		SELECT 'in_transaction';
		COMMIT;
		SELECT 'after_commit';
	`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "in_transaction")
	require.Contains(s.t, output, "after_commit")
}

func (s SwitchboardPostgresSuite) Test_Transaction_RollbackTransaction() {
	output, err := s.psql(`
		BEGIN;
		SELECT 'before_rollback';
		ROLLBACK;
		SELECT 'after_rollback';
	`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "after_rollback")
}

// ========================================
// Error Handling
// ========================================

func (s SwitchboardPostgresSuite) Test_Error_SyntaxError() {
	_, err := s.psql("SELCT 1")
	require.Error(s.t, err)
}

func (s SwitchboardPostgresSuite) Test_Error_TableNotExists() {
	_, err := s.psql("SELECT * FROM nonexistent_table_xyz")
	require.Error(s.t, err)
}

func (s SwitchboardPostgresSuite) Test_Error_ColumnNotExists() {
	_, err := s.psql("SELECT nonexistent_column FROM pg_class LIMIT 1")
	require.Error(s.t, err)
}

func (s SwitchboardPostgresSuite) Test_Error_TypeMismatch() {
	_, err := s.psql("SELECT 1 WHERE 1 = 'not_a_number'")
	require.Error(s.t, err)
}

// ========================================
// Catalog Queries
// ========================================

func (s SwitchboardPostgresSuite) Test_Catalog_DescribeCommand() {
	// Describe pg_class system table and verify output contains expected columns
	output, err := runPsql(s.t, s.peer.Name, "-c", "\\d pg_class")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "relname", "Should show relname column")
	require.Contains(s.t, output, "relnamespace", "Should show relnamespace column")
}

func (s SwitchboardPostgresSuite) Test_Catalog_QuerySystemCatalog() {
	output, err := s.psql("SELECT COUNT(*) > 0 FROM pg_class")
	require.NoError(s.t, err)
	require.Equal(s.t, "t", output)
}

// ========================================
// Large Results
// ========================================

func (s SwitchboardPostgresSuite) Test_LargeResults_ManyRows() {
	output, err := s.psql("SELECT * FROM generate_series(1, 1000)")
	require.NoError(s.t, err)
	lines := strings.Split(output, "\n")
	require.Len(s.t, lines, 1000)
	require.Equal(s.t, "1", lines[0])
	require.Equal(s.t, "1000", lines[999])
}

func (s SwitchboardPostgresSuite) Test_LargeResults_LargeText() {
	output, err := s.psql("SELECT repeat('x', 1000000)")
	require.NoError(s.t, err)
	require.Len(s.t, output, 1000000)
}

// ========================================
// Null Handling
// ========================================

func (s SwitchboardPostgresSuite) Test_NullHandling_SelectNull() {
	output, err := s.psql("SELECT NULL")
	require.NoError(s.t, err)
	require.Empty(s.t, output)
}

func (s SwitchboardPostgresSuite) Test_NullHandling_NullCoalesce() {
	output, err := s.psql("SELECT COALESCE(NULL, 'default')")
	require.NoError(s.t, err)
	require.Equal(s.t, "default", output)
}

func (s SwitchboardPostgresSuite) Test_NullHandling_IsNull() {
	output, err := s.psql("SELECT NULL IS NULL")
	require.NoError(s.t, err)
	require.Equal(s.t, "t", output)
}

// ========================================
// Special Characters
// ========================================

func (s SwitchboardPostgresSuite) Test_SpecialCharacters() {
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
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(fmt.Sprintf("SELECT $$%s$$", tt.value))
			require.NoError(t, err)
			require.Equal(t, tt.value, output)
		})
	}
}

// ========================================
// Multi-Statement Queries
// ========================================

func (s SwitchboardPostgresSuite) Test_MultiStatement_TwoSelects() {
	output, err := s.psql("SELECT 1 AS first; SELECT 2 AS second;")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "2")
}

func (s SwitchboardPostgresSuite) Test_MultiStatement_ThreeSelects() {
	output, err := s.psql("SELECT 'first'; SELECT 'second'; SELECT 'third';")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "first")
	require.Contains(s.t, output, "second")
	require.Contains(s.t, output, "third")
}

func (s SwitchboardPostgresSuite) Test_MultiStatement_TransactionWithMultipleSelects() {
	output, err := s.psql("BEGIN; SELECT 1 AS a; SELECT 2 AS b; COMMIT;")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "2")
}

func (s SwitchboardPostgresSuite) Test_MultiStatement_MixedStatementsWithResults() {
	output, err := s.psql(`
		SELECT 'setup' AS step;
		SELECT val FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, val) ORDER BY id;
	`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "a")
	require.Contains(s.t, output, "b")
}

// ========================================
// Read-Only Enforcement
// ========================================

func (s SwitchboardPostgresSuite) Test_ReadOnly_BypassAttemptBlocked() {
	output, err := s.psql("SET default_transaction_read_only = off")
	require.Error(s.t, err)
	require.Contains(s.t, output, "read-only mode")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_SetLocalBlocked() {
	output, err := s.psql("SET LOCAL default_transaction_read_only = off")
	require.Error(s.t, err)
	require.Contains(s.t, output, "read-only mode")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_ResetBlocked() {
	output, err := s.psql("RESET default_transaction_read_only")
	require.Error(s.t, err)
	require.Contains(s.t, output, "read-only mode")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_ResetAllBlocked() {
	output, err := s.psql("RESET ALL")
	require.Error(s.t, err)
	require.Contains(s.t, output, "RESET ALL")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_DiscardAllBlocked() {
	output, err := s.psql("DISCARD ALL")
	require.Error(s.t, err)
	require.Contains(s.t, output, "DISCARD ALL")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_SetConfigBlocked() {
	output, err := s.psql("SELECT set_config('work_mem', '64MB', false)")
	require.Error(s.t, err)
	require.Contains(s.t, output, "set_config")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_ObfuscatedSetConfigBlocked() {
	// Attempt to bypass by obfuscating the setting name via string concatenation
	output, err := s.psql("SELECT set_config('default_transaction' || '_read_only', 'off', false)")
	require.Error(s.t, err)
	require.Contains(s.t, output, "set_config")
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_HarmlessSetAllowed() {
	err := s.psqlExec("SET work_mem = '64MB'")
	require.NoError(s.t, err)
}

func (s SwitchboardPostgresSuite) Test_ReadOnly_DiscardPlansAllowed() {
	err := s.psqlExec("DISCARD PLANS")
	require.NoError(s.t, err)
}

// ========================================
// Blocked Commands
// ========================================

func (s SwitchboardPostgresSuite) Test_BlockedCommands() {
	tests := []struct {
		name string
		sql  string
	}{
		{"COPY", "COPY (SELECT 1) TO STDOUT"},
		{"VACUUM", "VACUUM"},
		{"ANALYZE", "ANALYZE"},
		{"CLUSTER", "CLUSTER"},
		{"REINDEX", "REINDEX DATABASE postgres"},
		{"REFRESH", "REFRESH MATERIALIZED VIEW nonexistent"},
		{"LISTEN", "LISTEN test_channel"},
		{"NOTIFY", "NOTIFY test_channel"},
		{"UNLISTEN", "UNLISTEN test_channel"},
		{"DO", "DO $$ BEGIN NULL; END $$"},
		{"LOCK", "LOCK TABLE pg_class IN ACCESS SHARE MODE"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.Error(t, err, "%s should be blocked", tt.name)
			require.Contains(t, output, "not allowed", "Should mention not allowed")
			require.Contains(t, output, tt.name, "Should mention %s", tt.name)
		})
	}
}

// ========================================
// Guardrails
// ========================================

func (s SwitchboardPostgresSuite) Test_Guardrails_RowLimitExceeded() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_rows": "3"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), "SELECT * FROM generate_series(1, 10)")
	require.Error(s.t, err, "Query should fail due to row limit")
	require.Contains(s.t, err.Error(), "row limit")
}

func (s SwitchboardPostgresSuite) Test_Guardrails_WithinRowLimit() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_rows": "3"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	rows, err := conn.Query(s.t.Context(), "SELECT * FROM generate_series(1, 2)")
	require.NoError(s.t, err, "Query should succeed within row limit")
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.Equal(s.t, 2, count)
}

func (s SwitchboardPostgresSuite) Test_Guardrails_ByteLimitExceeded() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_bytes": "32"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), "SELECT repeat('x', 20) FROM generate_series(1, 5)")
	require.Error(s.t, err, "Query should fail due to byte limit")
	require.Contains(s.t, err.Error(), "byte limit")
}

func (s SwitchboardPostgresSuite) Test_Guardrails_ConnectionUsableAfterLimitExceeded() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_rows": "3"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// First query exceeds limit
	rows, err := conn.Query(s.t.Context(), "SELECT * FROM generate_series(1, 10)")
	require.NoError(s.t, err, "Query should start successfully")

	var rowErr error
	for rows.Next() {
	}
	rowErr = rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to row limit")
	require.Contains(s.t, rowErr.Error(), "row limit")

	// Connection should still be usable
	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 42").Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after limit exceeded")
	require.Equal(s.t, 42, result)
}

func (s SwitchboardPostgresSuite) Test_Guardrails_NoOOM() {
	if testing.Short() {
		s.t.Skip("Skipping stress test in short mode")
	}

	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_rows": "100"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// This would generate 2 million rows, but we limit to 100
	_, err = conn.Exec(s.t.Context(), "SELECT * FROM generate_series(1, 2000000)")
	require.Error(s.t, err, "Query should fail due to row limit")
	require.Contains(s.t, err.Error(), "row limit")
}

func (s SwitchboardPostgresSuite) Test_Guardrails_ByteLimitWithNulls() {
	s.t.Run("NullsCountOverhead", func(t *testing.T) {
		dsn := switchboardDSN(s.peer.Name, map[string]string{"max_bytes": "100"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		_, err = conn.Exec(s.t.Context(), "SELECT NULL, NULL, NULL FROM generate_series(1, 20)")
		require.Error(t, err, "Should hit byte limit even with NULLs")
		require.Contains(t, err.Error(), "byte limit")
	})

	s.t.Run("LongTextExceedsLimit", func(t *testing.T) {
		dsn := switchboardDSN(s.peer.Name, map[string]string{"max_bytes": "100"})

		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		_, err = conn.Exec(s.t.Context(), "SELECT repeat('a', 50) FROM generate_series(1, 5)")
		require.Error(t, err, "Should hit byte limit with long text")
		require.Contains(t, err.Error(), "byte limit")
	})
}

// ========================================
// Empty Query
// ========================================

func (s SwitchboardPostgresSuite) Test_EmptyQuery() {
	tests := []struct {
		name string
		sql  string
	}{
		{"EmptySemicolon", ";"},
		{"MultipleSemicolons", ";;;"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			err := s.psqlExec(tt.sql)
			require.NoError(t, err, "Empty query should succeed")
		})
	}
}

func (s SwitchboardPostgresSuite) Test_EmptyQuery_WithRealQuery() {
	output, err := s.psql("; SELECT 1; ;")
	require.NoError(s.t, err, "Mixed empty and real queries should succeed")
	require.Contains(s.t, output, "1")
}

// ========================================
// Mid-Batch Error
// ========================================

func (s SwitchboardPostgresSuite) Test_MidBatchError_ErrorStopsExecution() {
	output, err := s.psql("SELECT 111 + 222; SELCT 2; SELECT 777 + 222")
	require.Error(s.t, err, "Query batch should fail on syntax error")
	errorText := output
	if err != nil {
		errorText += err.Error()
	}
	require.Contains(s.t, strings.ToLower(errorText), "not allowed", "Should report statement not allowed")
	require.NotContains(s.t, output, "333", "First query result should not appear")
	require.NotContains(s.t, output, "999", "Third query result should not appear")
}

func (s SwitchboardPostgresSuite) Test_MidBatchError_ErrorInTransaction() {
	output, err := s.psql("BEGIN; SELECT 1; SELCT 2; SELECT 3; COMMIT")
	require.Error(s.t, err, "Transaction should fail on syntax error")
	errorText := output
	if err != nil {
		errorText += err.Error()
	}
	require.Contains(s.t, strings.ToLower(errorText), "not allowed", "Should report statement not allowed")
}

// ========================================
// Extended Protocol Rejection
// ========================================

func (s SwitchboardPostgresSuite) Test_ExtendedProtocol_PrepareStatementRejected() {
	dsn := switchboardDSN(s.peer.Name, nil)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Prepare(s.t.Context(), "test_stmt", "SELECT $1::int")
	require.Error(s.t, err, "Extended protocol (Parse) should be rejected")
	require.Contains(s.t, err.Error(), "extended protocol not supported")
}

func (s SwitchboardPostgresSuite) Test_ExtendedProtocol_SimpleProtocolStillWorks() {
	dsn := switchboardDSN(s.peer.Name, nil)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 42").Scan(&result)
	require.NoError(s.t, err, "Simple protocol should work")
	require.Equal(s.t, 42, result)
}

func (s SwitchboardPostgresSuite) Test_ExtendedProtocol_ParameterizedQueryRejected() {
	dsn := switchboardDSN(s.peer.Name, nil)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	// QueryExecModeExec uses extended protocol
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// Try a simple query with this mode - should fail
	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 1").Scan(&result)
	require.Error(s.t, err, "Extended protocol should be rejected")
	require.Contains(s.t, err.Error(), "extended protocol not supported")
}

// ========================================
// Cancel Request
// ========================================

func (s SwitchboardPostgresSuite) Test_CancelRequest() {
	s.t.Run("CancelLongRunningQuery", func(t *testing.T) {
		dsn := switchboardDSN(s.peer.Name, nil)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		errCh := make(chan error, 1)
		go func() {
			var result int
			err := conn.QueryRow(s.t.Context(), "SELECT pg_sleep(10)").Scan(&result)
			errCh <- err
		}()

		time.Sleep(100 * time.Millisecond)

		err = conn.PgConn().CancelRequest(s.t.Context())
		require.NoError(t, err, "Cancel request should be sent")

		select {
		case queryErr := <-errCh:
			require.Error(t, queryErr, "Query should be canceled")
			t.Logf("Query error (expected cancellation): %v", queryErr)
		case <-time.After(5 * time.Second):
			t.Fatal("Query should have been canceled within 5 seconds")
		}
	})
}

func (s SwitchboardPostgresSuite) Test_CancelRequest_SpuriousCancelRequest() {
	dsn := switchboardDSN(s.peer.Name, nil)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	cfg2, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg2.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn2, err := pgx.ConnectConfig(s.t.Context(), cfg2)
	require.NoError(s.t, err)
	defer conn2.Close(s.t.Context())

	// Send cancel from conn2 (wrong connection) - should have no effect
	err = conn2.PgConn().CancelRequest(s.t.Context())
	require.NoError(s.t, err)

	// Original connection should still work fine
	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 1").Scan(&result)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, result)
}

// ========================================
// SSL Rejection
// ========================================

func (s SwitchboardPostgresSuite) Test_SSL_RejectionWithFallback() {
	dsn := fmt.Sprintf("postgres://peerdb:peerdb@127.0.0.1:5732/%s?sslmode=prefer", s.peer.Name)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err, "Should connect with sslmode=prefer and fall back to plaintext")
	defer conn.Close(s.t.Context())

	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 1").Scan(&result)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, result)
}

func (s SwitchboardPostgresSuite) Test_SSL_RequiredFails() {
	dsn := fmt.Sprintf("postgres://peerdb:peerdb@127.0.0.1:5732/%s?sslmode=require", s.peer.Name)

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	_, err = pgx.ConnectConfig(s.t.Context(), cfg)
	require.Error(s.t, err, "Should fail with sslmode=require")
	errorLower := strings.ToLower(err.Error())
	require.True(s.t, strings.Contains(errorLower, "ssl") || strings.Contains(errorLower, "tls"), "Should mention SSL or TLS in error")
}

// ========================================
// Concurrent Connections
// ========================================

func (s SwitchboardPostgresSuite) Test_ConcurrentConnections() {
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			output, err := s.psql(fmt.Sprintf("SELECT %d AS conn_id, pg_sleep(0.1)", id))
			if err != nil {
				errors <- fmt.Errorf("connection %d failed: %w (output: %s)", id, err, output)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		require.NoError(s.t, err)
	}
}

// ========================================
// Sequential Connections
// ========================================

func (s SwitchboardPostgresSuite) Test_MultipleSequentialConnections() {
	for i := range 10 {
		output, err := s.psql(fmt.Sprintf("SELECT %d", i))
		require.NoError(s.t, err)
		require.Equal(s.t, strconv.Itoa(i), output)
	}
}

// ========================================
// Idle Timeout
// ========================================

func (s SwitchboardPostgresSuite) Test_IdleTimeout_Short() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"idle_timeout": "1"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 1").Scan(&result)
	require.NoError(s.t, err, "First query should succeed")
	require.Equal(s.t, 1, result)

	time.Sleep(1500 * time.Millisecond)

	err = conn.QueryRow(s.t.Context(), "SELECT 2").Scan(&result)
	require.Error(s.t, err, "Query should fail after idle timeout")
}

// ========================================
// Catalog Write Access
// ========================================

func (s SwitchboardPostgresSuite) Test_Catalog_Writable() {
	tableName := "switchboard_test_" + s.suffix

	err := s.psqlExecRW(fmt.Sprintf("CREATE TABLE %s (id int, val text)", tableName))
	require.NoError(s.t, err, "CREATE TABLE should succeed on catalog")

	err = s.psqlExecRW(fmt.Sprintf("INSERT INTO %s VALUES (1, 'hello')", tableName))
	require.NoError(s.t, err, "INSERT should succeed on catalog")

	output, err := s.psqlRW(fmt.Sprintf("SELECT val FROM %s WHERE id = 1", tableName))
	require.NoError(s.t, err)
	require.Equal(s.t, "hello", output)

	err = s.psqlExecRW("DROP TABLE " + tableName)
	require.NoError(s.t, err, "DROP TABLE should succeed on catalog")
}
