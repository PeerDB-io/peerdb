package e2e

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

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// pgwireDSN builds a connection string for the pgwire proxy
func pgwireDSN(peer string, options map[string]string) string {
	dsn := fmt.Sprintf("postgres://peerdb:peerdb@127.0.0.1:5732/%s?sslmode=disable", peer)
	if len(options) > 0 {
		var optParts []string
		for k, v := range options {
			optParts = append(optParts, fmt.Sprintf("-c peerdb.%s=%s", k, v))
		}
		dsn += "&options=" + strings.Join(optParts, " ")
	}
	return dsn
}

// runPsql executes psql against the pgwire proxy
func runPsql(t *testing.T, peer string, extraArgs ...string) (string, error) {
	t.Helper()
	args := append([]string{"-h", "127.0.0.1", "-p", "5732", "-d", peer, "-U", "peerdb"}, extraArgs...)
	cmd := exec.Command("psql", args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD=peerdb")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("psql failed: %s\nOutput: %s", err, string(output))
	}
	return strings.TrimSpace(string(output)), err
}

type PgwireMySQLSuite struct {
	t      *testing.T
	source *MySqlSource
	peer   *protos.Peer
	suffix string
}

func (s PgwireMySQLSuite) T() *testing.T {
	return s.t
}

func (s PgwireMySQLSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
}

func SetupPgwireMySQLSuite(t *testing.T) PgwireMySQLSuite {
	t.Helper()

	suffix := "pgwmy_" + strings.ToLower(shared.RandomString(8))
	source, err := SetupMySQL(t, suffix)
	if err != nil {
		t.Skipf("MySQL setup failed: %v", err)
	}

	// Set the database in the config so pgwire connects to it
	source.Config.Database = "e2e_test_" + suffix

	// Create peer with unique name to avoid caching issues
	peer := &protos.Peer{
		Name: "mysql_" + suffix,
		Type: protos.DBType_MYSQL,
		Config: &protos.Peer_MysqlConfig{
			MysqlConfig: source.Config,
		},
	}
	CreatePeer(t, peer)

	// Create test table with sample data
	testTable := fmt.Sprintf(`e2e_test_%s.test_table`, suffix)
	require.NoError(t, source.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value DECIMAL(10,2)
		)
	`, testTable)))

	// Insert test data
	require.NoError(t, source.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, name, value) VALUES
		(1, 'alice', 100.50),
		(2, 'bob', 200.75),
		(3, 'charlie', 300.00)
	`, testTable)))

	// Verify pgwire proxy is available
	conn, err := pgx.Connect(t.Context(), pgwireDSN(peer.Name, nil))
	if err != nil {
		source.Teardown(t, t.Context(), suffix)
		t.Skipf("PgWire proxy not available: %v", err)
	}
	conn.Close(t.Context())

	return PgwireMySQLSuite{
		t:      t,
		source: source,
		peer:   peer,
		suffix: suffix,
	}
}

// queryWithOptions executes a query with custom connection options
func (s PgwireMySQLSuite) queryWithOptions(sql string, options map[string]string) error {
	dsn := pgwireDSN(s.peer.Name, options)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	if err != nil {
		return err
	}
	defer conn.Close(s.t.Context())

	rows, err := conn.Query(s.t.Context(), sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// drain rows
	}
	return rows.Err()
}

// psql executes a SQL query via psql and returns tuples-only output
func (s PgwireMySQLSuite) psql(sql string) (string, error) {
	return runPsql(s.t, s.peer.Name, "-tA", "-c", sql)
}

// testTable returns the fully qualified test table name
func (s PgwireMySQLSuite) testTable() string {
	return fmt.Sprintf("e2e_test_%s.test_table", s.suffix)
}

func TestPgwireMySQL(t *testing.T) {
	e2eshared.RunSuite(t, SetupPgwireMySQLSuite)
}

// ========================================
// Basic Connectivity
// ========================================

func (s PgwireMySQLSuite) Test_BasicConnectivity_SimpleSelect() {
	output, err := s.psql("SELECT 1")
	require.NoError(s.t, err)
	require.Equal(s.t, "1", output)
}

func (s PgwireMySQLSuite) Test_BasicConnectivity_SelectVersion() {
	output, err := s.psql("SELECT VERSION()")
	require.NoError(s.t, err)
	require.NotEmpty(s.t, output)
}

func (s PgwireMySQLSuite) Test_BasicConnectivity_CurrentDatabase() {
	output, err := s.psql("SELECT DATABASE()")
	require.NoError(s.t, err)
	require.Contains(s.t, output, s.suffix)
}

// ========================================
// Data Types
// ========================================

func (s PgwireMySQLSuite) Test_DataTypes() {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"Integer", "SELECT 42", "42"},
		{"BigInt", "SELECT 9223372036854775807", "9223372036854775807"},
		{"Text", "SELECT 'hello'", "hello"},
		{"Decimal", "SELECT 123.456", "123.456"},
		{"Boolean_True", "SELECT TRUE", "1"},
		{"Boolean_False", "SELECT FALSE", "0"},
		{"Timestamp", "SELECT TIMESTAMP '2024-01-01 12:00:00'", "2024-01-01 12:00:00"},
		{"NULL", "SELECT NULL", ""},
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

func (s PgwireMySQLSuite) Test_QueryComplexity_MultipleRows() {
	output, err := s.psql("SELECT 1 UNION SELECT 2 UNION SELECT 3")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "2")
	require.Contains(s.t, output, "3")
}

func (s PgwireMySQLSuite) Test_QueryComplexity_MultipleColumns() {
	output, err := s.psql(fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id LIMIT 1", s.testTable()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "alice")
}

func (s PgwireMySQLSuite) Test_QueryComplexity_SelfJoin() {
	output, err := s.psql(fmt.Sprintf(
		"SELECT a.id, b.name FROM %s a JOIN %s b ON a.id = b.id ORDER BY a.id LIMIT 1",
		s.testTable(), s.testTable()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "alice")
}

func (s PgwireMySQLSuite) Test_QueryComplexity_Subquery() {
	output, err := s.psql("SELECT * FROM (SELECT 1 AS x) AS sub")
	require.NoError(s.t, err)
	require.Equal(s.t, "1", output)
}

func (s PgwireMySQLSuite) Test_QueryComplexity_SubqueryInWhere() {
	output, err := s.psql(fmt.Sprintf(
		"SELECT name FROM %s WHERE id IN (SELECT id FROM %s WHERE id < 2)",
		s.testTable(), s.testTable()))
	require.NoError(s.t, err)
	require.Equal(s.t, "alice", output)
}

func (s PgwireMySQLSuite) Test_QueryComplexity_Aggregates() {
	output, err := s.psql("SELECT COUNT(*) FROM " + s.testTable())
	require.NoError(s.t, err)
	require.Equal(s.t, "3", output)
}

func (s PgwireMySQLSuite) Test_QueryComplexity_GroupBy() {
	output, err := s.psql(fmt.Sprintf(
		"SELECT COUNT(*) as cnt FROM %s GROUP BY id > 1 ORDER BY cnt",
		s.testTable()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "1")
	require.Contains(s.t, output, "2")
}

func (s PgwireMySQLSuite) Test_QueryComplexity_CTE() {
	output, err := s.psql("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte")
	require.NoError(s.t, err)
	require.Equal(s.t, "1", output)
}

func (s PgwireMySQLSuite) Test_QueryComplexity_UnionAll() {
	output, err := s.psql("SELECT 1 UNION ALL SELECT 1")
	require.NoError(s.t, err)
	require.Equal(s.t, "1\n1", output)
}

// ========================================
// SHOW/DESCRIBE/EXPLAIN
// ========================================

func (s PgwireMySQLSuite) Test_Show_Tables() {
	output, err := s.psql("SHOW TABLES FROM e2e_test_" + s.suffix)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "test_table")
}

func (s PgwireMySQLSuite) Test_Show_Databases() {
	output, err := s.psql("SHOW DATABASES")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "e2e_test_"+s.suffix)
}

func (s PgwireMySQLSuite) Test_Show_Columns() {
	output, err := s.psql("SHOW COLUMNS FROM " + s.testTable())
	require.NoError(s.t, err)
	require.Contains(s.t, output, "id")
	require.Contains(s.t, output, "name")
}

func (s PgwireMySQLSuite) Test_Show_CreateTable() {
	output, err := s.psql("SHOW CREATE TABLE " + s.testTable())
	require.NoError(s.t, err)
	require.Contains(s.t, output, "CREATE TABLE")
}

func (s PgwireMySQLSuite) Test_Show_Variables() {
	output, err := s.psql("SHOW VARIABLES LIKE 'version%'")
	require.NoError(s.t, err)
	require.Contains(s.t, output, "version")
}

func (s PgwireMySQLSuite) Test_Describe_Table() {
	output, err := s.psql("DESCRIBE " + s.testTable())
	require.NoError(s.t, err)
	require.Contains(s.t, output, "id")
	require.Contains(s.t, output, "name")
}

func (s PgwireMySQLSuite) Test_Desc_Table() {
	output, err := s.psql("DESC " + s.testTable())
	require.NoError(s.t, err)
	require.Contains(s.t, output, "id")
}

func (s PgwireMySQLSuite) Test_Explain_Select() {
	output, err := s.psql("EXPLAIN SELECT * FROM " + s.testTable())
	require.NoError(s.t, err)
	require.NotEmpty(s.t, output)
}

// ========================================
// Transactions
// ========================================

func (s PgwireMySQLSuite) Test_Transaction_BeginCommit() {
	dsn := pgwireDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), "BEGIN")
	require.NoError(s.t, err)

	rows, err := conn.Query(s.t.Context(), "SELECT 1")
	require.NoError(s.t, err)
	rows.Close()

	_, err = conn.Exec(s.t.Context(), "COMMIT")
	require.NoError(s.t, err)
}

func (s PgwireMySQLSuite) Test_Transaction_BeginRollback() {
	dsn := pgwireDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), "BEGIN")
	require.NoError(s.t, err)

	_, err = conn.Exec(s.t.Context(), "ROLLBACK")
	require.NoError(s.t, err)
}

func (s PgwireMySQLSuite) Test_Transaction_Savepoint() {
	dsn := pgwireDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), "BEGIN")
	require.NoError(s.t, err)

	_, err = conn.Exec(s.t.Context(), "SAVEPOINT sp1")
	require.NoError(s.t, err)

	_, err = conn.Exec(s.t.Context(), "RELEASE SAVEPOINT sp1")
	require.NoError(s.t, err)

	_, err = conn.Exec(s.t.Context(), "COMMIT")
	require.NoError(s.t, err)
}

// ========================================
// Blocked Statements
// ========================================

func (s PgwireMySQLSuite) Test_BlockedStatements() {
	tests := []struct {
		name string
		sql  string // use %s for testTable() substitution
	}{
		// DML
		{"Insert", "INSERT INTO %s (id, name, value) VALUES (99, 'test', 0)"},
		{"Update", "UPDATE %s SET name = 'x' WHERE id = 1"},
		{"Delete", "DELETE FROM %s WHERE id = 1"},
		{"Replace", "REPLACE INTO %s (id, name, value) VALUES (1, 'x', 0)"},
		{"Truncate", "TRUNCATE TABLE %s"},
		// DDL
		{"CreateTable", "CREATE TABLE blocked_test (id INT)"},
		{"AlterTable", "ALTER TABLE %s ADD COLUMN extra INT"},
		{"DropTable", "DROP TABLE %s"},
		{"CreateIndex", "CREATE INDEX idx ON %s (name)"},
		{"CreateDatabase", "CREATE DATABASE blocked_db"},
		{"DropDatabase", "DROP DATABASE IF EXISTS blocked_db"},
		// SET
		{"SetVariable", "SET @x = 1"},
		{"SetSession", "SET SESSION sql_mode = ''"},
		{"SetNames", "SET NAMES utf8"},
		// Admin
		{"Flush", "FLUSH TABLES"},
		{"Kill", "KILL 1"},
		{"AnalyzeTable", "ANALYZE TABLE %s"},
		{"OptimizeTable", "OPTIMIZE TABLE %s"},
		// Security
		{"Grant", "GRANT SELECT ON *.* TO 'testuser'@'%'"},
		{"Revoke", "REVOKE SELECT ON *.* FROM 'testuser'@'%'"},
		{"CreateUser", "CREATE USER 'testuser'@'%' IDENTIFIED BY 'pass'"},
		{"DropUser", "DROP USER IF EXISTS 'testuser'@'%'"},
		// Stored Procedures
		{"Call", "CALL some_procedure()"},
		{"Prepare", "PREPARE stmt FROM 'SELECT 1'"},
		// Locking
		{"LockTables", "LOCK TABLES %s READ"},
		{"UnlockTables", "UNLOCK TABLES"},
		// File
		{"LoadData", "LOAD DATA INFILE '/tmp/x' INTO TABLE %s"},
		// Other
		{"Use", "USE mysql"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			sql := tt.sql
			if strings.Contains(sql, "%s") {
				sql = fmt.Sprintf(sql, s.testTable())
			}
			output, err := s.psql(sql)
			require.Error(t, err, "%s should be blocked", tt.name)
			require.Contains(t, output, "not allowed", "%s error should mention 'not allowed'", tt.name)
		})
	}
}

// ========================================
// Denied Functions
// ========================================

func (s PgwireMySQLSuite) Test_DeniedFunctions() {
	tests := []struct {
		name string
		sql  string
	}{
		{"LoadFile", "SELECT LOAD_FILE('/etc/passwd')"},
		{"GetLock", "SELECT GET_LOCK('x', 1)"},
		{"ReleaseLock", "SELECT RELEASE_LOCK('x')"},
		{"IsFreeLock", "SELECT IS_FREE_LOCK('x')"},
		{"IsUsedLock", "SELECT IS_USED_LOCK('x')"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.Error(t, err, "%s should be blocked", tt.name)
			require.Contains(t, output, "not allowed", "%s error should mention 'not allowed'", tt.name)
		})
	}
}

// ========================================
// SELECT INTO Blocked
// ========================================

func (s PgwireMySQLSuite) Test_SelectIntoBlocked() {
	// SELECT INTO OUTFILE is blocked by guardrails
	output, err := s.psql("SELECT 1 INTO OUTFILE '/tmp/x'")
	require.Error(s.t, err, "SELECT INTO OUTFILE should be blocked")
	require.Contains(s.t, output, "INTO", "error should mention 'INTO'")
}

func (s PgwireMySQLSuite) Test_SelectIntoParseFailures() {
	// These SELECT INTO variants fail to parse with TiDB parser
	tests := []struct {
		name string
		sql  string
	}{
		{"Dumpfile", "SELECT 1 INTO DUMPFILE '/tmp/x'"},
		{"Variable", "SELECT 1 INTO @x"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(tt.sql)
			require.Error(t, err, "SELECT INTO %s should fail to parse", tt.name)
			require.Contains(t, output, "failed to parse", "%s error should mention parse failure", tt.name)
		})
	}
}

// ========================================
// Locking Reads Blocked
// ========================================

func (s PgwireMySQLSuite) Test_LockingReadsBlocked() {
	tests := []struct {
		name string
		sql  string
	}{
		{"ForUpdate", "SELECT * FROM %s FOR UPDATE"},
		{"ForShare", "SELECT * FROM %s FOR SHARE"},
		{"LockInShareMode", "SELECT * FROM %s LOCK IN SHARE MODE"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			sql := fmt.Sprintf(tt.sql, s.testTable())
			output, err := s.psql(sql)
			require.Error(t, err, "%s should be blocked", tt.name)
			require.Contains(t, output, "locking", "%s error should mention 'locking'", tt.name)
		})
	}
}

// ========================================
// EXPLAIN Restrictions
// ========================================

func (s PgwireMySQLSuite) Test_Explain_SelectAllowed() {
	_, err := s.psql("EXPLAIN SELECT 1")
	require.NoError(s.t, err)
}

func (s PgwireMySQLSuite) Test_ExplainNonSelectBlocked() {
	tests := []struct {
		name string
		sql  string
	}{
		{"Insert", "EXPLAIN INSERT INTO %s (id, name, value) VALUES (99, 'x', 0)"},
		{"Update", "EXPLAIN UPDATE %s SET name = 'x'"},
		{"Delete", "EXPLAIN DELETE FROM %s"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			sql := fmt.Sprintf(tt.sql, s.testTable())
			output, err := s.psql(sql)
			require.Error(t, err, "EXPLAIN %s should be blocked", tt.name)
			require.Contains(t, output, "only allowed for SELECT", "EXPLAIN %s error should mention restriction", tt.name)
		})
	}
}

// ========================================
// Error Handling
// ========================================

func (s PgwireMySQLSuite) Test_Error_SyntaxError() {
	_, err := s.psql("SELCT 1")
	require.Error(s.t, err)
}

func (s PgwireMySQLSuite) Test_Error_TableNotExists() {
	output, err := s.psql("SELECT * FROM nonexistent_table_xyz")
	require.Error(s.t, err)
	require.Contains(s.t, output, "exist")
}

func (s PgwireMySQLSuite) Test_Error_ColumnNotExists() {
	output, err := s.psql("SELECT bad_column FROM " + s.testTable())
	require.Error(s.t, err)
	require.Contains(s.t, output, "column")
}

// ========================================
// Result Guardrails
// ========================================

func (s PgwireMySQLSuite) Test_Guardrails_RowLimitExceeded() {
	err := s.queryWithOptions(
		"SELECT * FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t",
		map[string]string{"max_rows": "3"},
	)
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "row limit")
}

func (s PgwireMySQLSuite) Test_Guardrails_WithinRowLimit() {
	err := s.queryWithOptions(
		"SELECT 1 UNION SELECT 2",
		map[string]string{"max_rows": "10"},
	)
	require.NoError(s.t, err)
}

func (s PgwireMySQLSuite) Test_Guardrails_ByteLimitExceeded() {
	err := s.queryWithOptions(
		"SELECT REPEAT('x', 1000)",
		map[string]string{"max_bytes": "100"},
	)
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "byte limit")
}

// ========================================
// Null Handling and Special Values
// ========================================

func (s PgwireMySQLSuite) Test_Special_EmptyResult() {
	output, err := s.psql(fmt.Sprintf("SELECT * FROM %s WHERE 1=0", s.testTable()))
	require.NoError(s.t, err)
	require.Empty(s.t, output)
}

func (s PgwireMySQLSuite) Test_Special_NullHandling() {
	output, err := s.psql("SELECT IFNULL(NULL, 'default')")
	require.NoError(s.t, err)
	require.Equal(s.t, "default", output)
}

func (s PgwireMySQLSuite) Test_Special_Unicode() {
	output, err := s.psql("SELECT '世界'")
	require.NoError(s.t, err)
	require.Equal(s.t, "世界", output)
}

func (s PgwireMySQLSuite) Test_Special_Quotes() {
	output, err := s.psql("SELECT 'it''s'")
	require.NoError(s.t, err)
	require.Equal(s.t, "it's", output)
}

// ========================================
// Empty Query
// ========================================

func (s PgwireMySQLSuite) Test_EmptyQuery() {
	tests := []struct {
		name string
		sql  string
	}{
		{"EmptySemicolon", ";"},
		{"MultipleSemicolons", ";;;"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			dsn := pgwireDSN(s.peer.Name, nil)
			cfg, _ := pgx.ParseConfig(dsn)
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
			require.NoError(t, err)
			defer conn.Close(s.t.Context())

			// Empty queries should be handled as no-op by the proxy
			_, err = conn.Exec(s.t.Context(), tt.sql)
			require.NoError(t, err, "Empty query %q should succeed", tt.name)
		})
	}
}

// ========================================
// Mid-Batch Error
// ========================================

func (s PgwireMySQLSuite) Test_MidBatchError() {
	s.t.Run("ErrorStopsExecution", func(t *testing.T) {
		// Syntax error in middle should stop batch
		_, err := s.psql("SELECT 111 + 222; SELCT 2; SELECT 777 + 222")
		require.Error(t, err, "Query batch should fail on syntax error")
	})

	s.t.Run("ErrorInTransaction", func(t *testing.T) {
		dsn := pgwireDSN(s.peer.Name, nil)
		cfg, _ := pgx.ParseConfig(dsn)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		_, err = conn.Exec(s.t.Context(), "BEGIN; SELECT 1; SELCT 2; SELECT 3; COMMIT")
		require.Error(t, err, "Transaction should fail on syntax error")
	})
}

// ========================================
// Cancel Request
// ========================================

func (s PgwireMySQLSuite) Test_CancelRequest() {
	s.t.Run("CancelLongRunningQuery", func(t *testing.T) {
		dsn := pgwireDSN(s.peer.Name, nil)
		cfg, _ := pgx.ParseConfig(dsn)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		// Start a long-running query using SLEEP(10) - should take 10 seconds if not cancelled
		doneCh := make(chan struct{})
		go func() {
			// MySQL SLEEP returns 1 if interrupted, 0 if completed normally
			// It doesn't error on cancel, just returns early
			_, _ = conn.Exec(s.t.Context(), "SELECT SLEEP(10)")
			close(doneCh)
		}()

		// Wait a bit for query to start
		time.Sleep(100 * time.Millisecond)

		// Send cancel request
		err = conn.PgConn().CancelRequest(s.t.Context())
		require.NoError(t, err, "Cancel request should be sent")

		select {
		case <-doneCh:
			t.Log("Query completed (cancel successful)")
		case <-time.After(5 * time.Second):
			t.Fatal("Query should have been canceled within 5 seconds")
		}
	})
}

// ========================================
// Connection Recovery After Limit
// ========================================

func (s PgwireMySQLSuite) Test_ConnectionUsableAfterLimitExceeded() {
	dsn := pgwireDSN(s.peer.Name, map[string]string{"max_rows": "3"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// First query exceeds limit - use Query and iterate to properly drain
	rows, err := conn.Query(s.t.Context(), "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5")
	require.NoError(s.t, err, "Query should start successfully")

	var rowErr error
	for rows.Next() {
		// iterate until error
	}
	rowErr = rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to row limit")
	require.Contains(s.t, rowErr.Error(), "row limit")

	// Connection should still be usable for subsequent queries
	var result int
	err = conn.QueryRow(s.t.Context(), "SELECT 42").Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after limit exceeded")
	require.Equal(s.t, 42, result)

	// And another query within limits should work
	rows, err = conn.Query(s.t.Context(), "SELECT 1 UNION SELECT 2")
	require.NoError(s.t, err, "Query within limit should succeed")
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.Equal(s.t, 2, count)
}

// ========================================
// Connection Tests
// ========================================

func (s PgwireMySQLSuite) Test_Connection_Reuse() {
	dsn := pgwireDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// Execute multiple queries on same connection
	for i := range 5 {
		var result int
		err := conn.QueryRow(s.t.Context(), fmt.Sprintf("SELECT %d", i)).Scan(&result)
		require.NoError(s.t, err)
		require.Equal(s.t, i, result)
	}
}

// ========================================
// Concurrent Connections
// ========================================

func (s PgwireMySQLSuite) Test_ConcurrentConnections() {
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			output, err := s.psql(fmt.Sprintf("SELECT %d", id))
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

func (s PgwireMySQLSuite) Test_MultipleSequentialConnections() {
	for i := range 10 {
		output, err := s.psql(fmt.Sprintf("SELECT %d", i))
		require.NoError(s.t, err, "Sequential connection %d should succeed", i)
		require.Equal(s.t, strconv.Itoa(i), output)
	}
}

// ========================================
// Idle Timeout
// ========================================

func (s PgwireMySQLSuite) Test_IdleTimeout_Short() {
	dsn := pgwireDSN(s.peer.Name, map[string]string{"idle_timeout": "1"})

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
