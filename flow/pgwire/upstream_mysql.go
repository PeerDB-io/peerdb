package pgwire

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// wrapMySQLError converts a MySQL error to an UpstreamError
func wrapMySQLError(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MyError
	if errors.As(err, &mysqlErr) {
		return &UpstreamError{Resp: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "HY000",
			Message:  mysqlErr.Message,
		}}
	}
	// Non-MyError: wrap as internal error
	return &UpstreamError{Resp: &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	}}
}

// MySQLUpstream implements Upstream for MySQL databases
type MySQLUpstream struct {
	conn         *connmysql.MySqlConnector
	config       *protos.MySqlConfig
	connectionID uint32
	pid          uint32
	secret       uint32
}

// mysqlDeniedFunctions are functions that are denied even in SELECT statements
var mysqlDeniedFunctions = map[string]struct{}{
	"load_file":    {},
	"get_lock":     {},
	"release_lock": {},
	"is_free_lock": {},
	"is_used_lock": {},
}

// NewMySQLUpstream creates a new MySQL upstream connection
func NewMySQLUpstream(ctx context.Context, config *protos.MySqlConfig, queryTimeout time.Duration) (*MySQLUpstream, error) {
	conn, err := connmysql.NewMySqlConnector(ctx, config)
	if err != nil {
		return nil, err
	}

	// Get connection_id for cancel support
	rs, err := conn.Execute(ctx, "SELECT CONNECTION_ID()")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get connection ID: %w", err)
	}

	var connectionID uint64
	if len(rs.Values) > 0 && len(rs.Values[0]) > 0 {
		connectionID = rs.Values[0][0].AsUint64()
	}

	// Set session parameters for read-only mode and timeouts
	timeoutSec := int(queryTimeout.Seconds())
	if timeoutSec < 1 {
		timeoutSec = 1
	}

	_, err = conn.Execute(ctx, "SET SESSION TRANSACTION READ ONLY")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set read-only mode: %w", err)
	}

	_, err = conn.Execute(ctx, fmt.Sprintf(
		"SET SESSION max_execution_time = %d, "+
			"lock_wait_timeout = %d, "+
			"innodb_lock_wait_timeout = %d",
		queryTimeout.Milliseconds(), timeoutSec, timeoutSec,
	))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set session parameters: %w", err)
	}

	// Use MySQL connection ID as pid, generate random secret
	pid := uint32(connectionID)
	secret := rand.Uint32() //nolint:gosec // not security-critical, used for cancel routing

	return &MySQLUpstream{
		conn:         conn,
		config:       config,
		connectionID: uint32(connectionID),
		pid:          pid,
		secret:       secret,
	}, nil
}

// Exec executes a query and returns results for streaming
func (u *MySQLUpstream) Exec(ctx context.Context, query string) (ResultIterator, error) {
	result, err := u.conn.Execute(ctx, query)
	if err != nil {
		return nil, wrapMySQLError(err)
	}
	return &MySQLResultIterator{result: result, consumed: false}, nil
}

// TxStatus returns the transaction status - always 'I' (idle) for MySQL per requirements
func (u *MySQLUpstream) TxStatus() byte {
	return 'I'
}

// ServerParameters returns fake parameters sufficient for psql
func (u *MySQLUpstream) ServerParameters(ctx context.Context) map[string]string {
	return map[string]string{
		"server_version":              "8.0.0-mysql-proxy",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}
}

// BackendKeyData returns the synthetic PID and secret for cancel support
func (u *MySQLUpstream) BackendKeyData() (uint32, uint32) {
	return u.pid, u.secret
}

// Cancel cancels the currently running query via KILL QUERY
func (u *MySQLUpstream) Cancel(ctx context.Context) error {
	// Open ephemeral connection with same credentials
	ephemeral, err := connmysql.NewMySqlConnector(ctx, u.config)
	if err != nil {
		return fmt.Errorf("failed to create ephemeral connection for cancel: %w", err)
	}
	defer ephemeral.Close()

	_, err = ephemeral.Execute(ctx, fmt.Sprintf("KILL QUERY %d", u.connectionID))
	return err
}

// Close closes the upstream connection
func (u *MySQLUpstream) Close() error {
	return u.conn.Close()
}

// CheckQuery validates a query against MySQL security rules using an allowlist approach.
// Only SELECT, SHOW, DESCRIBE, EXPLAIN, and transaction control statements are allowed.
func (u *MySQLUpstream) CheckQuery(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	// Detect psql \d commands which query pg_catalog
	if SqlSelectPgCatalogRe.MatchString(query) {
		return errors.New("PostgreSQL catalog queries not supported; use 'SHOW TABLES' or 'SHOW DATABASES'")
	}

	// Parse with TiDB parser
	p := parser.New()
	stmts, _, err := p.Parse(query, "", "")
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	for _, stmt := range stmts {
		if err := checkMySQLStatement(stmt); err != nil {
			return err
		}
	}
	return nil
}

// checkMySQLStatement validates a single statement against the allowlist
func checkMySQLStatement(stmt ast.StmtNode) error {
	switch s := stmt.(type) {
	// Allowed: SELECT (with restrictions)
	case *ast.SelectStmt:
		return checkSelectStatement(s)

	// Allowed: UNION/INTERSECT/EXCEPT (SetOprStmt) with restrictions on each SELECT
	case *ast.SetOprStmt:
		return checkSetOprStatement(s)

	// Allowed: SHOW (no restrictions)
	case *ast.ShowStmt:
		return nil

	// Allowed: EXPLAIN and DESCRIBE/DESC
	// Note: TiDB parser treats DESC/DESCRIBE as ExplainStmt
	case *ast.ExplainStmt:
		return checkExplainStatement(s)

	// Allowed: Transaction control (read-only)
	case *ast.BeginStmt:
		return nil
	case *ast.CommitStmt:
		return nil
	case *ast.RollbackStmt:
		return nil
	case *ast.SavepointStmt:
		return nil
	case *ast.ReleaseSavepointStmt:
		return nil

	// Everything else is denied
	default:
		return fmt.Errorf("statement not allowed: %T", stmt)
	}
}

// checkSetOprStatement validates UNION/INTERSECT/EXCEPT statements
func checkSetOprStatement(s *ast.SetOprStmt) error {
	if s.SelectList != nil {
		for _, sel := range s.SelectList.Selects {
			switch sel := sel.(type) {
			case *ast.SelectStmt:
				if err := checkSelectStatement(sel); err != nil {
					return err
				}
			case *ast.SetOprSelectList:
				for _, nested := range sel.Selects {
					if nestedSelect, ok := nested.(*ast.SelectStmt); ok {
						if err := checkSelectStatement(nestedSelect); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

// checkSelectStatement validates SELECT restrictions per spec
func checkSelectStatement(s *ast.SelectStmt) error {
	// Check for SELECT ... INTO (any kind: OUTFILE, DUMPFILE, @var)
	if s.SelectIntoOpt != nil {
		return errors.New("SELECT INTO not allowed")
	}

	// Check for locking reads: FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE
	if s.LockInfo != nil && s.LockInfo.LockType != ast.SelectLockNone {
		return errors.New("locking reads (FOR UPDATE/FOR SHARE) not allowed")
	}

	// Check for denied functions in the SELECT expressions
	if err := checkDeniedFunctions(s); err != nil {
		return err
	}

	// Recursively check subqueries in FROM clause
	if s.From != nil {
		if err := checkTableRefsForSubqueries(s.From.TableRefs); err != nil {
			return err
		}
	}

	// Check WHERE clause for subqueries
	if s.Where != nil {
		if err := checkExprForSubqueries(s.Where); err != nil {
			return err
		}
	}

	return nil
}

// checkExplainStatement ensures EXPLAIN is only used with SELECT or DESCRIBE
func checkExplainStatement(s *ast.ExplainStmt) error {
	if s.Stmt == nil {
		return nil
	}

	switch stmt := s.Stmt.(type) {
	case *ast.SelectStmt:
		// EXPLAIN SELECT - validate the SELECT
		return checkSelectStatement(stmt)
	case *ast.ShowStmt:
		// DESCRIBE/DESC table - this is parsed as ExplainStmt with ShowStmt inside
		return nil
	default:
		// EXPLAIN of INSERT/UPDATE/DELETE etc is denied
		return errors.New("EXPLAIN only allowed for SELECT statements")
	}
}

// checkDeniedFunctions walks the AST to find denied function calls
func checkDeniedFunctions(node ast.Node) error {
	var err error
	visitor := &functionVisitor{err: &err}
	node.Accept(visitor)
	return err
}

type functionVisitor struct {
	err *error
}

func (v *functionVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if *v.err != nil {
		return n, true // skip if already errored
	}

	switch node := n.(type) {
	case *ast.FuncCallExpr:
		fnName := strings.ToLower(node.FnName.L)
		if _, denied := mysqlDeniedFunctions[fnName]; denied {
			*v.err = fmt.Errorf("function %s not allowed", node.FnName.O)
			return n, true
		}
	case *ast.FuncCastExpr:
		// CAST is allowed
	case *ast.AggregateFuncExpr:
		// Aggregate functions are allowed
	}
	return n, false
}

func (v *functionVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// checkTableRefsForSubqueries recursively checks table references for subqueries
func checkTableRefsForSubqueries(tr *ast.Join) error {
	if tr == nil {
		return nil
	}

	// Check left side
	if tr.Left != nil {
		if err := checkResultSetNode(tr.Left); err != nil {
			return err
		}
	}

	// Check right side
	if tr.Right != nil {
		if err := checkResultSetNode(tr.Right); err != nil {
			return err
		}
	}

	return nil
}

func checkResultSetNode(node ast.ResultSetNode) error {
	switch n := node.(type) {
	case *ast.SelectStmt:
		return checkSelectStatement(n)
	case *ast.SubqueryExpr:
		if sel, ok := n.Query.(*ast.SelectStmt); ok {
			return checkSelectStatement(sel)
		}
	case *ast.Join:
		return checkTableRefsForSubqueries(n)
	case *ast.TableSource:
		if n.Source != nil {
			return checkResultSetNode(n.Source)
		}
	case *ast.TableName:
		// Table reference, allowed
	}
	return nil
}

func checkExprForSubqueries(expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}

	var err error
	visitor := &subqueryVisitor{err: &err}
	expr.Accept(visitor)
	return err
}

type subqueryVisitor struct {
	err *error
}

func (v *subqueryVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if *v.err != nil {
		return n, true
	}

	if sq, ok := n.(*ast.SubqueryExpr); ok {
		if sel, ok := sq.Query.(*ast.SelectStmt); ok {
			if err := checkSelectStatement(sel); err != nil {
				*v.err = err
				return n, true
			}
		}
	}
	return n, false
}

func (v *subqueryVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// mysqlTypeToOID maps MySQL types to PostgreSQL OIDs for display alignment
func mysqlTypeToOID(mysqlType uint8) uint32 {
	switch mysqlType {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT,
		mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
		return 20 // INT8 - right-aligned
	case mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE:
		return 701 // FLOAT8 - right-aligned
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return 1700 // NUMERIC - right-aligned
	default:
		return 25 // TEXT - left-aligned
	}
}

// MySQLResultIterator implements ResultIterator for MySQL
//
//nolint:govet // fieldalignment: readability preferred
type MySQLResultIterator struct {
	result   *mysql.Result
	rowIndex int
	consumed bool
	err      error
}

// NextResult advances to the next result set
// MySQL results from Execute() are single result sets, so this returns true once
func (it *MySQLResultIterator) NextResult() bool {
	if it.consumed {
		return false
	}
	it.consumed = true
	return true
}

// FieldDescriptions returns column metadata for current result
func (it *MySQLResultIterator) FieldDescriptions() []FieldDescription {
	if it.result == nil || it.result.Fields == nil {
		return nil
	}

	fields := make([]FieldDescription, len(it.result.Fields))
	for i, f := range it.result.Fields {
		fields[i] = FieldDescription{
			Name:        string(f.Name),
			DataTypeOID: mysqlTypeToOID(f.Type),
			Format:      0, // Text format
		}
	}
	return fields
}

// NextRow advances to the next row
func (it *MySQLResultIterator) NextRow() bool {
	if it.result == nil || it.result.Values == nil {
		return false
	}
	if it.rowIndex >= len(it.result.Values) {
		return false
	}
	it.rowIndex++
	return true
}

// RowValues returns current row's values as text-encoded bytes
func (it *MySQLResultIterator) RowValues() [][]byte {
	if it.result == nil || it.rowIndex == 0 || it.rowIndex > len(it.result.Values) {
		return nil
	}

	row := it.result.Values[it.rowIndex-1]
	values := make([][]byte, len(row))
	for i, fv := range row {
		if fv.Type == mysql.FieldValueTypeNull {
			values[i] = nil
		} else {
			// Use AsString() for string types, otherwise use String() for formatted output
			if fv.Type == mysql.FieldValueTypeString {
				values[i] = fv.AsString()
			} else {
				// For numeric types, use the String() method which formats correctly
				// but strip the quotes that String() adds for string types
				values[i] = []byte(fv.String())
			}
		}
	}
	return values
}

// CommandTag returns the command tag
func (it *MySQLResultIterator) CommandTag() string {
	if it.result == nil {
		return ""
	}

	// For SELECT queries, return SELECT with row count
	if len(it.result.Fields) > 0 {
		return fmt.Sprintf("SELECT %d", len(it.result.Values))
	}

	// For DML statements, return based on affected rows
	if it.result.AffectedRows > 0 {
		return fmt.Sprintf("UPDATE %d", it.result.AffectedRows)
	}

	return "OK"
}

// Err returns any error encountered
func (it *MySQLResultIterator) Err() error {
	return it.err
}

// Close releases resources for current result set
func (it *MySQLResultIterator) Close() {
	// MySQL results don't need explicit close
}

// CloseAll closes the entire result iterator
// For MySQL, results are already fully consumed, so this is a no-op
func (it *MySQLResultIterator) CloseAll() error {
	return nil
}
