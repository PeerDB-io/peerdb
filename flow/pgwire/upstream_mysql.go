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

// mysqlBlockedCommands are statements that are always denied for MySQL
var mysqlBlockedCommands = map[string]string{
	"LOAD":  "LOAD DATA - file access security risk",
	"RESET": "admin command",
	"FLUSH": "admin command",
	"PURGE": "binary log purge - admin command",
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

	// Use MySQL connection ID as pid, generate random secret
	pid := uint32(connectionID)
	secret := rand.Uint32()

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

// CheckQuery validates a query against MySQL security rules
func (u *MySQLUpstream) CheckQuery(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	lower := strings.ToLower(query)

	// Check for INTO OUTFILE/DUMPFILE patterns (security risk)
	if strings.Contains(lower, "into outfile") || strings.Contains(lower, "into dumpfile") {
		return errors.New("INTO OUTFILE/DUMPFILE is not allowed")
	}

	// Simple semicolon-based statement splitting for MySQL
	// (MySQL doesn't have PostgreSQL's dollar-quoting complexity)
	stmts := strings.Split(query, ";")
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Get first word as the command keyword
		keyword, _, _ := strings.Cut(stmt, " ")
		keyword = strings.ToUpper(strings.TrimSpace(keyword))

		if reason, blocked := mysqlBlockedCommands[keyword]; blocked {
			return fmt.Errorf("statement denied: %s (%s)", keyword, reason)
		}
	}

	return nil
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
	if it.result.Fields != nil && len(it.result.Fields) > 0 {
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
