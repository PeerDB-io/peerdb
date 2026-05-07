package switchboard

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nickbruun/pgsplit"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// wrapPgError converts a PostgreSQL error to an UpstreamError
func wrapPgError(err error) error {
	if err == nil {
		return nil
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return &UpstreamError{Resp: &pgproto3.ErrorResponse{
			Severity:         pgErr.Severity,
			Code:             pgErr.Code,
			Message:          pgErr.Message,
			Detail:           pgErr.Detail,
			Hint:             pgErr.Hint,
			Position:         pgErr.Position,
			InternalPosition: pgErr.InternalPosition,
			InternalQuery:    pgErr.InternalQuery,
			Where:            pgErr.Where,
			SchemaName:       pgErr.SchemaName,
			TableName:        pgErr.TableName,
			ColumnName:       pgErr.ColumnName,
			DataTypeName:     pgErr.DataTypeName,
			ConstraintName:   pgErr.ConstraintName,
			File:             pgErr.File,
			Line:             pgErr.Line,
			Routine:          pgErr.Routine,
		}}
	}
	// Non-PgError: wrap as internal error
	return &UpstreamError{Resp: &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	}}
}

// PostgresUpstream implements Upstream for PostgreSQL databases
type PostgresUpstream struct {
	conn     *connpostgres.PostgresConnector
	secret   []byte
	pid      uint32
	readOnly bool
}

// NewPostgresUpstream creates a new PostgreSQL upstream connection
func NewPostgresUpstream(
	ctx context.Context, config *protos.PostgresConfig, queryTimeout time.Duration, readOnly bool,
) (*PostgresUpstream, error) {
	conn, err := connpostgres.NewPostgresConnector(ctx, nil, config)
	if err != nil {
		return nil, err
	}

	initSQL := fmt.Sprintf(
		"SET statement_timeout = '%dms'; SET idle_in_transaction_session_timeout = '%dms'",
		queryTimeout.Milliseconds(), queryTimeout.Milliseconds(),
	)
	if readOnly {
		initSQL += "; SET default_transaction_read_only = on"
	}
	if _, err = conn.Conn().Exec(ctx, initSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set session parameters: %w", err)
	}

	pgConn := conn.Conn().PgConn()
	return &PostgresUpstream{
		conn:     conn,
		pid:      pgConn.PID(),
		secret:   pgConn.SecretKey(),
		readOnly: readOnly,
	}, nil
}

// Exec executes a query and returns results for streaming
func (u *PostgresUpstream) Exec(ctx context.Context, query string) (ResultIterator, error) {
	multiResult := u.conn.Conn().PgConn().Exec(ctx, query)
	return &PostgresResultIterator{multiResult: multiResult}, nil
}

// TxStatus returns the current transaction status
func (u *PostgresUpstream) TxStatus() byte {
	return u.conn.Conn().PgConn().TxStatus()
}

// ServerParameters queries the upstream for actual parameter values
func (u *PostgresUpstream) ServerParameters(ctx context.Context) map[string]string {
	params := make(map[string]string)

	// Query all parameters in a single round-trip via pg_settings
	rows, err := u.conn.Conn().Query(ctx, `
		SELECT name, setting FROM pg_settings
		WHERE name IN (
			'server_version', 'server_encoding', 'client_encoding',
			'DateStyle', 'TimeZone', 'integer_datetimes',
			'standard_conforming_strings', 'application_name'
		)
	`)
	if err != nil {
		return params
	}
	defer rows.Close()

	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			continue
		}
		params[name] = setting
	}

	return params
}

// BackendKeyData returns the backend PID and secret key
func (u *PostgresUpstream) BackendKeyData() (uint32, []byte) {
	return u.pid, u.secret
}

// Cancel cancels the currently running query
func (u *PostgresUpstream) Cancel(ctx context.Context) error {
	return u.conn.Conn().PgConn().CancelRequest(ctx)
}

// Close closes the upstream connection
func (u *PostgresUpstream) Close() error {
	return u.conn.Close()
}

var postgresAllowedFirstKeywords = map[string]struct{}{
	"SELECT":     {},
	"TABLE":      {},
	"VALUES":     {},
	"WITH":       {},
	"EXPLAIN":    {},
	"SHOW":       {},
	"BEGIN":      {},
	"START":      {},
	"COMMIT":     {},
	"END":        {},
	"ROLLBACK":   {},
	"ABORT":      {},
	"SAVEPOINT":  {},
	"RELEASE":    {},
	"SET":        {},
	"RESET":      {},
	"DISCARD":    {},
	"DECLARE":    {},
	"FETCH":      {},
	"MOVE":       {},
	"CLOSE":      {},
	"PREPARE":    {},
	"EXECUTE":    {},
	"DEALLOCATE": {},
}

var postgresAllRe = regexp.MustCompile(`(?i)^\s*ALL\b`)

// CheckQuery validates a query against PostgreSQL security rules
func (u *PostgresUpstream) CheckQuery(query string) error {
	if !u.readOnly {
		return nil
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	// Check for read-only bypass attempts
	lower := strings.ToLower(query)
	if strings.Contains(lower, "default_transaction_read_only") {
		return errors.New("cannot modify read-only mode")
	}
	if strings.Contains(lower, "set_config") {
		return errors.New("set_config is not allowed")
	}

	// Check each statement against allowlist using pgsplit
	statements, err := pgsplit.SplitStatements(query)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}
	for _, stmt := range statements {
		fields := strings.Fields(stmt)
		if len(fields) == 0 {
			continue
		}
		keyword := strings.ToUpper(fields[0])
		rest := strings.TrimSpace(strings.TrimPrefix(stmt, fields[0]))
		if _, allowed := postgresAllowedFirstKeywords[keyword]; !allowed {
			return fmt.Errorf("statement not allowed: %s", keyword)
		}
		if keyword == "RESET" && postgresAllRe.MatchString(rest) {
			return errors.New("RESET ALL not allowed: would disable read-only mode")
		}
		if keyword == "DISCARD" && postgresAllRe.MatchString(rest) {
			return errors.New("DISCARD ALL not allowed: would disable read-only mode")
		}
	}

	return nil
}

// PostgresResultIterator implements ResultIterator for PostgreSQL
//
//nolint:govet // fieldalignment: readability preferred
type PostgresResultIterator struct {
	multiResult  *pgconn.MultiResultReader
	resultReader *pgconn.ResultReader
	commandTag   string
	err          error
}

// NextResult advances to the next result set
func (it *PostgresResultIterator) NextResult() bool {
	if it.multiResult.NextResult() {
		it.resultReader = it.multiResult.ResultReader()
		return true
	}
	return false
}

// FieldDescriptions returns column metadata for current result
func (it *PostgresResultIterator) FieldDescriptions() []FieldDescription {
	if it.resultReader == nil {
		return nil
	}

	pgFields := it.resultReader.FieldDescriptions()
	fields := make([]FieldDescription, len(pgFields))
	for i, fd := range pgFields {
		fields[i] = FieldDescription{
			Name:                 fd.Name,
			TableOID:             fd.TableOID,
			TableAttributeNumber: fd.TableAttributeNumber,
			DataTypeOID:          fd.DataTypeOID,
			DataTypeSize:         fd.DataTypeSize,
			TypeModifier:         fd.TypeModifier,
			Format:               fd.Format,
		}
	}
	return fields
}

// NextRow advances to the next row
func (it *PostgresResultIterator) NextRow() bool {
	if it.resultReader == nil {
		return false
	}
	return it.resultReader.NextRow()
}

// RowValues returns current row's values as bytes
func (it *PostgresResultIterator) RowValues() [][]byte {
	if it.resultReader == nil {
		return nil
	}
	return it.resultReader.Values()
}

// CommandTag returns the command tag after Close
func (it *PostgresResultIterator) CommandTag() string {
	return it.commandTag
}

// Err returns any error encountered
func (it *PostgresResultIterator) Err() error {
	return it.err
}

// Close closes the current result reader and captures the command tag
func (it *PostgresResultIterator) Close() {
	if it.resultReader != nil {
		commandTag, err := it.resultReader.Close()
		it.commandTag = commandTag.String()
		if err != nil {
			it.err = wrapPgError(err)
		}
	}
}

// CloseAll closes the entire multi-result and returns any error
func (it *PostgresResultIterator) CloseAll() error {
	return wrapPgError(it.multiResult.Close())
}
