package pgwire

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// createUpstreamConnection creates a new connection to the upstream PostgreSQL database
func createUpstreamConnection(ctx context.Context, dsn string, queryTimeout string) (*pgx.Conn, error) {
	// Parse the DSN
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse upstream DSN: %w", err)
	}

	// CRITICAL: Use simple protocol so RawValues() returns text-encoded data we can forward directly
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	// Set runtime parameters for safety and consistency
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}

	// Set timeout and session parameters
	connConfig.RuntimeParams["statement_timeout"] = queryTimeout
	connConfig.RuntimeParams["idle_in_transaction_session_timeout"] = queryTimeout
	connConfig.RuntimeParams["timezone"] = "UTC"
	connConfig.RuntimeParams["DateStyle"] = "ISO, MDY"
	connConfig.RuntimeParams["standard_conforming_strings"] = "on"
	connConfig.RuntimeParams["client_encoding"] = "UTF8"

	// Set application name for audit trail
	if connConfig.RuntimeParams["application_name"] == "" {
		connConfig.RuntimeParams["application_name"] = "peerdb-pgwire-proxy"
	}

	// Connect to upstream
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to upstream: %w", err)
	}

	return conn, nil
}

// getBackendKeyData extracts the backend PID and secret key from the connection
func getBackendKeyData(conn *pgx.Conn) (uint32, uint32) {
	pgConn := conn.PgConn()
	return pgConn.PID(), pgConn.SecretKey()
}

// queryServerParameters queries the upstream server for actual parameter values
// This ensures we report the real server parameters to the client
func queryServerParameters(ctx context.Context, conn *pgx.Conn) map[string]string {
	params := make(map[string]string)

	// Query for key server parameters that clients expect
	// Per PostgreSQL protocol, these are the most important ones
	queries := []struct {
		param string
		query string
	}{
		{"server_version", "SHOW server_version"},
		{"server_encoding", "SHOW server_encoding"},
		{"client_encoding", "SHOW client_encoding"},
		{"DateStyle", "SHOW DateStyle"},
		{"TimeZone", "SHOW TimeZone"},
		{"integer_datetimes", "SHOW integer_datetimes"},
		{"standard_conforming_strings", "SHOW standard_conforming_strings"},
		{"application_name", "SHOW application_name"},
	}

	for _, q := range queries {
		var value string
		err := conn.QueryRow(ctx, q.query).Scan(&value)
		if err != nil {
			// Log but don't fail - we'll use defaults
			continue
		}
		params[q.param] = value
	}

	return params
}
