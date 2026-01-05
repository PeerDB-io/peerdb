package pgwire

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// createUpstreamConnection creates a new connection to the upstream PostgreSQL database
func createUpstreamConnection(ctx context.Context, pgConfig *protos.PostgresConfig, queryTimeout time.Duration) (*pgx.Conn, error) {
	// Build DSN from PostgresConfig
	dsn := internal.GetPGConnectionString(pgConfig, "")

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
	queryTimeoutStr := fmt.Sprintf("%dms", queryTimeout.Milliseconds())
	connConfig.RuntimeParams["statement_timeout"] = queryTimeoutStr
	connConfig.RuntimeParams["idle_in_transaction_session_timeout"] = queryTimeoutStr
	connConfig.RuntimeParams["default_transaction_read_only"] = "on"
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

	// Query all parameters in a single round-trip via pg_settings
	rows, err := conn.Query(ctx, `
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
