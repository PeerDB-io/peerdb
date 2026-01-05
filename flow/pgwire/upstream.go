package pgwire

import (
	"context"

	"github.com/jackc/pgx/v5"
)

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
