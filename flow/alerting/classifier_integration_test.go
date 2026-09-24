//go:build tilt

package alerting

import (
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestPostgresQueryCancelledErrorShouldBeNotifyConnectivity(t *testing.T) {
	t.Parallel()

	connectionString := internal.GetCatalogConnectionStringFromEnv(t.Context())
	config, err := pgx.ParseConfig(connectionString)
	require.NoError(t, err)
	config.Config.RuntimeParams["statement_timeout"] = "1500"
	connectConfig, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer connectConfig.Close(t.Context())
	_, err = connectConfig.Exec(t.Context(), "SELECT pg_sleep(2)")

	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed querying: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.QueryCanceled,
	}, errInfo, "Unexpected error info")
}

func TestPostgresConnClosedErrorShouldBeNotifyConnectivity(t *testing.T) {
	t.Parallel()

	// Drive real pgx to produce the "conn closed" error rather than hand-building one, so this
	// guards against a pgx upgrade changing how the sentinel is wrapped (errors.Is must still match).
	connectionString := internal.GetCatalogConnectionStringFromEnv(t.Context())
	config, err := pgx.ParseConfig(connectionString)
	require.NoError(t, err)
	conn, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	require.NoError(t, conn.Close(t.Context()))

	// Using the connection after it has been closed yields pgconn.ErrConnClosed (wrapped in connLockError).
	_, err = conn.Exec(t.Context(), "SELECT 1")
	require.ErrorIs(t, err, pgconn.ErrConnClosed)

	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to sync records: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceNet,
		Code:   "CONN_CLOSED",
	}, errInfo, "Unexpected error info")
}
