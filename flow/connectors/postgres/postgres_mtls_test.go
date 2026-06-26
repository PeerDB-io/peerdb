package connpostgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// TestPostgresMutualTLSClientCertAuth validates client-certificate (mutual TLS) authentication
// against a TLS-enabled Postgres whose ssl_ca_file trusts the test client certificate.
func TestPostgresMutualTLSClientCertAuth(t *testing.T) {
	t.Parallel()

	if _, ok := internal.GetMutualTLSPostgresConfigFromEnv(); !ok {
		t.Skip("mutual-TLS Postgres fixtures not configured; " +
			"set PG_MTLS_CLIENT_CERT_PATH, PG_MTLS_CLIENT_KEY_PATH and PG_MTLS_ROOT_CA_PATH")
	}

	clientDN := func(t *testing.T, connector *PostgresConnector) pgtype.Text {
		t.Helper()
		var dn pgtype.Text
		require.NoError(t, connector.Conn().QueryRow(t.Context(),
			"SELECT client_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid()").Scan(&dn))
		return dn
	}

	t.Run("client certificate is presented and authenticated", func(t *testing.T) {
		t.Parallel()
		cfg, _ := internal.GetMutualTLSPostgresConfigFromEnv()
		connector, err := NewPostgresConnector(t.Context(), nil, cfg)
		require.NoError(t, err)
		defer connector.Close()

		dn := clientDN(t, connector)
		require.True(t, dn.Valid, "expected the server to record a verified client certificate")
		require.Contains(t, dn.String, "CN=postgres")
	})

	t.Run("without a client certificate no client identity is recorded", func(t *testing.T) {
		t.Parallel()
		cfg, _ := internal.GetMutualTLSPostgresConfigFromEnv()
		cfg.ClientTls = nil // Explicit client TLS configuration removal.
		connector, err := NewPostgresConnector(t.Context(), nil, cfg)
		require.NoError(t, err)
		defer connector.Close()

		require.False(t, clientDN(t, connector).Valid, "did not expect a client certificate identity")
	})
}
