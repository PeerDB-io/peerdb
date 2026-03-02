package e2e

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func readTLSEnvPaths(t *testing.T) ([]byte, []byte, []byte) {
	t.Helper()

	caPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CA_CERT_PATH")
	certPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CLIENT_CERT_PATH")
	keyPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CLIENT_KEY_PATH")

	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, nil, nil
	}

	caCert, err := os.ReadFile(caPath)
	require.NoError(t, err, "failed to read CA cert from %s", caPath)
	clientCert, err := os.ReadFile(certPath)
	require.NoError(t, err, "failed to read client cert from %s", certPath)
	clientKey, err := os.ReadFile(keyPath)
	require.NoError(t, err, "failed to read client key from %s", keyPath)

	return caCert, clientCert, clientKey
}

func tlsClickHousePort(t *testing.T) uint32 {
	t.Helper()
	portStr := os.Getenv("PEERDB_CLICKHOUSE_TLS_PORT")
	if portStr == "" {
		return 0
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(t, err, "invalid PEERDB_CLICKHOUSE_TLS_PORT value: %s", portStr)
	return uint32(port)
}

// TestClickHouseTLSInlineCerts verifies that connclickhouse.Connect works
// with inline PEM certificates against a TLS-enabled ClickHouse instance.
func TestClickHouseTLSInlineCerts(t *testing.T) {
	port := tlsClickHousePort(t)
	if port == 0 {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}

	caCert, clientCert, clientKey := readTLSEnvPaths(t)
	if caCert == nil {
		t.Skip("TLS certificate paths not set, skipping TLS test")
	}

	caCertStr := string(caCert)
	clientCertStr := string(clientCert)
	clientKeyStr := string(clientKey)

	config := &protos.ClickhouseConfig{
		Host:        "localhost",
		Port:        port,
		User:        "peerdb_tls",
		Database:    "default",
		DisableTls:  false,
		Certificate: &clientCertStr,
		PrivateKey:  &clientKeyStr,
		RootCa:      &caCertStr,
	}

	validateConnection(t, config)
}

// TestClickHouseTLSDirectory verifies that connclickhouse.Connect works
// with TLS certificates loaded from a directory (simulating a volume-mounted K8s Secret).
func TestClickHouseTLSDirectory(t *testing.T) {
	port := tlsClickHousePort(t)
	if port == 0 {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}

	caCert, clientCert, clientKey := readTLSEnvPaths(t)
	if caCert == nil {
		t.Skip("TLS certificate paths not set, skipping TLS test")
	}

	// Write cert files to a temp directory using cert-manager naming convention
	certDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.crt"), clientCert, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.key"), clientKey, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "ca.crt"), caCert, 0o600))

	config := &protos.ClickhouseConfig{
		Host:                    "localhost",
		Port:                    port,
		User:                    "peerdb_tls",
		Database:                "default",
		DisableTls:              false,
		TlsCertificateDirectory: shared.Ptr(certDir),
	}

	validateConnection(t, config)
}

// TestClickHouseTLSDirectoryWithoutCA verifies that the directory-based TLS config
// works when ca.crt is not present (only tls.crt and tls.key).
func TestClickHouseTLSDirectoryWithoutCA(t *testing.T) {
	port := tlsClickHousePort(t)
	if port == 0 {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}

	caCert, clientCert, clientKey := readTLSEnvPaths(t)
	if caCert == nil {
		t.Skip("TLS certificate paths not set, skipping TLS test")
	}

	// Write cert files to a temp directory without ca.crt
	certDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.crt"), clientCert, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.key"), clientKey, 0o600))

	// Also provide the CA via inline root_ca since the directory has no ca.crt
	caCertStr := string(caCert)

	config := &protos.ClickhouseConfig{
		Host:                    "localhost",
		Port:                    port,
		User:                    "peerdb_tls",
		Database:                "default",
		DisableTls:              false,
		TlsCertificateDirectory: shared.Ptr(certDir),
		RootCa:                  &caCertStr,
	}

	validateConnection(t, config)
}

func validateConnection(t *testing.T, config *protos.ClickhouseConfig) {
	t.Helper()

	conn, err := connclickhouse.Connect(t.Context(), nil, config)
	require.NoError(t, err, "failed to connect to ClickHouse")
	defer conn.Close()

	var result uint8
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT 1").Scan(&result), "failed to execute SELECT 1")
	require.Equal(t, uint8(1), result)
}
