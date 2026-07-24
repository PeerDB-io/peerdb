package e2e

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// TestClickHouseTLSDirectory verifies that connclickhouse.Connect works
// with TLS certificates loaded from a directory (simulating a volume-mounted K8s Secret).
func TestClickHouseTLSDirectory(t *testing.T) {
	portStr := os.Getenv("PEERDB_CLICKHOUSE_TLS_PORT")
	if portStr == "" {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(t, err, "invalid PEERDB_CLICKHOUSE_TLS_PORT value: %s", portStr)

	certDir := os.Getenv("PEERDB_CLICKHOUSE_TLS_CERT_DIR")
	if certDir == "" {
		t.Skip("PEERDB_CLICKHOUSE_TLS_CERT_DIR not set, skipping TLS test")
	}

	config := &protos.ClickhouseConfig{
		Host:                    "localhost",
		Port:                    uint32(port),
		User:                    "peerdb_tls",
		Database:                "default",
		DisableTls:              false,
		TlsCertificateDirectory: proto.String(certDir),
	}

	conn, err := connclickhouse.Connect(t.Context(), nil, config)
	require.NoError(t, err, "failed to connect to ClickHouse")
	defer conn.Close()

	var result uint8
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT 1").Scan(&result), "failed to execute SELECT 1")
	require.Equal(t, uint8(1), result)
}
