package conncockroachdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func ParseConfig(connectionString string, config *protos.CockroachDBConfig) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if config.RequireTls || config.RootCa != nil {
		tlsConfig, err := shared.CreateTlsConfig(tls.VersionTLS12, config.RootCa, connConfig.Host, config.TlsHost, false)
		if err != nil {
			return nil, err
		}
		connConfig.TLSConfig = tlsConfig
	}
	return connConfig, nil
}

func NewCockroachDBConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tlsHost string,
	tunnel *utils.SSHTunnel,
) (*pgx.Conn, error) {
	if tunnel != nil {
		connConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return tunnel.Client.Dial(network, addr)
		}
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CockroachDB: %w", err)
	}
	return conn, nil
}

func GetCRDBConnectionString(config *protos.CockroachDBConfig, flowName string) string {
	passwordEscaped := url.QueryEscape(config.Password)
	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?application_name=%s&client_encoding=UTF8",
		config.User,
		passwordEscaped,
		shared.JoinHostPort(config.Host, config.Port),
		config.Database,
		applicationName,
	)
	if config.RequireTls {
		connString += "&sslmode=require"
	}
	return connString
}

func (c *CockroachDBConnector) GetMajorVersion(ctx context.Context) (int, error) {
	var version string
	err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get CockroachDB version: %w", err)
	}
	// Parse version string like "CockroachDB CCL v23.1.0 ..."
	// Extract major version
	var major, minor, patch int
	_, err = fmt.Sscanf(version, "CockroachDB CCL v%d.%d.%d", &major, &minor, &patch)
	if err != nil {
		// Try without CCL
		_, err = fmt.Sscanf(version, "CockroachDB v%d.%d.%d", &major, &minor, &patch)
		if err != nil {
			return 0, fmt.Errorf("failed to parse CockroachDB version: %w", err)
		}
	}
	return major, nil
}
