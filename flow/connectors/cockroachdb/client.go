package conncockroachdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func ParseConfig(connectionString string, config *protos.CockroachDBConfig) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if config.RequireTls || config.RootCa != nil {
		tlsConfig, err := common.CreateTlsConfig(
			tls.VersionTLS12, config.RootCa, connConfig.Host, config.TlsHost, false, nil)
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
	tunnel *utils.SSHTunnel,
) (*pgx.Conn, error) {
	if tunnel.IsActive() {
		connConfig.DialFunc = tunnel.DialContext
		// DNS lookup seems to happen before connection is established which can be an issue if given host
		// can only be resolved on the SSH host https://github.com/jackc/pgx/issues/1724
		connConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
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
	return parseCrdbMajorVersion(version)
}

// parses version strings like "CockroachDB CCL v25.4.13 ..." or "CockroachDB v23.1.0 ..."
func parseCrdbMajorVersion(version string) (int, error) {
	var major, minor, patch int
	if _, err := fmt.Sscanf(version, "CockroachDB CCL v%d.%d.%d", &major, &minor, &patch); err != nil {
		if _, err := fmt.Sscanf(version, "CockroachDB v%d.%d.%d", &major, &minor, &patch); err != nil {
			return 0, fmt.Errorf("failed to parse CockroachDB version %q: %w", version, err)
		}
	}
	return major, nil
}
