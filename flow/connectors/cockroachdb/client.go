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
	// the password is set here instead of the connection string so it never
	// appears in errors or logs
	connConfig.Password = config.Password
	// Pin the wire protocol version so a pgx dependency update cannot switch
	// it silently: CockroachDB only negotiates protocol 3.2 from v26.1 on,
	// while this connector supports much older versions. Matches the pin in
	// the postgres connector.
	connConfig.Config.MaxProtocolVersion = "3.0"
	if !config.DisableTls {
		var clientCert *common.ClientCertificate
		if clientTls := config.GetClientTls(); clientTls != nil {
			clientCert, err = common.NewClientCertificate(clientTls.GetCertificate(), clientTls.GetPrivateKey())
			if err != nil {
				return nil, err
			}
		}
		tlsConfig, err := common.CreateTlsConfig(
			tls.VersionTLS12, config.RootCa, connConfig.Host, config.TlsHost, config.SkipCertVerification, clientCert)
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
) (*crdbConn, error) {
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
		return nil, fmt.Errorf("failed to connect to CockroachDB: %w", toCrdbError(err))
	}
	return &crdbConn{conn}, nil
}

func GetCRDBConnectionString(config *protos.CockroachDBConfig, flowName string) string {
	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	// the password is intentionally left out of the URL (ParseConfig sets it on
	// the parsed config) so it never appears in errors or logs
	u := &url.URL{
		Scheme: "postgres",
		User:   url.User(config.User),
		Host:   shared.JoinHostPort(config.Host, config.Port),
		Path:   "/" + config.Database,
	}
	q := u.Query()
	q.Set("application_name", applicationName)
	q.Set("client_encoding", "UTF8")
	if config.DisableTls {
		q.Set("sslmode", "disable")
	} else {
		q.Set("sslmode", "require")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *CockroachDBConnector) GetMajorVersion(ctx context.Context) (int, error) {
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
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
