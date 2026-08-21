package connpostgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func NewPostgresConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tlsHost string,
	rdsAuth *utils.RDSAuth,
	cloudSQLAuth auth.TokenProvider,
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
	connConfig, err := preparePostgresConnConfig(ctx, connConfig, tlsHost, rdsAuth, cloudSQLAuth)
	if err != nil {
		return nil, err
	}
	logger := internal.LoggerFromCtx(ctx)

	// If the endpoint is misbehaved (e.g. a TCP tunnel started pointing at something new),
	// the random initial sequence of bytes may be misinterpreted as a multi-GB message and
	// crash the process. Avoid that by limiting the max body length just for the initial handshake.
	connConfig.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		frontend := pgproto3.NewFrontend(r, w)
		frontend.SetMaxBodyLen(16 * 1024 * 1024)
		return frontend
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create connection", slog.Any("error", err))
		return nil, err
	}

	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		logger.Error("Failed to ping connection", slog.Any("error", err), slog.String("host", connConfig.Host))
		conn.Close(ctx)
		return nil, err
	}

	conn.PgConn().Frontend().SetMaxBodyLen(0)

	return conn, nil
}

func preparePostgresConnConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tlsHost string,
	rdsAuth *utils.RDSAuth,
	cloudSQLAuth auth.TokenProvider,
) (*pgx.ConnConfig, error) {
	logger := internal.LoggerFromCtx(ctx)
	if rdsAuth != nil {
		host := connConfig.Host
		if tlsHost != "" {
			host = tlsHost
		}
		logger.Info("Setting up IAM auth for Postgres")
		token, err := utils.GetRDSToken(ctx, utils.RDSConnectionConfig{
			Host: host,
			Port: uint32(connConfig.Port),
			User: connConfig.User,
		}, rdsAuth, "POSTGRES")
		if err != nil {
			return nil, err
		}
		connConfig = connConfig.Copy()
		connConfig.Password = token
	}
	if cloudSQLAuth != nil {
		token, err := cloudSQLAuth.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get PostgreSQL Cloud SQL IAM token: %w", err)
		}
		if token == nil || token.Value == "" {
			return nil, fmt.Errorf("PostgreSQL Cloud SQL IAM token is empty")
		}
		connConfig = connConfig.Copy()
		connConfig.Password = token.Value
	}
	return connConfig, nil
}
