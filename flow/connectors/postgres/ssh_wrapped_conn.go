package connpostgres

import (
	"context"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func NewPostgresConnFromConfig(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	tlsHost string,
	rdsAuth *utils.RDSAuth,
	tunnel *utils.SSHTunnel,
) (*pgx.Conn, error) {
	if tunnel != nil && tunnel.Client != nil {
		connConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := tunnel.Client.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &utils.NoDeadlineConn{Conn: conn}, nil
		}
		// DNS lookup seems to happen before connection is established which can be an issue if given host
		// can only be resolved on the SSH host https://github.com/jackc/pgx/issues/1724
		connConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
		}
	}
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
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		logger.Error("Failed to create connection", slog.Any("error", err))
		return nil, err
	}

	if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		logger.Error("Failed to ping connection", slog.Any("error", err), slog.String("host", connConfig.Host))
		return nil, err
	}

	return conn, nil
}
