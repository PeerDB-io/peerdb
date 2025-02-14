package connmysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MySqlConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MySqlConfig
	ssh    utils.SSHTunnel
	// go-mysql lacks context per query, cache connection per context
	conn   map[context.Context]*client.Conn
	logger log.Logger
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	ssh, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}
	return &MySqlConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		conn:             make(map[context.Context]*client.Conn),
		ssh:              ssh,
		logger:           shared.LoggerFromCtx(ctx),
	}, nil
}

func (c *MySqlConnector) Flavor(ctx context.Context) string {
	// it looks like gtid_mode was never supported in MariaDB, so hoping this clears the flavor up
	if _, err := c.Execute(ctx, "select @@gtid_mode"); err != nil {
		if mysql.ErrorCode(err.Error()) == mysql.ER_UNKNOWN_SYSTEM_VARIABLE {
			return mysql.MariaDBFlavor
		}
		return "unknown"
	}
	return mysql.MySQLFlavor
}

func (c *MySqlConnector) Close() error {
	var errs []error
	if c.conn != nil {
		for _, conn := range c.conn {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		c.conn = nil
	}
	if err := c.ssh.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c *MySqlConnector) ConnectionActive(context.Context) error {
	return nil
}

func (c *MySqlConnector) Dialer() client.Dialer {
	if c.ssh.Client == nil {
		return (&net.Dialer{Timeout: time.Minute}).DialContext
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return c.ssh.Client.DialContext(ctx, network, addr)
	}
}

func (c *MySqlConnector) connect(ctx context.Context) (*client.Conn, error) {
	conn := c.conn[ctx]
	if conn == nil {
		argF := []client.Option{func(conn *client.Conn) error {
			conn.SetCapability(mysql.CLIENT_COMPRESS)
			if !c.config.DisableTls {
				conn.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})
			}
			return nil
		}}
		var err error
		conn, err = client.ConnectWithDialer(ctx, "", fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
			c.config.User, c.config.Password, c.config.Database, c.Dialer(), argF...)
		if err != nil {
			return nil, err
		}
		if _, err := conn.Execute("SET sql_mode = 'ANSI,NO_BACKSLASH_ESCAPES'"); err != nil {
			return nil, fmt.Errorf("failed to set sql_mode to ANSI: %w", err)
		}
	}
	return conn, nil
}

// withRetries return an iterable over connections,
// consumer should break out of loop on success or error,
// to retry for mysql.ErrBadConn
func (c *MySqlConnector) withRetries(ctx context.Context) iter.Seq2[*client.Conn, error] {
	return func(yield func(*client.Conn, error) bool) {
		for range 3 {
			conn, err := c.connect(ctx)
			if err == nil {
				c.conn[ctx] = conn
			}
			if !yield(conn, err) {
				return
			}
			if err == nil {
				_ = conn.Close()
				delete(c.conn, ctx)
			}
		}
	}
}

func (c *MySqlConnector) Execute(ctx context.Context, cmd string, args ...interface{}) (*mysql.Result, error) {
	var connectionErr error
	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return nil, err
		}

		rs, err := conn.Execute(cmd, args...)
		if err != nil && mysql.ErrorEqual(err, mysql.ErrBadConn) {
			connectionErr = err
			continue
		}
		return rs, err
	}
	return nil, connectionErr
}

func (c *MySqlConnector) ExecuteSelectStreaming(ctx context.Context, cmd string, result *mysql.Result,
	rowCb client.SelectPerRowCallback,
	resultCb client.SelectPerResultCallback,
	args ...interface{},
) error {
	var connectionErr error
	for conn, err := range c.withRetries(ctx) {
		if err != nil {
			return err
		}

		if len(args) == 0 {
			if err := conn.ExecuteSelectStreaming(cmd, result, rowCb, resultCb); err != nil {
				if mysql.ErrorEqual(err, mysql.ErrBadConn) {
					connectionErr = err
					continue
				}
				return err
			}
		} else {
			stmt, err := conn.Prepare(cmd)
			if err != nil {
				if mysql.ErrorEqual(err, mysql.ErrBadConn) {
					connectionErr = err
					continue
				}
				return err
			}
			err = stmt.ExecuteSelectStreaming(result, rowCb, resultCb, args...)
			_ = stmt.Close()
			if err != nil {
				if mysql.ErrorEqual(err, mysql.ErrBadConn) {
					connectionErr = err
					continue
				}
				return err
			}
		}
		return nil
	}
	return connectionErr
}

func (c *MySqlConnector) GetGtidModeOn(ctx context.Context) (bool, error) {
	if c.Flavor(ctx) == mysql.MySQLFlavor {
		rr, err := c.Execute(ctx, "select @@gtid_mode")
		if err != nil {
			return false, err
		}

		gtid_mode, err := rr.GetString(0, 0)
		if err != nil {
			return false, err
		}

		return gtid_mode == "ON", nil
	} else {
		// mariadb always enabled: https://mariadb.com/kb/en/gtid/#using-global-transaction-ids
		// doesn't seem to work so disabling for now
		return false, nil
	}
}

func (c *MySqlConnector) CompareServerVersion(ctx context.Context, version string) (int, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return 0, err
	}
	return conn.CompareServerVersion(version)
}

func (c *MySqlConnector) GetMasterPos(ctx context.Context) (mysql.Position, error) {
	showBinlogStatus := "SHOW BINARY LOG STATUS"
	masterReplaced := "8.4.0" // https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-0.html
	if c.Flavor(ctx) == mysql.MariaDBFlavor {
		showBinlogStatus = "SHOW BINLOG STATUS"
		masterReplaced = "10.5.2" // https://mariadb.com/kb/en/show-binlog-status
	}
	if eq, err := c.CompareServerVersion(ctx, masterReplaced); err == nil && eq < 0 {
		showBinlogStatus = "SHOW MASTER STATUS"
	}

	rr, err := c.Execute(ctx, showBinlogStatus)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to %s: %w", showBinlogStatus, err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetUint(0, 1)
	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *MySqlConnector) GetMasterGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var query string
	switch c.Flavor(ctx) {
	case mysql.MariaDBFlavor:
		query = "select @@gtid_current_pos"
	default:
		query = "select @@gtid_executed"
	}
	rr, err := c.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to %s: %w", query, err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to GetString for %s: %w", query, err)
	}
	c.logger.Info("QQQQ", slog.String("gset", gx))
	gset, err := mysql.ParseGTIDSet(c.Flavor(ctx), gx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID from %s: %w", query, err)
	}
	return gset, nil
}

func (c *MySqlConnector) GetVersion(ctx context.Context) (string, error) {
	rr, err := c.Execute(ctx, "select @@version")
	if err != nil {
		return "", err
	}
	version, _ := rr.GetString(0, 0)
	c.logger.Info("[mysql] version", slog.String("version", version))
	return version, nil
}
