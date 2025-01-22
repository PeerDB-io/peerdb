package connmysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MySqlConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MySqlConfig
	// go-mysql lacks context per query, cache connection per context
	conn   map[context.Context]*client.Conn
	logger log.Logger
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	return &MySqlConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		conn:             make(map[context.Context]*client.Conn),
		logger:           shared.LoggerFromCtx(ctx),
	}, nil
}

func (c *MySqlConnector) Close() error {
	var errs []error
	if c.conn != nil {
		for _, conn := range c.conn {
			errs = append(errs, conn.Close())
		}
		c.conn = nil
	}
	return errors.Join(errs...)
}

func (c *MySqlConnector) ConnectionActive(context.Context) error {
	return nil
}

func (c *MySqlConnector) connect(ctx context.Context) (*client.Conn, error) {
	conn := c.conn[ctx]
	if conn == nil {
		argF := []client.Option{func(conn *client.Conn) error {
			if !c.config.DisableTls {
				conn.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})
			}
			return nil
		}}
		var err error
		conn, err = client.ConnectWithContext(ctx, fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
			c.config.User, c.config.Password, c.config.Database, time.Minute, argF...)
		if err != nil {
			return nil, err
		}
		if _, err := conn.Execute("SET sql_mode = ANSI"); err != nil {
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

func (c *MySqlConnector) GetVersion(ctx context.Context) (string, error) {
	rr, err := c.Execute(ctx, "select @@version")
	if err != nil {
		return "", err
	}
	version, _ := rr.GetString(0, 0)
	c.logger.Info("[mysql] version", slog.String("version", version))
	return version, nil
}
