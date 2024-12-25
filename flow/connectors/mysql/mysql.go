// https://airbyte.com/blog/replicating-mysql-a-look-at-the-binlog-and-gtids

package connmysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type MySqlConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MySqlConfig
	conn   *client.Conn
	syncer *replication.BinlogSyncer
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   1729, // TODO put in config
		Flavor:     config.Flavor,
		Host:       config.Host,
		Port:       uint16(config.Port),
		User:       config.User,
		Password:   config.Password,
		UseDecimal: true,
		ParseTime:  true,
	})
	return &MySqlConnector{
		config: config,
		syncer: syncer,
	}, nil
}

func (c *MySqlConnector) Close() error {
	if c.syncer != nil {
		c.syncer.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *MySqlConnector) ConnectionActive(context.Context) error {
	if c.conn != nil {
		return c.conn.Ping()
	}
	return nil
}

func (c *MySqlConnector) connect(ctx context.Context, options ...client.Option) (*client.Conn, error) {
	return client.ConnectWithContext(ctx, fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		c.config.User, c.config.Password, c.config.Database, time.Minute, options...)
}

func (c *MySqlConnector) Execute(ctx context.Context, cmd string, args ...interface{}) (*mysql.Result, error) {
	reconnects := 3
	for {
		if c.conn == nil {
			var err error
			var argF []client.Option
			if !c.config.DisableTls {
				argF = append(argF, func(conn *client.Conn) error {
					conn.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})
					return nil
				})
			}
			c.conn, err = c.connect(ctx, argF...)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to mysql server: %w", err)
			}
		}

		rr, err := c.conn.Execute(cmd, args...)
		if err != nil {
			if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
				reconnects -= 1
				c.conn.Close()
				c.conn = nil
				continue
			}
			return nil, err
		}
		return rr, nil
	}
}

func (c *MySqlConnector) GetMasterPos(ctx context.Context) (mysql.Position, error) {
	showBinlogStatus := "SHOW BINARY LOG STATUS"
	if eq, err := c.conn.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		showBinlogStatus = "SHOW MASTER STATUS"
	}

	rr, err := c.Execute(ctx, showBinlogStatus)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to SHOW BINARY LOG STATUS: %w", err)
	}

	// TODO: check error?
	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *MySqlConnector) GetMasterGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var query string
	switch c.config.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to SELECT @@GLOBAL.GTID_EXECUTED: %w", err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to GetString for GTID_EXECUTED: %w", err)
	}
	gset, err := mysql.ParseGTIDSet(c.config.Flavor, gx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID from GTID_EXECUTED: %w", err)
	}
	return gset, nil
}
