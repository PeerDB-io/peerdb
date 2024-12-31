// https://airbyte.com/blog/replicating-mysql-a-look-at-the-binlog-and-gtids

package connmysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type MySqlConnector struct {
	*metadataStore.PostgresMetadata
	config    *protos.MySqlConfig
	conn      *client.Conn
	syncer    *replication.BinlogSyncer
	logger    log.Logger
	replState mysql.GTIDSet
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   1729, // TODO put in config (or generate randomly, which is what go-mysql-org does)
		Flavor:     config.Flavor,
		Host:       config.Host,
		Port:       uint16(config.Port),
		User:       config.User,
		Password:   config.Password,
		UseDecimal: true,
		ParseTime:  true,
	})
	return &MySqlConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		syncer:           syncer,
		logger:           shared.LoggerFromCtx(ctx),
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
		// TODO need new connection if ctx changes between calls, or make upstream PR
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

		rs, err := c.conn.Execute(cmd, args...)
		if err != nil {
			if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
				reconnects -= 1
				c.conn.Close()
				c.conn = nil
				continue
			}
			return nil, err
		}
		return rs, nil
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

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *MySqlConnector) GetMasterGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var query string
	switch c.config.Flavor {
	case mysql.MariaDBFlavor:
		query = "select @@global.gtid_current_pos"
	default:
		query = "select @@global.gtid_executed"
	}
	rr, err := c.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to select @@global.gtid_executed: %w", err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to GetString for gtid_executed: %w", err)
	}
	gset, err := mysql.ParseGTIDSet(c.config.Flavor, gx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID from gtid_executed: %w", err)
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

func qkindFromMysql(ty uint8) (qvalue.QValueKind, error) {
	switch ty {
	case mysql.MYSQL_TYPE_DECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_TINY:
		return qvalue.QValueKindInt16, nil // TODO qvalue.QValueKindInt8
	case mysql.MYSQL_TYPE_SHORT:
		return qvalue.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_LONG:
		return qvalue.QValueKindInt32, nil
	case mysql.MYSQL_TYPE_FLOAT:
		return qvalue.QValueKindFloat32, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return qvalue.QValueKindFloat64, nil
	case mysql.MYSQL_TYPE_NULL:
		return qvalue.QValueKindInvalid, nil // TODO qvalue.QValueKindNothing
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_LONGLONG:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_INT24:
		return qvalue.QValueKindInt32, nil
	case mysql.MYSQL_TYPE_DATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_TIME:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_DATETIME:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_YEAR:
		return qvalue.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_NEWDATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_VARCHAR:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_BIT:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_DATETIME2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_TIME2:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_JSON:
		return qvalue.QValueKindJSON, nil
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_ENUM:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_SET:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_TINY_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_MEDIUM_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_LONG_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_VAR_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return qvalue.QValueKindGeometry, nil
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %d", ty)
	}
}

func qvalueFromMysqlFieldValue(qkind qvalue.QValueKind, fv mysql.FieldValue) (qvalue.QValue, error) {
	// TODO fill this in, maybe contribute upstream, figvure out how numeric etc fit in
	switch v := fv.Value().(type) {
	case nil:
		return qvalue.QValueNull(qkind), nil
	case uint64:
		// TODO unsigned integers
		return nil, errors.New("mysql unsigned integers not supported")
	case int64:
		switch qkind {
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert int to %s", qkind)
		}
	case float64:
		switch qkind {
		case qvalue.QValueKindFloat32:
			return qvalue.QValueFloat32{Val: float32(v)}, nil
		case qvalue.QValueKindFloat64:
			return qvalue.QValueFloat64{Val: float64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert float to %s", qkind)
		}
	case string:
		switch qkind {
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: v}, nil
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: []byte(v)}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert string to %s", qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %T", v)
	}
}
