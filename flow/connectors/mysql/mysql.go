package connmysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
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

func (c *MySqlConnector) Flavor() string {
	switch c.config.Flavor {
	case protos.MySqlFlavor_MYSQL_MYSQL:
		return mysql.MySQLFlavor
	case protos.MySqlFlavor_MYSQL_MARIA:
		return mysql.MariaDBFlavor
	default:
		return "unknown"
	}
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
			conn.SetCapability(mysql.CLIENT_COMPRESS)
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
	if c.Flavor() == mysql.MySQLFlavor {
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
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
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
	switch c.Flavor() {
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
	gset, err := mysql.ParseGTIDSet(c.Flavor(), gx)
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

func qkindFromMysql(field *mysql.Field) (qvalue.QValueKind, error) {
	unsigned := (field.Flag & mysql.UNSIGNED_FLAG) != 0
	switch field.Type {
	case mysql.MYSQL_TYPE_TINY:
		if unsigned {
			return qvalue.QValueKindUInt8, nil
		} else {
			return qvalue.QValueKindInt8, nil
		}
	case mysql.MYSQL_TYPE_SHORT:
		if unsigned {
			return qvalue.QValueKindUInt16, nil
		} else {
			return qvalue.QValueKindInt16, nil
		}
	case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
		if unsigned {
			return qvalue.QValueKindUInt32, nil
		} else {
			return qvalue.QValueKindInt32, nil
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		if unsigned {
			return qvalue.QValueKindUInt64, nil
		} else {
			return qvalue.QValueKindInt64, nil
		}
	case mysql.MYSQL_TYPE_FLOAT:
		return qvalue.QValueKindFloat32, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return qvalue.QValueKindFloat64, nil
	case mysql.MYSQL_TYPE_NULL:
		return qvalue.QValueKindInvalid, nil
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return qvalue.QValueKindTimestamp, nil
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
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_ENUM:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_SET:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
		if field.Charset == 0x3f { // binary https://dev.mysql.com/doc/dev/mysql-server/8.4.3/page_protocol_basic_character_set.html
			return qvalue.QValueKindBytes, nil
		} else {
			return qvalue.QValueKindString, nil
		}
	case mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return qvalue.QValueKindGeometry, nil
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %d", field.Type)
	}
}

func qkindFromMysqlColumnType(ct string) (qvalue.QValueKind, error) {
	ct, isUnsigned := strings.CutSuffix(ct, " unsigned")
	ct, param, _ := strings.Cut(ct, "(")
	switch ct {
	case "json":
		return qvalue.QValueKindJSON, nil
	case "char", "varchar", "text", "enum", "set":
		return qvalue.QValueKindString, nil
	case "binary", "varbinary", "blob":
		return qvalue.QValueKindBytes, nil
	case "date":
		return qvalue.QValueKindDate, nil
	case "time":
		return qvalue.QValueKindTime, nil
	case "datetime":
		return qvalue.QValueKindTimestamp, nil
	case "timestamp":
		return qvalue.QValueKindTimestamp, nil
	case "decimal", "numeric":
		return qvalue.QValueKindNumeric, nil
	case "float":
		return qvalue.QValueKindFloat32, nil
	case "double":
		return qvalue.QValueKindFloat64, nil
	case "tinyint":
		if strings.HasPrefix(param, "1)") {
			return qvalue.QValueKindBoolean, nil
		} else if isUnsigned {
			return qvalue.QValueKindUInt8, nil
		} else {
			return qvalue.QValueKindInt8, nil
		}
	case "smallint", "year":
		if isUnsigned {
			return qvalue.QValueKindUInt16, nil
		} else {
			return qvalue.QValueKindInt16, nil
		}
	case "mediumint", "int":
		if isUnsigned {
			return qvalue.QValueKindUInt32, nil
		} else {
			return qvalue.QValueKindInt32, nil
		}
	case "bigint", "bit":
		if isUnsigned {
			return qvalue.QValueKindUInt64, nil
		} else {
			return qvalue.QValueKindInt64, nil
		}
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %s", ct)
	}
}

func QRecordSchemaFromMysqlFields(tableSchema *protos.TableSchema, fields []*mysql.Field) (qvalue.QRecordSchema, error) {
	tableColumns := make(map[string]*protos.FieldDescription, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		tableColumns[col.Name] = col
	}

	schema := make([]qvalue.QField, 0, len(fields))
	for _, field := range fields {
		var precision int16
		var scale int16
		name := string(field.Name)
		var qkind qvalue.QValueKind
		if col, ok := tableColumns[name]; ok {
			qkind = qvalue.QValueKind(col.Type)
			if qkind == qvalue.QValueKindNumeric {
				precision, scale = datatypes.ParseNumericTypmod(col.TypeModifier)
			}
		} else {
			var err error
			qkind, err = qkindFromMysql(field)
			if err != nil {
				return qvalue.QRecordSchema{}, err
			}
		}

		schema = append(schema, qvalue.QField{
			Name:      name,
			Type:      qkind,
			Precision: precision,
			Scale:     scale,
			Nullable:  (field.Flag & mysql.NOT_NULL_FLAG) == 0,
		})
	}
	return qvalue.QRecordSchema{Fields: schema}, nil
}

func QValueFromMysqlFieldValue(qkind qvalue.QValueKind, fv mysql.FieldValue) (qvalue.QValue, error) {
	switch v := fv.Value().(type) {
	case nil:
		return qvalue.QValueNull(qkind), nil
	case uint64:
		switch qkind {
		case qvalue.QValueKindBoolean:
			return qvalue.QValueBoolean{Val: v != 0}, nil
		case qvalue.QValueKindInt8:
			return qvalue.QValueInt8{Val: int8(v)}, nil
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: int64(v)}, nil
		case qvalue.QValueKindUInt8:
			return qvalue.QValueUInt8{Val: uint8(v)}, nil
		case qvalue.QValueKindUInt16:
			return qvalue.QValueUInt16{Val: uint16(v)}, nil
		case qvalue.QValueKindUInt32:
			return qvalue.QValueUInt32{Val: uint32(v)}, nil
		case qvalue.QValueKindUInt64:
			return qvalue.QValueUInt64{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert uint64 to %s", qkind)
		}
	case int64:
		switch qkind {
		case qvalue.QValueKindBoolean:
			return qvalue.QValueBoolean{Val: v != 0}, nil
		case qvalue.QValueKindInt8:
			return qvalue.QValueInt8{Val: int8(v)}, nil
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: v}, nil
		case qvalue.QValueKindUInt8:
			return qvalue.QValueUInt8{Val: uint8(v)}, nil
		case qvalue.QValueKindUInt16:
			return qvalue.QValueUInt16{Val: uint16(v)}, nil
		case qvalue.QValueKindUInt32:
			return qvalue.QValueUInt32{Val: uint32(v)}, nil
		case qvalue.QValueKindUInt64:
			return qvalue.QValueUInt64{Val: uint64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert int64 to %s", qkind)
		}
	case float64:
		switch qkind {
		case qvalue.QValueKindFloat32:
			return qvalue.QValueFloat32{Val: float32(v)}, nil
		case qvalue.QValueKindFloat64:
			return qvalue.QValueFloat64{Val: float64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert float64 to %s", qkind)
		}
	case []byte:
		unsafeString := shared.UnsafeFastReadOnlyBytesToString(v)
		switch qkind {
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(v)}, nil
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: v}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: string(v)}, nil
		case qvalue.QValueKindNumeric:
			val, err := decimal.NewFromString(unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueNumeric{Val: val}, nil
		case qvalue.QValueKindTimestamp:
			if strings.HasPrefix(unsafeString, "0000-00-00") {
				return qvalue.QValueTimestamp{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse("2006-01-02 15:04:05.999999", unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTimestamp{Val: val}, nil
		case qvalue.QValueKindTime:
			val, err := time.Parse("15:04:05.999999", unsafeString)
			if err != nil {
				return nil, err
			}
			h, m, s := val.Clock()
			return qvalue.QValueTime{Val: time.Date(1970, 1, 1, h, m, s, val.Nanosecond(), val.Location())}, nil
		case qvalue.QValueKindDate:
			if unsafeString == "0000-00-00" {
				return qvalue.QValueDate{Val: time.Unix(0, 0)}, nil
			}
			val, err := time.Parse(time.DateOnly, unsafeString)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueDate{Val: val}, nil
		default:
			return nil, fmt.Errorf("cannot convert bytes %v to %s", v, qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %T", v)
	}
}
