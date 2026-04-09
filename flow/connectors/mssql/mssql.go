package connmssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"net/url"
	"sync/atomic"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-1205-database-engine-error
const errDeadlock = 1205

func isDeadlock(err error) bool {
	var mssqlErr mssql.Error
	if !errors.As(err, &mssqlErr) {
		return false
	}
	return slices.ContainsFunc(mssqlErr.All, func(e mssql.Error) bool { return e.Number == errDeadlock })
}

type MsSqlConnector struct {
	*metadataStore.PostgresMetadata
	config         *protos.SqlServerConfig
	conn           *sql.DB
	logger         log.Logger
	serverVersion  string
	totalBytesRead atomic.Int64
	deltaBytesRead atomic.Int64
}

func NewMsSqlConnector(ctx context.Context, config *protos.SqlServerConfig) (*MsSqlConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	logger := internal.LoggerFromCtx(ctx)

	c := &MsSqlConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           logger,
	}

	connURL := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(config.User, config.Password),
		Host:   fmt.Sprintf("%s:%d", config.Host, config.Port),
	}
	if config.Database != "" {
		connURL.RawQuery = "database=" + url.QueryEscape(config.Database)
	}

	connector, err := mssql.NewConnector(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("[mssql] failed to create connector: %w", err)
	}

	meteredDialer := utils.NewMeteredDialer(
		&c.totalBytesRead, &c.deltaBytesRead,
		(&net.Dialer{Timeout: time.Minute}).DialContext, false)
	connector.Dialer = &meteredDialer

	c.conn = sql.OpenDB(connector)
	return c, nil
}

func (c *MsSqlConnector) Close() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("[mssql] failed to close connection", slog.Any("error", err))
			return fmt.Errorf("[mssql] failed to close connection: %w", err)
		}
	}
	return nil
}

func (c *MsSqlConnector) ConnectionActive(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

// retryOnDeadlock retries f up to 5 times on SQL Server deadlock (error 1205).
func (c *MsSqlConnector) RetryOnDeadlock(f func() error) error {
	var err error
	for i := range 5 {
		if i > 0 {
			c.logger.Warn("[mssql] deadlock, retrying", slog.Int("attempt", i+1), slog.Any("error", err))
		}
		err = f()
		if err == nil {
			return nil
		}
		if !isDeadlock(err) {
			return err
		}
	}
	return fmt.Errorf("[mssql] deadlock persisted after retries: %w", err)
}

func (c *MsSqlConnector) GetVersion(ctx context.Context) (string, error) {
	if c.serverVersion != "" {
		return c.serverVersion, nil
	}
	err := c.conn.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&c.serverVersion)
	if err != nil {
		return "", fmt.Errorf("[mssql] failed to get version: %w", err)
	}
	return c.serverVersion, nil
}

func (c *MsSqlConnector) StatActivity(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	rows, err := c.conn.QueryContext(ctx,
		"SELECT session_id, command, status, total_elapsed_time, text"+
			" FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle)"+
			" WHERE r.session_id != @@SPID")
	if err != nil {
		return nil, fmt.Errorf("[mssql] StatActivity: %w", err)
	}
	defer rows.Close()

	var statInfoRows []*protos.StatInfo
	for rows.Next() {
		var pid int64
		var command, state, query string
		var elapsed int64
		if err := rows.Scan(&pid, &command, &state, &elapsed, &query); err != nil {
			return nil, err
		}
		statInfoRows = append(statInfoRows, &protos.StatInfo{
			Pid:      pid,
			State:    state,
			Query:    query,
			Duration: float32(elapsed) / 1000.0,
		})
	}
	return &protos.PeerStatResponse{StatData: statInfoRows}, rows.Err()
}

func (c *MsSqlConnector) GetTableSizeEstimatedBytes(ctx context.Context, tableIdentifier string) (int64, error) {
	parsed, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return 0, err
	}
	var sizeBytes int64
	err = c.conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COALESCE(SUM(ps.used_page_count) * 8 * 1024, 0) FROM sys.dm_db_partition_stats ps"+
			" WHERE ps.object_id = OBJECT_ID(%s) AND ps.index_id IN (0, 1)",
		sanitize.QuoteString(parsed.Namespace+"."+parsed.Table))).Scan(&sizeBytes)
	if err != nil {
		return 0, fmt.Errorf("[mssql] GetTableSizeEstimatedBytes: %w", err)
	}
	return sizeBytes, nil
}
