package connpostgres

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type ReplState struct {
	Slot        string
	Publication string
	Offset      int64
	LastOffset  atomic.Int64
}

type PostgresConnector struct {
	logger                 log.Logger
	customTypeMapping      map[uint32]shared.CustomDataType
	ssh                    *utils.SSHTunnel
	conn                   *pgx.Conn
	replConn               *pgx.Conn
	replState              *ReplState
	Config                 *protos.PostgresConfig
	hushWarnOID            map[uint32]struct{}
	relationMessageMapping model.RelationMessageMapping
	typeMap                *pgtype.Map
	rdsAuth                *utils.RDSAuth
	connStr                string
	metadataSchema         string
	replLock               sync.Mutex
	pgVersion              shared.PGVersion
}

func NewPostgresConnector(ctx context.Context, env map[string]string, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	flowNameInApplicationName, err := internal.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		logger.Error("Failed to get flow name from application name", slog.Any("error", err))
	}
	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := internal.GetPGConnectionString(pgConfig, flowName)
	connConfig, err := ParseConfig(connectionString, pgConfig)
	if err != nil {
		return nil, err
	}

	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"
	// Required for QueryExecModeSimpleProtocol, which is set in some places while querying;
	// pgx refuses simple protocol when the server reports standard_conforming_strings=off.
	connConfig.Config.RuntimeParams["standard_conforming_strings"] = "on"

	tunnel, err := utils.NewSSHTunnel(ctx, pgConfig.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	var rdsAuth *utils.RDSAuth
	if pgConfig.AuthType == protos.PostgresAuthType_POSTGRES_IAM_AUTH {
		rdsAuth = &utils.RDSAuth{
			AwsAuthConfig: pgConfig.AwsAuth,
		}
		if err := rdsAuth.VerifyAuthConfig(); err != nil {
			logger.Error("failed to verify auth config", slog.Any("error", err))
			return nil, fmt.Errorf("failed to verify auth config: %w", err)
		}
	}
	conn, err := NewPostgresConnFromConfig(ctx, connConfig, pgConfig.TlsHost, rdsAuth, tunnel)
	if err != nil {
		tunnel.Close()

		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			switch pgErr.Code {
			case pgerrcode.InvalidAuthorizationSpecification,
				pgerrcode.InvalidPassword,
				pgerrcode.InsufficientPrivilege:
				err = exceptions.NewAuthError(err)
			}
		}
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if pgConfig.MetadataSchema != nil {
		metadataSchema = *pgConfig.MetadataSchema
	}

	connector := &PostgresConnector{
		logger:                 logger,
		Config:                 pgConfig,
		ssh:                    tunnel,
		conn:                   conn,
		replConn:               nil,
		replState:              nil,
		customTypeMapping:      nil,
		hushWarnOID:            make(map[uint32]struct{}),
		relationMessageMapping: make(model.RelationMessageMapping),
		connStr:                connectionString,
		metadataSchema:         metadataSchema,
		replLock:               sync.Mutex{},
		pgVersion:              0,
		typeMap:                pgtype.NewMap(),
		rdsAuth:                rdsAuth,
	}

	tunnel.StartKeepalive(context.Background(), func() {
		connector.logger.Info("SSH keepalive failed, closing connections")
		bgCtx := context.Background()
		timeout, cancel := context.WithTimeout(bgCtx, 5*time.Second)
		defer cancel()
		if err := connector.conn.Close(timeout); err != nil {
			connector.logger.Error("Failed to close Postgres connection on SSH keepalive failure",
				slog.Any("error", err))
		}
		if connector.replConn != nil {
			timeout2, cancel2 := context.WithTimeout(bgCtx, 5*time.Second)
			defer cancel2()
			if err := connector.replConn.Close(timeout2); err != nil {
				connector.logger.Error("Failed to close Postgres replication connection on SSH keepalive failure",
					slog.Any("error", err))
			}
		}
	})

	return connector, nil
}

func ParseConfig(connectionString string, pgConfig *protos.PostgresConfig) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if pgConfig.RequireTls || pgConfig.RootCa != nil {
		tlsConfig, err := common.CreateTlsConfig(tls.VersionTLS12, pgConfig.RootCa, connConfig.Host, pgConfig.TlsHost, false)
		if err != nil {
			return nil, err
		}
		connConfig.TLSConfig = tlsConfig
	}
	return connConfig, nil
}

func (c *PostgresConnector) fetchCustomTypeMapping(ctx context.Context) (map[uint32]shared.CustomDataType, error) {
	if c.customTypeMapping == nil {
		customTypeMapping, err := shared.GetCustomDataTypes(ctx, c.conn)
		if err != nil {
			return nil, err
		}
		c.customTypeMapping = customTypeMapping
	}
	return c.customTypeMapping, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	var errs []error
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.conn.Close(timeout); err != nil {
			c.logger.Error("failed to close Postgres connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close Postgres connection: %w", err))
		}

		if c.replConn != nil {
			timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.replConn.Close(timeout); err != nil {
				c.logger.Error("failed to close Postgres replication connection", slog.Any("error", err))
				errs = append(errs, fmt.Errorf("failed to close Postgres replication connection: %w", err))
			}
		}

		if err := c.ssh.Close(); err != nil {
			c.logger.Error("[postgres] failed to close SSH tunnel", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("[postgres] failed to close SSH tunnel: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (c *PostgresConnector) Conn() *pgx.Conn {
	return c.conn
}

// ConnectionActive returns nil if the connection is active.
func (c *PostgresConnector) ConnectionActive(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	_, pingErr := c.conn.Exec(ctx, "SELECT 1")
	return pingErr
}

func (c *PostgresConnector) StatActivity(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	rows, err := c.Conn().Query(ctx, "SELECT pid, wait_event, wait_event_type, query_start::text, query,"+
		"CAST(EXTRACT(epoch FROM(now()-query_start)) AS float4) AS dur, state"+
		" FROM pg_stat_activity WHERE "+
		"usename=$1 AND application_name LIKE 'peerdb%';", c.Config.User)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get stat info", slog.Any("error", err))
		return nil, err
	}

	statInfoRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.StatInfo, error) {
		var pid int64
		var waitEvent pgtype.Text
		var waitEventType pgtype.Text
		var queryStart pgtype.Text
		var query pgtype.Text
		var duration pgtype.Float4
		// shouldn't be null
		var state string

		if err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration, &state); err != nil {
			slog.ErrorContext(ctx, "Failed to scan row", slog.Any("error", err))
			return nil, err
		}

		we := waitEvent.String
		if !waitEvent.Valid {
			we = ""
		}

		wet := waitEventType.String
		if !waitEventType.Valid {
			wet = ""
		}

		q := query.String
		if !query.Valid {
			q = ""
		}

		qs := queryStart.String
		if !queryStart.Valid {
			qs = ""
		}

		d := duration.Float32
		if !duration.Valid {
			d = -1
		}

		return &protos.StatInfo{
			Pid:           pid,
			WaitEvent:     we,
			WaitEventType: wet,
			QueryStart:    qs,
			Query:         q,
			Duration:      d,
			State:         state,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}

func (c *PostgresConnector) GetVersion(ctx context.Context) (string, error) {
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", err
	}
	c.logger.Info("[postgres] version", slog.String("version", version))
	return version, nil
}

func (c *PostgresConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// First check for Aurora by trying to look up aurora_version()
	var isAurora bool
	err := c.conn.QueryRow(ctx, "SELECT to_regproc('pg_catalog.aurora_version') IS NOT NULL").Scan(&isAurora)
	if err != nil {
		c.logger.Error("failed to query to_regproc for determining variant", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}
	if isAurora {
		return protos.DatabaseVariant_AWS_AURORA, nil
	}

	// It's not Aurora - continue checking other variants
	settingsQuery := `
		SELECT name, setting
		FROM pg_settings
		WHERE name IN (
			'rds.extensions',
			'cloudsql.logical_decoding',
			'azure.extensions',
			'neon.endpoint_id',
			'extwlist.pscale_allowed_extensions',
			'supautils.privileged_extensions'
		) AND setting IS NOT NULL AND setting != ''`

	rows, err := c.conn.Query(ctx, settingsQuery)
	if err != nil {
		c.logger.Error("failed to query pg_settings for determining variant", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			c.logger.Warn("failed to scan from pg_settings", slog.Any("error", err))
			continue
		}

		switch name {
		case "rds.extensions":
			return protos.DatabaseVariant_AWS_RDS, nil
		case "cloudsql.logical_decoding":
			return protos.DatabaseVariant_GOOGLE_CLOUD_SQL, nil
		case "azure.extensions":
			return protos.DatabaseVariant_AZURE_DATABASE, nil
		case "neon.endpoint_id":
			return protos.DatabaseVariant_NEON, nil
		case "extwlist.pscale_allowed_extensions":
			return protos.DatabaseVariant_PLANETSCALE, nil
		case "supautils.privileged_extensions":
			return protos.DatabaseVariant_SUPABASE, nil
		}
	}

	if err := rows.Err(); err != nil {
		c.logger.Error("error iterating pg_settings rows", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}

func (c *PostgresConnector) GetTableSizeEstimatedBytes(ctx context.Context, tableIdentifier string) (int64, error) {
	parsed, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", tableIdentifier, err)
	}
	tableSizeQuery := "SELECT pg_relation_size(to_regclass($1))"
	var tableSizeBytes pgtype.Int8
	if err := c.conn.QueryRow(ctx, tableSizeQuery, parsed.String()).Scan(&tableSizeBytes); err != nil {
		return 0, err
	}
	if !tableSizeBytes.Valid {
		return 0, fmt.Errorf("table size is not valid")
	}
	return tableSizeBytes.Int64, nil
}
