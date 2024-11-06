package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type PGVersion int32

const (
	POSTGRES_12 PGVersion = 120000
	POSTGRES_13 PGVersion = 130000
	POSTGRES_14 PGVersion = 140000
	POSTGRES_15 PGVersion = 150000
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig, flowName string) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?application_name=%s&client_encoding=UTF8",
		pgConfig.User,
		passwordEscaped,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Database,
		applicationName,
	)
	return connString
}

func GetCustomDataTypes(ctx context.Context, conn *pgx.Conn) (map[uint32]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT t.oid, t.typname as type
		FROM pg_type t
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
		AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
		AND n.nspname NOT IN ('pg_catalog', 'information_schema');
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get customTypeMapping: %w", err)
	}

	customTypeMap := map[uint32]string{}
	var typeID pgtype.Uint32
	var typeName pgtype.Text
	if _, err := pgx.ForEachRow(rows, []any{&typeID, &typeName}, func() error {
		customTypeMap[typeID.Uint32] = typeName.String
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to scan into customTypeMapping: %w", err)
	}
	return customTypeMap, nil
}

func RegisterHStore(ctx context.Context, conn *pgx.Conn) error {
	var hstoreOID uint32
	err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		// hstore isn't present, just proceed
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}

	conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})

	return nil
}

func GetMajorVersion(ctx context.Context, conn *pgx.Conn) (PGVersion, error) {
	var version int32
	err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::INTEGER").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get server version: %w", err)
	}

	return PGVersion(version), nil
}

func RollbackTx(tx pgx.Tx, logger log.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		logger.Error("error while rolling back transaction", slog.Any("error", err))
	}
}

func UpdateCDCConfigInCatalog(ctx context.Context, pool *pgxpool.Pool,
	logger log.Logger, cfg *protos.FlowConnectionConfigs,
) error {
	logger.Info("syncing state to catalog: updating config_proto in flows", slog.String("flowName", cfg.FlowJobName))

	cfgBytes, err := proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	_, err = pool.Exec(ctx, "UPDATE flows SET config_proto=$1,updated_at=now() WHERE name=$2", cfgBytes, cfg.FlowJobName)
	if err != nil {
		logger.Error("failed to update catalog", slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return fmt.Errorf("failed to update catalog: %w", err)
	}

	logger.Info("synced state to catalog: updated config_proto in flows", slog.String("flowName", cfg.FlowJobName))
	return nil
}

func LoadTableSchemaFromCatalog(
	ctx context.Context,
	pool *pgxpool.Pool,
	flowName string,
	tableName string,
) (*protos.TableSchema, error) {
	var tableSchemaBytes []byte
	if err := pool.QueryRow(
		ctx,
		"select table_schema from table_schema_mapping where flow_name = $1 and table_name = $2",
		flowName,
		tableName,
	).Scan(&tableSchemaBytes); err != nil {
		return nil, err
	}
	tableSchema := &protos.TableSchema{}
	return tableSchema, proto.Unmarshal(tableSchemaBytes, tableSchema)
}

func IsSQLStateError(err error, sqlStates ...string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && slices.Contains(sqlStates, pgErr.Code)
}
