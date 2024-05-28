package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type PGVersion int32

const (
	POSTGRES_12 PGVersion = 120000
	POSTGRES_13 PGVersion = 130000
	POSTGRES_15 PGVersion = 150000
)

func IsUniqueError(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == pgerrcode.UniqueViolation
}

func GetPGConnectionString(pgConfig *protos.PostgresConfig) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?application_name=peerdb&client_encoding=UTF8",
		pgConfig.User,
		passwordEscaped,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Database,
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
		return nil, fmt.Errorf("failed to get custom types: %w", err)
	}

	customTypeMap := map[uint32]string{}
	for rows.Next() {
		var typeID pgtype.Uint32
		var typeName pgtype.Text
		if err := rows.Scan(&typeID, &typeName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		customTypeMap[typeID.Uint32] = typeName.String
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
	err := tx.Rollback(ctx)
	if err != nil && err != pgx.ErrTxClosed {
		logger.Error("error while rolling back transaction", slog.Any("error", err))
	}
}
