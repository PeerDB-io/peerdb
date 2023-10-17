package utils

import (
	"context"
	"fmt"
	"net/url"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		pgConfig.User,
		passwordEscaped,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Database,
	)
	return connString
}

func GetCustomDataTypes(ctx context.Context, pool *pgxpool.Pool) (map[uint32]string, error) {
	rows, err := pool.Query(ctx, `
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
		var typeID uint32
		var typeName string
		if err := rows.Scan(&typeID, &typeName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		customTypeMap[typeID] = typeName
	}
	return customTypeMap, nil
}
