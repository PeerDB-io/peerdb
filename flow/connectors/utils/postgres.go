package utils

import (
	"context"
	"fmt"
	"net/url"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
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

func GetCustomDataType(ctx context.Context, connStr string, dataType uint32) (qvalue.QValueKind, error) {
	var typeName string
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return qvalue.QValueKindString, fmt.Errorf("failed to create postgres connection pool"+
			" for custom type handling: %w", err)
	}

	defer pool.Close()

	err = pool.QueryRow(ctx, "SELECT typname FROM pg_type WHERE oid = $1", dataType).Scan(&typeName)
	if err != nil {
		return qvalue.QValueKindString, fmt.Errorf("failed to query pg_type for custom type handling: %w", err)
	}

	var qValueKind qvalue.QValueKind
	switch typeName {
	case "geometry":
		qValueKind = qvalue.QValueKindGeometry
	case "geography":
		qValueKind = qvalue.QValueKindGeography
	default:
		qValueKind = qvalue.QValueKindString
	}
	return qValueKind, nil
}
