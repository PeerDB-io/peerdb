package utils

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peer-flow/generated/protos"
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

func PostgresIdentifierNormalize(identifier string) string {
	if IsUpper(identifier) {
		return strings.ToLower(identifier)
	}
	return identifier
}

func PostgresSchemaTableNormalize(schemaTable *SchemaTable) string {
	return fmt.Sprintf(`%s.%s`, PostgresIdentifierNormalize(schemaTable.Schema),
		PostgresIdentifierNormalize(schemaTable.Table))
}
