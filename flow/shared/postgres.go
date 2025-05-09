package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type PGVersion int32

const (
	POSTGRES_12 PGVersion = 120000
	POSTGRES_13 PGVersion = 130000
	POSTGRES_14 PGVersion = 140000
	POSTGRES_15 PGVersion = 150000
	POSTGRES_16 PGVersion = 160000
)

type CustomDataType struct {
	Name  string
	Type  byte
	Delim byte // non-zero character for arrays
}

func GetCustomDataTypes(ctx context.Context, conn *pgx.Conn) (map[uint32]CustomDataType, error) {
	rows, err := conn.Query(ctx, `
		SELECT t.oid, t.typname, coalesce(at.typtype, t.typtype), coalesce(at.typdelim, 0::"char")
		FROM pg_catalog.pg_type t
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		LEFT JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
		LEFT JOIN pg_catalog.pg_type at ON at.typarray = t.oid
		WHERE t.typrelid = 0 OR c.relkind = 'c'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema');
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get customTypeMapping: %w", err)
	}

	customTypeMap := map[uint32]CustomDataType{}
	var typeID pgtype.Uint32
	var cdt CustomDataType
	if _, err := pgx.ForEachRow(rows, []any{&typeID, &cdt.Name, &cdt.Type, &cdt.Delim}, func() error {
		customTypeMap[typeID.Uint32] = cdt
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to scan into custom type mapping: %w", err)
	}
	return customTypeMap, nil
}

func RegisterHStore(ctx context.Context, conn *pgx.Conn) error {
	var hstoreOID uint32
	err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		// hstore isn't present, just proceed
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}

	conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})

	return nil
}

func GetMajorVersion(ctx context.Context, conn *pgx.Conn) (PGVersion, error) {
	var version int32
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::INTEGER").Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to get server version: %w", err)
	}

	return PGVersion(version), nil
}

func RollbackTx(tx pgx.Tx, logger log.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		logger.Error("error while rolling back transaction", slog.Any("error", err))
	}
}

func IsSQLStateError(err error, sqlStates ...string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && slices.Contains(sqlStates, pgErr.Code)
}

type CatalogPool struct {
	Pool *pgxpool.Pool
}

type CatalogRow struct {
	Row pgx.Row
}

type CatalogRows struct {
	Rows pgx.Rows
}

type CatalogTx struct {
	Tx pgx.Tx
}

func (p CatalogPool) Ping(ctx context.Context) error {
	if err := p.Pool.Ping(ctx); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (p CatalogPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	r, err := p.Pool.Exec(ctx, sql, arguments...)
	if err != nil {
		return r, exceptions.NewCatalogError(err)
	}
	return r, nil
}

func (p CatalogPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return CatalogRow{p.Pool.QueryRow(ctx, sql, args...)}
}

func (p CatalogPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)
	if err != nil {
		return CatalogRows{rows}, exceptions.NewCatalogError(err)
	}
	return CatalogRows{rows}, nil
}

func (p CatalogPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.BeginTx(ctx, pgx.TxOptions{})
}

func (p CatalogPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, txOptions)
	if err != nil {
		return CatalogTx{tx}, exceptions.NewCatalogError(err)
	}
	return CatalogTx{tx}, nil
}

func (row CatalogRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (rows CatalogRows) Close() {
	rows.Rows.Close()
}

func (rows CatalogRows) Err() error {
	if err := rows.Rows.Err(); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (rows CatalogRows) CommandTag() pgconn.CommandTag {
	return rows.Rows.CommandTag()
}

func (rows CatalogRows) FieldDescriptions() []pgconn.FieldDescription {
	return rows.Rows.FieldDescriptions()
}

func (rows CatalogRows) Next() bool {
	return rows.Rows.Next()
}

func (rows CatalogRows) Scan(dest ...any) error {
	if err := rows.Rows.Scan(dest...); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (rows CatalogRows) Values() ([]any, error) {
	res, err := rows.Rows.Values()
	if err != nil {
		return res, exceptions.NewCatalogError(err)
	}
	return res, nil
}

func (rows CatalogRows) RawValues() [][]byte {
	return rows.Rows.RawValues()
}

func (rows CatalogRows) Conn() *pgx.Conn {
	return rows.Rows.Conn()
}

func (tx CatalogTx) Begin(ctx context.Context) (pgx.Tx, error) {
	subtx, err := tx.Tx.Begin(ctx)
	if err != nil {
		return CatalogTx{tx}, exceptions.NewCatalogError(err)
	}
	return subtx, nil
}

func (tx CatalogTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (tx CatalogTx) Rollback(ctx context.Context) error {
	if err := tx.Tx.Rollback(ctx); err != nil {
		return exceptions.NewCatalogError(err)
	}
	return nil
}

func (tx CatalogTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	val, err := tx.Tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
	if err != nil {
		return val, exceptions.NewCatalogError(err)
	}
	return val, nil
}

func (tx CatalogTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return tx.Tx.SendBatch(ctx, b)
}

func (tx CatalogTx) LargeObjects() pgx.LargeObjects {
	return tx.Tx.LargeObjects()
}

func (tx CatalogTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return tx.Tx.Prepare(ctx, name, sql)
}

func (tx CatalogTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func (tx CatalogTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := tx.Tx.Query(ctx, sql, args...)
	if err != nil {
		return CatalogRows{rows}, exceptions.NewCatalogError(err)
	}
	return CatalogRows{rows}, err
}

func (tx CatalogTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return CatalogRow{tx.Tx.QueryRow(ctx, sql, args...)}
}

func (tx CatalogTx) Conn() *pgx.Conn {
	return tx.Tx.Conn()
}

const (
	psSearch = iota
	psSearch2
	psQuoted
	psQuotedEscape
	psUnquoted
	psUnquotedEscape
)

// see array_in from postgres
func ParsePgArrayStringToStringSlice(data string, delim byte) []string {
	return ParsePgArrayToStringSlice(UnsafeFastStringToReadOnlyBytes(data), delim)
}

func ParsePgArrayToStringSlice(data []byte, delim byte) []string {
	var result []string
	var sb []byte
	ps := psSearch2
	for _, ch := range data {
		switch ps {
		case psSearch:
			if ch == delim {
				result = append(result, "")
			} else if ch == '}' {
				ps = psSearch2
			} else if ch == '"' {
				ps = psQuoted
			} else if ch == '\\' {
				ps = psUnquotedEscape
			} else if ch != '{' && ch != ' ' && ch != '\t' && ch != '\n' && ch != '\v' && ch != '\f' && ch != '\r' {
				sb = append(sb, ch)
				ps = psUnquoted
			}
		case psSearch2:
			if ch == '{' {
				ps = psSearch
			}
		case psQuoted:
			if ch == '\\' {
				ps = psQuotedEscape
			} else if ch == '"' {
				ps = psUnquoted
			} else {
				sb = append(sb, ch)
			}
		case psUnquoted:
			if ch == '\\' {
				ps = psUnquotedEscape
			} else if ch == '"' {
				ps = psQuoted
			} else if ch == delim || ch == '}' {
				result = append(result, string(sb))
				sb = sb[:0]
				if ch == '}' {
					ps = psSearch2
				} else {
					ps = psSearch
				}
			} else {
				sb = append(sb, ch)
			}
		case psQuotedEscape:
			sb = append(sb, ch)
			ps = psQuoted
		case psUnquotedEscape:
			sb = append(sb, ch)
			ps = psUnquoted
		}
	}
	return result
}
