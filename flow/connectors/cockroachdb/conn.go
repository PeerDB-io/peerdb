package conncockroachdb

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// crdbConn is a light wrapper around *pgx.Conn that tags every returned error
// with exceptions.CockroachDBError. CockroachDB shares the Postgres wire
// protocol and SQLSTATE codes, so without the tag error classification cannot
// tell CockroachDB errors apart from Postgres ones.
type crdbConn struct {
	conn *pgx.Conn
}

// toCrdbError wraps err in exceptions.CockroachDBError, leaving nil and
// already-wrapped errors untouched. The wrapper unwraps cleanly, so
// errors.Is/As checks against pgx.ErrNoRows, *pgconn.PgError etc. still work.
func toCrdbError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*exceptions.CockroachDBError](err); ok {
		return err
	}
	return exceptions.NewCockroachDBError(err)
}

func (c *crdbConn) Ping(ctx context.Context) error {
	return toCrdbError(c.conn.Ping(ctx))
}

func (c *crdbConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	res, err := c.conn.Exec(ctx, sql, args...)
	return res, toCrdbError(err)
}

func (c *crdbConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := c.conn.Query(ctx, sql, args...)
	if err != nil {
		return rows, toCrdbError(err)
	}
	return &crdbRows{rows}, nil
}

func (c *crdbConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &crdbRow{c.conn.QueryRow(ctx, sql, args...)}
}

func (c *crdbConn) Close(ctx context.Context) error {
	return toCrdbError(c.conn.Close(ctx))
}

func (c *crdbConn) TypeMap() *pgtype.Map {
	return c.conn.TypeMap()
}

// crdbRows tags errors surfaced while iterating a result set.
type crdbRows struct {
	pgx.Rows
}

func (r *crdbRows) Err() error {
	return toCrdbError(r.Rows.Err())
}

func (r *crdbRows) Scan(dest ...any) error {
	return toCrdbError(r.Rows.Scan(dest...))
}

// crdbRow tags errors surfaced when scanning a single row.
type crdbRow struct {
	row pgx.Row
}

func (r *crdbRow) Scan(dest ...any) error {
	return toCrdbError(r.row.Scan(dest...))
}
