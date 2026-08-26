package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestToCrdbError(t *testing.T) {
	require.NoError(t, toCrdbError(nil))

	// errors get tagged as CockroachDB errors
	pgErr := &pgconn.PgError{Code: pgerrcode.UndefinedTable, Message: `relation "t" does not exist`}
	wrapped := toCrdbError(fmt.Errorf("query failed: %w", pgErr))
	var crdbErr *exceptions.CockroachDBError
	require.ErrorAs(t, wrapped, &crdbErr)

	// the tag is transparent: sentinel and type checks still reach the cause
	var unwrappedPgErr *pgconn.PgError
	require.ErrorAs(t, wrapped, &unwrappedPgErr)
	require.Equal(t, pgerrcode.UndefinedTable, unwrappedPgErr.Code)
	require.ErrorIs(t, toCrdbError(fmt.Errorf("scan: %w", pgx.ErrNoRows)), pgx.ErrNoRows)
	require.ErrorIs(t, toCrdbError(context.Canceled), context.Canceled)

	// the message is unchanged
	cause := errors.New("connection refused")
	require.Equal(t, cause.Error(), toCrdbError(cause).Error())

	// already-tagged errors are not wrapped again
	require.Equal(t, wrapped, toCrdbError(wrapped))
}
