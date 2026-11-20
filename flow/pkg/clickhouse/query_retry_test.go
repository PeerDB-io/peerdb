package clickhouse

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

// retryableErrConn always fails Exec/Query with a retryable ClickHouse exception, triggering retry loop.
type retryableErrConn struct {
	driver.Conn
}

func (retryableErrConn) Exec(context.Context, string, ...any) error {
	return &clickhouse.Exception{Code: int32(chproto.ErrTooManyParts)}
}

func (retryableErrConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, &clickhouse.Exception{Code: int32(chproto.ErrTooManyParts)}
}

func TestExecRetryBackoffPreemptedByContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := Exec(ctx, nopLogger{}, retryableErrConn{}, "INSERT INTO t VALUES (1)")
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, time.Second, "cancellation should preempt the retry backoff")
}

func TestQueryRetryBackoffPreemptedByContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	rows, err := Query(ctx, nopLogger{}, retryableErrConn{}, "SELECT 1")
	elapsed := time.Since(start)

	require.Nil(t, rows)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, time.Second, "cancellation should preempt the retry backoff")
}

// incorrectDataConn always fails Exec with an INCORRECT_DATA exception carrying message.
type incorrectDataConn struct {
	driver.Conn
	message string
}

func (c incorrectDataConn) Exec(context.Context, string, ...any) error {
	return &clickhouse.Exception{Code: int32(chproto.ErrIncorrectData), Message: c.message}
}

func TestExecErrorTraits(t *testing.T) {
	const parseError = "Cannot parse JSON object here: bad data"
	tests := []struct {
		name    string
		message string
		query   string
		want    ExecTraits
	}{
		{
			name: "cast to JSON", message: parseError,
			query: "SELECT CAST(`js`, 'JSON') FROM s3('k', 'Avro')",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "cast to Nullable(JSON)", message: parseError,
			query: "SELECT CAST(`js`, 'Nullable(JSON)') FROM s3('k', 'Avro')",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "extract Array(JSON)", message: parseError,
			query: "SELECT JSONExtract(ifNull(`js`, ''), 'Array(JSON)') FROM s3('k', 'Avro')",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "::JSON", message: parseError,
			query: "SELECT JSONExtractString(_peerdb_data, 'js')::JSON AS `js` FROM `raw`",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "::Nullable(JSON)", message: parseError,
			query: "SELECT JSONExtractString(_peerdb_data, 'js')::Nullable(JSON) AS `js` FROM `raw`",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "::Array(Nullable(JSON))", message: parseError,
			query: "SELECT `js`::Array(Nullable(JSON)) AS `js` FROM `raw`",
			want:  ExecTraits{JSONParse: true, JSONCast: true},
		},
		{
			name: "String column", message: parseError,
			query: "SELECT JSONExtractString(_peerdb_data, 'js') AS `js` FROM `raw`",
			want:  ExecTraits{JSONParse: true},
		},
		{
			name: "no conversion", message: parseError,
			query: "SELECT `js` FROM s3('k', 'Avro')",
			want:  ExecTraits{JSONParse: true},
		},
		{
			name: "view", message: parseError + ": while pushing to view db.mv",
			query: "SELECT 1",
			want:  ExecTraits{View: true, JSONParse: true},
		},
		{
			name: "other INCORRECT_DATA message", message: "Unexpected value in column",
			query: "SELECT 1",
			want:  ExecTraits{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Exec(t.Context(), nopLogger{}, incorrectDataConn{message: tt.message}, tt.query)
			execErr, ok := errors.AsType[*ExecError](err)
			require.True(t, ok)
			require.Equal(t, tt.want, execErr.traits)
			require.Equal(t, "ClickHouse exec error: code: 117, message: REDACTED", err.Error())
		})
	}
}

// scriptedConn fails each call with errs in order, then succeeds.
type scriptedConn struct {
	driver.Conn
	errs  []error
	calls int
}

func (c *scriptedConn) next() error {
	c.calls++
	if c.calls <= len(c.errs) {
		return c.errs[c.calls-1]
	}
	return nil
}

func (c *scriptedConn) Ping(context.Context) error {
	return c.next()
}

func (c *scriptedConn) Exec(context.Context, string, ...any) error {
	return c.next()
}

func (c *scriptedConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, c.next()
}

func (c *scriptedConn) QueryRow(context.Context, string, ...any) driver.Row {
	return errRow{err: c.next()}
}

type errRow struct {
	driver.Row
	err error
}

func (r errRow) Err() error {
	return r.err
}

var (
	tooManyPartsErr = &clickhouse.Exception{Code: int32(chproto.ErrTooManyParts)}
	authErr         = &clickhouse.Exception{Code: int32(chproto.ErrAuthenticationFailed)}
)

func TestExecRetriesRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{tooManyPartsErr}}

	require.NoError(t, Exec(t.Context(), nopLogger{}, conn, "INSERT INTO t VALUES (1)"))
	require.Equal(t, 2, conn.calls)
}

func TestExecDoesNotRetryNonRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{authErr}}

	require.ErrorIs(t, Exec(t.Context(), nopLogger{}, conn, "INSERT INTO t VALUES (1)"), authErr)
	require.Equal(t, 1, conn.calls)
}

func TestQueryRetriesRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{tooManyPartsErr}}

	_, err := Query(t.Context(), nopLogger{}, conn, "SELECT 1")
	require.NoError(t, err)
	require.Equal(t, 2, conn.calls)
}

func TestQueryDoesNotRetryNonRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{authErr}}

	_, err := Query(t.Context(), nopLogger{}, conn, "SELECT 1")
	require.ErrorIs(t, err, authErr)
	require.Equal(t, 1, conn.calls)
}

func TestQueryRowRetriesRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{tooManyPartsErr}}

	require.NoError(t, QueryRow(t.Context(), nopLogger{}, conn, "SELECT 1").Err())
	require.Equal(t, 2, conn.calls)
}

func TestQueryRowDoesNotRetryNonRetryableException(t *testing.T) {
	conn := &scriptedConn{errs: []error{authErr}}

	require.ErrorIs(t, QueryRow(t.Context(), nopLogger{}, conn, "SELECT 1").Err(), authErr)
	require.Equal(t, 1, conn.calls)
}

func TestPingRetriesConnectionReset(t *testing.T) {
	connReset := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	conn := &scriptedConn{errs: []error{connReset}}

	require.NoError(t, Ping(t.Context(), nopLogger{}, conn))
	require.Equal(t, 2, conn.calls)
}

func TestPingDoesNotRetryAuthFailure(t *testing.T) {
	conn := &scriptedConn{errs: []error{authErr}}

	require.ErrorIs(t, Ping(t.Context(), nopLogger{}, conn), authErr)
	require.Equal(t, 1, conn.calls)
}
