package clickhouse

import (
	"context"
	"errors"
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
