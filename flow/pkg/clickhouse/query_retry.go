package clickhouse

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"syscall"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/log"
)

// https://github.com/ClickHouse/clickhouse-kafka-connect/blob/2e0c17e2f900d29c00482b9d0a1f55cb678244e5/src/main/java/com/clickhouse/kafka/connect/util/Utils.java#L78-L93
//
//nolint:lll
var retryableExceptions = map[chproto.Error]struct{}{
	chproto.ErrUnexpectedEndOfFile:        {},
	chproto.ErrFileDoesntExist:            {},
	chproto.ErrTimeoutExceeded:            {},
	chproto.ErrReadonly:                   {},
	chproto.ErrTooManySimultaneousQueries: {},
	chproto.ErrNoFreeConnection:           {},
	chproto.ErrSocketTimeout:              {},
	chproto.ErrNetworkError:               {},
	chproto.ErrMemoryLimitExceeded:        {},
	chproto.ErrTableIsReadOnly:            {},
	chproto.ErrTooManyParts:               {},
	chproto.ErrTooLessLiveReplicas:        {},
	chproto.ErrUnknownStatusOfInsert:      {},
	425:                                   {}, // SYSTEM_ERROR
	chproto.ErrKeeperException:            {},
	chproto.ErrCannotAssignAlter:          {}, // "Metadata on replica is not up to date ... You can retry this error"
}

var retryableExceptionSubstrings = map[chproto.Error][]string{
	chproto.ErrStdException: {"unspecified iostream_category error"},
}

// jsonCastRe matches the JSON type conversions in queries, looking for:
// CAST(x, 'JSON'), CAST(x, 'Nullable(JSON)'), JSONExtract(x, 'Array(JSON)'), x::JSON and x::Nullable(JSON).
var jsonCastRe = regexp.MustCompile(`'(?:Array\()?(?:Nullable\()?JSON\)*'|::(?:Array\()?(?:Nullable\()?JSON\b`)

// QueryCastsToJSON is used to best-effort identify whether the query casts one or more values to the JSON type.
func QueryCastsToJSON(query string) bool {
	return jsonCastRe.MatchString(query)
}

func isRetryableException(err error) bool {
	if ex, ok := errors.AsType[*clickhouse.Exception](err); ok {
		if ex == nil {
			return false
		}
		code := chproto.Error(ex.Code)
		if _, ok := retryableExceptions[code]; ok {
			return true
		}
		if substr, ok := retryableExceptionSubstrings[code]; ok {
			msg := ex.Error()
			for _, s := range substr {
				if strings.Contains(msg, s) {
					return true
				}
			}
		}
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

// withRetry runs fn up to 5 times with backoff while shouldRetry returns true for its error.
// On context cancellation during backoff it returns the last result along with ctx.Err().
func withRetry[T any](ctx context.Context, logger log.Logger, op string,
	shouldRetry func(error) bool, fn func() (T, error),
) (T, error) {
	const attempts = 5
	var res T
	var err error
	for i := range attempts {
		res, err = fn()
		if err == nil || !shouldRetry(err) {
			return res, err
		}
		logger.Warn("["+op+"] retryable error", slog.Any("error", err), slog.Int64("retry", int64(i)))
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return res, ctx.Err()
			case <-time.After(time.Second * time.Duration(i*5+1)):
			}
		}
	}
	return res, err
}

func Exec(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) error {
	_, err := withRetry(ctx, logger, "exec", isRetryableException, func() (struct{}, error) {
		return struct{}{}, conn.Exec(ctx, query, args...)
	})
	if ex, ok := errors.AsType[*clickhouse.Exception](err); ok {
		traits := ExecTraits{
			View:      strings.Contains(ex.Message, "while pushing to view"),
			JSONParse: strings.Contains(ex.Message, "Cannot parse JSON object here"),
			JSONCast:  QueryCastsToJSON(query),
		}
		if chproto.Error(ex.Code) == chproto.ErrIncorrectData {
			ex.Message = "REDACTED"
		}
		return NewExecError(err, traits)
	}
	return err
}

func Query(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) (driver.Rows, error) {
	return withRetry(ctx, logger, "query", isRetryableException, func() (driver.Rows, error) {
		return conn.Query(ctx, query, args...)
	})
}

func QueryRow(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) driver.Row {
	row, _ := withRetry(ctx, logger, "queryRow", isRetryableException, func() (driver.Row, error) {
		row := conn.QueryRow(ctx, query, args...)
		return row, row.Err()
	})
	return row
}

// Ping retries transient failures (network errors, retryable exceptions) with backoff.
// Non-retryable server exceptions such as auth failures are returned immediately.
func Ping(ctx context.Context, logger log.Logger, conn clickhouse.Conn) error {
	_, err := withRetry(ctx, logger, "ping", func(err error) bool {
		if ctx.Err() != nil {
			return false
		}
		ex, ok := errors.AsType[*clickhouse.Exception](err)
		return !ok || isRetryableException(ex)
	}, func() (struct{}, error) {
		return struct{}{}, conn.Ping(ctx)
	})
	return err
}
