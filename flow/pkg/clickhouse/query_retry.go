package clickhouse

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
}

var retryableExceptionSubstrings = map[chproto.Error][]string{
	chproto.ErrStdException: {"unspecified iostream_category error"},
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

func Exec(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) error {
	var err error
	for i := range 5 {
		err = conn.Exec(ctx, query, args...)
		if !isRetryableException(err) {
			break
		}
		logger.Info("[exec] retryable error", slog.Any("error", err), slog.Int64("retry", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	if ex, ok := errors.AsType[*clickhouse.Exception](err); ok {
		isMV := strings.Contains(ex.Error(), "while pushing to view")
		if chproto.Error(ex.Code) == chproto.ErrIncorrectData {
			ex.Message = "REDACTED"
		}
		if isMV {
			return NewViewError(ex)
		}
	}
	return err
}

func Query(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) (driver.Rows, error) {
	var rows driver.Rows
	var err error
	for i := range 5 {
		rows, err = conn.Query(ctx, query, args...)
		if !isRetryableException(err) {
			break
		}
		logger.Info("[query] retryable error", slog.Any("error", err), slog.Int64("retry", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return rows, err
}

func QueryRow(ctx context.Context, logger log.Logger,
	conn clickhouse.Conn, query string, args ...any,
) driver.Row {
	var row driver.Row
	for i := range 5 {
		row = conn.QueryRow(ctx, query, args...)
		err := row.Err()
		if !isRetryableException(err) {
			break
		}
		logger.Info("[queryRow] retryable error", slog.Any("error", err), slog.Int64("retry", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return row
}
