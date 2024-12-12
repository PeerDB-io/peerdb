package clickhouse

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/log"
)

// https://github.com/ClickHouse/clickhouse-kafka-connect/blob/2e0c17e2f900d29c00482b9d0a1f55cb678244e5/src/main/java/com/clickhouse/kafka/connect/util/Utils.java#L78-L93
//
//nolint:lll
var retryableExceptions = map[int32]struct{}{
	3:   {}, // UNEXPECTED_END_OF_FILE
	107: {}, // FILE_DOESNT_EXIST
	159: {}, // TIMEOUT_EXCEEDED
	164: {}, // READONLY
	202: {}, // TOO_MANY_SIMULTANEOUS_QUERIES
	203: {}, // NO_FREE_CONNECTION
	209: {}, // SOCKET_TIMEOUT
	210: {}, // NETWORK_ERROR
	241: {}, // MEMORY_LIMIT_EXCEEDED
	242: {}, // TABLE_IS_READ_ONLY
	252: {}, // TOO_MANY_PARTS
	285: {}, // TOO_FEW_LIVE_REPLICAS
	319: {}, // UNKNOWN_STATUS_OF_INSERT
	425: {}, // SYSTEM_ERROR
	999: {}, // KEEPER_EXCEPTION
}

func isRetryableException(err error) bool {
	if ex, ok := err.(*clickhouse.Exception); ok {
		if ex == nil {
			return false
		}
		_, yes := retryableExceptions[ex.Code]
		return yes
	}
	return errors.Is(err, io.EOF)
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
		logger.Info("[exec] retryable error", slog.Any("error", err), slog.String("query", query), slog.Int64("retry", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
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
		logger.Info("[query] retryable error", slog.Any("error", err), slog.String("query", query), slog.Int64("retry", int64(i)))
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
		logger.Info("[queryRow] retryable error", slog.Any("error", err), slog.String("query", query), slog.Int64("retry", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return row
}
