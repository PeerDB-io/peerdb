package alerting

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

type ErrorAction string

const (
	NotifyUser      ErrorAction = "notify_user"
	Ignore          ErrorAction = "ignore"
	NotifyTelemetry ErrorAction = "notify_telemetry"
)

func (e ErrorAction) String() string {
	return string(e)
}

type ErrorClass struct {
	Class  string
	action ErrorAction
}

var (
	ErrorNotifyOOM = ErrorClass{
		// ClickHouse Code 241
		Class: "NOTIFY_OOM", action: NotifyUser,
	}
	ErrorNotifyMVOrView = ErrorClass{
		// ClickHouse Code 349 / Code 48 with "while pushing to view", TODO(242,236)
		Class: "NOTIFY_MV_OR_VIEW", action: NotifyUser,
	}
	ErrorNotifyConnectivity = ErrorClass{
		// ClickHouse Code 81 or Postgres Code 28P01
		Class: "NOTIFY_CONNECTIVITY", action: NotifyUser,
	}
	ErrorNotifySlotInvalid = ErrorClass{
		// Postgres Code 55000 with "cannot read from logical replication slot"
		Class: "NOTIFY_SLOT_INVALID", action: NotifyUser,
	}
	ErrorNotifyTerminate = ErrorClass{
		// Postgres Code 57P01
		Class: "NOTIFY_TERMINATE", action: NotifyUser,
	}
	ErrorNotifyConnectTimeout = ErrorClass{
		// TODO
		Class: "NOTIFY_CONNECT_TIMEOUT", action: NotifyUser,
	}
	ErrorEventInternal = ErrorClass{
		// Level <= Info
		Class: "EVENT_INTERNAL", action: NotifyTelemetry,
	}
	ErrorIgnoreEOF = ErrorClass{
		// io.EOF || io.ErrUnexpectedEOF
		Class: "IGNORE_EOF", action: Ignore,
	}
	ErrorIgnoreContextCancelled = ErrorClass{
		// context.Canceled
		Class: "IGNORE_CONTEXT_CANCELLED", action: Ignore,
	}
	ErrorInternalClickHouse = ErrorClass{
		// Code 999 or 341
		Class: "INTERNAL_CLICKHOUSE", action: NotifyTelemetry,
	}
	ErrorOther = ErrorClass{
		// These are internal and should not be exposed
		Class: "OTHER", action: NotifyTelemetry,
	}
)

func (e ErrorClass) String() string {
	return e.Class
}
func (e ErrorClass) ErrorAction() ErrorAction {
	if e.action != "" {
		return e.action
	}
	return NotifyTelemetry
}

func GetErrorClass(ctx context.Context, err error) ErrorClass {
	if errors.Is(err, context.Canceled) {
		return ErrorIgnoreContextCancelled
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrorIgnoreEOF
	}
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		switch exception.Code {
		case 241: // MEMORY_LIMIT_EXCEEDED
			return ErrorNotifyOOM
		case 349: // CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
			if isClickHouseMvError(exception) {
				return ErrorNotifyMVOrView
			}
		case 48: // NOT_IMPLEMENTED
			if isClickHouseMvError(exception) {
				return ErrorNotifyMVOrView
			}
		case 81: // UNKNOWN_DATABASE
			return ErrorNotifyConnectivity
		case 999: // KEEPER_EXCEPTION
			return ErrorInternalClickHouse
		case 341: // UNFINISHED
			return ErrorInternalClickHouse
		}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01": // invalid_password
			return ErrorNotifyConnectivity
		case "57P01": // admin_shutdown
			return ErrorNotifyTerminate
		case "55000": // object_not_in_prerequisite_state
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") {
				return ErrorNotifySlotInvalid
			}
		}
	}
	return ErrorOther
}

func isClickHouseMvError(exception *clickhouse.Exception) bool {
	return strings.Contains(exception.Message, "while pushing to view")
}
