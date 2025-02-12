package alerting

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
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
		// ClickHouse Code 349 / Code 48 with "while pushing to view"
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
		// TODO(this is mostly done via NOTIFY_CONNECTIVITY, will remove later if not needed)
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
	ErrorIgnoreConnReset = ErrorClass{
		// net.OpError with "connection reset by peer"
		Class: "IGNORE_CONN_RESET", action: Ignore,
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
	// PeerDB error types
	var peerDBErr *exceptions.PostgresSetupError
	if errors.As(err, &peerDBErr) {
		return ErrorNotifyConnectivity
	}
	// Generally happens during workflow cancellation
	if errors.Is(err, context.Canceled) {
		return ErrorIgnoreContextCancelled
	}
	// Usually seen in ClickHouse cloud during instance scale-up
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrorIgnoreEOF
	}
	// ClickHouse specific errors
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
		case 236: // ABORTED
			return ErrorInternalClickHouse
		}
	}
	// Postgres specific errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28000": // invalid_authorization_specification
			return ErrorNotifyConnectivity
		case "28P01": // invalid_password
			return ErrorNotifyConnectivity
		case "42P01": // undefined_table
			return ErrorNotifyConnectivity
		case "42501": // insufficient_privilege
			return ErrorNotifyConnectivity
		case "57P01": // admin_shutdown
			return ErrorNotifyTerminate
		case "57P03": // cannot_connect_now
			return ErrorNotifyConnectivity
		case "55000": // object_not_in_prerequisite_state
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") {
				return ErrorNotifySlotInvalid
			}
		case "53300": // too_many_connections
			return ErrorNotifyConnectivity // Maybe we can return something else?
		}
	}

	// Network related errors
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		// Connection reset errors can mostly be ignored
		if netErr.Err.Error() == syscall.ECONNRESET.Error() {
			return ErrorIgnoreConnReset
		}
		return ErrorNotifyConnectivity
	}

	// SSH related errors
	var sshErr *ssh.OpenChannelError
	if errors.As(err, &sshErr) {
		return ErrorNotifyConnectivity
	}
	return ErrorOther
}

func isClickHouseMvError(exception *clickhouse.Exception) bool {
	return strings.Contains(exception.Message, "while pushing to view")
}
