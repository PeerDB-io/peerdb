package alerting

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgerrcode"
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
		Class: "NOTIFY_OOM", action: NotifyUser,
	}
	ErrorNotifyMVOrView = ErrorClass{
		Class: "NOTIFY_MV_OR_VIEW", action: NotifyUser,
	}
	ErrorNotifyConnectivity = ErrorClass{
		Class: "NOTIFY_CONNECTIVITY", action: NotifyUser,
	}
	ErrorNotifySlotInvalid = ErrorClass{
		Class: "NOTIFY_SLOT_INVALID", action: NotifyUser,
	}
	ErrorNotifyTerminate = ErrorClass{
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
		Class: "IGNORE_EOF", action: Ignore,
	}
	ErrorIgnoreConnReset = ErrorClass{
		Class: "IGNORE_CONN_RESET", action: Ignore,
	}
	ErrorIgnoreContextCancelled = ErrorClass{
		Class: "IGNORE_CONTEXT_CANCELLED", action: Ignore,
	}
	ErrorInternalClickHouse = ErrorClass{
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
		switch chproto.Error(exception.Code) {
		case chproto.ErrMemoryLimitExceeded:
			return ErrorNotifyOOM
		case chproto.ErrCannotInsertNullInOrdinaryColumn,
			chproto.ErrNotImplemented,
			chproto.ErrTooManyParts:
			if isClickHouseMvError(exception) {
				return ErrorNotifyMVOrView
			}
		case chproto.ErrUnknownDatabase:
			return ErrorNotifyConnectivity
		case chproto.ErrKeeperException,
			chproto.ErrUnfinished,
			chproto.ErrAborted:
			return ErrorInternalClickHouse
		default:
			if isClickHouseMvError(exception) {
				return ErrorNotifyMVOrView
			}
		}
	}
	// Postgres specific errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.InvalidAuthorizationSpecification,
			pgerrcode.InvalidPassword,
			pgerrcode.InsufficientPrivilege,
			pgerrcode.UndefinedTable,
			pgerrcode.CannotConnectNow:
			return ErrorNotifyConnectivity
		case pgerrcode.AdminShutdown:
			return ErrorNotifyTerminate
		case pgerrcode.ObjectNotInPrerequisiteState:
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") {
				return ErrorNotifySlotInvalid
			}
		case pgerrcode.TooManyConnections:
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
	var ssOpenChanErr *ssh.OpenChannelError
	if errors.As(err, &ssOpenChanErr) {
		return ErrorNotifyConnectivity
	}

	// Other SSH Initial Connection related errors
	var sshTunnelSetupErr *exceptions.SSHTunnelSetupError
	if errors.As(err, &sshTunnelSetupErr) {
		return ErrorNotifyConnectivity
	}

	return ErrorOther
}

func isClickHouseMvError(exception *clickhouse.Exception) bool {
	return strings.Contains(exception.Message, "while pushing to view")
}
