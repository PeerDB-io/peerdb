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
	ErrorNotifyInvalidSnapshotIdentifier = ErrorClass{
		Class: "NOTIFY_INVALID_SNAPSHOT_IDENTIFIER", action: NotifyUser,
	}
	ErrorNotifyTerminate = ErrorClass{
		Class: "NOTIFY_TERMINATE", action: NotifyUser,
	}
	ErrorNotifyConnectTimeout = ErrorClass{
		// TODO(this is mostly done via NOTIFY_CONNECTIVITY, will remove later if not needed)
		Class: "NOTIFY_CONNECT_TIMEOUT", action: NotifyUser,
	}
	ErrorNormalize = ErrorClass{
		Class: "NORMALIZE", action: NotifyTelemetry,
	}
	ErrorInternal = ErrorClass{
		Class: "INTERNAL", action: NotifyTelemetry,
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
		// These are unclassified and should not be exposed
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
	var catalogErr *exceptions.CatalogError
	if errors.As(err, &catalogErr) {
		return ErrorInternal
	}

	var peerDBErr *exceptions.PostgresSetupError
	if errors.As(err, &peerDBErr) {
		return ErrorNotifyConnectivity
	}

	if errors.Is(err, context.Canceled) {
		// Generally happens during workflow cancellation
		return ErrorIgnoreContextCancelled
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		// Usually seen in ClickHouse cloud during instance scale-up
		return ErrorIgnoreEOF
	}

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
		case pgerrcode.InvalidParameterValue:
			if strings.Contains(pgErr.Message, "invalid snapshot identifier") {
				return ErrorNotifyInvalidSnapshotIdentifier
			}
		case pgerrcode.TooManyConnections:
			return ErrorNotifyConnectivity // Maybe we can return something else?
		}
	}

	// Connection reset errors can mostly be ignored
	if errors.Is(err, syscall.ECONNRESET) {
		return ErrorIgnoreConnReset
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return ErrorNotifyConnectivity
	}

	var ssOpenChanErr *ssh.OpenChannelError
	if errors.As(err, &ssOpenChanErr) {
		return ErrorNotifyConnectivity
	}

	var sshTunnelSetupErr *exceptions.SSHTunnelSetupError
	if errors.As(err, &sshTunnelSetupErr) {
		return ErrorNotifyConnectivity
	}

	var normalizationErr *exceptions.NormalizationError
	if exception != nil && errors.As(err, &normalizationErr) {
		// notify if normalization hits error on destination
		return ErrorNotifyGeneral
	}

	return ErrorOther
}

func isClickHouseMvError(exception *clickhouse.Exception) bool {
	return strings.Contains(exception.Message, "while pushing to view")
}
