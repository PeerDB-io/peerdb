package alerting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-mysql-org/go-mysql/mysql"
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

var (
	ClickHouseDecimalParsingRe = regexp.MustCompile(
		`Cannot parse type Decimal\(\d+, \d+\), expected non-empty binary data with size equal to or less than \d+, got \d+`,
	)
	// ID(a14c2a1c-edcd-5fcb-73be-bd04e09fccb7) not found in user directories
	ClickHouseNotFoundInUserDirsRe    = regexp.MustCompile(`ID\([a-z0-9-]+\) not found in user directories`)
	PostgresPublicationDoesNotExistRe = regexp.MustCompile(`publication ".*?" does not exist`)
	PostgresWalSegmentRemovedRe       = regexp.MustCompile(`requested WAL segment \w+ has already been removed`)
)

func (e ErrorAction) String() string {
	return string(e)
}

type ErrorSource string

const (
	ErrorSourceClickHouse      ErrorSource = "clickhouse"
	ErrorSourcePostgres        ErrorSource = "postgres"
	ErrorSourceMySQL           ErrorSource = "mysql"
	ErrorSourcePostgresCatalog ErrorSource = "postgres_catalog"
	ErrorSourceSSH             ErrorSource = "ssh_tunnel"
	ErrorSourceNet             ErrorSource = "net"
	ErrorSourceOther           ErrorSource = "other"
)

func (e ErrorSource) String() string {
	return string(e)
}

type ErrorInfo struct {
	Source ErrorSource
	Code   string
}

type ErrorClass struct {
	Class  string
	action ErrorAction
}

var (
	ErrorNotifyDestinationModified = ErrorClass{
		Class: "NOTIFY_DESTINATION_MODIFIED", action: NotifyUser,
	}
	ErrorNotifyOOM = ErrorClass{
		Class: "NOTIFY_OOM", action: NotifyUser,
	}
	ErrorNotifyMVOrView = ErrorClass{
		Class: "NOTIFY_MV_OR_VIEW", action: NotifyUser,
	}
	ErrorNotifyConnectivity = ErrorClass{
		Class: "NOTIFY_CONNECTIVITY", action: NotifyUser,
	}
	ErrorNotifyOOMSource = ErrorClass{
		Class: "NOTIFY_OOM_SOURCE", action: NotifyUser,
	}
	ErrorNotifySlotInvalid = ErrorClass{
		Class: "NOTIFY_SLOT_INVALID", action: NotifyUser,
	}
	ErrorNotifyPublicationMissing = ErrorClass{
		Class: "NOTIFY_PUBLICATION_MISSING", action: NotifyUser,
	}
	ErrorUnsupportedDatatype = ErrorClass{
		Class: "NOTIFY_UNSUPPORTED_DATATYPE", action: NotifyUser,
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
	ErrorDropFlow = ErrorClass{
		Class: "DROP_FLOW", action: NotifyTelemetry,
	}
	ErrorIgnoreEOF = ErrorClass{
		Class: "IGNORE_EOF", action: Ignore,
	}
	ErrorIgnoreConnTemporary = ErrorClass{
		Class: "IGNORE_CONN_TEMPORARY", action: Ignore,
	}
	ErrorIgnoreContextCancelled = ErrorClass{
		Class: "IGNORE_CONTEXT_CANCELLED", action: Ignore,
	}
	ErrorRetryRecoverable = ErrorClass{
		// These errors are generally recoverable, but need to be escalated if they persist
		Class: "ERROR_RETRY_RECOVERABLE", action: NotifyTelemetry,
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

func GetErrorClass(ctx context.Context, err error) (ErrorClass, ErrorInfo) {
	var pgErrorInfo ErrorInfo
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		pgErrorInfo = ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   pgErr.Code,
		}
	}

	var catalogErr *exceptions.CatalogError
	if errors.As(err, &catalogErr) {
		errorClass := ErrorInternal
		if pgErr != nil {
			return errorClass, pgErrorInfo
		}
		return errorClass, ErrorInfo{
			Source: ErrorSourcePostgresCatalog,
			Code:   "UNKNOWN",
		}
	}

	var dropFlowErr *exceptions.DropFlowError
	if errors.As(err, &dropFlowErr) {
		errorClass := ErrorDropFlow
		if pgErr != nil {
			return errorClass, pgErrorInfo
		}
		// For now we are not making it as verbose, will take this up later
		return errorClass, ErrorInfo{
			Source: ErrorSourceOther,
			Code:   "UNKNOWN",
		}
	}

	var peerDBErr *exceptions.PostgresSetupError
	if errors.As(err, &peerDBErr) {
		errorClass := ErrorNotifyConnectivity
		if pgErr != nil {
			return errorClass, pgErrorInfo
		}
		return errorClass, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNKNOWN",
		}
	}

	if errors.Is(err, context.Canceled) {
		// Generally happens during workflow cancellation
		return ErrorIgnoreContextCancelled, ErrorInfo{
			Source: ErrorSourceOther,
			Code:   "CONTEXT_CANCELLED",
		}
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		// Usually seen in ClickHouse cloud during instance scale-up
		return ErrorIgnoreEOF, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "EOF",
		}
	}

	if pgErr != nil {
		switch pgErr.Code {
		case pgerrcode.InvalidAuthorizationSpecification,
			pgerrcode.InvalidPassword,
			pgerrcode.InsufficientPrivilege,
			pgerrcode.UndefinedTable,
			pgerrcode.UndefinedObject,
			pgerrcode.CannotConnectNow:
			return ErrorNotifyConnectivity, pgErrorInfo
		case pgerrcode.AdminShutdown, pgerrcode.IdleSessionTimeout:
			return ErrorNotifyTerminate, pgErrorInfo
		case pgerrcode.ObjectNotInPrerequisiteState:
			// same underlying error but 2 different messages
			// based on PG version, newer ones have second error
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") ||
				strings.Contains(pgErr.Message, "can no longer get changes from replication slot") {
				return ErrorNotifySlotInvalid, pgErrorInfo
			}
		case pgerrcode.InvalidParameterValue:
			if strings.Contains(pgErr.Message, "invalid snapshot identifier") {
				return ErrorNotifyInvalidSnapshotIdentifier, pgErrorInfo
			}
		case pgerrcode.TooManyConnections, // Maybe we can return something else?
			pgerrcode.ConnectionException,
			pgerrcode.ConnectionDoesNotExist,
			pgerrcode.ConnectionFailure,
			pgerrcode.SQLClientUnableToEstablishSQLConnection,
			pgerrcode.SQLServerRejectedEstablishmentOfSQLConnection,
			pgerrcode.ProtocolViolation:
			return ErrorNotifyConnectivity, pgErrorInfo
		case pgerrcode.OutOfMemory:
			return ErrorNotifyOOMSource, pgErrorInfo
		}
	}

	var pgConnErr *pgconn.ConnectError
	if errors.As(err, &pgConnErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNKNOWN",
		}
	}

	var myErr *mysql.MyError
	if errors.As(err, &myErr) {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   strconv.Itoa(int(myErr.Code)),
		}
	}

	var chException *clickhouse.Exception
	if errors.As(err, &chException) {
		chErrorInfo := ErrorInfo{
			Source: ErrorSourceClickHouse,
			Code:   strconv.Itoa(int(chException.Code)),
		}
		switch chproto.Error(chException.Code) {
		case chproto.ErrUnknownTable, chproto.ErrNoSuchColumnInTable:
			return ErrorNotifyDestinationModified, chErrorInfo
		case chproto.ErrMemoryLimitExceeded:
			return ErrorNotifyOOM, chErrorInfo
		case chproto.ErrCannotInsertNullInOrdinaryColumn,
			chproto.ErrNotImplemented,
			chproto.ErrTooManyParts:
			if isClickHouseMvError(chException) {
				return ErrorNotifyMVOrView, chErrorInfo
			}
		case chproto.ErrUnknownDatabase:
			return ErrorNotifyConnectivity, chErrorInfo
		case chproto.ErrKeeperException,
			chproto.ErrUnfinished,
			chproto.ErrAborted:
			return ErrorInternalClickHouse, chErrorInfo
		case chproto.ErrAuthenticationFailed:
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrTooManySimultaneousQueries:
			return ErrorIgnoreConnTemporary, chErrorInfo
		case chproto.ErrCannotParseUUID, chproto.ErrValueIsOutOfRangeOfDataType: // https://github.com/ClickHouse/ClickHouse/pull/78540
			if ClickHouseDecimalParsingRe.MatchString(chException.Message) {
				return ErrorUnsupportedDatatype, chErrorInfo
			}
		case 492: // `ACCESS_ENTITY_NOT_FOUND` TBD via https://github.com/ClickHouse/ch-go/pull/1058
			if ClickHouseNotFoundInUserDirsRe.MatchString(chException.Message) {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case 439: // CANNOT_SCHEDULE_TASK
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrIllegalTypeOfArgument:
			var qrepSyncError *exceptions.QRepSyncError
			if errors.As(err, &qrepSyncError) {
				unexpectedSelectRe, reErr := regexp.Compile(
					fmt.Sprintf(`FROM\s+(%s\.)?%s`,
						regexp.QuoteMeta(qrepSyncError.DestinationDatabase), regexp.QuoteMeta(qrepSyncError.DestinationTable)))
				if reErr != nil {
					slog.Error("regexp compilation error while checking for err", "err", reErr, "original_err", err)
					return ErrorOther, chErrorInfo
				}
				if unexpectedSelectRe.MatchString(chException.Message) {
					// Select query from destination table in QRepSync = MV error
					return ErrorNotifyMVOrView, chErrorInfo
				}
				return ErrorOther, chErrorInfo
			}
		case chproto.ErrQueryWasCancelled:
			return ErrorRetryRecoverable, chErrorInfo
		default:
			if isClickHouseMvError(chException) {
				return ErrorNotifyMVOrView, chErrorInfo
			}
			var normalizationErr *exceptions.NormalizationError
			if errors.As(err, &normalizationErr) {
				// notify if normalization hits error on destination
				return ErrorNormalize, chErrorInfo
			}
			return ErrorOther, chErrorInfo
		}
	}

	// Connection reset errors can mostly be ignored
	if errors.Is(err, syscall.ECONNRESET) {
		return ErrorIgnoreConnTemporary, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   syscall.ECONNRESET.Error(),
		}
	}

	if errors.Is(err, net.ErrClosed) {
		return ErrorIgnoreConnTemporary, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "net.ErrClosed",
		}
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   netErr.Err.Error(),
		}
	}

	var ssOpenChanErr *ssh.OpenChannelError
	if errors.As(err, &ssOpenChanErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceSSH,
			Code:   ssOpenChanErr.Reason.String(),
		}
	}

	var sshTunnelSetupErr *exceptions.SSHTunnelSetupError
	if errors.As(err, &sshTunnelSetupErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceSSH,
			Code:   "UNKNOWN",
		}
	}
	var pgWalErr *exceptions.PostgresWalError
	if errors.As(err, &pgWalErr) {
		errorInfo := ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   pgWalErr.Msg.Code,
		}
		if pgWalErr.Msg.Severity == "ERROR" {
			if pgWalErr.Msg.Code == pgerrcode.InternalError &&
				(strings.HasPrefix(pgWalErr.Msg.Message, "could not read from reorderbuffer spill file") ||
					(strings.HasPrefix(pgWalErr.Msg.Message, "could not stat file ") &&
						strings.HasSuffix(pgWalErr.Msg.Message, "Stale file handle")) ||
					// Below error is transient and Aurora Specific
					(strings.HasPrefix(pgWalErr.Msg.Message, "Internal error encountered during logical decoding"))) {
				// Example errors:
				// could not stat file "pg_logical/snapshots/1B6-2A845058.snap": Stale file handle
				// could not read from reorderbuffer spill file: Bad address
				// could not read from reorderbuffer spill file: Bad file descriptor
				return ErrorRetryRecoverable, errorInfo
			}
			// &{Code:XX000 Message:requested WAL segment 000000010001337F0000002E has already been removed}
			if pgWalErr.Msg.Code == pgerrcode.InternalError && PostgresWalSegmentRemovedRe.MatchString(pgWalErr.Msg.Message) {
				return ErrorRetryRecoverable, errorInfo
			}

			// &{Severity:ERROR Code:42704 Message:publication "custom_pub" does not exist}
			if pgWalErr.Msg.Code == pgerrcode.UndefinedObject && PostgresPublicationDoesNotExistRe.MatchString(pgWalErr.Msg.Message) {
				return ErrorNotifyPublicationMissing, errorInfo
			}
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "net.DNSError",
		}
	}

	return ErrorOther, ErrorInfo{
		Source: ErrorSourceOther,
		Code:   "UNKNOWN",
	}
}

func isClickHouseMvError(exception *clickhouse.Exception) bool {
	return strings.Contains(exception.Message, "while pushing to view")
}
