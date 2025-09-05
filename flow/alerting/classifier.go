package alerting

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
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
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/shared"
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
	ClickHouseNotFoundInUserDirsRe    = regexp.MustCompile("ID\\([a-z0-9-]+\\) not found in `?user directories`?")
	PostgresPublicationDoesNotExistRe = regexp.MustCompile(`publication ".*?" does not exist`)
	PostgresSnapshotDoesNotExistRe    = regexp.MustCompile(`snapshot ".*?" does not exist`)
	PostgresWalSegmentRemovedRe       = regexp.MustCompile(`requested WAL segment \w+ has already been removed`)
	MySqlRdsBinlogFileNotFoundRe      = regexp.MustCompile(`File '/rdsdbdata/log/binlog/mysql-bin-changelog.\d+' not found`)
)

func (e ErrorAction) String() string {
	return string(e)
}

type ErrorSource string

const (
	ErrorSourceClickHouse      ErrorSource = "clickhouse"
	ErrorSourcePostgres        ErrorSource = "postgres"
	ErrorSourceMySQL           ErrorSource = "mysql"
	ErrorSourceMongoDB         ErrorSource = "mongodb"
	ErrorSourcePostgresCatalog ErrorSource = "postgres_catalog"
	ErrorSourceSSH             ErrorSource = "ssh_tunnel"
	ErrorSourceNet             ErrorSource = "net"
	ErrorSourceTemporal        ErrorSource = "temporal"
	ErrorSourceOther           ErrorSource = "other"
)

func (e ErrorSource) String() string {
	return string(e)
}

type AdditionalErrorAttributeKey string

func (e AdditionalErrorAttributeKey) String() string {
	return string(e)
}

const (
	ErrorAttributeKeyTable  AdditionalErrorAttributeKey = "errorAdditionalAttributeTable"
	ErrorAttributeKeyColumn AdditionalErrorAttributeKey = "errorAdditionalAttributeColumn"
)

type ErrorInfo struct {
	AdditionalAttributes map[AdditionalErrorAttributeKey]string
	Source               ErrorSource
	Code                 string
}

type ErrorClass struct {
	Class  string
	action ErrorAction
}

type RetryableError interface {
	error
	Retryable() bool
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
	ErrorNotifyBinlogInvalid = ErrorClass{
		Class: "NOTIFY_BINLOG_INVALID", action: NotifyUser,
	}
	ErrorNotifySourceTableMissing = ErrorClass{
		Class: "NOTIFY_SOURCE_TABLE_MISSING", action: NotifyUser,
	}
	ErrorNotifyPublicationMissing = ErrorClass{
		Class: "NOTIFY_PUBLICATION_MISSING", action: NotifyUser,
	}
	ErrorNotifyReplicationSlotMissing = ErrorClass{
		Class: "NOTIFY_REPLICATION_SLOT_MISSING", action: NotifyUser,
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
	ErrorLossyConversion = ErrorClass{
		Class: "WARNING_LOSSY_CONVERSION", action: NotifyUser,
	}
	ErrorUnsupportedSchemaChange = ErrorClass{
		Class: "NOTIFY_UNSUPPORTED_SCHEMA_CHANGE", action: NotifyUser,
	}
	// Postgres 16.9/17.5 etc. introduced a bug where certain workloads can cause logical replication to
	// request a memory allocation of >1GB, which is not allowed by Postgres. Fixed already, but we need to handle this error
	// https://github.com/postgres/postgres/commit/d87d07b7ad3b782cb74566cd771ecdb2823adf6a
	ErrorNotifyPostgresSlotMemalloc = ErrorClass{
		Class: "NOTIFY_POSTGRES_SLOT_MEMALLOC", action: NotifyUser,
	}
	// Mongo specific, equivalent to slot invalidation in Postgres
	ErrorNotifyChangeStreamHistoryLost = ErrorClass{
		Class: "NOTIFY_CHANGE_STREAM_HISTORY_LOST", action: NotifyUser,
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
	var pgErr *pgconn.PgError
	var pgWalErr *exceptions.PostgresWalError
	if errors.As(err, &pgWalErr) {
		pgErr = pgconn.ErrorResponseToPgError(pgWalErr.UnderlyingError())
	}
	var pgErrorInfo ErrorInfo
	if pgErr != nil || errors.As(err, &pgErr) {
		pgErrorInfo = ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   pgErr.Code,
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
	}

	if errors.Is(err, context.Canceled) {
		// Generally happens during workflow cancellation
		return ErrorIgnoreContextCancelled, ErrorInfo{
			Source: ErrorSourceOther,
			Code:   "CONTEXT_CANCELLED",
		}
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, mysql.ErrBadConn) {
		// Usually seen in ClickHouse cloud during instance scale-up
		return ErrorIgnoreEOF, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "EOF",
		}
	}

	if errors.Is(err, shared.ErrTableDoesNotExist) {
		return ErrorNotifySourceTableMissing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "TABLE_DOES_NOT_EXIST",
		}
	}

	var temporalErr *temporal.ApplicationError
	if errors.As(err, &temporalErr) {
		switch exceptions.ApplicationErrorType(temporalErr.Type()) {
		case exceptions.ApplicationErrorTypeIrrecoverablePublicationMissing:
			return ErrorNotifyPublicationMissing, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   "PUBLICATION_DOES_NOT_EXIST",
			}
		case exceptions.ApplicationErrorTypeIrrecoverableSlotMissing:
			return ErrorNotifyReplicationSlotMissing, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   "REPLICATION_SLOT_DOES_NOT_EXIST",
			}
		}
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceTemporal,
			Code:   temporalErr.Type(),
		}
	}

	var pgConnErr *pgconn.ConnectError
	if errors.As(err, &pgConnErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNKNOWN",
		}
	}

	// Consolidated PostgreSQL error handling
	if pgErr != nil {
		switch pgErr.Code {
		case pgerrcode.InvalidAuthorizationSpecification,
			pgerrcode.InvalidPassword,
			pgerrcode.InsufficientPrivilege,
			pgerrcode.UndefinedTable,
			pgerrcode.CannotConnectNow,
			pgerrcode.ConfigurationLimitExceeded,
			pgerrcode.DiskFull:
			return ErrorNotifyConnectivity, pgErrorInfo

		case pgerrcode.UndefinedObject:
			// Check for publication does not exist error
			if PostgresPublicationDoesNotExistRe.MatchString(pgErr.Message) {
				return ErrorNotifyPublicationMissing, pgErrorInfo
			}
			if PostgresSnapshotDoesNotExistRe.MatchString(pgErr.Message) {
				return ErrorNotifyInvalidSnapshotIdentifier, pgErrorInfo
			}
			return ErrorNotifyConnectivity, pgErrorInfo

		case pgerrcode.AdminShutdown, pgerrcode.IdleSessionTimeout:
			return ErrorNotifyTerminate, pgErrorInfo

		case pgerrcode.UndefinedFile:
			// Handle WAL segment removed errors
			if PostgresWalSegmentRemovedRe.MatchString(pgErr.Message) {
				return ErrorRetryRecoverable, pgErrorInfo
			}

		case pgerrcode.InternalError:
			// Handle reorderbuffer spill file and stale file handle errors
			if strings.HasPrefix(pgErr.Message, "could not read from reorderbuffer spill file") ||
				(strings.HasPrefix(pgErr.Message, "could not stat file ") &&
					strings.HasSuffix(pgErr.Message, "Stale file handle")) ||
				// Below error is transient and Aurora Specific
				(strings.HasPrefix(pgErr.Message, "Internal error encountered during logical decoding")) ||
				//nolint:lll
				// Handle missing record during logical decoding
				// https://github.com/postgres/postgres/blob/a0c7b765372d949cec54960dafcaadbc04b3204e/src/backend/access/transam/xlogreader.c#L921
				strings.HasPrefix(pgErr.Message, "could not find record while sending logically-decoded data") {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// Handle WAL segment removed errors
			if PostgresWalSegmentRemovedRe.MatchString(pgErr.Message) {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// Handle Neon quota exceeded errors
			if strings.Contains(pgErr.Message,
				"Your account or project has exceeded the compute time quota. Upgrade your plan to increase limits.") {
				return ErrorNotifyConnectivity, pgErrorInfo
			}

			// Handle Neon's custom WAL reading error
			if pgErr.Routine == "NeonWALPageRead" && strings.Contains(pgErr.Message, "server closed the connection unexpectedly") {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			if strings.Contains(pgErr.Message, "invalid memory alloc request size") {
				return ErrorNotifyPostgresSlotMemalloc, pgErrorInfo
			}

			// Fall through for other internal errors
			return ErrorOther, pgErrorInfo

		case pgerrcode.ObjectNotInPrerequisiteState:
			// same underlying error but 3 different messages
			// based on PG version, newer ones have second error
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") ||
				strings.Contains(pgErr.Message, "can no longer get changes from replication slot") ||
				strings.Contains(pgErr.Message, "could not import the requested snapshot") {
				return ErrorNotifySlotInvalid, pgErrorInfo
			}

		case pgerrcode.InvalidParameterValue:
			if strings.Contains(pgErr.Message, "invalid snapshot identifier") {
				return ErrorNotifyInvalidSnapshotIdentifier, pgErrorInfo
			}
		case pgerrcode.SerializationFailure, pgerrcode.DeadlockDetected:
			if strings.Contains(pgErr.Message, "canceling statement due to conflict with recovery") {
				return ErrorNotifyConnectivity, pgErrorInfo
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

		case pgerrcode.QueryCanceled, pgerrcode.DuplicateFile:
			return ErrorRetryRecoverable, pgErrorInfo
		}
	}

	var myErr *mysql.MyError
	if errors.As(err, &myErr) {
		// https://mariadb.com/kb/en/mariadb-error-code-reference
		myErrorInfo := ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   strconv.Itoa(int(myErr.Code)),
		}
		switch myErr.Code {
		case 29: // EE_FILENOTFOUND
			if MySqlRdsBinlogFileNotFoundRe.MatchString(myErr.Message) {
				return ErrorNotifyBinlogInvalid, myErrorInfo
			}
			return ErrorNotifyConnectivity, myErrorInfo
		case 1037, 1038, 1041, 3015: // ER_OUTOFMEMORY, ER_OUT_OF_SORTMEMORY, ER_OUT_OF_RESOURCES, ER_ENGINE_OUT_OF_MEMORY
			return ErrorNotifyOOMSource, myErrorInfo
		case 1021, // ER_DISK_FULL
			1040, // ER_CON_COUNT_ERROR
			1044, // ER_DBACCESS_DENIED_ERROR
			1045, // ER_ACCESS_DENIED_ERROR
			1049, // ER_BAD_DB_ERROR
			1051, // ER_BAD_TABLE_ERROR
			1053, // ER_SERVER_SHUTDOWN
			1094, // ER_NO_SUCH_THREAD
			1102, // ER_WRONG_DB_NAME
			1103, // ER_WRONG_TABLE_NAME
			1109, // ER_UNKNOWN_TABLE
			1119, // ER_STACK_OVERRUN
			1129, // ER_HOST_IS_BLOCKED
			1130, // ER_HOST_NOT_PRIVILEGED
			1133, // ER_PASSWORD_NO_MATCH
			1135, // ER_CANT_CREATE_THREAD
			1152, // ER_ABORTING_CONNECTION
			1194, // ER_CRASHED_ON_USAGE
			1195, // ER_CRASHED_ON_REPAIR
			1827: // ER_PASSWORD_FORMAT
			return ErrorNotifyConnectivity, myErrorInfo
		case 1236, // ER_MASTER_FATAL_ERROR_READING_BINLOG
			1373: // ER_UNKNOWN_TARGET_BINLOG
			return ErrorNotifyBinlogInvalid, myErrorInfo
		case 1105: // ER_UNKNOWN_ERROR
			if myErr.State == "HY000" && myErr.Message == "The last transaction was aborted due to Zero Downtime Patch. Please retry." {
				return ErrorRetryRecoverable, myErrorInfo
			}
			return ErrorOther, myErrorInfo
		case 1146: // ER_NO_SUCH_TABLE
			return ErrorNotifySourceTableMissing, myErrorInfo
		default:
			return ErrorOther, myErrorInfo
		}
	}

	var mongoErr driver.Error
	if errors.As(err, &mongoErr) {
		// https://www.mongodb.com/docs/manual/reference/error-codes/
		mongoErrorInfo := ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   strconv.Itoa(int(mongoErr.Code)),
		}

		if mongoErr.RetryableRead() {
			return ErrorRetryRecoverable, mongoErrorInfo
		}

		// some driver errors do not provide error code, such as `poolClearedError`, so we check label instead
		for _, label := range mongoErr.Labels {
			if label == driver.TransientTransactionError {
				return ErrorRetryRecoverable, mongoErrorInfo
			}
		}

		// some error codes are defined to be retryable by the driver, so we retry them
		var retryableError RetryableError
		if errors.As(mongoErr, &retryableError) && retryableError.Retryable() {
			return ErrorRetryRecoverable, mongoErrorInfo
		}

		switch mongoErr.Code {
		case 13: // Unauthorized
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 286: // ChangeStreamHistoryLost
			return ErrorNotifyChangeStreamHistoryLost, mongoErrorInfo
		default:
			return ErrorOther, mongoErrorInfo
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
			if isClickHouseMvError(chException) {
				return ErrorNotifyMVOrView, chErrorInfo
			}
			return ErrorNotifyDestinationModified, chErrorInfo
		case chproto.ErrMemoryLimitExceeded:
			return ErrorNotifyOOM, chErrorInfo
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
		case chproto.ErrAccessEntityNotFound:
			if ClickHouseNotFoundInUserDirsRe.MatchString(chException.Message) {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case 439: // CANNOT_SCHEDULE_TASK
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrUnsupportedMethod,
			chproto.ErrIllegalColumn,
			chproto.ErrDuplicateColumn,
			chproto.ErrNotFoundColumnInBlock,
			chproto.ErrUnknownIdentifier,
			chproto.ErrUnknownFunction,
			chproto.ErrBadTypeOfField,
			chproto.ErrTooDeepRecursion,
			chproto.ErrTypeMismatch,
			chproto.ErrCannotConvertType,
			chproto.ErrIncompatibleColumns,
			chproto.ErrUnexpectedExpression,
			chproto.ErrIllegalAggregation,
			chproto.ErrNotAnAggregate,
			chproto.ErrSizesOfArraysDoesntMatch,
			chproto.ErrAliasRequired,
			691, // UNKNOWN_ELEMENT_OF_ENUM
			chproto.ErrNoCommonType,
			chproto.ErrIllegalTypeOfArgument:
			var qrepSyncError *exceptions.QRepSyncError
			if errors.As(err, &qrepSyncError) {
				// could cause false positives, but should be rare
				return ErrorNotifyMVOrView, chErrorInfo
			}
		case chproto.ErrQueryWasCancelled,
			chproto.ErrPocoException,
			chproto.ErrCannotReadFromSocket,
			chproto.ErrSocketTimeout,
			517: // CANNOT_ASSIGN_ALTER
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrTimeoutExceeded:
			if strings.HasSuffix(chException.Message, "distributed_ddl_task_timeout") {
				return ErrorRetryRecoverable, chErrorInfo
			}
		}
		var normalizationErr *exceptions.NormalizationError
		if isClickHouseMvError(chException) {
			return ErrorNotifyMVOrView, chErrorInfo
		} else if errors.As(err, &normalizationErr) {
			// notify if normalization hits error on destination
			return ErrorNotifyMVOrView, chErrorInfo
		}
		return ErrorOther, chErrorInfo
	}

	// Connection reset errors can mostly be ignored
	if errors.Is(err, syscall.ECONNRESET) {
		return ErrorIgnoreConnTemporary, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   syscall.ECONNRESET.Error(),
		}
	}

	if errors.Is(err, net.ErrClosed) || strings.HasSuffix(err.Error(), "use of closed network connection") {
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

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "net.DNSError",
		}
	}

	var peerCreateError *exceptions.PeerCreateError
	if errors.As(err, &peerCreateError) {
		// Check for context deadline exceeded error
		if errors.Is(peerCreateError, context.DeadlineExceeded) {
			return ErrorNotifyConnectivity, ErrorInfo{
				Source: ErrorSourceOther,
				Code:   "CONTEXT_DEADLINE_EXCEEDED",
			}
		}
	}

	var numericOutOfRangeError *exceptions.NumericOutOfRangeError
	if errors.As(err, &numericOutOfRangeError) {
		return ErrorLossyConversion, ErrorInfo{
			Source: "typeConversion",
			Code:   "NUMERIC_OUT_OF_RANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  numericOutOfRangeError.DestinationTable,
				ErrorAttributeKeyColumn: numericOutOfRangeError.DestinationColumn,
			},
		}
	}

	var tlsCertVerificationError *tls.CertificateVerificationError
	if errors.As(err, &tlsCertVerificationError) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "tls.CertificateVerificationError",
		}
	}

	var numericTruncatedError *exceptions.NumericTruncatedError
	if errors.As(err, &numericTruncatedError) {
		return ErrorLossyConversion, ErrorInfo{
			Source: "typeConversion",
			Code:   "NUMERIC_TRUNCATED",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  numericTruncatedError.DestinationTable,
				ErrorAttributeKeyColumn: numericTruncatedError.DestinationColumn,
			},
		}
	}

	var incompatibleColumnTypeError *exceptions.MySQLIncompatibleColumnTypeError
	if errors.As(err, &incompatibleColumnTypeError) {
		return ErrorUnsupportedSchemaChange, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  incompatibleColumnTypeError.TableName,
				ErrorAttributeKeyColumn: incompatibleColumnTypeError.ColumnName,
			},
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
