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
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
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
	ClickHouseDecimalInsertRe = regexp.MustCompile(
		`Cannot insert Avro decimal with scale \d+ and precision \d+ to ClickHouse type Decimal\(\d+, \d+\) with scale \d+ and precision \d+`,
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
	ErrorNotifyBadGTIDSetup = ErrorClass{
		Class: "NOTIFY_BAD_MULTISOURCE_GTID_SETUP", action: NotifyUser,
	}
	ErrorNotifySourceTableMissing = ErrorClass{
		Class: "NOTIFY_SOURCE_TABLE_MISSING", action: NotifyUser,
	}
	ErrorNotifyBadSourceTableReplicaIdentity = ErrorClass{
		Class: "NOTIFY_BAD_POSTGRES_TABLE_REPLICA_IDENTITY", action: NotifyUser,
	}
	ErrorNotifyPublicationMissing = ErrorClass{
		Class: "NOTIFY_PUBLICATION_MISSING", action: NotifyUser,
	}
	ErrorNotifyTablesNotInPublication = ErrorClass{
		Class: "NOTIFY_TABLES_NOT_IN_PUBLICATION", action: NotifyUser,
	}
	ErrorNotifyReplicationSlotMissing = ErrorClass{
		Class: "NOTIFY_REPLICATION_SLOT_MISSING", action: NotifyUser,
	}
	ErrorNotifyIncreaseLogicalDecodingWorkMem = ErrorClass{
		Class: "NOTIFY_INCREASE_LOGICAL_DECODING_WORK_MEM", action: NotifyUser,
	}
	ErrorUnsupportedDatatype = ErrorClass{
		Class: "NOTIFY_UNSUPPORTED_DATATYPE", action: NotifyUser,
	}
	ErrorNotifyInvalidSnapshotIdentifier = ErrorClass{
		Class: "NOTIFY_INVALID_SNAPSHOT_IDENTIFIER", action: NotifyUser,
	}
	ErrorNotifyInvalidSynchronizedStandbySlots = ErrorClass{
		Class: "NOTIFY_INVALID_SYNCHRONIZED_STANDBY_SLOTS", action: NotifyUser,
	}
	ErrorNotifyTerminate = ErrorClass{
		Class: "NOTIFY_TERMINATE", action: NotifyUser,
	}
	ErrorNotifyReplicationStandbySetup = ErrorClass{
		Class: "NOTIFY_REPLICATION_STANDBY_SETUP", action: NotifyUser,
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
	ErrorNotifyPostgresLogicalMessageProcessing = ErrorClass{
		Class: "NOTIFY_POSTGRES_LOGICAL_MESSAGE_PROCESSING_ERROR", action: NotifyUser,
	}
	// Catch-all for unclassified errors
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

	// Reference:
	// https://github.dev/jackc/pgx/blob/master/pgconn/pgconn.go#L733-L740
	if strings.Contains(err.Error(), "conn closed") {
		return ErrorRetryRecoverable, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "UNKNOWN",
		}
	}

	if errors.Is(err, shared.ErrTableDoesNotExist) {
		return ErrorNotifySourceTableMissing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "TABLE_DOES_NOT_EXIST",
		}
	}

	var replicaIdentityNothingErr *exceptions.ReplicaIdentityNothingError
	if errors.As(err, &replicaIdentityNothingErr) {
		return ErrorNotifyBadSourceTableReplicaIdentity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "REPLICA_IDENTITY_NOTHING",
		}
	}

	var tablesNotInPubErr *exceptions.TablesNotInPublicationError
	if errors.As(err, &tablesNotInPubErr) {
		return ErrorNotifyTablesNotInPublication, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "TABLES_NOT_IN_PUBLICATION",
		}
	}

	var missingPrimaryKeyErr *exceptions.MissingPrimaryKeyError
	if errors.As(err, &missingPrimaryKeyErr) {
		return ErrorNotifyBadSourceTableReplicaIdentity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "MISSING_PRIMARY_KEY",
		}
	}

	var logicalMessageProcessingErr *exceptions.PostgresLogicalMessageProcessingError
	if errors.As(err, &logicalMessageProcessingErr) {
		return ErrorNotifyPostgresLogicalMessageProcessing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "LOGICAL_MESSAGE_PROCESSING_ERROR",
		}
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

	var sshOpenChanErr *ssh.OpenChannelError
	if errors.As(err, &sshOpenChanErr) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceSSH,
			Code:   sshOpenChanErr.Reason.String(),
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

	var tlsCertVerificationError *tls.CertificateVerificationError
	if errors.As(err, &tlsCertVerificationError) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "tls.CertificateVerificationError",
		}
	}

	var temporalErr *temporal.ApplicationError
	if errors.As(err, &temporalErr) {
		switch exceptions.ApplicationErrorType(temporalErr.Type()) {
		case exceptions.ApplicationErrorTypeIrrecoverablePublicationMissing:
			return ErrorNotifyPublicationMissing, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   temporalErr.Type(),
			}
		case exceptions.ApplicationErrorTypeIrrecoverableSlotMissing:
			return ErrorNotifyReplicationSlotMissing, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   temporalErr.Type(),
			}
		case exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot:
			return ErrorNotifyInvalidSnapshotIdentifier, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   temporalErr.Type(),
			}

		case exceptions.ApplicationErrorTypeIrrecoverableExistingSlot, exceptions.ApplicationErrorTypeIrrecoverableMissingTables:
			return ErrorNotifyConnectivity, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   temporalErr.Type(),
			}
		}
		// Just in case we forget to classify some irrecoverable errors
		if _, irrecoverable := exceptions.IrrecoverableApplicationErrorTypesMap[temporalErr.Type()]; irrecoverable {
			return ErrorNotifyConnectivity, ErrorInfo{
				Source: ErrorSourceTemporal,
				Code:   temporalErr.Type(),
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
			// Handle logical decoding error in ReorderBufferPreserveLastSpilledSnapshot routine
			if strings.HasPrefix(pgErr.Message, "Internal error encountered during logical decoding of aborted sub-transaction") &&
				strings.Contains(pgErr.Hint, "increase logical_decoding_work_mem") {
				return ErrorNotifyIncreaseLogicalDecodingWorkMem, pgErrorInfo
			}

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
				return ErrorNotifyConnectivity, pgErrorInfo
			}

			if strings.Contains(pgErr.Message, "invalid memory alloc request size") {
				return ErrorNotifyPostgresSlotMemalloc, pgErrorInfo
			}

			// Fall through for other internal errors
			return ErrorOther, pgErrorInfo

		case pgerrcode.ObjectNotInPrerequisiteState:
			if pgErr.Message == "logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary" {
				return ErrorNotifyReplicationStandbySetup, pgErrorInfo
			}

			// same underlying error but 3 different messages
			// based on PG version, newer ones have second error
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") ||
				strings.Contains(pgErr.Message, "can no longer get changes from replication slot") ||
				strings.Contains(pgErr.Message, "could not import the requested snapshot") {
				return ErrorNotifySlotInvalid, pgErrorInfo
			}

			if strings.Contains(pgErr.Message,
				`specified in parameter "synchronized_standby_slots" does not have active_pid`) {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			if strings.Contains(pgErr.Message,
				`specified in parameter "synchronized_standby_slots" does not have active_pid`) {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// this can't happen for slots we created
			// from our perspective, the slot is missing
			if strings.Contains(pgErr.Message, "was not created in this database") {
				return ErrorNotifyReplicationSlotMissing, pgErrorInfo
			}

		case pgerrcode.InvalidParameterValue:
			if strings.Contains(pgErr.Message, "invalid snapshot identifier") {
				return ErrorNotifyInvalidSnapshotIdentifier, pgErrorInfo
			}

			if strings.Contains(pgErr.Message, "synchronized_standby_slots") {
				return ErrorNotifyInvalidSynchronizedStandbySlots, pgErrorInfo
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

		case pgerrcode.QueryCanceled:
			return ErrorNotifyConnectivity, pgErrorInfo

		case pgerrcode.DuplicateFile,
			pgerrcode.DeadlockDetected,
			pgerrcode.SerializationFailure,
			pgerrcode.IdleInTransactionSessionTimeout:
			return ErrorRetryRecoverable, pgErrorInfo
		default:
			return ErrorOther, pgErrorInfo
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
			if myErr.State == "HY000" &&
				strings.HasPrefix(myErr.Message, "The last transaction was aborted due to") &&
				strings.HasSuffix(myErr.Message, "Please Retry.") {
				return ErrorRetryRecoverable, myErrorInfo
			}
			return ErrorOther, myErrorInfo
		case 1146: // ER_NO_SUCH_TABLE
			return ErrorNotifySourceTableMissing, myErrorInfo
		case 1943:
			return ErrorNotifyBadGTIDSetup, myErrorInfo
		default:
			return ErrorOther, myErrorInfo
		}
	}

	var mongoCmdErr mongo.CommandError
	if errors.As(err, &mongoCmdErr) {
		mongoErrorInfo := ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   strconv.Itoa(int(mongoCmdErr.Code)),
		}

		if mongoCmdErr.HasErrorMessage("connection reset by peer") {
			return ErrorRetryRecoverable, mongoErrorInfo
		}

		// this often happens on Mongo Atlas as part of maintenance, and should recover, but we notify if exceed default threshold
		// (ShutdownInProgress code should be 91, but we have observed 0 in the past, so string match to be safe)
		if mongoCmdErr.HasErrorMessage("(ShutdownInProgress) The server is in quiesce mode and will shut down") {
			return ErrorNotifyConnectivity, mongoErrorInfo
		}

		// This should recover, but we notify if exceed default threshold
		if mongoCmdErr.HasErrorLabel(driver.TransientTransactionError) {
			return ErrorNotifyConnectivity, mongoErrorInfo
		}

		// https://www.mongodb.com/docs/manual/reference/error-codes/
		switch mongoCmdErr.Code {
		case 13: // Unauthorized
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 91: // ShutdownInProgress
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 286: // ChangeStreamHistoryLost
			return ErrorNotifyChangeStreamHistoryLost, mongoErrorInfo
		default:
			return ErrorOther, mongoErrorInfo
		}
	}

	var mongoMarshalErr mongo.MarshalError
	if errors.As(err, &mongoMarshalErr) {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "MARSHAL_ERROR",
		}
	}

	var mongoEncryptError mongo.MongocryptError
	if errors.As(err, &mongoEncryptError) {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "MONGOCRYPT_ERROR",
		}
	}

	var mongoServerError topology.ServerSelectionError
	if errors.As(err, &mongoServerError) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "SERVER_SELECTION_ERROR",
		}
	}

	var mongoConnError topology.ConnectionError
	if errors.As(err, &mongoConnError) {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "CONNECTION_ERROR",
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
		case chproto.ErrBadArguments:
			if ClickHouseDecimalInsertRe.MatchString(chException.Message) {
				return ErrorUnsupportedDatatype, chErrorInfo
			}
		case chproto.ErrAccessEntityNotFound:
			if ClickHouseNotFoundInUserDirsRe.MatchString(chException.Message) {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case chproto.ErrTableAlreadyExists:
			// We are already running query CREATE TABLE IF NOT EXISTS so this is likely a replica synchronization issue
			if strings.HasSuffix(chException.Message, "is either DETACHED PERMANENTLY or was just created by another replica") {
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
		case 529: // NOT_A_LEADER
			if strings.HasPrefix(chException.Message, "Cannot enqueue query on this replica, because it has replication lag") {
				return ErrorNotifyConnectivity, chErrorInfo
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
		case chproto.ErrQueryIsProhibited:
			if strings.Contains(chException.Message, "Replicated DDL queries are disabled") {
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

	var postgresPrimaryKeyModifiedError *exceptions.PrimaryKeyModifiedError
	if errors.As(err, &postgresPrimaryKeyModifiedError) {
		return ErrorUnsupportedSchemaChange, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  postgresPrimaryKeyModifiedError.TableName,
				ErrorAttributeKeyColumn: postgresPrimaryKeyModifiedError.ColumnName,
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
