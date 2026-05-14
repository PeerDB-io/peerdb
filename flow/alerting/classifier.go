package alerting

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"cloud.google.com/go/bigquery"
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
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/internal"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type ErrorAction string

const (
	NotifyUser      ErrorAction = "notify_user"
	Ignore          ErrorAction = "ignore"
	NotifyTelemetry ErrorAction = "notify_telemetry"
)

const (
	MongoShutdownInProgress              = "(ShutdownInProgress) The server is in quiesce mode and will shut down"
	MongoInterruptedDueToReplStateChange = "(InterruptedDueToReplStateChange) operation was interrupted"
	MongoIncompleteReadOfMessageHeader   = "incomplete read of message header"

	// mysqlGeometryLinearRingNotClosedError is the specific WKB parse failure raised by the
	// go-geos library when a LinearRing's points do not close. Used to give a more specific code
	// once we already know the error came from MySQL geometry parsing.
	mysqlGeometryLinearRingNotClosedError = "Points of LinearRing do not form a closed linestring"
)

var (
	ClickHouseDecimalParsingRe = regexp.MustCompile(
		`Cannot parse type Decimal\(\d+, \d+\), expected non-empty binary data with size equal to or less than \d+, got \d+`,
	)
	ClickHouseDecimalInsertRe = regexp.MustCompile(
		`Cannot insert Avro decimal with scale \d+ and precision \d+ to ClickHouse type Decimal\(\d+, \d+\) with scale \d+ and precision \d+`,
	)
	// ID(a14c2a1c-edcd-5fcb-73be-bd04e09fccb7) not found in user directories
	ClickHouseNotFoundInUserDirsRe   = regexp.MustCompile("ID\\([a-z0-9-]+\\) not found in `?user directories`?")
	ClickHouseTooManyPartsTableRe    = regexp.MustCompile(`in table '(.+)'\.`)
	ClickHouseObjectStorageIOErrorRe = regexp.MustCompile(
		`unspecified iostream_category error: while reading .+: While executing ReadFromObjectStorage`,
	)
	PostgresPublicationDoesNotExistRe = regexp.MustCompile(`publication ".*?" does not exist`)
	PostgresSnapshotDoesNotExistRe    = regexp.MustCompile(`snapshot ".*?" does not exist`)
	PostgresWalSegmentRemovedRe       = regexp.MustCompile(`requested WAL segment \w+ has already been removed`)
	PostgresSpillFileMissingRe        = regexp.MustCompile(`Unable to restore changes for xid \d+`)
	MySqlRdsBinlogFileNotFoundRe      = regexp.MustCompile(`File '/rdsdbdata/log/binlog/mysql-bin-changelog.\d+' not found`)
	MongoPoolClearedErrorRe           = regexp.MustCompile(`connection pool for .+ was cleared because another operation failed with`)
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
	ErrorSourceBigQuery        ErrorSource = "bigquery"
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
	ErrorNotifyBinlogRowMetadataInvalid = ErrorClass{
		Class: "NOTIFY_BINLOG_ROW_METADATA_INVALID", action: NotifyUser,
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
	ErrorNotifyInvalidSortKey = ErrorClass{
		Class: "NOTIFY_INVALID_SORT_KEY", action: NotifyUser,
	}
	ErrorNotifyInvalidSnapshotIdentifier = ErrorClass{
		Class: "NOTIFY_INVALID_SNAPSHOT_IDENTIFIER", action: NotifyUser,
	}
	ErrorNotifyInvalidSynchronizedStandbySlots = ErrorClass{
		Class: "NOTIFY_INVALID_SYNCHRONIZED_STANDBY_SLOTS", action: NotifyUser,
	}
	ErrorNotifySnapshotExportDisabled = ErrorClass{
		Class: "NOTIFY_SNAPSHOT_EXPORT_DISABLED", action: NotifyUser,
	}
	ErrorNotifyTerminate = ErrorClass{
		Class: "NOTIFY_TERMINATE", action: NotifyUser,
	}
	ErrorNotifyReplicationStandbySetup = ErrorClass{
		Class: "NOTIFY_REPLICATION_STANDBY_SETUP", action: NotifyUser,
	}
	ErrorNotifyLogicalDecodingStandbyNotSupported = ErrorClass{
		Class: "NOTIFY_LOGICAL_DECODING_STANDBY_NOT_SUPPORTED", action: NotifyUser,
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
	// This RDS specific error is seen when we try to create a replication slot on a read-replica
	ErrNotifyPostgresCreatingSlotOnReader = ErrorClass{
		Class: "NOTIFY_POSTGRES_CREATING_SLOT_ON_READER", action: NotifyUser,
	}
	// Mongo specific, equivalent to slot invalidation in Postgres
	ErrorNotifyChangeStreamHistoryLost = ErrorClass{
		Class: "NOTIFY_CHANGE_STREAM_HISTORY_LOST", action: NotifyUser,
	}
	ErrorNotifyPostgresLogicalMessageProcessing = ErrorClass{
		Class: "NOTIFY_POSTGRES_LOGICAL_MESSAGE_PROCESSING_ERROR", action: NotifyUser,
	}
	ErrorNotifyWalSegmentRemoved = ErrorClass{
		Class: "NOTIFY_WAL_SEGMENT_REMOVED", action: NotifyUser,
	}
	ErrorNotifyClickHouseSupportIsDisabledError = ErrorClass{
		Class: "NOTIFY_CLICKHOUSE_SUPPORT_IS_DISABLED_ERROR", action: NotifyUser,
	}
	ErrorNotifyTooManyPartsError = ErrorClass{
		Class: "NOTIFY_TOO_MANY_PARTS", action: NotifyUser,
	}
	// Catch-all for misc ClickHouse errors
	ErrorNotifyClickHouseError = ErrorClass{
		Class: "NOTIFY_CLICKHOUSE_ERROR", action: NotifyUser,
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
	if pgWalErr, ok := errors.AsType[*exceptions.PostgresWalError](err); ok {
		pgErr = pgconn.ErrorResponseToPgError(pgWalErr.UnderlyingError())
	}
	var pgErrorInfo ErrorInfo
	if pgErrFromErr, ok := errors.AsType[*pgconn.PgError](err); pgErr != nil || ok {
		if pgErr == nil {
			pgErr = pgErrFromErr
		}
		pgErrorInfo = ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   pgErr.Code,
		}

		if _, ok := errors.AsType[*exceptions.CatalogError](err); ok {
			errorClass := ErrorInternal
			if pgErr != nil {
				return errorClass, pgErrorInfo
			}
			return errorClass, ErrorInfo{
				Source: ErrorSourcePostgresCatalog,
				Code:   "UNKNOWN",
			}
		}

		if _, ok := errors.AsType[*exceptions.DropFlowError](err); ok {
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

		if _, ok := errors.AsType[*exceptions.PostgresSetupError](err); ok {
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

	if _, ok := errors.AsType[*exceptions.ReplicaIdentityNothingError](err); ok {
		return ErrorNotifyBadSourceTableReplicaIdentity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "REPLICA_IDENTITY_NOTHING",
		}
	}

	if _, ok := errors.AsType[*exceptions.TablesNotInPublicationError](err); ok {
		return ErrorNotifyTablesNotInPublication, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "TABLES_NOT_IN_PUBLICATION",
		}
	}

	if _, ok := errors.AsType[*exceptions.MissingPrimaryKeyError](err); ok {
		return ErrorNotifyBadSourceTableReplicaIdentity, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "MISSING_PRIMARY_KEY",
		}
	}

	if _, ok := errors.AsType[*exceptions.PostgresLogicalMessageProcessingError](err); ok {
		return ErrorNotifyPostgresLogicalMessageProcessing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "LOGICAL_MESSAGE_PROCESSING_ERROR",
		}
	}

	if _, ok := errors.AsType[*exceptions.PublicationMissingError](err); ok {
		return ErrorNotifyPublicationMissing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "irrecoverable_publication_missing",
		}
	}

	if _, ok := errors.AsType[*exceptions.SlotMissingError](err); ok {
		return ErrorNotifyReplicationSlotMissing, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "irrecoverable_slot_missing",
		}
	}

	if _, ok := errors.AsType[*exceptions.ReplStateDesyncError](err); ok {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "desync",
		}
	}

	if peerCreateError, ok := errors.AsType[*exceptions.PeerCreateError](err); ok {
		if errors.Is(peerCreateError, context.DeadlineExceeded) {
			return ErrorNotifyConnectivity, ErrorInfo{
				Source: ErrorSourceOther,
				Code:   "CONTEXT_DEADLINE_EXCEEDED",
			}
		}
		if errors.Is(peerCreateError, syscall.ECONNRESET) {
			return ErrorNotifyConnectivity, ErrorInfo{
				Source: ErrorSourceNet,
				Code:   syscall.ECONNRESET.Error(),
			}
		}
	}

	// Other connection reset errors can mostly be ignored
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

	if netErr, ok := errors.AsType[*net.OpError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   netErr.Err.Error(),
		}
	}

	if sshOpenChanErr, ok := errors.AsType[*ssh.OpenChannelError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceSSH,
			Code:   sshOpenChanErr.Reason.String(),
		}
	}

	if _, ok := errors.AsType[*exceptions.SSHTunnelSetupError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceSSH,
			Code:   "UNKNOWN",
		}
	}

	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "net.DNSError",
		}
	}

	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceNet,
			Code:   "tls.CertificateVerificationError",
		}
	}

	if temporalErr, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		switch exceptions.ApplicationErrorType(temporalErr.Type()) {
		case exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot:
			return ErrorNotifyInvalidSnapshotIdentifier, ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   temporalErr.Type(),
			}
		case exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot:
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

	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
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
			pgerrcode.DiskFull,
			pgerrcode.DuplicateFile:
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
			// It either shows up once then disappears
			// (quirk in some PG installs where replication can try read a segment that hasn't been created yet)
			// or shows up and persists
			// NotifyUser with repeat threshold accommodates both
			if PostgresWalSegmentRemovedRe.MatchString(pgErr.Message) {
				return ErrorNotifyWalSegmentRemoved, pgErrorInfo
			}
			// Handles missing spill-to-disk file during logical decoding (transient error)
			if PostgresSpillFileMissingRe.MatchString(pgErr.Message) {
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
			// It either shows up once then disappears
			// (quirk in some PG installs where replication can try read a segment that hasn't been created yet)
			// or shows up and persists
			// NotifyUser with repeat threshold accommodates both
			if PostgresWalSegmentRemovedRe.MatchString(pgErr.Message) {
				return ErrorNotifyWalSegmentRemoved, pgErrorInfo
			}

			// Handle Neon quota exceeded errors
			if strings.Contains(pgErr.Message,
				"Your account or project has exceeded the compute time quota. Upgrade your plan to increase limits.") {
				return ErrorNotifyConnectivity, pgErrorInfo
			}

			// Handle Neon disk quota exceeded errors
			if strings.Contains(pgErr.Message, "Disk quota exceeded") {
				return ErrorNotifyConnectivity, pgErrorInfo
			}

			// Handle Neon's custom WAL reading error
			if pgErr.Routine == "NeonWALPageRead" && strings.Contains(pgErr.Message, "server closed the connection unexpectedly") {
				return ErrorNotifyConnectivity, pgErrorInfo
			}

			if strings.Contains(pgErr.Message, "invalid memory alloc request size") {
				return ErrorNotifyPostgresSlotMemalloc, pgErrorInfo
			}

			// Usually a single occurrence then reconnect immediately helps
			if strings.Contains(pgErr.Message, "pfree called with invalid pointer") {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// Transient reorderbuffer spill file restoration failure (e.g. "Resource temporarily unavailable")
			if PostgresSpillFileMissingRe.MatchString(pgErr.Message) {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// Shared invalidation message corruption - usually transient, reconnect helps
			// https://github.com/postgres/postgres/blob/e82e9aaa6a2942505c2c328426778787e4976ea6/src/backend/utils/cache/inval.c#L901
			if strings.Contains(pgErr.Message, "unrecognized SI message ID:") {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			if strings.Contains(pgErr.Message, "Create the replication slot from the writer node instead") {
				return ErrNotifyPostgresCreatingSlotOnReader, pgErrorInfo
			}

			// low-level Postgres memory management bug, single occurrence and fixed by retry
			if pgErr.Routine == "GenerationFree" && strings.Contains(pgErr.Message, "could not find block containing chunk") {
				return ErrorRetryRecoverable, pgErrorInfo
			}

			// Fall through for other internal errors
			return ErrorOther, pgErrorInfo

		case pgerrcode.ObjectNotInPrerequisiteState:
			if pgErr.Message == "logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary" {
				return ErrorNotifyReplicationStandbySetup, pgErrorInfo
			}

			// same underlying error but different messages, depending on PG version
			if strings.Contains(pgErr.Message, "cannot read from logical replication slot") || // PG13-17
				strings.Contains(pgErr.Message, "can no longer get changes from replication slot") || // PG13-17
				strings.Contains(pgErr.Message, "could not import the requested snapshot") || // All
				strings.Contains(pgErr.Message, "can no longer access replication slot") { // PG18
				return ErrorNotifySlotInvalid, pgErrorInfo
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

			// Aurora failover: reader was promoted, slot can't be used on old RO node
			if strings.Contains(pgErr.Message, "replication slots cannot be used on RO (Read Only) node") {
				return ErrorRetryRecoverable, pgErrorInfo
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

		case pgerrcode.FeatureNotSupported:
			if strings.Contains(pgErr.Message, "logical decoding cannot be used while in recovery") {
				return ErrorNotifyLogicalDecodingStandbyNotSupported, pgErrorInfo
			}

		case pgerrcode.SyntaxError:
			if strings.Contains(pgErr.Message, "ysql_enable_pg_export_snapshot") {
				return ErrorNotifySnapshotExportDisabled, pgErrorInfo
			}

		case pgerrcode.DeadlockDetected,
			pgerrcode.SerializationFailure,
			pgerrcode.IdleInTransactionSessionTimeout:
			return ErrorRetryRecoverable, pgErrorInfo
		default:
			return ErrorOther, pgErrorInfo
		}
	}

	if myErr, ok := errors.AsType[*mysql.MyError](err); ok {
		// https://mariadb.com/docs/server/reference/error-codes/mariadb-error-code-reference
		// Error code < 1000 indicates OS-level errors being passed through MySQL's error reporting system
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
			1226, // ER_USER_LIMIT_REACHED
			1827: // ER_PASSWORD_FORMAT
			return ErrorNotifyConnectivity, myErrorInfo
		case 1236, // ER_MASTER_FATAL_ERROR_READING_BINLOG
			1373: // ER_UNKNOWN_TARGET_BINLOG
			return ErrorNotifyBinlogInvalid, myErrorInfo
		case 1105: // ER_UNKNOWN_ERROR
			// RDS Aurora MySQL specific errors due to "Zero Downtime Patch" or "Zero Downtime Restart"
			// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Updates.ZDP.html
			if myErr.State == "HY000" &&
				strings.HasPrefix(myErr.Message, "The last transaction was aborted due to") &&
				strings.HasSuffix(myErr.Message, "Please retry.") {
				return ErrorRetryRecoverable, myErrorInfo
			}
			return ErrorOther, myErrorInfo
		case 1146: // ER_NO_SUCH_TABLE
			return ErrorNotifySourceTableMissing, myErrorInfo
		case 1943: // ER_DUPLICATE_GTID_DOMAIN (MariaDB)
			return ErrorNotifyBadGTIDSetup, myErrorInfo
		case 5, // ERR_OUT_OF_MEMORY
			1317: // ER_QUERY_INTERRUPTED
			return ErrorRetryRecoverable, myErrorInfo
		default:
			return ErrorOther, myErrorInfo
		}
	}

	if mongoCmdErr, ok := errors.AsType[mongo.CommandError](err); ok {
		mongoErrorInfo := ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   strconv.Itoa(int(mongoCmdErr.Code)),
		}

		if mongoCmdErr.HasErrorMessage("connection reset by peer") {
			return ErrorRetryRecoverable, mongoErrorInfo
		}

		// this often happens on Mongo Atlas as part of maintenance and should recover
		// (ShutdownInProgress code should be 91, but we have observed 0 in the past, so string match to be safe)
		if mongoCmdErr.HasErrorMessage(MongoShutdownInProgress) {
			return ErrorIgnoreConnTemporary, mongoErrorInfo
		}

		// This should recover, but we notify if exceed default threshold
		if mongoCmdErr.HasErrorLabel(driver.TransientTransactionError) {
			return ErrorNotifyConnectivity, mongoErrorInfo
		}

		// https://www.mongodb.com/docs/manual/reference/error-codes/
		switch mongoCmdErr.Code {
		case 6: // HostUnreachable
			return ErrorRetryRecoverable, mongoErrorInfo
		case 13: // Unauthorized
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 18: // AuthenticationFailed
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 40: // ConflictingUpdateOperators
			return ErrorRetryRecoverable, mongoErrorInfo
		case 43: // CursorNotFound
			return ErrorRetryRecoverable, mongoErrorInfo
		case 91: // ShutdownInProgress
			return ErrorIgnoreConnTemporary, mongoErrorInfo
		case 202: // NetworkInterfaceExceededTimeLimit
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 136, // CappedPositionLost
			286: // ChangeStreamHistoryLost
			return ErrorNotifyChangeStreamHistoryLost, mongoErrorInfo
		case 11600, //  InterruptedAtShutdown
			11601, // Interrupted
			11602: // InterruptedDueToReplStateChange
			return ErrorRetryRecoverable, mongoErrorInfo
		case 13436: // NotPrimaryOrSecondary
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 133: // FailedToSatisfyReadPreference
			return ErrorNotifyConnectivity, mongoErrorInfo
		case 17287: // Bad query specified (documentDB only)
			return ErrorRetryRecoverable, mongoErrorInfo
		default:
			return ErrorOther, mongoErrorInfo
		}
	}

	if _, ok := errors.AsType[mongo.MarshalError](err); ok {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "MARSHAL_ERROR",
		}
	}

	if _, ok := errors.AsType[mongo.MongocryptError](err); ok {
		return ErrorOther, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "MONGOCRYPT_ERROR",
		}
	}

	if _, ok := errors.AsType[topology.ServerSelectionError](err); ok {
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "SERVER_SELECTION_ERROR",
		}
	}

	if _, ok := errors.AsType[topology.ConnectionError](err); ok {
		if strings.Contains(err.Error(), MongoIncompleteReadOfMessageHeader) {
			return ErrorRetryRecoverable, ErrorInfo{
				Source: ErrorSourceMongoDB,
				Code:   "CONNECTION_ERROR",
			}
		}
		return ErrorNotifyConnectivity, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "CONNECTION_ERROR",
		}
	}

	if _, ok := errors.AsType[topology.WaitQueueTimeoutError](err); ok {
		return ErrorRetryRecoverable, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "WAIT_QUEUE_TIMEOUT_ERROR",
		}
	}

	// ErrClientDisconnected is only produced when the topology is explicitly closed via
	// Client.Disconnect(); classify as internal to flag client lifecycle bugs in our code.
	if errors.Is(err, mongo.ErrClientDisconnected) {
		return ErrorInternal, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "CLIENT_DISCONNECTED",
		}
	}

	// MongoDB can leak error without properly encapsulate it into a pre-defined error type.
	// Use string matching as a catch-all for poolClearedErrors. These errors occur when a
	// connection pool is cleared due to another operation failure; they are always retryable
	if MongoPoolClearedErrorRe.MatchString(err.Error()) {
		mongoErrorInfo := ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "POOL_CLEARED_ERROR",
		}
		if strings.Contains(err.Error(), MongoShutdownInProgress) {
			mongoErrorInfo.Code += fmt.Sprintf("(%d)", 91)
			return ErrorIgnoreConnTemporary, mongoErrorInfo
		}
		if strings.Contains(err.Error(), MongoInterruptedDueToReplStateChange) {
			mongoErrorInfo.Code += fmt.Sprintf("(%d)", 11602)
			return ErrorRetryRecoverable, mongoErrorInfo
		}
		if strings.Contains(err.Error(), MongoIncompleteReadOfMessageHeader) {
			mongoErrorInfo.Code += "(CONNECTION_ERROR)"
			return ErrorRetryRecoverable, mongoErrorInfo
		}
		return ErrorRetryRecoverable, mongoErrorInfo
	}

	if _, ok := errors.AsType[*exceptions.BigQueryError](err); ok {
		bqErrorInfo := ErrorInfo{
			Source: ErrorSourceBigQuery,
			Code:   "UNKNOWN",
		}
		if apiErr, ok := errors.AsType[*googleapi.Error](err); ok {
			bqErrorInfo.Code = strconv.Itoa(apiErr.Code)
			switch apiErr.Code {
			case 401, // Unauthorized
				403, // Forbidden
				404: // Not Found (e.g. missing dataset/table/staging bucket)
				return ErrorNotifyConnectivity, bqErrorInfo
			}
		} else if bqErr, ok := errors.AsType[*bigquery.Error](err); ok && bqErr.Reason != "" {
			bqErrorInfo.Code = bqErr.Reason
		}
		return ErrorOther, bqErrorInfo
	}

	if chException, ok := errors.AsType[*clickhouse.Exception](err); ok {
		chErrorInfo := ErrorInfo{
			Source: ErrorSourceClickHouse,
			Code:   strconv.Itoa(int(chException.Code)),
		}
		switch chproto.Error(chException.Code) {
		case chproto.ErrUnknownTable,
			chproto.ErrNoSuchColumnInTable,
			// "Too large string for FixedString column: (at row 10195)"
			// The only one created by us is FixedString(1) for PG QChar so assuming the user did it for a string and it didn't work
			chproto.ErrTooLargeStringSize:
			if _, ok := errors.AsType[*peerdb_clickhouse.ViewError](err); ok {
				return ErrorNotifyMVOrView, chErrorInfo
			}
			return ErrorNotifyDestinationModified, chErrorInfo
		case chproto.ErrIncorrectData:
			if _, ok := errors.AsType[*peerdb_clickhouse.ViewError](err); ok {
				return ErrorNotifyMVOrView, chErrorInfo
			}
		case chproto.ErrMemoryLimitExceeded:
			return ErrorNotifyOOM, chErrorInfo
		case chproto.ErrUnknownDatabase,
			chproto.ErrAuthenticationFailed:
			return ErrorNotifyConnectivity, chErrorInfo
		case chproto.ErrKeeperException:
			if chException.Message == "Session expired" || strings.HasPrefix(chException.Message, "Coordination error: Connection loss") {
				return ErrorRetryRecoverable, chErrorInfo
			}
			return ErrorInternalClickHouse, chErrorInfo
		case chproto.ErrNotImplemented:
			if strings.HasSuffix(chException.Message, "is not supported by storage View") {
				return ErrorNotifyDestinationModified, chErrorInfo
			}
		case chproto.ErrUnfinished:
			if strings.Contains(chException.Message, "Failed to load all data parts") {
				return ErrorNotifyClickHouseError, chErrorInfo
			}
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrCannotAssignAlter:
			return ErrorNotifyClickHouseError, chErrorInfo
		case chproto.ErrAborted:
			return ErrorInternalClickHouse, chErrorInfo
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
			chproto.ErrUnknownElementOfEnum,
			chproto.ErrNoCommonType,
			chproto.ErrIllegalTypeOfArgument:
			if _, ok := errors.AsType[*exceptions.ClickHouseQRepSyncError](err); ok {
				// could cause false positives, but should be rare
				return ErrorNotifyMVOrView, chErrorInfo
			}
		case chproto.ErrNotALeader:
			if strings.HasPrefix(chException.Message, "Cannot enqueue query on this replica, because it has replication lag") {
				return ErrorNotifyConnectivity, chErrorInfo
			}
		case chproto.ErrCannotScheduleTask,
			chproto.ErrQueryWasCancelled,
			chproto.ErrPocoException,
			chproto.ErrCannotReadFromSocket,
			chproto.ErrSocketTimeout,
			chproto.ErrTableIsReadOnly:
			return ErrorRetryRecoverable, chErrorInfo
		case chproto.ErrStdException:
			if ClickHouseObjectStorageIOErrorRe.MatchString(chException.Message) {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case chproto.ErrTimeoutExceeded:
			if strings.HasSuffix(chException.Message, "distributed_ddl_task_timeout") {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case chproto.ErrQueryIsProhibited:
			if strings.Contains(chException.Message, "Replicated DDL queries are disabled") {
				return ErrorRetryRecoverable, chErrorInfo
			}
		case chproto.ErrSupportIsDisabled:
			return ErrorNotifyClickHouseSupportIsDisabledError, chErrorInfo
		case chproto.ErrTooManyParts:
			var additionalAttributes map[AdditionalErrorAttributeKey]string
			if matches := ClickHouseTooManyPartsTableRe.FindStringSubmatch(chException.Message); len(matches) > 1 {
				additionalAttributes = map[AdditionalErrorAttributeKey]string{
					ErrorAttributeKeyTable: matches[1],
				}
			}
			return ErrorNotifyTooManyPartsError, ErrorInfo{
				Source:               chErrorInfo.Source,
				Code:                 chErrorInfo.Code,
				AdditionalAttributes: additionalAttributes,
			}
		}
		// a catch-all for MV or view errors
		if _, ok := errors.AsType[*peerdb_clickhouse.ViewError](err); ok {
			return ErrorNotifyMVOrView, chErrorInfo
		}
		// a catch-all for normalization errors, which typically indicate a bad MV or view
		if _, ok := errors.AsType[*exceptions.NormalizationError](err); ok {
			logger := internal.LoggerFromCtx(ctx)
			logger.Warn("Assuming a normalization error is bad MV or view", slog.Any("error", err))
			return ErrorNotifyMVOrView, chErrorInfo
		}
		return ErrorOther, chErrorInfo
	}

	if numericOutOfRangeError, ok := errors.AsType[*exceptions.NumericOutOfRangeError](err); ok {
		return ErrorLossyConversion, ErrorInfo{
			Source: "typeConversion",
			Code:   "NUMERIC_OUT_OF_RANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  numericOutOfRangeError.DestinationTable,
				ErrorAttributeKeyColumn: numericOutOfRangeError.DestinationColumn,
			},
		}
	}

	if numericTruncatedError, ok := errors.AsType[*exceptions.NumericTruncatedError](err); ok {
		return ErrorLossyConversion, ErrorInfo{
			Source: "typeConversion",
			Code:   "NUMERIC_TRUNCATED",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  numericTruncatedError.DestinationTable,
				ErrorAttributeKeyColumn: numericTruncatedError.DestinationColumn,
			},
		}
	}

	if incompatibleColumnTypeError, ok := errors.AsType[*exceptions.MySQLIncompatibleColumnTypeError](err); ok {
		return ErrorUnsupportedSchemaChange, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  incompatibleColumnTypeError.TableName,
				ErrorAttributeKeyColumn: incompatibleColumnTypeError.ColumnName,
			},
		}
	}

	if _, ok := errors.AsType[*exceptions.MySQLUnsupportedBinlogRowMetadataError](err); ok {
		return ErrorNotifyBinlogRowMetadataInvalid, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   "UNSUPPORTED_BINLOG_ROW_METADATA",
		}
	}

	if unsupportedDDLError, ok := errors.AsType[*exceptions.MySQLUnsupportedDDLError](err); ok {
		return ErrorNotifyBinlogRowMetadataInvalid, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable: unsupportedDDLError.TableName,
			},
		}
	}

	if mysqlStreamingError, ok := errors.AsType[*exceptions.MySQLStreamingError](err); ok {
		if mysqlStreamingError.Retryable {
			return ErrorRetryRecoverable, ErrorInfo{
				Source: ErrorSourceMySQL,
				Code:   "STREAMING_TRANSIENT_ERROR",
			}
		} else {
			return ErrorOther, ErrorInfo{
				Source: ErrorSourceMySQL,
				Code:   "UNKNOWN",
			}
		}
	}

	if mysqlGeometryParseError, ok := errors.AsType[*exceptions.MySQLGeometryParseError](err); ok &&
		strings.Contains(mysqlGeometryParseError.Error(), mysqlGeometryLinearRingNotClosedError) {
		return ErrorUnsupportedDatatype, ErrorInfo{
			Source: ErrorSourceMySQL,
			Code:   "UNSUPPORTED_GEOMETRY_LINEAR_RING_NOT_CLOSED",
		}
	}

	if postgresPrimaryKeyModifiedError, ok := errors.AsType[*exceptions.PrimaryKeyModifiedError](err); ok {
		return ErrorUnsupportedSchemaChange, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  postgresPrimaryKeyModifiedError.TableName,
				ErrorAttributeKeyColumn: postgresPrimaryKeyModifiedError.ColumnName,
			},
		}
	}

	if postgresReplicaIdentityIndexError, ok := errors.AsType[*exceptions.ReplicaIdentityIndexError](err); ok {
		return ErrorUnsupportedSchemaChange, ErrorInfo{
			Source: ErrorSourcePostgres,
			Code:   "UNSUPPORTED_SCHEMA_CHANGE",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable:  postgresReplicaIdentityIndexError.Table,
				ErrorAttributeKeyColumn: "n/a",
			},
		}
	}

	if mongoInvalidIdValueError, ok := errors.AsType[*exceptions.MongoInvalidIdValueError](err); ok {
		return ErrorNotifyInvalidSortKey, ErrorInfo{
			Source: ErrorSourceMongoDB,
			Code:   "INVALID_SORT_KEY",
			AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
				ErrorAttributeKeyTable: mongoInvalidIdValueError.Table,
			},
		}
	}

	return ErrorOther, ErrorInfo{
		Source: ErrorSourceOther,
		Code:   "UNKNOWN",
	}
}
