package alerting

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	pErrors "github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestPostgresDNSErrorShouldBeConnectivity(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig("postgres://non-existent.domain.name.here:123/db")
	require.NoError(t, err)
	_, err = pgx.ConnectConfig(t.Context(), config)
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceNet,
		Code:   "net.DNSError",
	}, errInfo, "Unexpected error info")
}

func TestOtherDNSErrorsShouldBeConnectivity(t *testing.T) {
	t.Parallel()

	hostName := "non-existent.domain.name.here"
	_, err := net.Dial("tcp", hostName+":123")
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorSourceNet, errInfo.Source, "Unexpected error source")
	assert.Regexp(t, "^lookup "+hostName+"( on [\\w\\d\\.:]*)?: no such host$", errInfo.Code, "Unexpected error code")
}

func TestNeonConnectivityErrorShouldBeConnectivity(t *testing.T) {
	t.Skip("Not a good idea to run this test in CI as it goes to Neon, maybe we need a better mock")
	config, err := pgx.ParseConfig("postgres://random-endpoint-id-here.us-east-2.aws.neon.tech:5432/db?options=endpoint%3Dtest_endpoint")
	require.NoError(t, err)
	_, err = pgx.ConnectConfig(t.Context(), config)
	t.Logf("Error: %v", err)
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   "UNKNOWN",
	}, errInfo, "Unexpected error info")
}

func TestClickHouseAvroDecimalErrorShouldBeUnsupportedDatatype(t *testing.T) {
	// Simulate an Avro decimal error
	errCodes := []int{int(chproto.ErrCannotParseUUID), int(chproto.ErrValueIsOutOfRangeOfDataType)}
	for _, code := range errCodes {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			exception := clickhouse.Exception{
				Code: int32(code),
				// can't split across lines as regex will not match
				//nolint:lll
				Message: `Cannot parse type Decimal(76, 38), expected non-empty binary data with size equal to or less than 32, got 57: (at row 72423)....`,
			}
			errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to sync records: %w", &exception))
			assert.Equal(t, ErrorUnsupportedDatatype, errorClass, "Unexpected error class")
			assert.Equal(t, ErrorInfo{
				Source: ErrorSourceClickHouse,
				Code:   strconv.Itoa(code),
			}, errInfo, "Unexpected error info")
		})
	}
}

func TestClickHouseSelectFromDestinationDuringQrepAsMvError(t *testing.T) {
	// Simulate an Avro decimal error
	err := &clickhouse.Exception{
		Code: int32(chproto.ErrIllegalTypeOfArgument),
		Message: `Nested type Array(String) cannot be inside Nullable type: In scope SELECT
				col1, col2, col3 AS some_other_col, _peerdb_synced_at, _peerdb_is_deleted, _peerdb_version
				FROM    db_name_xyz.error_table_name_abc AS inp ARRAY JOIN JSONExtractArrayRaw(some_json_data) AS s ARRAY JOIN
				JSONExtractArrayRaw(JSONExtractRaw(s, 'more_data')) AS md SETTINGS final = 1`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to sync records: %w",
		exceptions.NewQRepSyncError(err, "error_table_name_abc", "db_name_xyz")))
	assert.Equal(t, ErrorNotifyMVOrView, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   strconv.Itoa(int(chproto.ErrIllegalTypeOfArgument)),
	}, errInfo, "Unexpected error info")
}

func TestPostgresWalRemovedErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a WAL removed error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "requested WAL segment 000000010001337F0000002E has already been removed",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestAuroraInternalWALErrorShouldBeRecoverable(t *testing.T) {
	// Simulate Aurora Internal WAL error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "Internal error encountered during logical decoding: 131",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestNeonProjectQuotaExceededErrorShouldBeConnectivity(t *testing.T) {
	// Simulate a Neon project quota exceeded error
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.InternalError,
		Message:  "Your account or project has exceeded the compute time quota. Upgrade your plan to increase limits.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewPeerCreateError(fmt.Errorf("failed to create connection: failed to connect to `<user, host>: server error: `: %w", err)))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresMemoryAllocErrorShouldBeSlotMemalloc(t *testing.T) {
	// Simulate a Postgres memory allocation error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "invalid memory alloc request size 1073741824",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorNotifyPostgresSlotMemalloc, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresPfreeInvalidPointerErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a Postgres pfree invalid pointer error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "pfree called with invalid pointer 0x400720764ed0 (header 0x0000400720825ae0) ",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresUnrecognizedSIMessageIDErrorShouldBeRecoverable(t *testing.T) {
	// Simulate shared invalidation message corruption error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     pgerrcode.InternalError,
			Message:  "unrecognized SI message ID: -60",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("ReceiveMessage failed: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestClickHouseAccessEntityNotFoundErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a ClickHouse access entity not found error
	for idx, msg := range []string{
		"ID(a14c2a1c-edcd-5fcb-73be-bd04e09fccb7) not found in user directories",
		// With backticks
		"ID(a14c2a1c-edcd-5fcb-73be-bd04e09fccb7) not found in `user directories`",
	} {
		t.Run(fmt.Sprintf("Test case %d", idx), func(t *testing.T) {
			err := &clickhouse.Exception{
				Code:    492,
				Message: msg,
			}
			errorClass, errInfo := GetErrorClass(t.Context(), exceptions.NewQRepSyncError(fmt.Errorf("error in WAL: %w", err), "", ""))
			assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
			assert.Equal(t, ErrorInfo{
				Source: ErrorSourceClickHouse,
				Code:   "492",
			}, errInfo, "Unexpected error info")
		})
	}
}

func TestClickHousePushingToViewShouldBeMvError(t *testing.T) {
	err := &clickhouse.Exception{
		Code: int32(chproto.ErrCannotConvertType),
		Message: `Conversion from AggregateFunction(argMax, DateTime64(9), DateTime64(9)) to
		AggregateFunction(argMax, Nullable(DateTime64(9)), DateTime64(9))
		is not supported: while converting source column created_at to destination column created_at:
		while pushing to view db_name.hello_mv`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), exceptions.NewNormalizationError(fmt.Errorf("error in WAL: %w", err)))
	assert.Equal(t, ErrorNotifyMVOrView, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "70",
	}, errInfo, "Unexpected error info")
}

func TestPostgresQueryCancelledErrorShouldBeNotifyConnectivity(t *testing.T) {
	t.Parallel()

	connectionString := internal.GetCatalogConnectionStringFromEnv(t.Context())
	config, err := pgx.ParseConfig(connectionString)
	require.NoError(t, err)
	config.Config.RuntimeParams["statement_timeout"] = "1500"
	connectConfig, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer connectConfig.Close(t.Context())
	_, err = connectConfig.Exec(t.Context(), "SELECT pg_sleep(2)")

	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed querying: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.QueryCanceled,
	}, errInfo, "Unexpected error info")
}

func TestClickHouseChaoticNormalizeErrorShouldBeNotifyMVNow(t *testing.T) {
	err := &clickhouse.Exception{
		Code: int32(chproto.ErrNoCommonType),
		Message: `There is no supertype for types String, Int64 because some of them are String/FixedString/Enum and some of them are not:
				JOIN INNER JOIN ... ON table_B.column_1 = table_A.column_2 cannot infer common type in ON section for keys.
				Left key __table1.column_2 type String. Right key __table2.column_1 type Int64`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewNormalizationError(fmt.Errorf(`Normalization Error: failed to normalize records:
		 error while inserting into normalized table table_A: %w`, err)))
	assert.Equal(t, ErrorNotifyMVOrView, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "386",
	}, errInfo, "Unexpected error info")
}

func TestPostgresPublicationDoesNotExistErrorShouldBePublicationMissing(t *testing.T) {
	// Simulate a publication does not exist error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.UndefinedObject,
			Message:  `publication "custom_pub" does not exist`,
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorNotifyPublicationMissing, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.UndefinedObject,
	}, errInfo, "Unexpected error info")
}

func TestPostgresSnapshotDoesNotExistErrorShouldBeInvalidSnapshot(t *testing.T) {
	// Simulate a snapshot does not exist error
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.UndefinedObject,
		Message:  `snapshot "custom_snap" does not exist`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to set snapshot: %w", err))
	assert.Equal(t, ErrorNotifyInvalidSnapshotIdentifier, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.UndefinedObject,
	}, errInfo, "Unexpected error info")
}

func TestPostgresInvalidValueForSynchronizedStandbySlots(t *testing.T) {
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.InvalidParameterValue,
		Message:  `"synchronized_standby_slots"`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to query for total rows: %w", err))
	assert.Equal(t, ErrorNotifyInvalidSynchronizedStandbySlots, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InvalidParameterValue,
	}, errInfo, "Unexpected error info")
}

func TestPostgresLogicalDecodingNotSupportedOnStandby(t *testing.T) {
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.FeatureNotSupported,
		Message:  "logical decoding cannot be used while in recovery",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error starting replication at startLsn - 11763874329649: %w", err))
	assert.Equal(t, ErrorNotifyLogicalDecodingStandbyNotSupported, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.FeatureNotSupported,
	}, errInfo)
}

func TestPostgresCreatingSlotOnReader(t *testing.T) {
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.InternalError,
		Message: `ERROR: Creating logical replication slot peerflow_slot_mirror_1cd7f87b__d143__4cea__a247__a2acc5f5b746
		is not supported on the Multi-AZ DB cluster reader node.
		Create the replication slot from the writer node instead. (SQLSTATE XX000)`,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("slot error: [slot] error creating replication slot: %w", err))
	assert.Equal(t, ErrNotifyPostgresCreatingSlotOnReader, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresStaleFileHandleErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a stale file handle error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  `could not stat file "pg_logical/snapshots/1B6-2A845058.snap": Stale file handle`,
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresReorderbufferSpillFileBadAddressErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a "could not read from reorderbuffer spill file: Bad address" error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "could not read from reorderbuffer spill file: Bad address",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresReorderbufferSpillFileBadFileDescriptorErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a "could not read from reorderbuffer spill file: Bad file descriptor" error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "could not read from reorderbuffer spill file: Bad file descriptor",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestUndefinedObjectWithoutPublicationErrorIsNotifyConnectivity(t *testing.T) {
	// Simulate an "undefined object" error without publication related message
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.UndefinedObject,
		Message:  "SomeErrorHere",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.UndefinedObject,
	}, errInfo, "Unexpected error info")
}

func TestPostgresQueryCancelledDuringWalShouldBeNotifyConnectivity(t *testing.T) {
	// Simulate a query cancelled error during WAL
	err := exceptions.NewPostgresWalError(errors.New("testing query cancelled during WAL"), &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgerrcode.QueryCanceled,
		Message:  "canceling statement due to user request",
	})
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.QueryCanceled,
	}, errInfo, "Unexpected error info")
}

func TestRandomErrorShouldBeOther(t *testing.T) {
	// Simulate a random error
	err := errors.New("some random error")
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorOther, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceOther,
		Code:   "UNKNOWN",
	}, errInfo, "Unexpected error info")
}

func TestPeerCreateTimeoutErrorShouldBeConnectivity(t *testing.T) {
	// Simulate a peer create timeout error, this is just a unit test, maybe we should try recreating this error in a more realistic way
	err := exceptions.NewPeerCreateError(context.DeadlineExceeded)
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in peer create: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceOther,
		Code:   "CONTEXT_DEADLINE_EXCEEDED",
	}, errInfo, "Unexpected error info")
}

func TestConnectionResetDuringPeerCreateShouldBeConnectivity(t *testing.T) {
	t.Parallel()

	err := exceptions.NewPeerCreateError(
		fmt.Errorf("failed to open connection to ClickHouse peer: failed to ping to ClickHouse peer: read: %w", syscall.ECONNRESET))
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed to recreate destination connector: %w", err))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceNet,
		Code:   syscall.ECONNRESET.Error(),
	}, errInfo, "Unexpected error info")
}

func TestConnectionResetFromSourceShouldBeIgnored(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to pull records: read tcp 10.0.0.1:5432: %w", syscall.ECONNRESET)
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorIgnoreConnTemporary, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceNet,
		Code:   syscall.ECONNRESET.Error(),
	}, errInfo, "Unexpected error info")
}

func TestPostgresCouldNotFindRecordWalErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a "could not find record while sending logically-decoded data" error
	err := &exceptions.PostgresWalError{
		Msg: &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     pgerrcode.InternalError,
			Message:  "could not find record while sending logically-decoded data: missing contrecord at 6410/14023FF0",
		},
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestNeonQuotaExceededErrorShouldBeConnectivity(t *testing.T) {
	// Simulate a Neon quota exceeded error
	err := &pgconn.PgError{
		Severity: "ERROR",
		Code:     pgerrcode.InternalError,
		Message:  "Your account or project has exceeded the compute time quota. Upgrade your plan to increase limits.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewPeerCreateError(fmt.Errorf("failed to create connection: failed to connect to `<user, host>: server error: `: %w", err)))
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   pgerrcode.InternalError,
	}, errInfo, "Unexpected error info")
}

func TestPostgresConnectionRefusedErrorShouldBeConnectivity(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig("postgres://localhost:1001/db")
	require.NoError(t, err)
	_, err = pgx.ConnectConfig(t.Context(), config)
	require.Error(t, err, "Expected connection refused error")
	t.Logf("Error: %v", err)
	for _, e := range []error{err, exceptions.NewPeerCreateError(err)} {
		t.Run(fmt.Sprintf("Testing error: %T", e), func(t *testing.T) {
			t.Parallel()

			errorClass, errInfo := GetErrorClass(t.Context(), err)
			assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
			assert.Equal(t, ErrorInfo{
				Source: ErrorSourceNet,
				Code:   "connect: connection refused",
			}, errInfo, "Unexpected error info")
		})
	}
}

func TestClickHouseUnknownTableShouldBeDestinationModified(t *testing.T) {
	// Simulate an unknown table error
	err := &clickhouse.Exception{
		Code:    60,
		Message: "Table abc does not exist.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewNormalizationError(fmt.Errorf("failed to normalize records: %w", err)))
	assert.Equal(t, ErrorNotifyDestinationModified, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "60",
	}, errInfo, "Unexpected error info")
}

func TestClickHouseUnkownTableWhilePushingToViewShouldBeNotifyMVNow(t *testing.T) {
	// Simulate an unknown table error while pushing to view
	err := &clickhouse.Exception{
		Code: 60,
		//nolint:lll
		Message: "Table abc does not exist. Maybe you meant abc2?: while executing 'FUNCTION func()': while pushing to view some_mv (some-uuid-here)",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewNormalizationError(fmt.Errorf("failed to normalize records: %w", err)))
	assert.Equal(t, ErrorNotifyMVOrView, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "60",
	}, errInfo, "Unexpected error info")
}

func TestNonClassifiedNormalizeErrorShouldBeNotifyMVNow(t *testing.T) {
	// Simulate an unclassified normalize error
	err := &clickhouse.Exception{
		Code:    207,
		Message: "JOIN  ANY LEFT JOIN ... ON a.id = b.b_id ambiguous identifier 'c_id'. In scope SELECT ...",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewNormalizationError(fmt.Errorf("failed to normalize records: %w", err)))
	assert.Equal(t, ErrorNotifyMVOrView, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "207",
	}, errInfo, "Unexpected error info")
}

func TestNonClassifiedNonNormalizeErrorShouldBeOtherWithSourceClickHouse(t *testing.T) {
	// Simulate an unclassified non-normalize error
	err := &clickhouse.Exception{
		Code:    -1,
		Message: "Some random exception",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("random exception: %w", err))
	assert.Equal(t, ErrorOther, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "-1",
	}, errInfo, "Unexpected error info")
}

func TestNumericTruncateOrOutOfRangeWarningShouldBeLossyConversion(t *testing.T) {
	for code, err := range map[string]error{
		"NUMERIC_TRUNCATED":    exceptions.NewNumericTruncatedError(errors.New("testing numeric truncated warning"), "tableA1", "columnB2"),
		"NUMERIC_OUT_OF_RANGE": exceptions.NewNumericOutOfRangeError(errors.New("testing numeric out of range warning"), "tableA1", "columnB2"),
	} {
		t.Run(code, func(t *testing.T) {
			errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("lossy conversion: %w", err))
			assert.Equal(t, ErrorLossyConversion, errorClass, "Unexpected error class")
			assert.Equal(t, ErrorInfo{
				Source: "typeConversion",
				Code:   code,
				AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
					ErrorAttributeKeyTable:  "tableA1",
					ErrorAttributeKeyColumn: "columnB2",
				},
			}, errInfo, "Unexpected error info")
		})
	}
}

func TestTemporalKnownErrorsShouldBeCorrectlyClassified(t *testing.T) {
	type classAndInfo struct {
		errorClass ErrorClass
		errInfo    ErrorInfo
	}
	for code, cinfo := range map[exceptions.ApplicationErrorType]classAndInfo{
		exceptions.ApplicationErrorTypeIrrecoverableSlotMissing: {
			errorClass: ErrorNotifyReplicationSlotMissing,
			errInfo: ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   exceptions.ApplicationErrorTypeIrrecoverableSlotMissing.String(),
			},
		},
		exceptions.ApplicationErrorTypeIrrecoverablePublicationMissing: {
			errorClass: ErrorNotifyPublicationMissing,
			errInfo: ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   exceptions.ApplicationErrorTypeIrrecoverablePublicationMissing.String(),
			},
		},
		exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot: {
			errorClass: ErrorNotifyInvalidSnapshotIdentifier,
			errInfo: ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   exceptions.ApplicationErrorTypeIrrecoverableInvalidSnapshot.String(),
			},
		},
		exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot: {
			errorClass: ErrorNotifyInvalidSnapshotIdentifier,
			errInfo: ErrorInfo{
				Source: ErrorSourcePostgres,
				Code:   exceptions.ApplicationErrorTypeIrrecoverableCouldNotImportSnapshot.String(),
			},
		},
	} {
		t.Run(code.String(), func(t *testing.T) {
			errorClass, errInfo := GetErrorClass(t.Context(), temporal.NewNonRetryableApplicationError(
				"irrecoverable error",
				code.String(),
				nil,
			))
			assert.Equal(t, cinfo.errorClass, errorClass, "Unexpected error class")
			assert.Equal(t, cinfo.errInfo, errInfo, "Unexpected error info")
		})
	}
}

func TestTemporalKnownIrrecoverableErrorTypesHaveCorrectClassification(t *testing.T) {
	for _, code := range exceptions.IrrecoverableApplicationErrorTypesList {
		t.Run(code, func(t *testing.T) {
			errorClass, errInfo := GetErrorClass(t.Context(), temporal.NewNonRetryableApplicationError("unknown", code, nil))
			assert.NotEqual(t, ErrorOther, errorClass, "Error class should not be other")
			assert.NotEqual(t, ErrorSourceTemporal, errInfo.Source)
		})
	}
}

func TestTemporalUnknownErrorShouldBeOther(t *testing.T) {
	errorClass, errInfo := GetErrorClass(t.Context(), temporal.NewNonRetryableApplicationError("irrecoverable error", "UNKNOWN_ERROR", nil))
	assert.Equal(t, ErrorOther, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceTemporal,
		Code:   "UNKNOWN_ERROR",
	}, errInfo, "Unexpected error info")
}

func TestMongoShutdownInProgressErrorShouldBeIgnored(t *testing.T) {
	// Simulate a MongoDB shutdown in progress error (quiesce mode)
	de := driver.Error{
		Code: 0,
		//nolint:lll
		Message: "connection pool for <host>:<port> was cleared because another operation failed with: (ShutdownInProgress) The server is in quiesce mode and will shut down",
	}
	err := mongo.CommandError{
		Message: de.Message,
		Code:    de.Code,
		Wrapped: de,
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("change stream error: %w", err))
	assert.Equal(t, ErrorIgnoreConnTemporary, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMongoDB,
		Code:   "0",
	}, errInfo, "Unexpected error info")
}

func TestMongoPoolErrorShouldBeRecoverable(t *testing.T) {
	//nolint:lll
	err := errors.New("change stream error: connection pool for abc.123.mongodb.net:27017 was cleared because another operation failed with: (InterruptedDueToReplStateChange) operation was interrupted")
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("change stream error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMongoDB,
		Code:   "POOL_CLEARED_ERROR(11602)",
	}, errInfo, "Unexpected error info")
}

func TestMongoCursorErrors(t *testing.T) {
	err := mongo.CommandError{
		Code:    6,
		Message: "(HostUnreachable) Error on remote shard test.mongodb.net:27017 :: caused by :: interrupted at shutdown",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("cursor error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMongoDB,
		Code:   "6",
	}, errInfo)

	err = mongo.CommandError{
		Code:    43,
		Message: "cursor id 1234567890 not found",
	}
	errorClass, errInfo = GetErrorClass(t.Context(), fmt.Errorf("cursor error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMongoDB,
		Code:   "43",
	}, errInfo)
}

func TestAuroraMySQLZeroDowntimePatchErrorShouldBeRecoverable(t *testing.T) {
	// Simulate Aurora MySQL Zero Downtime Patch error
	mysqlErr := &mysql.MyError{
		Code:    1105, // ER_UNKNOWN_ERROR
		State:   "HY000",
		Message: "The last transaction was aborted due to Zero Downtime Patch. Please retry.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("mysql error: %w", mysqlErr))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "1105",
	}, errInfo, "Unexpected error info")
}

func TestAuroraMySQLZeroDowntimeRestartErrorShouldBeRecoverable(t *testing.T) {
	// Simulate Aurora MySQL Zero Downtime Restart error
	mysqlErr := &mysql.MyError{
		Code:    1105, // ER_UNKNOWN_ERROR
		State:   "HY000",
		Message: "The last transaction was aborted due to Zero Downtime Restart. Please retry.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("mysql errors: %w", mysqlErr))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "1105",
	}, errInfo, "Unexpected error info")
}

func TestMySQLStreamingTLSHandshakeErrorShouldBeRecoverable(t *testing.T) {
	err := exceptions.NewMySQLStreamingError(
		tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"})
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("mysql error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "STREAMING_TRANSIENT_ERROR",
	}, errInfo, "Unexpected error info")

	err = exceptions.NewMySQLStreamingError(
		tls.RecordHeaderError{Msg: "unsupported SSLv2 handshake received"})
	errorClass, errInfo = GetErrorClass(t.Context(), fmt.Errorf("mysql error: %w", err))
	assert.Equal(t, ErrorOther, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "UNKNOWN",
	}, errInfo, "Unexpected error info")

	err = exceptions.NewMySQLStreamingError(context.DeadlineExceeded)
	errorClass, errInfo = GetErrorClass(t.Context(), fmt.Errorf("mysql error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "STREAMING_TRANSIENT_ERROR",
	}, errInfo, "Unexpected error info")

	tlsErr := tls.RecordHeaderError{Msg: "remote error: tls: error decoding message"}
	innerErr := pErrors.Wrapf(mysql.ErrBadConn, "io.ReadFull(header) failed. err %v", tlsErr)
	err = exceptions.NewMySQLStreamingError(pErrors.Errorf("failed to set @slave_gtid_strict_mode=1: %v", innerErr))
	errorClass, errInfo = GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorRetryRecoverable, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "STREAMING_TRANSIENT_ERROR",
	}, errInfo)
}

func TestClickHouseTooManyPartsWithTableName(t *testing.T) {
	err := &clickhouse.Exception{
		Code: int32(chproto.ErrTooManyParts),
		//nolint:lll
		Message: "Too many parts (3025 with average size of 65.51 MiB) in table 'ss_replica.posts_resync (db2b0f62-f577-4116-8b5d-e0f760a42bee)'. Merges are processing significantly slower than inserts",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewQRepSyncError(fmt.Errorf("QRepSync Error: %w", err), "", ""))
	assert.Equal(t, ErrorNotifyTooManyPartsError, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   strconv.Itoa(int(chproto.ErrTooManyParts)),
		AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
			ErrorAttributeKeyTable: "ss_replica.posts_resync (db2b0f62-f577-4116-8b5d-e0f760a42bee)",
		},
	}, errInfo)
}

func TestClickHouseTooManyPartsWithoutTableName(t *testing.T) {
	err := &clickhouse.Exception{
		Code: int32(chproto.ErrTooManyParts),
		//nolint:lll
		Message: "Too many partitions for single INSERT block (more than 9999). The limit is controlled by 'max_partitions_per_insert_block' setting.",
	}
	errorClass, errInfo := GetErrorClass(t.Context(),
		exceptions.NewQRepSyncError(fmt.Errorf("QRepSync Error: %w", err), "", ""))
	assert.Equal(t, ErrorNotifyTooManyPartsError, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   strconv.Itoa(int(chproto.ErrTooManyParts)),
	}, errInfo)
}

func TestClickHouseFailedToLoadAllDataPartsShouldNotifyUser(t *testing.T) {
	err := &clickhouse.Exception{
		Code:    int32(chproto.ErrUnfinished),
		Message: "Failed to load all data parts",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("clickhouse error: %w", err))
	assert.Equal(t, ErrorNotifyClickHouseError, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   strconv.Itoa(int(chproto.ErrUnfinished)),
	}, errInfo)
}

func TestClickHouseOtherUnfinishedShouldBeRecoverable(t *testing.T) {
	err := &clickhouse.Exception{
		Code:    int32(chproto.ErrUnfinished),
		Message: "some other unfinished error",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("clickhouse error: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   strconv.Itoa(int(chproto.ErrUnfinished)),
	}, errInfo)
}

func TestMySQLUnsupportedDDLShouldNotifyUser(t *testing.T) {
	err := exceptions.NewMySQLUnsupportedDDLError("test_db.test_table")
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("mysql error: %w", err))
	assert.Equal(t, ErrorNotifyBinlogRowMetadataInvalid, errorClass)
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceMySQL,
		Code:   "UNSUPPORTED_SCHEMA_CHANGE",
		AdditionalAttributes: map[AdditionalErrorAttributeKey]string{
			ErrorAttributeKeyTable: "test_db.test_table",
		},
	}, errInfo)
}
