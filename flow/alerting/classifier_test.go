package alerting

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestPostgresDNSErrorShouldBeConnectivity(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://non-existent.domain.name.here:123/db")
	require.NoError(t, err)
	_, err = pgx.ConnectConfig(t.Context(), config)
	errorClass, errInfo := GetErrorClass(t.Context(), err)
	assert.Equal(t, ErrorNotifyConnectivity, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourcePostgres,
		Code:   "UNKNOWN",
	}, errInfo, "Unexpected error info")
}

func TestOtherDNSErrorsShouldBeConnectivity(t *testing.T) {
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

func TestClickHouseAccessEntityNotFoundErrorShouldBeRecoverable(t *testing.T) {
	// Simulate a ClickHouse access entity not found error
	err := &clickhouse.Exception{
		Code:    492,
		Message: "ID(a14c2a1c-edcd-5fcb-73be-bd04e09fccb7) not found in user directories",
	}
	errorClass, errInfo := GetErrorClass(t.Context(), exceptions.NewQRepSyncError(fmt.Errorf("error in WAL: %w", err), "", ""))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
	assert.Equal(t, ErrorInfo{
		Source: ErrorSourceClickHouse,
		Code:   "492",
	}, errInfo, "Unexpected error info")
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

func TestPostgresQueryCancelledErrorShouldBeRecoverable(t *testing.T) {
	connectionString := internal.GetCatalogConnectionStringFromEnv(t.Context())
	config, err := pgx.ParseConfig(connectionString)
	require.NoError(t, err)
	config.Config.RuntimeParams["statement_timeout"] = "1500"
	connectConfig, err := pgx.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer connectConfig.Close(t.Context())
	_, err = connectConfig.Exec(t.Context(), "SELECT pg_sleep(2)")

	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("failed querying: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
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

func TestPostgresQueryCancelledDuringWalShouldBeRecoverable(t *testing.T) {
	// Simulate a query cancelled error during WAL
	err := exceptions.NewPostgresWalError(errors.New("testing query cancelled during WAL"), &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     pgerrcode.QueryCanceled,
		Message:  "canceling statement due to user request",
	})
	errorClass, errInfo := GetErrorClass(t.Context(), fmt.Errorf("error in WAL: %w", err))
	assert.Equal(t, ErrorRetryRecoverable, errorClass, "Unexpected error class")
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
