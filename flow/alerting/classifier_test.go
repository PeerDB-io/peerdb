package alerting

import (
	"fmt"
	"strconv"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestPostgresDNSErrorShouldBeConnectivity(t *testing.T) {
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

func TestClickHouseAvroDecimalErrorShouldBeUnsupportedDatatype(t *testing.T) {
	// Simulate an Avro decimal error
	errCodes := []int{int(chproto.ErrCannotParseUUID), int(chproto.ErrValueIsOutOfRangeOfDataType)}
	for _, code := range errCodes {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			exception := clickhouse.Exception{
				Code: int32(code),
				//nolint:lll
				Message: "Cannot parse type Decimal(76, 38), expected non-empty binary data with size equal to or less than 32, got 57: (at row 72423)....",
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
		//nolint:lll
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
