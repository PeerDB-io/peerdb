package connclickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestProjectionTypeKey(t *testing.T) {
	testCases := []struct {
		chType   string
		expected string
	}{
		// canonical spellings are fixed points
		{"DateTime64(6)", "DateTime64(6)"},
		{"Nullable(DateTime64(6))", "DateTime64(6)"},
		{"Date32", "Date32"},
		{"Time64(6)", "Time64(6)"},
		{"JSON", "JSON"},
		{"Array(DateTime64(6))", "Array(DateTime64(6))"},
		// free spellings collapse onto the tailored cases
		{"DateTime64(3)", "DateTime64(6)"},
		{"DateTime64(9, 'UTC')", "DateTime64(6)"},
		{"DateTime", "DateTime64(6)"},
		{"Nullable(DateTime('Europe/Madrid'))", "DateTime64(6)"},
		{"LowCardinality(Nullable(DateTime))", "DateTime64(6)"},
		{"Date", "Date32"},
		{"Nullable(Date)", "Date32"},
		{"Time", "Time64(6)"},
		{"Time64(3)", "Time64(6)"},
		{"JSON(max_dynamic_paths=16)", "JSON"},
		{"Nullable(JSON)", "JSON"},
		{"Array(DateTime64(3))", "Array(DateTime64(6))"},
		{"Array(Nullable(DateTime))", "Array(DateTime64(6))"},
		{" Nullable( DateTime64(3) ) ", "DateTime64(6)"},
		// anything else is left to the generic JSONExtract with the declared type
		{"String", "String"},
		{"Nullable(String)", "Nullable(String)"},
		{"LowCardinality(String)", "LowCardinality(String)"},
		{"Int64", "Int64"},
		{"Decimal(38, 9)", "Decimal(38, 9)"},
		{"Array(String)", "Array(String)"},
		{"Array(Int64)", "Array(Int64)"},
		{"Map(String, DateTime64(3))", "Map(String, DateTime64(3))"},
		{"Tuple(DateTime64(3), String)", "Tuple(DateTime64(3), String)"},
	}

	for _, tc := range testCases {
		t.Run(tc.chType, func(t *testing.T) {
			require.Equal(t, tc.expected, projectionTypeKey(tc.chType))
		})
	}
}

func TestExtendedTimeToDateTime(t *testing.T) {
	ctx := context.Background()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	ch, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	if err := ch.Ping(ctx); err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer ch.Close()

	testCases := []struct {
		name     string
		duration time.Duration
	}{
		{"zero", 0},
		{"normal", 1*time.Hour + 2*time.Minute + 3*time.Second + 123456*time.Microsecond},
		{"extended_hours", 838*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
		{"negative", -1 * time.Minute},
		{"negative_extended", -123*time.Hour - 45*time.Minute - 7*time.Second - 899999*time.Microsecond},
		{"precision_test", 18*time.Hour + 18*time.Minute + 8*time.Second + 511787*time.Microsecond},
		{"trailing_zeros_test", 18*time.Hour + 18*time.Minute + 8*time.Second + 500000*time.Microsecond},
	}

	for _, time64Supported := range []bool{false, true} {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/time64Supported=%v", tc.name, time64Supported), func(t *testing.T) {
				projectionExpr := extendedTimeToDateTime(
					fmt.Sprintf("'%s'", types.FormatExtendedTimeDuration(tc.duration)), time64Supported)

				var result time.Time
				require.NoError(t, ch.QueryRow(ctx, "SELECT "+projectionExpr).Scan(&result))
				expected := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(tc.duration)
				require.Equal(t, expected.UnixMicro(), result.UnixMicro())
			})
		}
		t.Run(fmt.Sprintf("%s/time64Supported=%v", "null", time64Supported), func(t *testing.T) {
			var result *time.Time
			require.NoError(t, ch.QueryRow(ctx, "SELECT "+extendedTimeToDateTime("null", time64Supported)).Scan(&result))
			require.Nil(t, result)
		})
	}
}
