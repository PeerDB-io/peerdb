package connclickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestExtendedTimeToDateTime(t *testing.T) {
	ctx := context.Background()
	ch, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"localhost:9000"}})
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
