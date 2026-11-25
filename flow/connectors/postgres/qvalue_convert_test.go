package connpostgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestDecodeTimeTZText feeds TIMETZ values in the text form Postgres sends over
// logical replication. Postgres omits the fraction when it is zero, so its upper
// bound '24:00:00+00'::timetz arrives as "24:00:00+00", not "24:00:00.000000+00".
func TestDecodeTimeTZText(t *testing.T) {
	t.Parallel()

	p := &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{typeMap: pgtype.NewMap()},
		otelManager:       &otel_metrics.OtelManager{},
	}
	endOfDay := 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond

	for _, tc := range []struct {
		text string
		want time.Duration
	}{
		{"12:30:45+05", 7*time.Hour + 30*time.Minute + 45*time.Second},
		{"12:30:45.5-08:30", 21*time.Hour + 45*time.Second + 500*time.Millisecond},
		{"24:00:00+00", endOfDay},
		{"24:00:00.000000+00", endOfDay},
		{"24:00:00-02", endOfDay + 2*time.Hour},
	} {
		t.Run(tc.text, func(t *testing.T) {
			t.Parallel()
			qv, err := p.decodeColumnData(
				[]byte(tc.text), pgtype.TimetzOID, -1, pgtype.TextFormatCode, nil, shared.InternalVersion_Latest,
			)
			require.NoError(t, err)
			require.Equal(t, types.QValueTimeTZ{Val: tc.want}, qv)
		})
	}
}
