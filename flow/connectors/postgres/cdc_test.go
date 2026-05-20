package connpostgres

import (
	"testing"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

func newTestCDCSource(t *testing.T) *PostgresCDCSource {
	t.Helper()
	return &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{},
		otelManager:       &otel_metrics.OtelManager{},
	}
}

func TestProcessMessageInvalidMessage(t *testing.T) {
	t.Parallel()

	p := newTestCDCSource(t)
	batch := model.NewCDCStream[model.RecordItems](0)

	xld := pglogrepl.XLogData{
		WALStart:     pglogrepl.LSN(0x01),
		ServerWALEnd: pglogrepl.LSN(0x02),
		// 'S' is a v2 Stream Start tag — not handled by pglogrepl.Parse (v1)
		WALData: []byte{'S', 0, 1, 0, 1 /*arbitrary bytes*/},
	}

	rec, err := processMessage(t.Context(), p, batch, xld, xld.WALStart, qProcessor{})
	require.Nil(t, rec)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error parsing logical message (msgType=\"S\", walStart=0/1)")
}
