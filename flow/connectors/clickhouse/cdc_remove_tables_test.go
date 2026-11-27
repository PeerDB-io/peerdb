package connclickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// probeRows is the result of a "SELECT 1 ... LIMIT 1" probe: one row or none.
type probeRows struct {
	driver.Rows
	hasRow bool
}

func (r *probeRows) Next() bool {
	hasRow := r.hasRow
	r.hasRow = false
	return hasRow
}
func (*probeRows) Close() error { return nil }
func (*probeRows) Err() error   { return nil }

// rawDeleteConn records the DELETEs it receives and answers each probe through remaining,
// which reports whether the probed condition still matches rows.
type rawDeleteConn struct {
	driver.Conn
	remaining func(probe string, probeCount int) bool
	probeErr  error
	deletes   []string
	probes    []string
	probedAt  []time.Duration
	start     time.Time
}

func (c *rawDeleteConn) Exec(_ context.Context, query string, _ ...any) error {
	c.deletes = append(c.deletes, query)
	return nil
}

func (c *rawDeleteConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.probes = append(c.probes, query)
	c.probedAt = append(c.probedAt, time.Since(c.start))
	if c.probeErr != nil {
		return nil, c.probeErr
	}
	return &probeRows{hasRow: c.remaining(query, len(c.probes))}, nil
}

func newRawDeleteConnector(conn *rawDeleteConn, version *chproto.Version) *ClickHouseConnector {
	conn.start = time.Now()
	return &ClickHouseConnector{
		database:  conn,
		logger:    log.NewStructuredLogger(slog.New(slog.DiscardHandler)),
		Config:    &protos.ClickhouseConfig{},
		chVersion: version,
	}
}

func removeTablesInput(tables ...string) *protos.RemoveTablesFromRawTableInput {
	return &protos.RemoveTablesFromRawTableInput{
		FlowJobName:           "test_flow",
		DestinationTableNames: tables,
		NormalizeBatchId:      1,
		SyncBatchId:           2,
	}
}

const rawDeleteCondition = "_peerdb_destination_table_name IN ('t1') AND _peerdb_batch_id > 1 AND _peerdb_batch_id <= 2"

func TestRemoveTableEntriesFromRawTableReturnsWhenNoRowsRemain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &rawDeleteConn{remaining: func(string, int) bool { return false }}
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 25, Minor: 8})

		require.NoError(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput("t1")))

		require.Equal(t, []string{
			"DELETE FROM _peerdb_raw_test_flow WHERE " + rawDeleteCondition + " SETTINGS lightweight_deletes_sync=0",
		}, conn.deletes)
		require.Equal(t, []string{"SELECT 1 FROM _peerdb_raw_test_flow WHERE " + rawDeleteCondition + " LIMIT 1"}, conn.probes)
		require.Equal(t, []time.Duration{5 * time.Second}, conn.probedAt, "first check comes after the first interval")
	})
}

func TestRemoveTableEntriesFromRawTablePollsWithIncreasingIntervals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// rows remain for the first five checks
		conn := &rawDeleteConn{remaining: func(_ string, probeCount int) bool { return probeCount <= 5 }}
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 25, Minor: 8})

		require.NoError(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput("t1")))

		require.Len(t, conn.deletes, 1, "the DELETE must not be reissued while waiting")
		require.Equal(t, []time.Duration{
			5 * time.Second,
			35 * time.Second,
			95 * time.Second,
			215 * time.Second,
			335 * time.Second,
			455 * time.Second,
		}, conn.probedAt)
	})
}

func TestRemoveTableEntriesFromRawTableFinishesEachChunkBeforeTheNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tables := make([]string, 150)
		for i := range tables {
			tables[i] = fmt.Sprintf("t%03d", i)
		}
		// the first chunk (t000..t099) needs two waits, the second (t100..t149) is finished on its first check
		conn := &rawDeleteConn{}
		conn.remaining = func(probe string, probeCount int) bool {
			require.Len(t, conn.deletes, 1+strings.Count(probe, "'t100'"), "a chunk's DELETE must wait for the previous chunk")
			return strings.Contains(probe, "'t000'") && probeCount <= 2
		}
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 25, Minor: 8})

		require.NoError(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput(tables...)))

		require.Len(t, conn.deletes, 2)
		// the second chunk's polling starts over from the first interval
		require.Equal(t, []time.Duration{5 * time.Second, 35 * time.Second, 95 * time.Second, 100 * time.Second}, conn.probedAt)
		require.Contains(t, conn.probes[3], "'t100'")
	})
}

func TestRemoveTableEntriesFromRawTableStopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &rawDeleteConn{remaining: func(string, int) bool { return true }}
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 25, Minor: 8})

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(45*time.Second, cancel)

		err := c.RemoveTableEntriesFromRawTable(ctx, removeTablesInput("t1"))

		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 45*time.Second, time.Since(conn.start))
		require.Len(t, conn.probes, 2)
	})
}

func TestRemoveTableEntriesFromRawTableReturnsProbeError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probeErr := errors.New("probe failed")
		conn := &rawDeleteConn{probeErr: probeErr}
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 25, Minor: 8})

		require.ErrorIs(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput("t1")), probeErr)
	})
}

func TestRemoveTableEntriesFromRawTableOmitsSettingOnOldClickHouse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &rawDeleteConn{remaining: func(string, int) bool { return false }}
		// lightweight_deletes_sync was added in ClickHouse 24.4
		c := newRawDeleteConnector(conn, &chproto.Version{Major: 24, Minor: 3})

		require.NoError(t, c.RemoveTableEntriesFromRawTable(t.Context(), removeTablesInput("t1")))
		require.Equal(t, []string{"DELETE FROM _peerdb_raw_test_flow WHERE " + rawDeleteCondition}, conn.deletes)
	})
}
