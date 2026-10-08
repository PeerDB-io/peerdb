package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/cmd"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/internal/benchfixtures"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// TestSerializationBacklog is opt-in: pause CDC after slot creation, commit a
// large deterministic backlog, measure source slot acknowledgements after S3 sync.
// Run against separately built baseline/candidate workers with identical settings.
func TestSerializationBacklog(t *testing.T) {
	if os.Getenv("PEERDB_BENCH_BACKLOG") != "1" {
		t.Skip("set PEERDB_BENCH_BACKLOG=1 with local Tilt dependencies and one worker")
	}
	require.NotEmpty(t, internal.PeerDBDeploymentUID(), "use a dedicated benchmark task queue")
	rows := 2000000
	if value := os.Getenv("PEERDB_BENCH_ROWS"); value != "" {
		var err error
		rows, err = strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, rows)
	}
	s := SetupClickHouseSuite(t, false, func(t *testing.T) (*PostgresSource, string, error) {
		t.Helper()
		suffix := "ser_" + strings.ToLower(common.RandomString(8))
		src, err := SetupPostgres(t, suffix)
		return src, suffix, err
	})(t)
	defer s.Teardown(context.Background())
	ch, err := connclickhouse.Connect(t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(t, err)
	defer ch.Close()
	defer func() {
		require.NoError(t, ch.Exec(context.Background(), "DROP DATABASE IF EXISTS e2e_test_"+s.suffix))
	}()
	src := s.source.(*PostgresSource)
	table := s.attachSchemaSuffix("backlog")
	columns, values := benchfixtures.Postgres()
	require.NoError(t, src.Exec(t.Context(), "CREATE TABLE "+table+" ("+strings.Join(columns, ",")+")"))
	gen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("serbacklog"),
		TableNameMapping: map[string]string{table: "backlog"},
		Destination:      s.Peer().Name,
	}
	cfg := gen.GenerateFlowConnectionConfigs(s)
	cfg.MaxBatchSize = 100000
	cfg.IdleTimeoutSeconds = 1
	cfg.DoInitialSnapshot = false
	tc := NewTemporalClient(t)
	defer tc.Close()
	handler := cmd.NewFlowRequestHandler(t.Context(), tc, s.catalog, internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue))
	response, createErr := handler.CreateCDCFlow(t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: cfg})
	require.NoError(t, createErr)
	run := WorkflowRun{WorkflowRun: tc.GetWorkflow(t.Context(), response.WorkflowId, ""), c: tc}
	defer func() { run.Cancel(context.Background()); RequireEnvCanceled(t, run) }()
	SetupCDCFlowStatusQuery(t, run, cfg)
	SignalWorkflow(t.Context(), run, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, run, time.Minute, "paused before loading backlog", func() bool {
		return run.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})
	// Bounded transactions are representative of CDC batches. All commit before
	// resuming, keeping database data generation outside the measurement.
	for start := 1; start <= rows; start += 10000 {
		require.NoError(t, src.Exec(t.Context(), fmt.Sprintf(
			"INSERT INTO %s SELECT %s FROM generate_series(%d,%d) g",
			table, strings.Join(values, ","), start, min(rows, start+9999),
		)))
	}
	var backlogBytes int64
	require.NoError(t, src.Conn().QueryRow(t.Context(),
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name=$1`,
		connpostgres.GetDefaultSlotName(cfg.FlowJobName),
	).Scan(&backlogBytes))
	require.Positive(t, backlogBytes)
	// Reconstruct the fetched_event_size histogram's INSERT wire size: 25
	// XLogData bytes, 8 INSERT/tuple header bytes, and 5 bytes per non-null
	// column plus its PostgreSQL text representation. Sample across the table.
	names := benchfixtures.Names()
	sizeTerms := []string{strconv.Itoa(33 + 5*len(names))}
	for _, name := range names {
		sizeTerms = append(sizeTerms, "octet_length("+name+"::text)")
	}
	var eventBytes float64
	require.NoError(t, src.Conn().QueryRow(t.Context(),
		"SELECT avg("+strings.Join(sizeTerms, "+")+")::float8 FROM "+table+" TABLESAMPLE SYSTEM (1)",
	).Scan(&eventBytes))
	t.Logf("BACKLOG_EVENT_SIZE sampled_mean_wire_bytes=%.2f", eventBytes)
	t.Logf("BACKLOG_READY rows=%d columns=%d wal_bytes=%d flow=%s", rows, len(names), backlogBytes, cfg.FlowJobName)
	start := time.Now()
	SignalWorkflow(t.Context(), run, model.FlowSignal, model.NoopSignal)
	// Use with benchmark-only workers whose normalization returns immediately.
	// Match durable catalog batches to the actual source slot acknowledgement.
	batchRows := int(cfg.MaxBatchSize)
	require.Zero(t, rows%batchRows)
	require.GreaterOrEqual(t, rows, 3*batchRows)
	var firstAck time.Time
	var firstRows, measuredRows int64
	var measured time.Duration
	var previous int64
	deadline := time.Now().Add(10 * time.Minute)
	for {
		var confirmed, acknowledged int64
		require.NoError(t, src.Conn().QueryRow(t.Context(),
			`SELECT (confirmed_flush_lsn-'0/0'::pg_lsn)::bigint FROM pg_replication_slots WHERE slot_name=$1`,
			connpostgres.GetDefaultSlotName(cfg.FlowJobName),
		).Scan(&confirmed))
		require.NoError(t, s.catalog.QueryRow(t.Context(),
			`SELECT coalesce(sum(rows_in_batch),0)::bigint FROM (
			 SELECT batch_id,max(rows_in_batch) AS rows_in_batch,max(batch_end_lsn) AS batch_end_lsn
			 FROM peerdb_stats.cdc_batches WHERE flow_name=$1 AND sync_time IS NOT NULL
			 GROUP BY batch_id) batches WHERE batch_end_lsn <= $2`,
			cfg.FlowJobName, confirmed,
		).Scan(&acknowledged))
		now := time.Now()
		if acknowledged != previous {
			t.Logf("SLOT_PROGRESS rows=%d seconds=%.6f confirmed_lsn=%d", acknowledged, now.Sub(start).Seconds(), confirmed)
			previous = acknowledged
		}
		if firstAck.IsZero() && acknowledged >= int64(batchRows) {
			firstAck = now
			firstRows = acknowledged
		}
		if measured == 0 && acknowledged >= int64(rows-batchRows) {
			require.Less(t, acknowledged, int64(rows), "polling missed the window before the final batch")
			measured = now.Sub(firstAck)
			// The sync interval can cut a batch short even while backlogged.
			// Use actual acknowledged rows, excluding the final batch's delay.
			measuredRows = acknowledged - firstRows
		}
		if acknowledged == int64(rows) {
			break
		}
		require.LessOrEqual(t, acknowledged, int64(rows))
		require.True(t, now.Before(deadline), "slot checkpoint timeout")
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	require.Positive(t, measured)
	require.Positive(t, measuredRows)
	t.Logf("BACKLOG_SLOT_RESULT rows=%d measured_rows=%d batch_rows=%d seconds=%.6f rows_per_second=%.2f wal_bytes=%d",
		rows, measuredRows, batchRows, measured.Seconds(), float64(measuredRows)/measured.Seconds(), backlogBytes)
}
