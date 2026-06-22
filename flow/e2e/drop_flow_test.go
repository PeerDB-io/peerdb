package e2e

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// TestDropRunningFlow verifies that sending a TERMINATING signal to a running CDC pipe
// causes the flow to be cleanly torn down and removed from the catalog.
func (s APITestSuite) TestDropRunningFlow() {
	tableName := "drop_running"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "drop_running_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitFor(s.t, env, 3*time.Minute, "wait for flow to be running", func() bool {
		return env.GetFlowStatus(s.t) == protos.FlowStatus_STATUS_RUNNING
	})

	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	require.NoError(s.t, err)

	s.waitForFlowDropped(env, flowConnConfig.FlowJobName)
}

// TestTerminateDuringResyncDropFlow creates a pipe, resyncs it, then sends a TERMINATING
// signal while the DropFlowWorkflow is blocked dropping the replication slot. An idle
// open transaction on the source keeps the slot active (pg_drop_replication_slot blocks
// while the slot's xmin cannot advance). Once the TERMINATING signal is queued, the
// transaction is committed so the drop activity can finish — but the DropFlowWorkflow's
// signal goroutine has already set TerminateSignal, so it does NOT ContinueAsNew back
// to CDCFlowWorkflow and the pipe is fully torn down.
func (s APITestSuite) TestTerminateDuringResyncDropFlow() {
	if _, ok := s.source.(*PostgresSource); !ok {
		s.t.Skip("only testing with PostgreSQL source")
	}

	// Open a separate raw PG connection and start a transaction that will block
	// pg_drop_replication_slot during the resync drop flow.
	pgCfg := internal.GetAncillaryPostgresConfigFromEnv()
	blockConn, err := pgx.Connect(s.t.Context(), internal.GetPGConnectionString(pgCfg, ""))
	require.NoError(s.t, err)
	defer blockConn.Close(s.t.Context())

	blockTx, err := blockConn.Begin(s.t.Context())
	require.NoError(s.t, err)
	// Acquire a transaction ID; keeping this transaction open prevents the replication
	// slot's xmin from advancing, causing pg_drop_replication_slot to block.
	var xid uint32
	require.NoError(s.t, blockTx.QueryRow(s.t.Context(), "SELECT txid_current()").Scan(&xid))
	s.t.Logf("blocking transaction XID: %d", xid)

	tableName := "terminate_resync"
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id int primary key, val text)", AttachSchema(s, tableName))))
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, val) values (1,'first')", AttachSchema(s, tableName))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      "terminate_resync_" + s.suffix,
		TableNameMapping: map[string]string{AttachSchema(s, tableName): tableName},
		Destination:      s.ch.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	response, err := s.CreateCDCFlow(s.t.Context(), &protos.CreateCDCFlowRequest{ConnectionConfigs: flowConnConfig})
	require.NoError(s.t, err)
	require.NotNil(s.t, response)

	tc := NewTemporalClient(s.t)
	env, err := GetPeerflow(s.t.Context(), s.catalog, tc, flowConnConfig.FlowJobName)
	require.NoError(s.t, err)

	// Wait for STATUS_SNAPSHOT without using SetupCDCFlowStatusQuery, which would
	// t.Fatal after 60s if the flow stays in STATUS_SNAPSHOT. We query the catalog
	// directly so "not found yet" errors are silently skipped.
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for initial snapshot to start", func() bool {
		status, err := internal.GetWorkflowStatus(s.t.Context(), s.catalog, env.GetID())
		return err == nil && status == protos.FlowStatus_STATUS_SNAPSHOT
	})

	// Trigger resync: CDC flow ContinueAsNew → DropFlowWorkflow(Resync=true).
	// The drop flow will block on pg_drop_replication_slot because of the open transaction.
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_RESYNC,
	})
	require.NoError(s.t, err)

	// Wait until DropFlowWorkflow has started (catalog status = RESYNC).
	// Use direct catalog query so transient "not found" errors during ContinueAsNew
	// handoff are silently skipped rather than failing the test.
	EnvWaitFor(s.t, env, 3*time.Minute, "wait for resync drop flow to start", func() bool {
		status, err := internal.GetWorkflowStatus(s.t.Context(), s.catalog, env.GetID())
		return err == nil && status == protos.FlowStatus_STATUS_RESYNC
	})

	// While the drop flow is blocked on the slot drop, send TERMINATING.
	// The signal goroutine inside DropFlowWorkflow catches this and sets
	// activeSignal = TerminateSignal, preventing ContinueAsNew after drop completes.
	_, err = s.FlowStateChange(s.t.Context(), &protos.FlowStateChangeRequest{
		FlowJobName:        flowConnConfig.FlowJobName,
		RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING,
	})
	// Release the blocker — the drop activity can now finish.
	require.NoError(s.t, blockTx.Rollback(s.t.Context()))

	require.NoError(s.t, err)

	// The flow must be fully removed from the catalog, not restarted as a new CDC run.
	s.waitForFlowDropped(env, flowConnConfig.FlowJobName)
}
