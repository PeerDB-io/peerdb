package peerflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func TestApplyResyncedTableMappings(t *testing.T) {
	unchanged := &protos.TableMapping{
		SourceTableIdentifier:      "public.customers",
		DestinationTableIdentifier: "analytics.customers",
	}
	replacement := &protos.TableMapping{
		SourceTableIdentifier:      "public.orders",
		DestinationTableIdentifier: "analytics.orders",
		Columns: []*protos.ColumnSetting{
			{SourceName: "customer_id", Ordering: 1},
			{SourceName: "id", Ordering: 2},
		},
	}
	options := &protos.SyncFlowOptions{
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "analytics.orders"},
			unchanged,
		},
		SrcTableIdNameMapping: map[uint32]string{
			10: "public.orders",
			20: "public.customers",
		},
	}
	result := &CDCFlowWorkflowResult{SyncFlowOptions: &protos.SyncFlowOptions{
		SrcTableIdNameMapping: map[uint32]string{30: "public.orders"},
	}}

	applyResyncedTableMappings(options, []*protos.TableMapping{replacement}, result)

	require.Equal(t, replacement.Columns, options.TableMappings[0].Columns)
	require.NotSame(t, replacement, options.TableMappings[0])
	require.Same(t, unchanged, options.TableMappings[1])
	require.Equal(t, map[uint32]string{
		20: "public.customers",
		30: "public.orders",
	}, options.SrcTableIdNameMapping)
}

type tableResyncTestEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	state    *cdc_state.CDCFlowWorkflowState
	original []*protos.TableMapping
	resync   *protos.TableMapping
	next     nextRun
	// flow status observed an hour after the resync was requested
	statusAfterResync protos.FlowStatus
}

// newTableResyncTestEnv runs handlePaused on a paused mirror with two tables, and signals a resync of
// one of them the same way the FlowStateChange API does.
func newTableResyncTestEnv(t *testing.T) *tableResyncTestEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	te := &tableResyncTestEnv{
		env: suite.NewTestWorkflowEnvironment(),
		original: []*protos.TableMapping{
			{SourceTableIdentifier: "public.orders", DestinationTableIdentifier: "orders"},
			{SourceTableIdentifier: "public.customers", DestinationTableIdentifier: "customers"},
		},
		resync: &protos.TableMapping{
			SourceTableIdentifier:      "public.orders",
			DestinationTableIdentifier: "orders",
			PartitionByExpr:            "toYYYYMM(created_at)",
		},
	}
	tableMappings := make([]*protos.TableMapping, 0, len(te.original))
	for _, tm := range te.original {
		tableMappings = append(tableMappings, proto.CloneOf(tm))
	}
	te.state = &cdc_state.CDCFlowWorkflowState{
		ActiveSignal:      model.PauseSignal,
		CurrentFlowStatus: protos.FlowStatus_STATUS_PAUSED,
		SyncFlowOptions: &protos.SyncFlowOptions{
			TableMappings:         tableMappings,
			SrcTableIdNameMapping: map[uint32]string{10: "public.orders", 20: "public.customers"},
		},
	}

	te.env.RegisterActivity(&activities.FlowableActivity{})
	te.env.RegisterWorkflow(CDCFlowWorkflow)
	te.env.OnActivity(flowable.Alert, mock.Anything, mock.Anything).Return(nil)
	te.env.OnActivity(flowable.UpdateCDCConfigInCatalogActivity, mock.Anything, mock.Anything).Return(nil)

	te.env.RegisterDelayedCallback(func() {
		te.env.SignalWorkflow(model.CDCDynamicPropertiesSignal.Name, &protos.CDCFlowConfigUpdate{
			ResyncTables: []*protos.TableMapping{te.resync},
		})
	}, time.Second)
	te.env.RegisterDelayedCallback(func() {
		encoded, err := te.env.QueryWorkflow(shared.CDCFlowStateQuery)
		require.NoError(t, err)
		var state cdc_state.CDCFlowWorkflowState
		require.NoError(t, encoded.Get(&state))
		te.statusAfterResync = state.CurrentFlowStatus
		te.env.SignalWorkflow(model.FlowSignal.Name, model.TerminateSignal)
	}, time.Hour)
	return te
}

func (te *tableResyncTestEnv) run(t *testing.T) {
	t.Helper()
	cfg := &protos.FlowConnectionConfigsCore{FlowJobName: "table_resync_test", TableMappings: te.original}
	te.env.ExecuteWorkflow(func(ctx workflow.Context) error {
		if err := workflow.SetQueryHandler(ctx, shared.CDCFlowStateQuery, func() (cdc_state.CDCFlowWorkflowState, error) {
			return *te.state, nil
		}); err != nil {
			return err
		}
		var err error
		te.next, err = handlePaused(ctx, workflow.GetLogger(ctx), cfg, te.state,
			model.FlowSignal.GetSignalChannel(ctx), model.FlowSignalStateChange.GetSignalChannel(ctx))
		return err
	})
	require.True(t, te.env.IsWorkflowCompleted())
	require.NoError(t, te.env.GetWorkflowError())
}

// requireStayedPaused checks that a failed resync kept the mirror paused with the old mappings
// until it was terminated, instead of resuming CDC.
func (te *tableResyncTestEnv) requireStayedPaused(t *testing.T) {
	t.Helper()
	require.Equal(t, nextRunDrop, te.next, "mirror must not resume after a failed resync")
	require.Equal(t, protos.FlowStatus_STATUS_PAUSED, te.statusAfterResync)
	require.Len(t, te.state.SyncFlowOptions.TableMappings, len(te.original))
	for i, tm := range te.original {
		require.True(t, proto.Equal(tm, te.state.SyncFlowOptions.TableMappings[i]),
			"table mappings must not change after a failed resync")
	}
	require.Equal(t, map[uint32]string{10: "public.orders", 20: "public.customers"},
		te.state.SyncFlowOptions.SrcTableIdNameMapping)
	te.env.AssertActivityCalled(t, "Alert", mock.Anything, mock.Anything)
}

func TestTableResyncSucceeded(t *testing.T) {
	te := newTableResyncTestEnv(t)
	te.env.OnWorkflow(CDCFlowWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(
		&CDCFlowWorkflowResult{SyncFlowOptions: &protos.SyncFlowOptions{
			SrcTableIdNameMapping: map[uint32]string{30: "public.orders"},
		}}, nil)
	te.env.OnActivity(flowable.SkipPendingNormalizeForTables, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	te.env.OnActivity(flowable.RemoveTablesFromRawTable, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	te.run(t)

	require.Equal(t, nextRunCDC, te.next)
	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, te.state.CurrentFlowStatus)
	require.True(t, proto.Equal(te.resync, te.state.SyncFlowOptions.TableMappings[0]))
	require.True(t, proto.Equal(te.original[1], te.state.SyncFlowOptions.TableMappings[1]))
	require.Equal(t, map[uint32]string{20: "public.customers", 30: "public.orders"},
		te.state.SyncFlowOptions.SrcTableIdNameMapping)
	te.env.AssertActivityNotCalled(t, "Alert", mock.Anything, mock.Anything)
}

// A child failure includes a failure in the middle of RenameTables, where some tables are already swapped.
func TestTableResyncChildFailureStaysPaused(t *testing.T) {
	te := newTableResyncTestEnv(t)
	te.env.OnWorkflow(CDCFlowWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(
		nil, temporal.NewNonRetryableApplicationError("rename tables failed", "test", nil))

	te.run(t)

	te.requireStayedPaused(t)
	te.env.AssertActivityNotCalled(t, "SkipPendingNormalizeForTables", mock.Anything, mock.Anything, mock.Anything)
	te.env.AssertActivityNotCalled(t, "RemoveTablesFromRawTable", mock.Anything, mock.Anything, mock.Anything)
}

func TestTableResyncCleanupFailureStaysPaused(t *testing.T) {
	te := newTableResyncTestEnv(t)
	te.env.OnWorkflow(CDCFlowWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(
		&CDCFlowWorkflowResult{SyncFlowOptions: &protos.SyncFlowOptions{
			SrcTableIdNameMapping: map[uint32]string{30: "public.orders"},
		}}, nil)
	te.env.OnActivity(flowable.SkipPendingNormalizeForTables, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	te.env.OnActivity(flowable.RemoveTablesFromRawTable, mock.Anything, mock.Anything, mock.Anything).Return(
		temporal.NewNonRetryableApplicationError("raw table cleanup failed", "test", nil))

	te.run(t)

	te.requireStayedPaused(t)
}
