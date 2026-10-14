package peerflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func drainTestEnvironment(t *testing.T, onStatus ...func(protos.FlowStatus)) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	updateStatus := func(_ context.Context, _ string, status protos.FlowStatus) (protos.FlowStatus, error) {
		for _, callback := range onStatus {
			callback(status)
		}
		return status, nil
	}
	env.RegisterActivityWithOptions(updateStatus, activity.RegisterOptions{Name: "updateFlowStatusInCatalogActivity"})
	env.OnActivity("updateFlowStatusInCatalogActivity", mock.Anything, mock.Anything, mock.Anything).Return(updateStatus)
	env.RegisterActivity(flowable.DrainFlow)
	return env
}

func assertDrainStatus(t *testing.T, env *testsuite.TestWorkflowEnvironment, status protos.FlowStatus) {
	t.Helper()
	value, err := env.QueryWorkflow(shared.CDCFlowStateQuery)
	require.NoError(t, err)
	var state cdc_state.CDCFlowWorkflowState
	require.NoError(t, value.Get(&state))
	require.Equal(t, status, state.CurrentFlowStatus)
}

func TestPauseDrainWaitsForNormalize(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary pause", true: "drain retries before paused"}[drain], func(t *testing.T) {
			env := drainTestEnvironment(t)
			if drain {
				env.OnActivity(flowable.DrainFlow, mock.Anything, mock.Anything).
					Return(errors.New("destination unavailable")).Once()
				env.OnActivity(flowable.DrainFlow, mock.Anything, mock.Anything).
					Return(nil).After(10 * time.Second).Once()
			}
			env.RegisterDelayedCallback(func() {
				expected := protos.FlowStatus_STATUS_PAUSED
				if drain {
					expected = protos.FlowStatus_STATUS_DRAINING
				}
				assertDrainStatus(t, env, expected)
			}, 5*time.Second)
			env.RegisterDelayedCallback(func() {
				assertDrainStatus(t, env, protos.FlowStatus_STATUS_PAUSED)
				env.SignalWorkflow(model.FlowSignal.Name, model.NoopSignal)
			}, 20*time.Second)
			initialStatus := protos.FlowStatus_STATUS_PAUSING
			if drain {
				initialStatus = protos.FlowStatus_STATUS_DRAINING
			}
			env.ExecuteWorkflow(CDCFlowWorkflow, &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
				&cdc_state.CDCFlowWorkflowState{
					ActiveSignal: model.PauseSignal, CurrentFlowStatus: initialStatus,
				})
			var continueErr *workflow.ContinueAsNewError
			require.ErrorAs(t, env.GetWorkflowError(), &continueErr)
			env.AssertExpectations(t)
			if !drain {
				env.AssertNotCalled(t, "DrainFlow", mock.Anything, mock.Anything)
			}
		})
	}
}

func TestPauseDrainFailureDoesNotReportPaused(t *testing.T) {
	env := drainTestEnvironment(t)
	env.OnActivity(flowable.DrainFlow, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError("cannot normalize", "test", nil)).Once()
	env.ExecuteWorkflow(CDCFlowWorkflow, &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
		&cdc_state.CDCFlowWorkflowState{
			ActiveSignal: model.PauseSignal, CurrentFlowStatus: protos.FlowStatus_STATUS_DRAINING,
		})
	require.ErrorContains(t, env.GetWorkflowError(), "cannot normalize")
	assertDrainStatus(t, env, protos.FlowStatus_STATUS_DRAINING)
}

func TestPauseDrainCanBeInterrupted(t *testing.T) {
	var statuses []protos.FlowStatus
	env := drainTestEnvironment(t, func(status protos.FlowStatus) { statuses = append(statuses, status) })
	env.OnActivity(flowable.DrainFlow, mock.Anything, mock.Anything).
		Return(nil).After(time.Hour)
	env.RegisterDelayedCallback(func() {
		assertDrainStatus(t, env, protos.FlowStatus_STATUS_DRAINING)
		env.SignalWorkflow(model.FlowSignal.Name, model.NoopSignal)
	}, time.Second)
	env.ExecuteWorkflow(CDCFlowWorkflow, &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
		&cdc_state.CDCFlowWorkflowState{
			ActiveSignal: model.PauseSignal, CurrentFlowStatus: protos.FlowStatus_STATUS_DRAINING,
		})
	var continueErr *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &continueErr)
	assertDrainStatus(t, env, protos.FlowStatus_STATUS_RUNNING)
	require.NotContains(t, statuses, protos.FlowStatus_STATUS_PAUSED)
}

func TestDrainPauseSignalStopsSyncAndSurvivesContinueAsNew(t *testing.T) {
	env := drainTestEnvironment(t)
	env.RegisterActivity(flowable.SyncFlow)
	env.OnActivity(flowable.SyncFlow, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).After(time.Hour)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(model.FlowSignalStateChange.Name, &protos.FlowStateChangeRequest{
			RequestedFlowState: protos.FlowStatus_STATUS_PAUSED, Drain: true,
		})
	}, time.Second)
	env.ExecuteWorkflow(func(ctx workflow.Context) (*cdc_state.CDCFlowWorkflowState, error) {
		state := &cdc_state.CDCFlowWorkflowState{CurrentFlowStatus: protos.FlowStatus_STATUS_RUNNING}
		next, err := startCDC(ctx, workflow.GetLogger(ctx), &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
			state, model.FlowSignal.GetSignalChannel(ctx), model.FlowSignalStateChange.GetSignalChannel(ctx))
		if next != nextRunCDC {
			return nil, errors.New("expected continue as new")
		}
		return state, err
	})
	require.NoError(t, env.GetWorkflowError())
	var state cdc_state.CDCFlowWorkflowState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, model.PauseSignal, state.ActiveSignal)
	require.Equal(t, protos.FlowStatus_STATUS_DRAINING, state.CurrentFlowStatus)
}

func TestResumeWhileSyncIsStoppingSkipsDrain(t *testing.T) {
	var env *testsuite.TestWorkflowEnvironment
	env = drainTestEnvironment(t, func(status protos.FlowStatus) {
		if status == protos.FlowStatus_STATUS_DRAINING {
			env.SignalWorkflow(model.FlowSignal.Name, model.NoopSignal)
		}
	})
	env.RegisterActivity(flowable.SyncFlow)
	env.OnActivity(flowable.SyncFlow, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).After(time.Hour)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(model.FlowSignalStateChange.Name, &protos.FlowStateChangeRequest{
			RequestedFlowState: protos.FlowStatus_STATUS_PAUSED, Drain: true,
		})
	}, time.Second)
	env.ExecuteWorkflow(func(ctx workflow.Context) (*cdc_state.CDCFlowWorkflowState, error) {
		state := &cdc_state.CDCFlowWorkflowState{CurrentFlowStatus: protos.FlowStatus_STATUS_RUNNING}
		next, err := startCDC(ctx, workflow.GetLogger(ctx), &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
			state, model.FlowSignal.GetSignalChannel(ctx), model.FlowSignalStateChange.GetSignalChannel(ctx))
		if next != nextRunCDC {
			return nil, errors.New("expected continue as new")
		}
		return state, err
	})
	require.NoError(t, env.GetWorkflowError())
	var state cdc_state.CDCFlowWorkflowState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, model.NoopSignal, state.ActiveSignal)
	require.Equal(t, protos.FlowStatus_STATUS_RUNNING, state.CurrentFlowStatus)
}

func TestNormalPauseWhileSyncIsStoppingSkipsDrain(t *testing.T) {
	var env *testsuite.TestWorkflowEnvironment
	env = drainTestEnvironment(t, func(status protos.FlowStatus) {
		if status == protos.FlowStatus_STATUS_DRAINING {
			env.SignalWorkflow(model.FlowSignal.Name, model.PauseSignal)
		}
	})
	env.RegisterActivity(flowable.SyncFlow)
	env.OnActivity(flowable.SyncFlow, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).After(time.Hour)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(model.FlowSignalStateChange.Name, &protos.FlowStateChangeRequest{
			RequestedFlowState: protos.FlowStatus_STATUS_PAUSED, Drain: true,
		})
	}, time.Second)
	env.ExecuteWorkflow(func(ctx workflow.Context) (*cdc_state.CDCFlowWorkflowState, error) {
		state := &cdc_state.CDCFlowWorkflowState{CurrentFlowStatus: protos.FlowStatus_STATUS_RUNNING}
		next, err := startCDC(ctx, workflow.GetLogger(ctx), &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
			state, model.FlowSignal.GetSignalChannel(ctx), model.FlowSignalStateChange.GetSignalChannel(ctx))
		if next != nextRunCDC {
			return nil, errors.New("expected continue as new")
		}
		return state, err
	})
	require.NoError(t, env.GetWorkflowError())
	var state cdc_state.CDCFlowWorkflowState
	require.NoError(t, env.GetWorkflowResult(&state))
	require.Equal(t, model.PauseSignal, state.ActiveSignal)
	require.Equal(t, protos.FlowStatus_STATUS_PAUSING, state.CurrentFlowStatus)
}

func TestPauseDrainNormalPauseCancelsNormalize(t *testing.T) {
	var statuses []protos.FlowStatus
	env := drainTestEnvironment(t, func(status protos.FlowStatus) { statuses = append(statuses, status) })
	env.OnActivity(flowable.DrainFlow, mock.Anything, mock.Anything).Return(nil).After(time.Hour)
	env.RegisterDelayedCallback(func() {
		assertDrainStatus(t, env, protos.FlowStatus_STATUS_DRAINING)
		env.SignalWorkflow(model.FlowSignal.Name, model.PauseSignal)
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		assertDrainStatus(t, env, protos.FlowStatus_STATUS_PAUSED)
		env.SignalWorkflow(model.FlowSignal.Name, model.NoopSignal)
	}, 2*time.Second)
	env.ExecuteWorkflow(CDCFlowWorkflow, &protos.FlowConnectionConfigsCore{FlowJobName: "drain-test"},
		&cdc_state.CDCFlowWorkflowState{ActiveSignal: model.PauseSignal, CurrentFlowStatus: protos.FlowStatus_STATUS_DRAINING})
	var continueErr *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &continueErr)
	require.Contains(t, statuses, protos.FlowStatus_STATUS_PAUSING)
	require.Contains(t, statuses, protos.FlowStatus_STATUS_PAUSED)
}
