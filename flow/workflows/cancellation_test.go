package peerflow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func childWithCancellationCleanup(ctx workflow.Context, delay time.Duration) error {
	err := workflow.Await(ctx, func() bool { return false })
	cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	if cleanupErr := workflow.Sleep(cleanupCtx, delay); cleanupErr != nil {
		return cleanupErr
	}
	return err
}

func TestWaitForCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childWithCancellationCleanup)
	env.ExecuteWorkflow(func(ctx workflow.Context) (time.Duration, error) {
		start := workflow.Now(ctx)
		childCtx, cancel := workflow.WithCancel(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WaitForCancellation: true,
		}))
		child := workflow.ExecuteChildWorkflow(childCtx, childWithCancellationCleanup, 10*time.Second)
		if err := child.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
			return 0, err
		}
		cancel()
		waitForCancellation(childCtx, child)
		return workflow.Now(ctx).Sub(start), nil
	})
	require.NoError(t, env.GetWorkflowError())
	var elapsed time.Duration
	require.NoError(t, env.GetWorkflowResult(&elapsed))
	require.GreaterOrEqual(t, elapsed, 10*time.Second)
}

func TestQRepProcessPartitionsWaitsForAllCancelledChildren(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ *protos.QRepConfig,
		batch *protos.QRepPartitionBatch, _ string,
	) error {
		return childWithCancellationCleanup(ctx, time.Duration(batch.BatchId)*10*time.Second)
	}, workflow.RegisterOptions{Name: "QRepPartitionWorkflow"})
	env.ExecuteWorkflow(func(ctx workflow.Context) (time.Duration, error) {
		start := workflow.Now(ctx)
		childCtx, cancel := workflow.WithCancel(ctx)
		defer cancel()
		workflow.Go(ctx, func(ctx workflow.Context) {
			_ = workflow.Sleep(ctx, time.Second)
			cancel()
		})
		q := newQRepFlowExecution(childCtx, &protos.QRepConfig{FlowJobName: "cancel-partitions"}, "run")
		if err := q.processPartitions(childCtx, 2, makePartitions(2)); !temporal.IsCanceledError(err) {
			return 0, fmt.Errorf("expected partition cancellation, got %v", err)
		}
		return workflow.Now(ctx).Sub(start), nil
	})
	require.NoError(t, env.GetWorkflowError())
	var elapsed time.Duration
	require.NoError(t, env.GetWorkflowResult(&elapsed))
	require.GreaterOrEqual(t, elapsed, 21*time.Second, "wait for both partition children, not just the first")
}

func TestBoundSelectorWaitsForAllCancelledChildren(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childWithCancellationCleanup)
	env.ExecuteWorkflow(func(ctx workflow.Context) (time.Duration, error) {
		start := workflow.Now(ctx)
		childCtx, cancel := workflow.WithCancel(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WaitForCancellation: true,
		}))
		defer cancel()
		workflow.Go(ctx, func(ctx workflow.Context) {
			_ = workflow.Sleep(ctx, time.Second)
			cancel()
		})
		selector := shared.NewBoundSelector(childCtx, "cancel-clones", 2)
		for _, delay := range []time.Duration{10 * time.Second, 20 * time.Second} {
			if err := selector.SpawnChild(childCtx, childWithCancellationCleanup, nil, delay); err != nil {
				return 0, err
			}
		}
		if err := selector.Wait(childCtx); !temporal.IsCanceledError(err) {
			return 0, fmt.Errorf("expected clone cancellation, got %v", err)
		}
		selector.WaitForCancellation(childCtx)
		return workflow.Now(ctx).Sub(start), nil
	})
	require.NoError(t, env.GetWorkflowError())
	var elapsed time.Duration
	require.NoError(t, env.GetWorkflowResult(&elapsed))
	require.GreaterOrEqual(t, elapsed, 21*time.Second, "wait for both clone children, not just the first")
}
