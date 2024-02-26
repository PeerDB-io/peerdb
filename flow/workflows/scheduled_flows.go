package peerflow

import (
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

// RecordSlotSizeWorkflow monitors replication slot size
func RecordSlotSizeWorkflow(ctx workflow.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
	})
	slotSizeFuture := workflow.ExecuteActivity(ctx, flowable.RecordSlotSizes)
	return slotSizeFuture.Get(ctx, nil)
}

// HeartbeatFlowWorkflow sends WAL heartbeats
func HeartbeatFlowWorkflow(ctx workflow.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
	})
	heartbeatFuture := workflow.ExecuteActivity(ctx, flowable.SendWALHeartbeat)
	return heartbeatFuture.Get(ctx, nil)
}

func withCronOptions(ctx workflow.Context, workflowID string, cron string) workflow.Context {
	return workflow.WithChildOptions(ctx,
		workflow.ChildWorkflowOptions{
			WorkflowID:          workflowID,
			ParentClosePolicy:   enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			WaitForCancellation: true,
			CronSchedule:        cron,
		},
	)
}

func GlobalScheduleManagerWorkflow(ctx workflow.Context) error {
	info := workflow.GetInfo(ctx)

	walHeartbeatEnabled := GetSideEffect(ctx, func(_ workflow.Context) bool {
		return peerdbenv.PeerDBEnableWALHeartbeat()
	})
	if walHeartbeatEnabled {
		heartbeatCtx := withCronOptions(ctx,
			fmt.Sprintf("wal-heartbeat-%s", info.OriginalRunID),
			"*/12 * * * *")
		workflow.ExecuteChildWorkflow(
			heartbeatCtx,
			HeartbeatFlowWorkflow,
		)
	}

	slotSizeCtx := withCronOptions(ctx,
		fmt.Sprintf("record-slot-size-%s", info.OriginalRunID),
		"*/5 * * * *")
	workflow.ExecuteChildWorkflow(slotSizeCtx, RecordSlotSizeWorkflow)

	ctx.Done().Receive(ctx, nil)
	return ctx.Err()
}
