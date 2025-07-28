package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func GlobalScheduleManagerWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 24 * 365,
		HeartbeatTimeout:    time.Hour,
		WaitForCancellation: true,
	})
	slotSizeFuture := workflow.ExecuteActivity(ctx, flowable.ScheduledTasks)
	if err := slotSizeFuture.Get(ctx, nil); err != nil {
		return err
	}
	return ctx.Err()
}
