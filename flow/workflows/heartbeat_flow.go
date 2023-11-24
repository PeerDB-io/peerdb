package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// HeartbeatFlowWorkflow is the workflow that sets up heartbeat sending.
func HeartbeatFlowWorkflow(ctx workflow.Context) error {

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
	})

	heartbeatFuture := workflow.ExecuteActivity(ctx, flowable.SendWALHeartbeat)
	if err := heartbeatFuture.Get(ctx, nil); err != nil {
		return err
	}

	return nil
}
