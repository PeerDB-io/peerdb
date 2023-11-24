package peerflow

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/workflow"
)

// HeartbeatFlowWorkflow is the workflow that sets up heartbeat sending.
func HeartbeatFlowWorkflow(ctx workflow.Context,
	config *protos.PostgresPeerConfigs) error {

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
	})

	heartbeatFuture := workflow.ExecuteActivity(ctx, flowable.SendWALHeartbeat, config.Configs)
	if err := heartbeatFuture.Get(ctx, nil); err != nil {
		return err
	}

	return nil
}
