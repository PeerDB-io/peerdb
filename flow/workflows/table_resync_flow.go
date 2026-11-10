package peerflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// TableResyncWorkflow re-snapshots mirror tables after a schema change that cannot be replayed in place.
func TableResyncWorkflow(
	ctx workflow.Context, flowName string, dstPeer string, env map[string]string, update *protos.CDCFlowConfigUpdate,
) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 12 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Minute,
			MaximumAttempts: 5,
		},
	})
	return workflow.ExecuteActivity(ctx, flowable.ResyncTables, flowName, dstPeer, env, update).Get(ctx, nil)
}
