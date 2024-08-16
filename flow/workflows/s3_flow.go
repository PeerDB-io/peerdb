package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func S3Workflow(ctx workflow.Context, config *protos.FlowConnectionConfigs) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
	})
	exportFuture := workflow.ExecuteActivity(ctx, snapshot.S3Export, config)
	if err := exportFuture.Get(ctx, nil); err != nil {
		return err
	}

	importFuture := workflow.ExecuteActivity(ctx, snapshot.S3Import, config)
	return importFuture.Get(ctx, nil)
}
