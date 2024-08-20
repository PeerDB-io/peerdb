package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func S3Workflow(ctx workflow.Context, config *protos.CreateImportS3Request) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
	})
	return workflow.ExecuteActivity(ctx, snapshot.S3Export, config).Get(ctx, nil)
}
