package peerflow

import (
	"errors"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"go.temporal.io/sdk/workflow"
)

func DropFlowWorkflow(ctx workflow.Context, req *protos.ShutdownRequest) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("performing cleanup for flow ", req.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	ctx = workflow.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, req)
	dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, req)

	var sourceError, destinationError error
	selector := workflow.NewNamedSelector(ctx, fmt.Sprintf("%s-drop", req.FlowJobName))
	selector.AddFuture(dropSourceFuture, func(f workflow.Future) {
		sourceError = f.Get(ctx, nil)
	})
	selector.AddFuture(dropDestinationFuture, func(f workflow.Future) {
		destinationError = f.Get(ctx, nil)
	})
	selector.Select(ctx)
	selector.Select(ctx)

	return errors.Join(sourceError, destinationError)
}
