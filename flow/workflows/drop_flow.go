package peerflow

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
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
	var sourceOk, destinationOk bool
	selector := workflow.NewNamedSelector(ctx, fmt.Sprintf("%s-drop", req.FlowJobName))

	var dropSource, dropDestination func(f workflow.Future)
	dropSource = func(f workflow.Future) {
		sourceError = f.Get(ctx, nil)
		sourceOk = sourceError == nil
		if !sourceOk {
			dropSourceFuture = workflow.ExecuteActivity(ctx, flowable.DropFlowSource, req)
			selector.AddFuture(dropSourceFuture, dropSource)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}
	dropDestination = func(f workflow.Future) {
		destinationError = f.Get(ctx, nil)
		destinationOk = destinationError == nil
		if !destinationOk {
			dropDestinationFuture = workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, req)
			selector.AddFuture(dropDestinationFuture, dropDestination)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}

	for {
		selector.Select(ctx)
		if ctx.Err() != nil {
			return errors.Join(sourceError, destinationError)
		} else if sourceOk && destinationOk {
			return nil
		}
	}
}
