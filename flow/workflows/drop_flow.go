package peerflow

import (
	"errors"
	"log/slog"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

func DropFlowWorkflow(ctx workflow.Context, req *protos.ShutdownRequest) error {
	workflow.GetLogger(ctx).Info("performing cleanup for flow", slog.String(string(shared.FlowNameKey), req.FlowJobName))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	ctx = workflow.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)

	var sourceError, destinationError error
	var sourceOk, destinationOk, canceled bool
	selector := workflow.NewNamedSelector(ctx, req.FlowJobName+"-drop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	var dropSource, dropDestination func(f workflow.Future)
	dropSource = func(f workflow.Future) {
		sourceError = f.Get(ctx, nil)
		sourceOk = sourceError == nil
		if !sourceOk {
			dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, req)
			selector.AddFuture(dropSourceFuture, dropSource)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}
	dropDestination = func(f workflow.Future) {
		destinationError = f.Get(ctx, nil)
		destinationOk = destinationError == nil
		if !destinationOk {
			dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, req)
			selector.AddFuture(dropDestinationFuture, dropDestination)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}
	dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, req)
	selector.AddFuture(dropSourceFuture, dropSource)
	dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, req)
	selector.AddFuture(dropDestinationFuture, dropDestination)

	for {
		selector.Select(ctx)
		if canceled {
			return errors.Join(ctx.Err(), sourceError, destinationError)
		} else if sourceOk && destinationOk {
			return nil
		}
	}
}
