package peerflow

import (
	"errors"
	"log/slog"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

func DropFlowWorkflow(ctx workflow.Context, config *protos.DropFlowInput) error {
	workflow.GetLogger(ctx).Info("performing cleanup for flow", slog.String(string(shared.FlowNameKey), config.FlowJobName))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	ctx = workflow.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)

	converter.GetDefaultDataConverter()
	ctx = workflow.WithDataConverter(ctx,
		converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))

	var sourceError, destinationError error
	var sourceOk, destinationOk, canceled bool
	selector := workflow.NewNamedSelector(ctx, config.FlowJobName+"-drop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	var dropSource, dropDestination func(f workflow.Future)
	dropSource = func(f workflow.Future) {
		sourceError = f.Get(ctx, nil)
		sourceOk = sourceError == nil
		if !sourceOk {
			dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
				FlowJobName: config.FlowJobName,
				PeerName:    config.SourcePeerName,
			})
			selector.AddFuture(dropSourceFuture, dropSource)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}
	dropDestination = func(f workflow.Future) {
		destinationError = f.Get(ctx, nil)
		destinationOk = destinationError == nil
		if !destinationOk {
			dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, &protos.DropFlowActivityInput{
				FlowJobName: config.FlowJobName,
				PeerName:    config.DestinationPeerName,
			})
			selector.AddFuture(dropDestinationFuture, dropDestination)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}
	dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
		FlowJobName: config.FlowJobName,
		PeerName:    config.SourcePeerName,
	})
	selector.AddFuture(dropSourceFuture, dropSource)
	dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, &protos.DropFlowActivityInput{
		FlowJobName: config.FlowJobName,
		PeerName:    config.DestinationPeerName,
	})
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
