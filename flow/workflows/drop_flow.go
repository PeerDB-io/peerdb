package peerflow

import (
	"errors"
	"log/slog"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func executeCDCDropActivities(ctx workflow.Context, input *protos.DropFlowInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})
	ctx = workflow.WithDataConverter(ctx, converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))

	var sourceError, destinationError error
	var sourceOk, destinationOk, canceled bool
	selector := workflow.NewNamedSelector(ctx, input.FlowJobName+"-drop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	var dropSource, dropDestination func(f workflow.Future)
	dropSource = func(f workflow.Future) {
		sourceError = f.Get(ctx, nil)
		sourceOk = sourceError == nil
		if !sourceOk {
			dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
				FlowJobName: input.FlowJobName,
				PeerName:    input.FlowConnectionConfigs.SourceName,
			})
			selector.AddFuture(dropSourceFuture, dropSource)
			_ = workflow.Sleep(ctx, time.Second)
		}
	}

	dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
		FlowJobName: input.FlowJobName,
		PeerName:    input.FlowConnectionConfigs.SourceName,
	})
	selector.AddFuture(dropSourceFuture, dropSource)

	if !input.SkipDestinationDrop {
		dropDestination = func(f workflow.Future) {
			destinationError = f.Get(ctx, nil)
			destinationOk = destinationError == nil
			if !destinationOk {
				dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, &protos.DropFlowActivityInput{
					FlowJobName: input.FlowJobName,
					PeerName:    input.FlowConnectionConfigs.DestinationName,
				})
				selector.AddFuture(dropDestinationFuture, dropDestination)
				_ = workflow.Sleep(ctx, time.Second)
			}
		}
		dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, &protos.DropFlowActivityInput{
			FlowJobName: input.FlowJobName,
			PeerName:    input.FlowConnectionConfigs.DestinationName,
		})
		selector.AddFuture(dropDestinationFuture, dropDestination)
	} else {
		destinationOk = true
	}

	for {
		selector.Select(ctx)
		if canceled {
			return errors.Join(ctx.Err(), sourceError, destinationError)
		} else if sourceOk && destinationOk {
			return nil
		}
	}
}

func DropFlowWorkflow(ctx workflow.Context, input *protos.DropFlowInput) error {
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, input.FlowJobName)
	workflow.GetLogger(ctx).Info("performing cleanup for flow",
		slog.String(string(shared.FlowNameKey), input.FlowJobName))

	if input.FlowConnectionConfigs != nil {
		if input.DropFlowStats {
			dropStatsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 2 * time.Minute,
				HeartbeatTimeout:    1 * time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval: 1 * time.Minute,
				},
			})
			if err := workflow.ExecuteActivity(
				dropStatsCtx, flowable.DeleteMirrorStats, input.FlowJobName,
			).Get(dropStatsCtx, nil); err != nil {
				workflow.GetLogger(ctx).Error("failed to delete mirror stats", slog.Any("error", err))
				return err
			}
		}

		if err := executeCDCDropActivities(ctx, input); err != nil {
			workflow.GetLogger(ctx).Error("failed to drop CDC flow", slog.Any("error", err))
			return err
		}
		workflow.GetLogger(ctx).Info("CDC flow dropped successfully")
	}

	removeFlowEntriesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})
	if err := workflow.ExecuteActivity(
		removeFlowEntriesCtx, flowable.RemoveFlowEntryFromCatalog, input.FlowJobName,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("failed to remove flow entries from catalog", slog.Any("error", err))
		return err
	}

	return nil
}
