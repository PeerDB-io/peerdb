package peerflow

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func executeCDCDropActivities(ctx workflow.Context, input *protos.DropFlowInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	ctx = workflow.WithDataConverter(ctx, converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))

	if err := workflow.SetQueryHandler(ctx, shared.FlowStatusQuery, func() (protos.FlowStatus, error) {
		if input.Resync {
			return protos.FlowStatus_STATUS_RESYNC, nil
		} else {
			return protos.FlowStatus_STATUS_TERMINATING, nil
		}
	}); err != nil {
		return fmt.Errorf("failed to set `%s` query handler: %w", shared.FlowStatusQuery, err)
	}

	var sourceError, destinationError error
	var sourceOk, destinationOk, canceled bool
	var sourceTries, destinationTries int
	selector := workflow.NewNamedSelector(ctx, input.FlowJobName+"-drop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})

	var sleepSource, sleepDestination func(workflow.Future)
	var dropSource, dropDestination func(workflow.Future)
	if !input.SkipSourceDrop {
		sleepSource = func(_ workflow.Future) {
			dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
				FlowJobName: input.FlowJobName,
				PeerName:    input.FlowConnectionConfigs.SourceName,
			})
			selector.AddFuture(dropSourceFuture, dropSource)
		}
		dropSource = func(f workflow.Future) {
			sourceError = f.Get(ctx, nil)
			sourceOk = sourceError == nil
			if !sourceOk {
				sourceTries += 1
				var dropSourceFuture workflow.Future
				if sourceTries < 50 {
					sleep := model.SleepFuture(ctx, time.Duration(sourceTries*sourceTries)*time.Second)
					selector.AddFuture(sleep, sleepSource)
				} else {
					dropSourceFuture = workflow.ExecuteActivity(ctx, flowable.Alert, &protos.AlertInput{
						FlowName: input.FlowJobName,
						Message:  "failed to drop source peer " + input.FlowConnectionConfigs.SourceName,
					})
					selector.AddFuture(dropSourceFuture, dropSource)
				}
			}
		}

		dropSourceFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowSource, &protos.DropFlowActivityInput{
			FlowJobName: input.FlowJobName,
			PeerName:    input.FlowConnectionConfigs.SourceName,
		})
		selector.AddFuture(dropSourceFuture, dropSource)
	} else {
		sourceOk = true
	}

	if !input.SkipDestinationDrop {
		sleepDestination = func(_ workflow.Future) {
			dropDestinationFuture := workflow.ExecuteActivity(ctx, flowable.DropFlowDestination, &protos.DropFlowActivityInput{
				FlowJobName: input.FlowJobName,
				PeerName:    input.FlowConnectionConfigs.DestinationName,
			})
			selector.AddFuture(dropDestinationFuture, dropDestination)
		}
		dropDestination = func(f workflow.Future) {
			destinationError = f.Get(ctx, nil)
			destinationOk = destinationError == nil
			if !destinationOk {
				destinationTries += 1
				var dropDestinationFuture workflow.Future
				if destinationTries < 50 {
					sleep := model.SleepFuture(ctx, time.Duration(destinationTries*destinationTries)*time.Second)
					selector.AddFuture(sleep, sleepDestination)
				} else {
					dropDestinationFuture = workflow.ExecuteActivity(ctx, flowable.Alert, &protos.AlertInput{
						FlowName: input.FlowJobName,
						Message:  "failed to drop destination peer " + input.FlowConnectionConfigs.DestinationName,
					})
					selector.AddFuture(dropDestinationFuture, dropDestination)
				}
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

	for !sourceOk || !destinationOk {
		selector.Select(ctx)
		if canceled {
			if input.Resync {
				break
			}
			return errors.Join(ctx.Err(), sourceError, destinationError)
		}
	}
	if input.Resync {
		input.FlowConnectionConfigs.Resync = true
		input.FlowConnectionConfigs.DoInitialSnapshot = true
		// TODO need to update catalog etc, copy FlowRequestHandler CreateCDCFlow logic to activity
		return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, input.FlowConnectionConfigs, nil)
	}
	return nil
}

func DropFlowWorkflow(ctx workflow.Context, input *protos.DropFlowInput) error {
	ctx = workflow.WithValue(ctx, shared.FlowNameKey, input.FlowJobName)
	workflow.GetLogger(ctx).Info("performing cleanup for flow",
		slog.String(string(shared.FlowNameKey), input.FlowJobName))
	contextMetadataInput := &protos.FlowContextMetadataInput{
		FlowName: input.FlowJobName,
		Status:   protos.FlowStatus_STATUS_UNKNOWN,
		IsResync: false,
	}
	if input.FlowConnectionConfigs != nil {
		contextMetadataInput.SourceName = input.FlowConnectionConfigs.SourceName
		contextMetadataInput.DestinationName = input.FlowConnectionConfigs.DestinationName
	}
	ctx, err := GetFlowMetadataContext(ctx, contextMetadataInput)
	if err != nil {
		return fmt.Errorf("failed to get flow metadata context: %w", err)
	}
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
