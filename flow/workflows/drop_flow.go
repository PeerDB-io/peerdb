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
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

func executeCDCDropActivities(ctx workflow.Context, input *protos.DropFlowInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	ctx = workflow.WithDataConverter(ctx, converter.NewCompositeDataConverter(converter.NewJSONPayloadConverter()))
	logger := workflow.GetLogger(ctx)

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
				var applicationError *temporal.ApplicationError
				if sourceTries < 50 && (!errors.As(sourceError, &applicationError) || !applicationError.NonRetryable()) {
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
				var applicationError *temporal.ApplicationError
				if destinationTries < 50 && (!errors.As(destinationError, &applicationError) || !applicationError.NonRetryable()) {
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
				if err := errors.Join(ctx.Err(), sourceError, destinationError); err != nil {
					logger.Warn("resync failed drop, proceeding with cdc flow", slog.Any("error", err))
				}
				break
			}
			return errors.Join(ctx.Err(), sourceError, destinationError)
		}
	}

	return nil
}

func DropFlowWorkflow(ctx workflow.Context, input *protos.DropFlowInput) error {
	if err := workflow.SetQueryHandler(ctx, shared.CDCFlowStateQuery, func() (cdc_state.CDCFlowWorkflowState, error) {
		state := cdc_state.CDCFlowWorkflowState{DropFlowInput: input}
		if input.Resync {
			state.CurrentFlowStatus = protos.FlowStatus_STATUS_RESYNC
			state.ActiveSignal = model.ResyncSignal
		} else {
			state.CurrentFlowStatus = protos.FlowStatus_STATUS_TERMINATING
			state.ActiveSignal = model.TerminateSignal
		}
		return state, nil
	}); err != nil {
		return fmt.Errorf("failed to set `%s` query handler: %w", shared.CDCFlowStateQuery, err)
	}

	status := protos.FlowStatus_STATUS_TERMINATING
	if input.Resync {
		status = protos.FlowStatus_STATUS_RESYNC
	}
	logger := workflow.GetLogger(ctx)
	cdc_state.SyncStatusToCatalogWithFlowName(ctx, logger, status, input.FlowJobName)

	ctx = workflow.WithValue(ctx, shared.FlowNameKey, input.FlowJobName)
	logger.Info("performing cleanup for flow",
		slog.String(string(shared.FlowNameKey), input.FlowJobName))
	contextMetadataInput := &protos.FlowContextMetadataInput{
		FlowName:           input.FlowJobName,
		Status:             status,
		IsResync:           false,
		FetchSourceVariant: false,
	}
	if input.FlowConnectionConfigs != nil {
		contextMetadataInput.SourceName = input.FlowConnectionConfigs.SourceName
		contextMetadataInput.DestinationName = input.FlowConnectionConfigs.DestinationName
		contextMetadataInput.IsResync = input.Resync
	}

	ctx, err := GetFlowMetadataContext(ctx, contextMetadataInput)
	if err != nil {
		return fmt.Errorf("failed to get flow metadata context: %w", err)
	}

	// Must be called after GetFlowMetadataContext to build flow context, then
	// ContextPropagator ensures attributes get propagated from flow to activity
	_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}), flowable.ReportStatusMetric, status).Get(ctx, nil)

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

	req := model.RemoveFlowDetailsFromCatalogRequest{
		FlowName: input.FlowJobName,
		Resync:   input.Resync,
	}
	if err := workflow.ExecuteActivity(
		removeFlowEntriesCtx, flowable.RemoveFlowDetailsFromCatalog, &req,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("failed to remove flow details from catalog", slog.Any("error", err))
		return err
	}

	if input.Resync {
		return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, input.FlowConnectionConfigs, nil)
	}

	return nil
}
