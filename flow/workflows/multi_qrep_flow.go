package peerflow

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

// getChildWorkflowID returns the child workflow ID for a new sync flow.
func getChildWorkflowID(ctx workflow.Context, flowJobName string) string {
	id := GetUUID(ctx)
	return fmt.Sprintf("%s_%s", flowJobName, id)
}

type workflowChanInput struct {
	flowJobName string
	isWait      bool
}

func MultiQRepFlowWorkflow(
	ctx workflow.Context,
	config *protos.MultiQRepConfig,
) error {
	if config.Mode != protos.MultiQRepConfigMode_CONFIG_GLOBAL {
		return errors.New("invalid/unsupported mode for multi qrep flow")
	}

	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey),
		config.GlobalConfig.FlowJobName))

	// key is flow job name for these
	workflowFnCache := make(map[string]any)
	configCache := make(map[string]*protos.QRepConfig)
	stateCache := make(map[string]*protos.QRepFlowState)
	syncSelector := shared.NewBoundSelector(ctx, "MultiQRepSyncSelector", int(config.TableParallelism))
	// no limit for waiting for new rows
	// if initial copy is selected, we never wait for new rows and exit after workflows run once
	waitSelector := shared.NewBoundSelector(ctx, "MultiQRepWaitSelector", 0)

	// workflow future callbacks send to this, for dispatching new sync/wait workflows
	workflowChan := workflow.NewNamedBufferedChannel(ctx, "MultiQRepWorkflowChan", len(config.TableMappings))
	var childWorkflowError error
	handleError := func(err error) {
		logger.Error("Error in child workflow", slog.Any("error", err))
		childWorkflowError = err
	}

	for i, mapping := range config.TableMappings {
		childWorkflowConfig := proto.Clone(config.GlobalConfig).(*protos.QRepConfig)
		childWorkflowConfig.DestinationTableIdentifier = mapping.DestinationTableIdentifier
		childWorkflowConfig.WatermarkTable = mapping.WatermarkTableIdentifier
		childWorkflowConfig.WatermarkColumn = mapping.WatermarkColumn
		childWorkflowConfig.WriteMode = mapping.WriteMode
		childWorkflowConfig.FlowJobName = fmt.Sprintf("%s_qrepflow_%d", config.GlobalConfig.FlowJobName, i)
		if mapping.Query != "" {
			childWorkflowConfig.Query = mapping.Query
			// full table partition, doesn't make much sense unless overwrite mode
		} else if mapping.WatermarkColumn == "" {
			childWorkflowConfig.Query = "SELECT * FROM " + mapping.WatermarkTableIdentifier
			// Postgres enforces pkey so APPEND will fail when updates are also happening.
		} else {
			childWorkflowConfig.Query = fmt.Sprintf("SELECT * FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
				mapping.WatermarkTableIdentifier, mapping.WatermarkColumn)
		}
		childWorkflowConfig.InitialCopyOnly = true
		configCache[childWorkflowConfig.FlowJobName] = childWorkflowConfig

		workflowFn := QRepFlowWorkflow
		if mapping.WatermarkColumn == "xmin" {
			workflowFn = XminFlowWorkflow
		}
		workflowFnCache[childWorkflowConfig.FlowJobName] = workflowFn

		childWorkflowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:        getChildWorkflowID(ctx, childWorkflowConfig.FlowJobName),
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			SearchAttributes: map[string]interface{}{
				shared.MirrorNameSearchAttribute: config.GlobalConfig.FlowJobName,
			},
		})
		logger.Info("Spawning initial child workflow", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
		syncSelector.SpawnChild(childWorkflowCtx, workflowFn, func(f workflow.Future) {
			var state *protos.QRepFlowState
			if err := f.Get(ctx, &state); err != nil {
				handleError(err)
				return
			}
			stateCache[childWorkflowConfig.FlowJobName] = state
			// only begin waiting for new rows if initial copy is disabled
			if !config.GlobalConfig.InitialCopyOnly {
				logger.Info("Initial sync completed, dispatching for wait", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
				workflowChan.Send(childWorkflowCtx, workflowChanInput{
					flowJobName: childWorkflowConfig.FlowJobName,
					isWait:      true,
				})
			}
		}, childWorkflowConfig, nil)
	}

	if !config.GlobalConfig.InitialCopyOnly {
		for {
			if childWorkflowError != nil {
				break
			}
			if err := ctx.Err(); err != nil {
				logger.Info("Context canceled, waiting on existing sync workflows before exiting")
				break
			}

			syncSelector.DrainAsync(ctx)
			waitSelector.DrainAsync(ctx)
			var input workflowChanInput
			ok, _ := workflowChan.ReceiveWithTimeout(ctx, 5*time.Second, &input)
			if !ok {
				continue
			}

			childWorkflowConfig := configCache[input.flowJobName]
			childWorkflowState := stateCache[input.flowJobName]
			if input.isWait {
				childWorkflowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					WorkflowID:        getChildWorkflowID(ctx, childWorkflowConfig.FlowJobName+"-wait"),
					ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
					SearchAttributes: map[string]interface{}{
						shared.MirrorNameSearchAttribute: config.GlobalConfig.FlowJobName,
					},
				})
				logger.Info("Spawning wait child workflow", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
				waitSelector.SpawnChild(childWorkflowCtx, QRepWaitForNewRowsWorkflow, func(f workflow.Future) {
					if err := f.Get(ctx, nil); err != nil {
						handleError(err)
						return
					}
					logger.Info("Wait completed, dispatching for sync", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
					workflowChan.Send(ctx, workflowChanInput{
						flowJobName: childWorkflowConfig.FlowJobName,
						isWait:      false,
					})
				}, childWorkflowConfig, childWorkflowState.LastPartition)
			} else {
				childWorkflowCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					WorkflowID:        getChildWorkflowID(ctx, childWorkflowConfig.FlowJobName),
					ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
					SearchAttributes: map[string]interface{}{
						shared.MirrorNameSearchAttribute: config.GlobalConfig.FlowJobName,
					},
				})
				logger.Info("Spawning child workflow", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
				syncSelector.SpawnChild(childWorkflowCtx, workflowFnCache[input.flowJobName], func(f workflow.Future) {
					var state *protos.QRepFlowState
					if err := f.Get(childWorkflowCtx, &state); err != nil {
						handleError(err)
						return
					}
					stateCache[childWorkflowConfig.FlowJobName] = state
					logger.Info("Sync completed, dispatching for wait", slog.String("flowJobName", childWorkflowConfig.FlowJobName))
					workflowChan.Send(childWorkflowCtx, workflowChanInput{
						flowJobName: childWorkflowConfig.FlowJobName,
						isWait:      true,
					})
				}, childWorkflowConfig, childWorkflowState)
			}
		}
	}

	// if initial copy is enabled then this selector will eventually finish waiting
	// otherwise it will keep waiting until an error
	// wait workflows won't return, so don't wait on selector and rely on cancel propagation from when we return
	_ = syncSelector.Wait(ctx)
	workflowChan.Close()

	return childWorkflowError
}
