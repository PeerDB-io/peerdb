package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"go.temporal.io/sdk/workflow"
)

func NormalizeFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.NormalizeFlowOptions,
) (*model.NormalizeFlowResponse, error) {
	logger := workflow.GetLogger(ctx)
	tableNameSchemaMapping := options.TableNameSchemaMapping

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	results := make([]model.NormalizeResponse, 0, 4)
	errors := make([]string, 0)
	syncChan := workflow.GetSignalChannel(ctx, "Sync")

	var stopLoop, canceled bool
	var lastSyncBatchID, syncBatchID int64
	lastSyncBatchID = -1
	syncBatchID = -1
	selector := workflow.NewNamedSelector(ctx, fmt.Sprintf("%s-normalize", config.FlowJobName))
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})
	selector.AddReceive(syncChan, func(c workflow.ReceiveChannel, _ bool) {
		var s model.NormalizeSignal
		c.ReceiveAsync(&s)
		if s.Done {
			stopLoop = true
		}
		if s.SyncBatchID > syncBatchID {
			syncBatchID = s.SyncBatchID
		}
		if len(s.TableNameSchemaMapping) != 0 {
			tableNameSchemaMapping = s.TableNameSchemaMapping
		}
	})
	for !stopLoop {
		selector.Select(ctx)
		for !canceled && selector.HasPending() {
			selector.Select(ctx)
		}
		if canceled || (stopLoop && lastSyncBatchID == syncBatchID) {
			break
		}
		if lastSyncBatchID != syncBatchID {
			lastSyncBatchID = syncBatchID

			logger.Info("executing normalize - ", config.FlowJobName)
			startNormalizeInput := &protos.StartNormalizeInput{
				FlowConnectionConfigs:  config,
				TableNameSchemaMapping: tableNameSchemaMapping,
				SyncBatchID:            syncBatchID,
			}
			fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

			var normalizeResponse *model.NormalizeResponse
			if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
				errors = append(errors, err.Error())
			} else if normalizeResponse != nil {
				results = append(results, *normalizeResponse)
			}
		}

		if !peerdbenv.PeerDBEnableParallelSyncNormalize() {
			parent := workflow.GetInfo(ctx).ParentWorkflowExecution
			if parent != nil {
				workflow.SignalExternalWorkflow(
					ctx,
					parent.ID,
					parent.RunID,
					"SyncDone",
					struct{}{},
				)
			}
		}
	}

	return &model.NormalizeFlowResponse{
		Results: results,
		Errors:  errors,
	}, nil
}
