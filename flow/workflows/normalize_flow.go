package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

func NormalizeFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (*model.NormalizeFlowResponse, error) {
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	results := make([]model.NormalizeResponse, 0, 4)
	errors := make([]string, 0)
	syncChan := model.NormalizeSyncSignal.GetSignalChannel(ctx)

	var stopLoop, canceled bool
	var lastSyncBatchID, syncBatchID int64
	var tableNameSchemaMapping map[string]*protos.TableSchema
	lastSyncBatchID = -1
	syncBatchID = -1
	selector := workflow.NewNamedSelector(ctx, config.FlowJobName+"-normalize")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})
	syncChan.AddToSelector(selector, func(s model.NormalizePayload, _ bool) {
		if s.Done {
			stopLoop = true
		}
		if s.SyncBatchID > syncBatchID {
			syncBatchID = s.SyncBatchID
		}
		tableNameSchemaMapping = s.TableNameSchemaMapping
	})

	parallel, _ := GetSideEffect(ctx, func(_ workflow.Context) bool {
		return peerdbenv.PeerDBEnableParallelSyncNormalize()
	})

	for !stopLoop {
		selector.Select(ctx)
		for !canceled && selector.HasPending() {
			selector.Select(ctx)
		}
		if canceled || (stopLoop && lastSyncBatchID == syncBatchID) {
			if canceled {
				logger.Info("normalize canceled")
			} else {
				logger.Info("normalize finished")
			}
			break
		}
		if lastSyncBatchID != syncBatchID {
			lastSyncBatchID = syncBatchID

			logger.Info("executing normalize")
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

		if !parallel {
			parent := workflow.GetInfo(ctx).ParentWorkflowExecution
			model.NormalizeSyncDoneSignal.SignalExternalWorkflow(
				ctx,
				parent.ID,
				parent.RunID,
				struct{}{},
			)
		}
	}

	return &model.NormalizeFlowResponse{
		Results: results,
		Errors:  errors,
	}, nil
}
