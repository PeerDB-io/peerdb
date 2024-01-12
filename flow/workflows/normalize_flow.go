package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"go.temporal.io/sdk/workflow"
)

func NormalizeFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (*model.NormalizeFlowResponse, error) {
	logger := workflow.GetLogger(ctx)

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	results := make([]model.NormalizeResponse, 0, 4)
	errors := make([]string, 0)
	syncChan := workflow.GetSignalChannel(ctx, "Sync")

	var stopLoop, canceled bool
	var lastSyncBatchID, syncBatchID int64
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
		if s.SyncBatchID != 0 {
			syncBatchID = s.SyncBatchID
		}
		if len(s.TableNameSchemaMapping) != 0 {
			config.TableNameSchemaMapping = s.TableNameSchemaMapping
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
		lastSyncBatchID = syncBatchID

		logger.Info("executing normalize - ", config.FlowJobName)
		startNormalizeInput := &protos.StartNormalizeInput{
			FlowConnectionConfigs: config,
			SyncBatchID:           syncBatchID,
		}
		fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

		var normalizeResponse *model.NormalizeResponse
		if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
			errors = append(errors, err.Error())
		} else if normalizeResponse != nil {
			results = append(results, *normalizeResponse)
		}
	}

	return &model.NormalizeFlowResponse{
		Results: results,
		Errors:  errors,
	}, nil
}
