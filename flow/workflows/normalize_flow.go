package peerflow

import (
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
		HeartbeatTimeout:    5 * time.Minute,
	})

	results := make([]model.NormalizeResponse, 0, 4)
	errors := make([]string, 0)
	syncChan := workflow.GetSignalChannel(normalizeFlowCtx, "Sync")

	err := workflow.SetQueryHandler(ctx, CDCNormFlowStatusQuery, func(jobName string) (int, error) {
		return len(results), nil
	})
	if err != nil {
		errors = append(errors, err.Error())
	}

	stopLoop := false
	needSync := true
	for {
		if needSync {
			logger.Info("executing normalize - ", config.FlowJobName)
			startNormalizeInput := &protos.StartNormalizeInput{
				FlowConnectionConfigs: config,
			}
			fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

			var normalizeResponse *model.NormalizeResponse
			if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
				errors = append(errors, err.Error())
			} else if normalizeResponse != nil {
				results = append(results, *normalizeResponse)
			}
		}

		if !stopLoop {
			var stopLoopVal bool
			if !syncChan.Receive(normalizeFlowCtx, &stopLoopVal) {
				break
			}
			stopLoop = stopLoopVal
			needSync = !stopLoopVal
			for syncChan.ReceiveAsync(&stopLoopVal) {
				stopLoop = stopLoop || stopLoopVal
				needSync = needSync || !stopLoopVal
			}

			if stopLoop && !needSync {
				break
			}
		} else {
			break
		}
	}

	return &model.NormalizeFlowResponse{
		Results: results,
		Errors:  errors,
	}, nil
}
