package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type NormalizeFlowState struct {
	CDCFlowName string
	Progress    []string
}

type NormalizeFlowExecution struct {
	NormalizeFlowState
	executionID string
	logger      log.Logger
}

func NewNormalizeFlowExecution(ctx workflow.Context, state *NormalizeFlowState) *NormalizeFlowExecution {
	return &NormalizeFlowExecution{
		NormalizeFlowState: *state,
		executionID:        workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:             workflow.GetLogger(ctx),
	}
}

func NormalizeFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) ([]model.NormalizeResponse, error) {
	s := NewNormalizeFlowExecution(ctx, &NormalizeFlowState{
		CDCFlowName: config.FlowJobName,
		Progress:    []string{},
	})

	return s.executeNormalizeFlow(ctx, config)
}

func (s *NormalizeFlowExecution) executeNormalizeFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) ([]model.NormalizeResponse, error) {
	s.logger.Info("executing normalize flow - ", s.CDCFlowName)

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
	})

	result := make([]model.NormalizeResponse, 0)
	syncChan := workflow.GetSignalChannel(normalizeFlowCtx, "Sync")

	stopLoop := false
	needSync := true
	for {
		if needSync {
			startNormalizeInput := &protos.StartNormalizeInput{
				FlowConnectionConfigs: config,
			}
			fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

			var normalizeResponse model.NormalizeResponse
			if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
				return result, fmt.Errorf("failed to flow: %w", err)
			}
			result = append(result, normalizeResponse)
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

	return result, nil
}
