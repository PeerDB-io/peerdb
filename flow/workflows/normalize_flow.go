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
) (*model.NormalizeResponse, error) {
	s := NewNormalizeFlowExecution(ctx, &NormalizeFlowState{
		CDCFlowName: config.FlowJobName,
		Progress:    []string{},
	})

	return s.executeNormalizeFlow(ctx, config)
}

func (s *NormalizeFlowExecution) executeNormalizeFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (*model.NormalizeResponse, error) {
	s.logger.Info("executing normalize flow - ", s.CDCFlowName)

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
	})

	// execute StartFlow on the peers to start the flow
	startNormalizeInput := &protos.StartNormalizeInput{
		FlowConnectionConfigs: config,
	}
	fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

	var normalizeResponse *model.NormalizeResponse
	if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
		return nil, fmt.Errorf("failed to flow: %w", err)
	}

	return normalizeResponse, nil
}
