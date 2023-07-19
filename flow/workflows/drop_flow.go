package peerflow

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// DropFlowWorkflowExecution represents the state for execution of a drop flow.
type DropFlowWorkflowExecution struct {
	shutDownRequest *protos.ShutdownRequest
	flowExecutionID string
	logger          log.Logger
}

func newDropFlowWorkflowExecution(ctx workflow.Context, req *protos.ShutdownRequest) *DropFlowWorkflowExecution {
	return &DropFlowWorkflowExecution{
		shutDownRequest: req,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
	}
}

func DropFlowWorkflow(ctx workflow.Context, req *protos.ShutdownRequest) error {
	execution := newDropFlowWorkflowExecution(ctx, req)
	execution.logger.Info("performing cleanup for flow ", req.FlowJobName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	dropFlowFuture := workflow.ExecuteActivity(ctx, flowable.DropFlow, req)
	if err := dropFlowFuture.Get(ctx, nil); err != nil {
		return err
	}

	return nil
}
