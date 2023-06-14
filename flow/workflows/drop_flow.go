package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type DropFlowWorkflowInput struct {
	CatalogJdbcURL string
	FlowName       string
	WorkflowID     string
}

// DropFlowWorkflowExecution represents the state for execution of a drop flow.
type DropFlowWorkflowExecution struct {
	// The state of the peer flow.
	DropFlowWorkflowInput
	flowExecutionID string
	logger          log.Logger
}

func newDropFlowWorkflowExecution(
	ctx workflow.Context,
	state *DropFlowWorkflowInput,
) *DropFlowWorkflowExecution {
	return &DropFlowWorkflowExecution{
		DropFlowWorkflowInput: *state,
		flowExecutionID:       workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:                workflow.GetLogger(ctx),
	}
}

// fetchConnectionConfigs fetches the connection configs for source and destination peers.
func (w *DropFlowWorkflowExecution) fetchConnectionConfigs(
	ctx workflow.Context) (*protos.FlowConnectionConfigs, error) {
	w.logger.Info("fetching connection configs for peer flow - ", w.FlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	fetchConfigActivityInput := &activities.FetchConfigActivityInput{
		CatalogJdbcURL: w.CatalogJdbcURL,
		PeerFlowName:   w.FlowName,
	}

	configsFuture := workflow.ExecuteActivity(
		ctx,
		fetchConfig.FetchConfig,
		fetchConfigActivityInput,
	)

	flowConnectionConfigs := &protos.FlowConnectionConfigs{}
	if err := configsFuture.Get(ctx, &flowConnectionConfigs); err != nil {
		return nil, fmt.Errorf("failed to fetch connection configs: %w", err)
	}

	if flowConnectionConfigs == nil ||
		flowConnectionConfigs.Source == nil ||
		flowConnectionConfigs.Destination == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	flowConnectionConfigs.FlowJobName = w.FlowName
	w.logger.Info("fetched connection configs for peer flow - ", w.FlowName)
	return flowConnectionConfigs, nil
}

func DropFlowWorkflow(ctx workflow.Context, config *DropFlowWorkflowInput) error {
	execution := newDropFlowWorkflowExecution(ctx, config)

	execution.logger.Info("performing cleanup for flow ", config.FlowName)

	flowConnectionConfigs, err := execution.fetchConnectionConfigs(ctx)
	if err != nil {
		return err
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})
	dropFlowFuture := workflow.ExecuteActivity(ctx, flowable.DropFlow, flowConnectionConfigs)
	if err = dropFlowFuture.Get(ctx, nil); err != nil {
		return err
	}

	return nil
}
