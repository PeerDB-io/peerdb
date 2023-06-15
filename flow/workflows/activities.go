package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

var (
	fetchConfig *activities.FetchConfigActivity
	flowable    *activities.FlowableActivity
)

type FetchConnectionConfigInput struct {
	PeerFlowName   string
	CatalogJdbcURL string
	logger         log.Logger
}

// fetchConnectionConfigsHelper fetches the connection configs for source and destination peers.
func fetchConnectionConfigs(
	ctx workflow.Context, input *FetchConnectionConfigInput) (*protos.FlowConnectionConfigs, error) {
	input.logger.Info("fetching connection configs for peer flow - ", input.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	fetchConfigActivityInput := &activities.FetchConfigActivityInput{
		CatalogJdbcURL: input.CatalogJdbcURL,
		PeerFlowName:   input.PeerFlowName,
	}

	configsFuture := workflow.ExecuteActivity(ctx, fetchConfig, fetchConfigActivityInput)

	flowConnectionConfigs := &protos.FlowConnectionConfigs{}
	if err := configsFuture.Get(ctx, &flowConnectionConfigs); err != nil {
		return nil, fmt.Errorf("failed to fetch connection configs: %w", err)
	}

	if flowConnectionConfigs == nil ||
		flowConnectionConfigs.Source == nil ||
		flowConnectionConfigs.Destination == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	flowConnectionConfigs.FlowJobName = input.PeerFlowName
	input.logger.Info("fetched connection configs for peer flow - ", input.PeerFlowName)
	return flowConnectionConfigs, nil
}
