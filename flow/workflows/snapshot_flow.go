package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type SnapshotFlowExecution struct {
	config *protos.FlowConnectionConfigs
	logger log.Logger
}

// ensurePullability ensures that the source peer is pullable.
func (s *SnapshotFlowExecution) setupReplication(
	ctx workflow.Context,
) error {
	flowName := s.config.FlowJobName
	s.logger.Info("setting up replication on source for peer flow - ", flowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	setupReplicationInput := &protos.SetupReplicationInput{
		PeerConnectionConfig: s.config.Source,
		FlowJobName:          flowName,
		TableNameMapping:     s.config.TableNameMapping,
	}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, flowable.SetupReplication, setupReplicationInput)
	if err := setupReplicationFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup replication on source peer: %w", err)
	}

	return nil
}

func SnapshotFlowWorkflow(ctx workflow.Context, config *protos.FlowConnectionConfigs) error {
	se := &SnapshotFlowExecution{
		config: config,
		logger: workflow.GetLogger(ctx),
	}

	if err := se.setupReplication(ctx); err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	return nil
}
