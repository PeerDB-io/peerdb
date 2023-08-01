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
) (*protos.SetupReplicationOutput, error) {
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

	res := &protos.SetupReplicationOutput{}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, flowable.SetupReplication, setupReplicationInput)
	if err := setupReplicationFuture.Get(ctx, &res); err != nil {
		return nil, fmt.Errorf("failed to setup replication on source peer: %w", err)
	}

	s.logger.Info("replication slot live for on source for peer flow - ", flowName)

	return res, nil
}

func (s *SnapshotFlowExecution) closeSlotKeepAlive(
	ctx workflow.Context,
) error {
	flowName := s.config.FlowJobName
	s.logger.Info("closing slot keep alive for peer flow - ", flowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	if err := workflow.ExecuteActivity(ctx, flowable.CloseSlotKeepAlive, flowName).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to close slot keep alive for peer flow: %w", err)
	}

	s.logger.Info("closed slot keep alive for peer flow - ", flowName)

	return nil
}

// startChildQrepWorkflow starts a child workflow for query based replication.
func (s *SnapshotFlowExecution) startQrepWorkflow(
	ctx workflow.Context,
	slotInfo *protos.SetupReplicationOutput,
) error {
	flowName := s.config.FlowJobName
	s.logger.Info("starting child qrep workflow for peer flow - ", flowName)

	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("qrep-%s", flowName),
		WorkflowTaskTimeout: 5 * time.Minute,
	})

	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}

	config := &protos.QRepConfig{}

	numPartitionsProcessed := 0

	qrepFuture := workflow.ExecuteChildWorkflow(
		ctx,
		QRepFlowWorkflow,
		config,
		lastPartition,
		numPartitionsProcessed,
	)
	if err := qrepFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to start child qrep workflow for peer flow: %w", err)
	}

	s.logger.Info("started child qrep workflow for peer flow - ", flowName)

	return nil
}

func SnapshotFlowWorkflow(ctx workflow.Context, config *protos.FlowConnectionConfigs) error {
	logger := workflow.GetLogger(ctx)

	se := &SnapshotFlowExecution{
		config: config,
		logger: logger,
	}

	sessionOpts := &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: time.Hour * 24 * 365 * 100, // 100 years
		HeartbeatTimeout: 15 * time.Minute,
	}

	sessionCtx, err := workflow.CreateSession(ctx, sessionOpts)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer workflow.CompleteSession(sessionCtx)

	slotInfo, err := se.setupReplication(sessionCtx)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo == nil {
		logger.Info("no slot info returned, skipping qrep workflow")
		return nil
	}

	if err := se.startQrepWorkflow(ctx, slotInfo); err != nil {
		return fmt.Errorf("failed to finish qrep workflow: %w", err)
	}

	if err := se.closeSlotKeepAlive(sessionCtx); err != nil {
		return fmt.Errorf("failed to close slot keep alive: %w", err)
	}

	return nil
}
