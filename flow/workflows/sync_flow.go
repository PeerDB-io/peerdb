package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type SyncFlowState struct {
	PeerFlowName string
	Progress     []string
}

type SyncFlowExecution struct {
	SyncFlowState
	executionID string
	logger      log.Logger
}

type NormalizeFlowState struct {
	PeerFlowName string
	Progress     []string
}

type NormalizeFlowExecution struct {
	NormalizeFlowState
	executionID string
	logger      log.Logger
}

// NewSyncFlowExecution creates a new instance of SyncFlowExecution.
func NewSyncFlowExecution(ctx workflow.Context, state *SyncFlowState) *SyncFlowExecution {
	return &SyncFlowExecution{
		SyncFlowState: *state,
		executionID:   workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:        workflow.GetLogger(ctx),
	}
}

func NewNormalizeFlowExecution(
	ctx workflow.Context,
	state *NormalizeFlowState,
) *NormalizeFlowExecution {
	return &NormalizeFlowExecution{
		NormalizeFlowState: *state,
		executionID:        workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:             workflow.GetLogger(ctx),
	}
}

// executeSyncFlow executes the sync flow.
func (s *SyncFlowExecution) executeSyncFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	opts *protos.SyncFlowOptions,
) (*model.SyncResponse, error) {
	s.logger.Info("executing sync flow - ", s.PeerFlowName)

	syncMetaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	})

	// execute GetLastSyncedID on destination peer
	lastSyncInput := &protos.GetLastSyncedIDInput{
		PeerConnectionConfig: config.Destination,
		FlowJobName:          s.PeerFlowName,
	}
	lastSyncFuture := workflow.ExecuteActivity(syncMetaCtx, flowable.GetLastSyncedID, lastSyncInput)
	var dstSyncStaste *protos.LastSyncState
	if err := lastSyncFuture.Get(syncMetaCtx, &dstSyncStaste); err != nil {
		return nil, fmt.Errorf("failed to get last synced ID from destination peer: %w", err)
	}

	if dstSyncStaste != nil {
		msg := fmt.Sprintf("last synced ID from destination peer - %d\n", dstSyncStaste.Checkpoint)
		s.logger.Info(msg)
	} else {
		s.logger.Info("no last synced ID from destination peer")
	}

	syncFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		// TODO: activity needs to call heartbeat.
		// see https://github.com/PeerDB-io/nexus/issues/216
		HeartbeatTimeout: 1 * time.Minute,
	})

	// execute StartFlow on the peers to start the flow
	startFlowInput := &protos.StartFlowInput{
		FlowConnectionConfigs: config,
		LastSyncState:         dstSyncStaste,
		SyncFlowOptions:       opts,
	}
	fStartFlow := workflow.ExecuteActivity(syncFlowCtx, flowable.StartFlow, startFlowInput)

	var syncRes *model.SyncResponse
	if err := fStartFlow.Get(syncFlowCtx, &syncRes); err != nil {
		return nil, fmt.Errorf("failed to flow: %w", err)
	}

	return syncRes, nil
}

// SyncFlowWorkflow is the synchronization workflow for a peer flow.
// This workflow assumes that the metadata tables have already been setup,
// and the checkpoint for the source peer is known.
func SyncFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions) (*model.SyncResponse, error) {
	s := NewSyncFlowExecution(ctx, &SyncFlowState{
		PeerFlowName: config.FlowJobName,
		Progress:     []string{},
	})

	return s.executeSyncFlow(ctx, config, options)
}

func NormalizeFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs) (*model.NormalizeResponse, error) {
	s := NewNormalizeFlowExecution(ctx, &NormalizeFlowState{
		PeerFlowName: config.FlowJobName,
		Progress:     []string{},
	})

	return s.executeNormalizeFlow(ctx, config)
}

func (s *NormalizeFlowExecution) executeNormalizeFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs) (*model.NormalizeResponse, error) {
	s.logger.Info("executing normalize flow - ", s.PeerFlowName)

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		// TODO: activity needs to call heartbeat.
		// see https://github.com/PeerDB-io/nexus/issues/216
		HeartbeatTimeout: 1 * time.Minute,
	})

	// execute StartFlow on the peers to start the flow
	startNormalizeInput := &protos.StartNormalizeInput{
		FlowConnectionConfigs: config,
	}
	fStartNormalize := workflow.ExecuteActivity(
		normalizeFlowCtx,
		flowable.StartNormalize,
		startNormalizeInput,
	)

	var normalizeResponse *model.NormalizeResponse
	if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
		return nil, fmt.Errorf("failed to flow: %w", err)
	}

	return normalizeResponse, nil
}
