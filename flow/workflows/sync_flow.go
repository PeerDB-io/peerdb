package peerflow

import (
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

type SyncFlowState struct {
	CDCFlowName string
	Progress    []string
}

type SyncFlowExecution struct {
	SyncFlowState
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

// executeSyncFlow executes the sync flow.
func (s *SyncFlowExecution) executeSyncFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	opts *protos.SyncFlowOptions,
	relationMessageMapping model.RelationMessageMapping,
) (*model.SyncResponse, error) {
	s.logger.Info("executing sync flow", slog.String("flowName", s.CDCFlowName))

	startFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 72 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	// execute StartFlow on the peers to start the flow
	startFlowInput := &protos.StartFlowInput{
		FlowConnectionConfigs:  config,
		SyncFlowOptions:        opts,
		RelationMessageMapping: relationMessageMapping,
		SrcTableIdNameMapping:  opts.SrcTableIdNameMapping,
		TableNameSchemaMapping: opts.TableNameSchemaMapping,
	}
	fStartFlow := workflow.ExecuteActivity(startFlowCtx, flowable.StartFlow, startFlowInput)

	var syncRes *model.SyncResponse
	if err := fStartFlow.Get(startFlowCtx, &syncRes); err != nil {
		return nil, fmt.Errorf("failed to flow: %w", err)
	}
	return syncRes, nil
}

// SyncFlowWorkflow is the synchronization workflow for a peer flow.
// This workflow assumes that the metadata tables have already been setup,
// and the checkpoint for the source peer is known.
func SyncFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
) (*model.SyncResponse, error) {
	s := NewSyncFlowExecution(ctx, &SyncFlowState{
		CDCFlowName: config.FlowJobName,
		Progress:    []string{},
	})
	return s.executeSyncFlow(ctx, config, options, options.RelationMessageMapping)
}
