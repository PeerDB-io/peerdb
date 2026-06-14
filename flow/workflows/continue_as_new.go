package peerflow

import (
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

// Continue-As-New payloads are the one place workflow inputs persist across a
// deploy, so legacy string identifiers are refilled alongside the structs before
// continuing: a rolled-back pre-QualifiedTable release reads only the strings,
// while current code re-normalizes at workflow entry (struct wins), making the
// refill a no-op for it. Denormalization is pure, so this is replay-safe.

func continueAsNewCDCFlow(ctx workflow.Context, cfg *protos.FlowConnectionConfigsCore, state *cdc_state.CDCFlowWorkflowState) error {
	internal.DenormalizeFlowConfigCore(cfg)
	if state != nil {
		internal.DenormalizeSyncFlowOptions(state.SyncFlowOptions)
		internal.DenormalizeDropFlowInput(state.DropFlowInput)
	}
	return workflow.NewContinueAsNewError(ctx, CDCFlowWorkflow, cfg, state)
}

func continueAsNewDropFlow(ctx workflow.Context, input *protos.DropFlowInput) error {
	internal.DenormalizeDropFlowInput(input)
	return workflow.NewContinueAsNewError(ctx, DropFlowWorkflow, input)
}

func continueAsNewQRepFlow(ctx workflow.Context, config *protos.QRepConfig, state *protos.QRepFlowState) error {
	internal.DenormalizeQRepConfig(config)
	return workflow.NewContinueAsNewError(ctx, QRepFlowWorkflow, config, state)
}

func continueAsNewXminFlow(ctx workflow.Context, config *protos.QRepConfig, state *protos.QRepFlowState) error {
	internal.DenormalizeQRepConfig(config)
	return workflow.NewContinueAsNewError(ctx, XminFlowWorkflow, config, state)
}

func continueAsNewQRepWaitForNewRows(ctx workflow.Context, config *protos.QRepConfig, lastPartition *protos.QRepPartition) error {
	internal.DenormalizeQRepConfig(config)
	return workflow.NewContinueAsNewError(ctx, QRepWaitForNewRowsWorkflow, config, lastPartition)
}
