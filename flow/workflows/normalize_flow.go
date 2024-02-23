package peerflow

import (
	"log/slog"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type NormalizeState struct {
	Wait                   bool
	StopLoop               bool
	LastSyncBatchID        int64
	SyncBatchID            int64
	TableNameSchemaMapping map[string]*protos.TableSchema
}

// returns reason string when workflow should exit
// signals are flushed when ProcessLoop returns
func ProcessLoop(ctx workflow.Context, logger log.Logger, selector workflow.Selector, state *NormalizeState) string {
	canceled := ctx.Err() != nil
	for !canceled && selector.HasPending() {
		selector.Select(ctx)
		canceled = ctx.Err() != nil
	}

	if canceled {
		return "normalize canceled"
	} else if state.StopLoop && state.LastSyncBatchID == state.SyncBatchID {
		return "normalize finished"
	}
	return ""
}

func NormalizeFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	state *NormalizeState,
) error {
	parent := workflow.GetInfo(ctx).ParentWorkflowExecution
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	if state == nil {
		state = &NormalizeState{
			Wait:                   true,
			StopLoop:               false,
			LastSyncBatchID:        -1,
			SyncBatchID:            -1,
			TableNameSchemaMapping: nil,
		}
	}

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	syncChan := model.NormalizeSignal.GetSignalChannel(ctx)

	selector := workflow.NewNamedSelector(ctx, "NormalizeLoop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	syncChan.AddToSelector(selector, func(s model.NormalizePayload, _ bool) {
		if s.Done {
			state.StopLoop = true
		}
		if s.SyncBatchID > state.SyncBatchID {
			state.SyncBatchID = s.SyncBatchID
		}
		state.TableNameSchemaMapping = s.TableNameSchemaMapping
		state.Wait = false
	})

	for state.Wait && ctx.Err() == nil {
		selector.Select(ctx)
	}
	if exit := ProcessLoop(ctx, logger, selector, state); exit != "" {
		logger.Info(exit)
		return ctx.Err()
	}

	if state.LastSyncBatchID != state.SyncBatchID {
		state.LastSyncBatchID = state.SyncBatchID

		logger.Info("executing normalize")
		startNormalizeInput := &protos.StartNormalizeInput{
			FlowConnectionConfigs:  config,
			TableNameSchemaMapping: state.TableNameSchemaMapping,
			SyncBatchID:            state.SyncBatchID,
		}
		fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

		var normalizeResponse *model.NormalizeResponse
		if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
			model.NormalizeErrorSignal.SignalExternalWorkflow(
				ctx,
				parent.ID,
				"",
				err.Error(),
			).Get(ctx, nil)
		} else if normalizeResponse != nil {
			model.NormalizeResultSignal.SignalExternalWorkflow(
				ctx,
				parent.ID,
				"",
				*normalizeResponse,
			).Get(ctx, nil)
		}
	}

	if !peerdbenv.PeerDBEnableParallelSyncNormalize() {
		model.NormalizeDoneSignal.SignalExternalWorkflow(
			ctx,
			parent.ID,
			"",
			struct{}{},
		).Get(ctx, nil)
	}

	state.Wait = true
	if exit := ProcessLoop(ctx, logger, selector, state); exit != "" {
		logger.Info(exit)
		return ctx.Err()
	}
	return workflow.NewContinueAsNewError(ctx, NormalizeFlowWorkflow, config, state)
}
