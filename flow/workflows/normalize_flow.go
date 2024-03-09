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
	Stop                   bool
	LastSyncBatchID        int64
	SyncBatchID            int64
	TableNameSchemaMapping map[string]*protos.TableSchema
}

func NewNormalizeState() *NormalizeState {
	return &NormalizeState{
		Wait:                   true,
		Stop:                   false,
		LastSyncBatchID:        -1,
		SyncBatchID:            -1,
		TableNameSchemaMapping: nil,
	}
}

// returns whether workflow should finish
// signals are flushed when ProcessLoop returns
func ProcessLoop(ctx workflow.Context, logger log.Logger, selector workflow.Selector, state *NormalizeState) bool {
	for ctx.Err() == nil && selector.HasPending() {
		selector.Select(ctx)
	}

	if ctx.Err() != nil {
		logger.Info("normalize canceled")
		return true
	} else if state.Stop && state.LastSyncBatchID == state.SyncBatchID {
		logger.Info("normalize finished")
		return true
	}
	return false
}

func NormalizeFlowWorkflow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	state *NormalizeState,
) error {
	parent := workflow.GetInfo(ctx).ParentWorkflowExecution
	logger := log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	if state == nil {
		state = NewNormalizeState()
	}

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	selector := workflow.NewNamedSelector(ctx, "NormalizeLoop")
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {})
	model.NormalizeSignal.GetSignalChannel(ctx).AddToSelector(selector, func(s model.NormalizePayload, _ bool) {
		if s.Done {
			state.Stop = true
		}
		if s.SyncBatchID > state.SyncBatchID {
			state.SyncBatchID = s.SyncBatchID
		}
		if s.TableNameSchemaMapping != nil {
			state.TableNameSchemaMapping = s.TableNameSchemaMapping
		}

		state.Wait = false
	})

	for state.Wait && ctx.Err() == nil {
		selector.Select(ctx)
	}
	if ProcessLoop(ctx, logger, selector, state) {
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
			logger.Info("Normalize errored", slog.Any("error", err))
		} else if normalizeResponse != nil {
			logger.Info("Normalize finished", slog.Any("result", normalizeResponse))
		}
	}

	if ctx.Err() == nil && !state.Stop {
		parallel := GetSideEffect(ctx, func(_ workflow.Context) bool {
			return peerdbenv.PeerDBEnableParallelSyncNormalize()
		})

		if !parallel {
			_ = model.NormalizeDoneSignal.SignalExternalWorkflow(
				ctx,
				parent.ID,
				"",
				struct{}{},
			).Get(ctx, nil)
		}
	}

	state.Wait = true
	if ProcessLoop(ctx, logger, selector, state) {
		return ctx.Err()
	}
	return workflow.NewContinueAsNewError(ctx, NormalizeFlowWorkflow, config, state)
}
