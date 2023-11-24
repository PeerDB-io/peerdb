package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/hashicorp/go-multierror"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type NormalizeFlowState struct {
	CDCFlowName string
	Progress    []string
}

type NormalizeFlowExecution struct {
	NormalizeFlowState
	executionID string
	logger      log.Logger
}

func NewNormalizeFlowExecution(ctx workflow.Context, state *NormalizeFlowState) *NormalizeFlowExecution {
	return &NormalizeFlowExecution{
		NormalizeFlowState: *state,
		executionID:        workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:             workflow.GetLogger(ctx),
	}
}

func NormalizeFlowWorkflow(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	if state == nil {
		state = NewCDCFlowWorkflowState()
	}

	w := NewCDCFlowWorkflowExecution(ctx)

	normalizeFlowID, err := GetChildWorkflowID(ctx, "normalize-flow", cfg.FlowJobName)
	if err != nil {
		return state, err
	}

	childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        normalizeFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
	}
	ctx = workflow.WithChildOptions(ctx, childNormalizeFlowOpts)

	/* TODO LISTEN FOR TABLE SCHEMA DELTAS */
	var tableSchemaDeltas []*protos.TableSchemaDelta = nil
	/*
		if childSyncFlowRes != nil {
			tableSchemaDeltas = childSyncFlowRes.TableSchemaDeltas
		}
	*/

	// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
	if tableSchemaDeltas != nil {
		modifiedSrcTables := make([]string, 0, len(tableSchemaDeltas))
		modifiedDstTables := make([]string, 0, len(tableSchemaDeltas))

		for _, tableSchemaDelta := range tableSchemaDeltas {
			modifiedSrcTables = append(modifiedSrcTables, tableSchemaDelta.SrcTableName)
			modifiedDstTables = append(modifiedDstTables, tableSchemaDelta.DstTableName)
		}

		getModifiedSchemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
		})
		getModifiedSchemaFuture := workflow.ExecuteActivity(getModifiedSchemaCtx, flowable.GetTableSchema,
			&protos.GetTableSchemaBatchInput{
				PeerConnectionConfig: cfg.Source,
				TableIdentifiers:     modifiedSrcTables,
			})

		var getModifiedSchemaRes *protos.GetTableSchemaBatchOutput
		if err := getModifiedSchemaFuture.Get(ctx, &getModifiedSchemaRes); err != nil {
			w.logger.Error("failed to execute schema update at source: ", err)
			state.SyncFlowErrors = multierror.Append(state.SyncFlowErrors, err)
		} else {
			for i := range modifiedSrcTables {
				cfg.TableNameSchemaMapping[modifiedDstTables[i]] =
					getModifiedSchemaRes.TableNameSchemaMapping[modifiedSrcTables[i]]
			}
		}
	}

	childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
		ctx,
		NormalizeFlowWorkflow,
		cfg,
	)

	selector := workflow.NewSelector(ctx)
	selector.AddFuture(childNormalizeFlowFuture, func(f workflow.Future) {
		var childNormalizeFlowRes *model.NormalizeResponse
		if err := f.Get(ctx, &childNormalizeFlowRes); err != nil {
			w.logger.Error("failed to execute normalize flow: ", err)
			state.NormalizeFlowErrors = multierror.Append(state.NormalizeFlowErrors, err)
		} else {
			state.NormalizeFlowStatuses = append(state.NormalizeFlowStatuses, childNormalizeFlowRes)
		}
	})
	selector.Select(ctx)

	s := NewNormalizeFlowExecution(ctx, &NormalizeFlowState{
		CDCFlowName: cfg.FlowJobName,
		Progress:    []string{},
	})

	s.logger.Info("executing normalize flow - ", s.CDCFlowName)

	normalizeFlowCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 7 * 24 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
	})

	// execute StartFlow on the peers to start the flow
	startNormalizeInput := &protos.StartNormalizeInput{
		FlowConnectionConfigs: cfg,
	}
	fStartNormalize := workflow.ExecuteActivity(normalizeFlowCtx, flowable.StartNormalize, startNormalizeInput)

	var normalizeResponse *model.NormalizeResponse
	if err := fStartNormalize.Get(normalizeFlowCtx, &normalizeResponse); err != nil {
		return nil, fmt.Errorf("failed to flow: %w", err)
	}

	return state, nil
}
