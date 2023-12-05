package peerflow

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/hashicorp/go-multierror"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type NormalizeFlowExecution struct {
	executionID string
	logger      log.Logger
}

type NormalizeFlowResult struct {
	NormalizeFlowStatuses []*model.NormalizeResponse
	NormalizeFlowErrors   error
}

func NewNormalizeFlowExecution(ctx workflow.Context) *NormalizeFlowExecution {
	return &NormalizeFlowExecution{
		executionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:      workflow.GetLogger(ctx),
	}
}

func NormalizeFlowWorkflow(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	syncNormChan workflow.Channel,
) (*NormalizeFlowResult, error) {
	w := NewCDCFlowWorkflowExecution(ctx)

	res := NormalizeFlowResult{}

	normalizeFlowID, err := GetChildWorkflowID(ctx, "normalize-flow", cfg.FlowJobName)
	if err != nil {
		return nil, err
	}

	childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
		WorkflowID:        normalizeFlowID,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 20,
		},
	}
	ctx = workflow.WithChildOptions(ctx, childNormalizeFlowOpts)

	for {
		var tableSchemaDeltas []*protos.TableSchemaDelta
		if !syncNormChan.Receive(ctx, &tableSchemaDeltas) {
			break
		}

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
				res.NormalizeFlowErrors = multierror.Append(res.NormalizeFlowErrors, err)
			} else {
				for i := range modifiedSrcTables {
					cfg.TableNameSchemaMapping[modifiedDstTables[i]] =
						getModifiedSchemaRes.TableNameSchemaMapping[modifiedSrcTables[i]]
				}
			}
		}

		s := NewNormalizeFlowExecution(ctx)

		s.logger.Info("executing normalize flow - ", cfg.FlowJobName)

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
			res.NormalizeFlowErrors = multierror.Append(res.NormalizeFlowErrors, err)
		} else {
			res.NormalizeFlowStatuses = append(res.NormalizeFlowStatuses, normalizeResponse)
		}
	}

	return &res, nil
}
