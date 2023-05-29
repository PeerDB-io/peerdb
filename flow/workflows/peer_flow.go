package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	PeerFlowStatusQuery = "q-peer-flow-status"
)

type PeerFlowWorkflowInput struct {
	// The JDBC URL for the catalog database.
	CatalogJdbcURL string
	// The name of the peer flow to execute.
	PeerFlowName string
	// Number of sync flows to execute in total.
	// If 0, the number of sync flows will be continuously executed until the peer flow is cancelled.
	// This is typically non-zero for testing purposes.
	TotalSyncFlows int
	// Number of normalize flows to execute in total.
	// If 0, the number of sync flows will be continuously executed until the peer flow is cancelled.
	// This is typically non-zero for testing purposes.
	TotalNormalizeFlows int
	// Maximum number of rows in a sync flow batch.
	MaxBatchSize int
}

type PeerFlowState struct {
	// Input for the PeerFlowWorkflow.
	PeerFlowWorkflowInput
	// Progress events for the peer flow.
	Progress []string
	// Accumulates status for sync flows spawned.
	SyncFlowStatuses []*model.SyncResponse
	// Accumulates status for sync flows spawned.
	NormalizeFlowStatuses []*model.NormalizeResponse
	// Current signalled state of the peer flow.
	ActiveSignal shared.PeerFlowSignal
	// SetupComplete indicates whether the peer flow setup has completed.
	SetupComplete bool
	// Errors encountered during child sync flow executions.
	SyncFlowErrors error
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors error
}

// PeerFlowWorkflowExecution represents the state for execution of a peer flow.
type PeerFlowWorkflowExecution struct {
	// The state of the peer flow.
	PeerFlowState
	flowExecutionID string
	logger          log.Logger
}

// NewPeerFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewPeerFlowWorkflowExecution(ctx workflow.Context, state *PeerFlowState) *PeerFlowWorkflowExecution {
	return &PeerFlowWorkflowExecution{
		PeerFlowState:   *state,
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
	}
}

// fetchConnectionConfigs fetches the connection configs for source and destination peers.
func (w *PeerFlowWorkflowExecution) fetchConnectionConfigs(
	ctx workflow.Context) (*protos.FlowConnectionConfigs, error) {
	w.logger.Info("fetching connection configs for peer flow - ", w.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	fetchConfigActivityInput := &activities.FetchConfigActivityInput{
		CatalogJdbcURL: w.CatalogJdbcURL,
		PeerFlowName:   w.PeerFlowName,
	}

	configsFuture := workflow.ExecuteActivity(ctx, fetchConfig.FetchConfig, fetchConfigActivityInput)

	flowConnectionConfigs := &protos.FlowConnectionConfigs{}
	if err := configsFuture.Get(ctx, &flowConnectionConfigs); err != nil {
		return nil, fmt.Errorf("failed to fetch connection configs: %w", err)
	}

	if flowConnectionConfigs == nil ||
		flowConnectionConfigs.Source == nil ||
		flowConnectionConfigs.Destination == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	w.logger.Info("fetched connection configs for peer flow - ", w.PeerFlowName)
	return flowConnectionConfigs, nil
}

// getChildWorkflowID returns the child workflow ID for a new sync flow.
func (w *PeerFlowWorkflowExecution) getChildWorkflowID(
	ctx workflow.Context,
	prefix string,
	peerFlowName string) (string, error) {
	childWorkflowIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("%s-%s-%s", prefix, peerFlowName, uuid.New().String())
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		return "", fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	return childWorkflowID, nil
}

// PeerFlowWorkflowResult is the result of the PeerFlowWorkflow.
type PeerFlowWorkflowResult = PeerFlowState

// PeerFlowWorkflow is the workflow that executes the specified peer flow.
// This is the main entry point for the application.
func PeerFlowWorkflow(ctx workflow.Context, input *PeerFlowWorkflowInput) (*PeerFlowWorkflowResult, error) {
	w := NewPeerFlowWorkflowExecution(ctx, &PeerFlowState{
		PeerFlowWorkflowInput: *input,
		Progress:              []string{"started"},
		SyncFlowStatuses:      []*model.SyncResponse{},
		NormalizeFlowStatuses: []*model.NormalizeResponse{},
		SetupComplete:         false,
		ActiveSignal:          shared.NoopSignal,
		SyncFlowErrors:        nil,
	})

	// Support a Query for the current state of the peer flow.
	err := workflow.SetQueryHandler(ctx, PeerFlowStatusQuery, func() (PeerFlowState, error) {
		return w.PeerFlowState, nil
	})
	if err != nil {
		return &w.PeerFlowState, fmt.Errorf("failed to set `%s` query handler: %w", PeerFlowStatusQuery, err)
	}

	selector := workflow.NewSelector(ctx)

	signalChan := workflow.GetSignalChannel(ctx, shared.PeerFlowSignalName)
	signalHandler := func(_ workflow.Context, v shared.PeerFlowSignal) {
		w.logger.Info("received signal - ", v)
		w.PeerFlowState.ActiveSignal = v
	}

	// Support a signal to pause the peer flow.
	selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
		var signalVal shared.PeerFlowSignal
		c.Receive(ctx, &signalVal)
		signalHandler(ctx, signalVal)
	})

	// Fetch the connection configs for the source and destination peers.
	flowConnectionConfigs, err := w.fetchConnectionConfigs(ctx)
	if err != nil {
		return &w.PeerFlowState, err
	}
	flowConnectionConfigs.FlowJobName = w.PeerFlowName
	w.Progress = append(w.Progress, "fetched connection configs")

	{
		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID, err := w.getChildWorkflowID(ctx, "setup-flow", w.PeerFlowName)
		if err != nil {
			return &w.PeerFlowState, err
		}
		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 10,
			},
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, flowConnectionConfigs)
		if err := setupFlowFuture.Get(setupFlowCtx, &flowConnectionConfigs.TableSchema); err != nil {
			return &w.PeerFlowState, fmt.Errorf("failed to execute child workflow: %w", err)
		}

		w.SetupComplete = true
		w.Progress = append(w.Progress, "executed setup flow")
	}

	syncFlowOptions := &protos.SyncFlowOptions{
		BatchSize: int32(input.MaxBatchSize),
	}

	currentFlowNumber := 0
	currentNormalizeNumber := 0

	for {
		// check if the peer flow has been shutdown
		if w.PeerFlowState.ActiveSignal == shared.ShutdownSignal {
			w.logger.Info("peer flow has been shutdown")
			break
		}

		/*
			SyncFlow - sync raw cdc changes from source to target.
			SyncFlow will always be running, even when Initial Load is going on.
		*/
		// check if total sync flows have been completed
		if input.TotalSyncFlows != 0 && currentFlowNumber == input.TotalSyncFlows {
			w.logger.Info("All the syncflows have completed successfully, there was a"+
				" limit on the number of syncflows to be executed: ", input.TotalSyncFlows)
			break
		}
		currentFlowNumber++

		syncFlowID, err := w.getChildWorkflowID(ctx, "sync-flow", w.PeerFlowName)
		if err != nil {
			return &w.PeerFlowState, err
		}

		// execute the sync flow as a child workflow
		childSyncFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        syncFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 10,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childSyncFlowOpts)
		childSyncFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			SyncFlowWorkflow,
			flowConnectionConfigs,
			syncFlowOptions,
		)

		selector.AddFuture(childSyncFlowFuture, func(f workflow.Future) {
			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				w.logger.Error("failed to execute sync flow: ", err)
				w.SyncFlowErrors = multierror.Append(w.SyncFlowErrors, err)
			} else {
				w.SyncFlowStatuses = append(w.SyncFlowStatuses, childSyncFlowRes)
			}
		})
		selector.Select(ctx)

		/*
			NormalizeFlow - normalize raw changes on target to final table
			SyncFlow and NormalizeFlow are independent.
			TODO -
			1. Currently NormalizeFlow runs right after SyncFlow. We need to make it asynchronous
			NormalizeFlow will start only after Initial Load
		*/
		if input.TotalNormalizeFlows != 0 && currentNormalizeNumber == input.TotalNormalizeFlows {
			w.logger.Info("All the normalizer flows have completed successfully, there was a"+
				" limit on the number of normalizer to be executed: ", input.TotalNormalizeFlows)
			break
		}
		currentNormalizeNumber++

		normalizeFlowID, err := w.getChildWorkflowID(ctx, "normalize-flow", w.PeerFlowName)
		if err != nil {
			return &w.PeerFlowState, err
		}

		// execute the normalize flow as a child workflow
		childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        normalizeFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 10,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childNormalizeFlowOpts)
		childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			NormalizeFlowWorkflow,
			flowConnectionConfigs,
		)

		selector.AddFuture(childNormalizeFlowFuture, func(f workflow.Future) {
			var childNormalizeFlowRes *model.NormalizeResponse
			if err := f.Get(ctx, &childNormalizeFlowRes); err != nil {
				w.logger.Error("failed to execute normalize flow: ", err)
				w.SyncFlowErrors = multierror.Append(w.SyncFlowErrors, err)
			} else {
				w.NormalizeFlowStatuses = append(w.NormalizeFlowStatuses, childNormalizeFlowRes)
			}
		})
		selector.Select(ctx)
	}

	return &w.PeerFlowState, w.SyncFlowErrors
}
