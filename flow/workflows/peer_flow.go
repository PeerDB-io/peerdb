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
	PeerFlowStatusQuery          = "q-peer-flow-status"
	maxSyncFlowsPerPeerFlow      = 32
	maxNormalizeFlowsPerPeerFlow = 32
)

type PeerFlowLimits struct {
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

type PeerFlowWorkflowInput struct {
	PeerFlowLimits
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
	// SyncFlowSnapshotComplete indicates whether the sync flow snapshot has completed.
	SnapshotComplete bool
	// Errors encountered during child sync flow executions.
	SyncFlowErrors error
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors error
}

// returns a new empty PeerFlowState
func NewStartedPeerFlowState() *PeerFlowState {
	return &PeerFlowState{
		Progress:              []string{"started"},
		SyncFlowStatuses:      nil,
		NormalizeFlowStatuses: nil,
		ActiveSignal:          shared.NoopSignal,
		SetupComplete:         false,
		SnapshotComplete:      false,
		SyncFlowErrors:        nil,
		NormalizeFlowErrors:   nil,
	}
}

// PeerFlowWorkflowExecution represents the state for execution of a peer flow.
type PeerFlowWorkflowExecution struct {
	flowExecutionID string
	logger          log.Logger
}

// NewPeerFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewPeerFlowWorkflowExecution(ctx workflow.Context) *PeerFlowWorkflowExecution {
	return &PeerFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
	}
}

// fetchConnectionConfigs fetches the connection configs for source and destination peers.
func fetchConnectionConfigs(
	ctx workflow.Context,
	logger log.Logger,
	input *PeerFlowWorkflowInput,
) (*protos.FlowConnectionConfigs, error) {
	logger.Info("fetching connection configs for peer flow - ", input.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
	})

	fetchConfigActivityInput := &activities.FetchConfigActivityInput{
		CatalogJdbcURL: input.CatalogJdbcURL,
		PeerFlowName:   input.PeerFlowName,
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

	logger.Info("fetched connection configs for peer flow - ", input.PeerFlowName)
	return flowConnectionConfigs, nil
}

func GetChildWorkflowID(
	ctx workflow.Context,
	prefix string,
	peerFlowName string,
) (string, error) {
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

func PeerFlowWorkflowWithConfig(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	limits *PeerFlowLimits,
	state *PeerFlowState,
) (*PeerFlowWorkflowResult, error) {
	if state == nil {
		state = NewStartedPeerFlowState()
	}

	if cfg == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	w := NewPeerFlowWorkflowExecution(ctx)

	if limits.TotalNormalizeFlows == 0 {
		limits.TotalNormalizeFlows = maxNormalizeFlowsPerPeerFlow
	}

	if limits.TotalSyncFlows == 0 {
		limits.TotalSyncFlows = maxSyncFlowsPerPeerFlow
	}

	// Support a Query for the current state of the peer flow.
	err := workflow.SetQueryHandler(ctx, PeerFlowStatusQuery, func(jobName string) (PeerFlowState, error) {
		return *state, nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", PeerFlowStatusQuery, err)
	}

	signalChan := workflow.GetSignalChannel(ctx, shared.PeerFlowSignalName)
	signalHandler := func(_ workflow.Context, v shared.PeerFlowSignal) {
		w.logger.Info("received signal - ", v)
		state.ActiveSignal = v
	}

	// Support a signal to pause the peer flow.
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
		var signalVal shared.PeerFlowSignal
		c.Receive(ctx, &signalVal)
		signalHandler(ctx, signalVal)
	})

	if !state.SetupComplete {
		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID, err := GetChildWorkflowID(ctx, "setup-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}
		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 2,
			},
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, cfg)
		if err := setupFlowFuture.Get(setupFlowCtx, &cfg); err != nil {
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
		}

		state.SetupComplete = true
		state.Progress = append(state.Progress, "executed setup flow")
	}

	if !state.SnapshotComplete {
		// next part of the setup is to snapshot-initial-copy and setup replication slots.
		snapshotFlowID, err := GetChildWorkflowID(ctx, "snapshot-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}
		childSnapshotFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        snapshotFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 2,
			},
		}
		snapshotFlowCtx := workflow.WithChildOptions(ctx, childSnapshotFlowOpts)
		snapshotFlowFuture := workflow.ExecuteChildWorkflow(snapshotFlowCtx, SnapshotFlowWorkflow, cfg)
		if err := snapshotFlowFuture.Get(snapshotFlowCtx, nil); err != nil {
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
		}

		state.SnapshotComplete = true
		state.Progress = append(state.Progress, "executed snapshot flow")
	}

	syncFlowOptions := &protos.SyncFlowOptions{
		BatchSize: int32(limits.MaxBatchSize),
	}

	currentSyncFlowNum := 0
	currentNormalizeFlowNum := 0

	for {
		// check if the peer flow has been shutdown
		if state.ActiveSignal == shared.ShutdownSignal {
			w.logger.Info("peer flow has been shutdown")
			break
		}

		// check if total sync flows have been completed
		if limits.TotalSyncFlows != 0 && currentSyncFlowNum == limits.TotalSyncFlows {
			w.logger.Info("All the syncflows have completed successfully, there was a"+
				" limit on the number of syncflows to be executed: ", limits.TotalSyncFlows)
			break
		}
		currentSyncFlowNum++

		syncFlowID, err := GetChildWorkflowID(ctx, "sync-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		// execute the sync flow as a child workflow
		childSyncFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        syncFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 2,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childSyncFlowOpts)
		childSyncFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			SyncFlowWorkflow,
			cfg,
			syncFlowOptions,
		)

		selector.AddFuture(childSyncFlowFuture, func(f workflow.Future) {
			var childSyncFlowRes *model.SyncResponse
			if err := f.Get(ctx, &childSyncFlowRes); err != nil {
				w.logger.Error("failed to execute sync flow: ", err)
				state.SyncFlowErrors = multierror.Append(state.SyncFlowErrors, err)
			} else {
				state.SyncFlowStatuses = append(state.SyncFlowStatuses, childSyncFlowRes)
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
		if limits.TotalNormalizeFlows != 0 && currentNormalizeFlowNum == limits.TotalNormalizeFlows {
			w.logger.Info("All the normalizer flows have completed successfully, there was a"+
				" limit on the number of normalizer to be executed: ", limits.TotalNormalizeFlows)
			break
		}
		currentNormalizeFlowNum++

		normalizeFlowID, err := GetChildWorkflowID(ctx, "normalize-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		// execute the normalize flow as a child workflow
		childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        normalizeFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 2,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childNormalizeFlowOpts)
		childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			NormalizeFlowWorkflow,
			cfg,
		)

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
	}

	return nil, workflow.NewContinueAsNewError(ctx, PeerFlowWorkflowWithConfig, cfg, limits, state)
}
