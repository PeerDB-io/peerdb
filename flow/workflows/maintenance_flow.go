package peerflow

import (
	"context"
	"log/slog"
	"time"

	tEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

// RunStartMaintenanceWorkflow is a helper function to start the StartMaintenanceWorkflow with sane defaults
func RunStartMaintenanceWorkflow(
	ctx context.Context,
	temporalClient client.Client,
	input *protos.StartMaintenanceFlowInput,
) (client.WorkflowRun, error) {
	startWorkflowOptions := client.StartWorkflowOptions{
		// This is to ensure that maintenance workflows are deduped
		WorkflowIDReusePolicy:    tEnums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: tEnums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		TaskQueue:                peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue),
	}
	startWorkflowOptions.ID = "start-maintenance"
	if deploymentUid := peerdbenv.PeerDBDeploymentUID(); deploymentUid != "" {
		startWorkflowOptions.ID += "-" + deploymentUid
	}
	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, startWorkflowOptions, StartMaintenanceWorkflow, input)
	if err != nil {
		return nil, err
	}
	return workflowRun, nil
}

// RunEndMaintenanceWorkflow is a helper function to start the EndMaintenanceWorkflow with sane defaults
func RunEndMaintenanceWorkflow(
	ctx context.Context,
	temporalClient client.Client,
	input *protos.EndMaintenanceFlowInput,
) (client.WorkflowRun, error) {
	startWorkflowOptions := client.StartWorkflowOptions{
		// This is to ensure that maintenance workflows are deduped
		WorkflowIDReusePolicy:    tEnums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: tEnums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		TaskQueue:                peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue),
	}
	startWorkflowOptions.ID = "end-maintenance"
	if deploymentUid := peerdbenv.PeerDBDeploymentUID(); deploymentUid != "" {
		startWorkflowOptions.ID += "-" + deploymentUid
	}

	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, startWorkflowOptions, EndMaintenanceWorkflow, &protos.EndMaintenanceFlowInput{})
	if err != nil {
		return nil, err
	}
	return workflowRun, nil
}

func StartMaintenanceWorkflow(ctx workflow.Context, input *protos.StartMaintenanceFlowInput) (*protos.StartMaintenanceFlowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting StartMaintenance workflow", "input", input)
	defer runBackgroundAlerter(ctx)()

	maintenanceFlowOutput, err := startMaintenance(ctx, logger)
	if err != nil {
		slog.Error("Error in StartMaintenance workflow", "error", err)
		return nil, err
	}
	return maintenanceFlowOutput, nil
}

func startMaintenance(ctx workflow.Context, logger log.Logger) (*protos.StartMaintenanceFlowOutput, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
	})

	snapshotWaitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    1 * time.Minute,
	})
	waitSnapshotsFuture := workflow.ExecuteActivity(snapshotWaitCtx,
		maintenance.WaitForRunningSnapshots,
	)
	err := waitSnapshotsFuture.Get(snapshotWaitCtx, nil)
	if err != nil {
		return nil, err
	}

	enableMaintenanceFuture := workflow.ExecuteActivity(ctx, maintenance.EnableMaintenanceMode)
	err = enableMaintenanceFuture.Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	logger.Info("Waiting for all snapshot mirrors to finish snapshotting")
	waitSnapshotsPostEnableFuture := workflow.ExecuteActivity(snapshotWaitCtx,
		maintenance.WaitForRunningSnapshots,
	)
	err = waitSnapshotsPostEnableFuture.Get(snapshotWaitCtx, nil)
	if err != nil {
		return nil, err
	}

	mirrorsList, err := getAllMirrors(ctx)
	if err != nil {
		return nil, err
	}

	runningMirrors, err := pauseAndGetRunningMirrors(ctx, mirrorsList, logger)
	if err != nil {
		return nil, err
	}

	future := workflow.ExecuteActivity(ctx, maintenance.BackupAllPreviouslyRunningFlows, runningMirrors)
	err = future.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	logger.Info("StartMaintenance workflow completed")
	return &protos.StartMaintenanceFlowOutput{
		Version: peerdbenv.PeerDBVersionShaShort(),
	}, nil
}

func pauseAndGetRunningMirrors(
	ctx workflow.Context,
	mirrorsList activities.MaintenanceMirrorsInfo,
	logger log.Logger,
) (activities.MaintenanceMirrorsInfo, error) {
	selector := workflow.NewSelector(ctx)
	runningMirrors := make([]bool, len(mirrorsList.Mirrors))
	for i, mirror := range mirrorsList.Mirrors {
		activityInput := mirror
		f := workflow.ExecuteActivity(
			ctx,
			maintenance.PauseMirrorIfRunning,
			activityInput,
		)

		selector.AddFuture(f, func(f workflow.Future) {
			var wasRunning bool
			err := f.Get(ctx, &wasRunning)
			if err != nil {
				logger.Error("Error checking and pausing mirror", "mirror", mirror, "error", err)
			} else {
				logger.Info("Finished check and pause for mirror", "mirror", mirror, "wasRunning", wasRunning)
				runningMirrors[i] = wasRunning
			}
			ctx.Done()
		})
	}
	onlyRunningMirrors := make([]activities.MaintenanceMirrorInfoItem, 0)
	for range mirrorsList.Mirrors {
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return activities.MaintenanceMirrorsInfo{}, err
		}
	}
	for i, mirror := range mirrorsList.Mirrors {
		if runningMirrors[i] {
			onlyRunningMirrors = append(onlyRunningMirrors, mirror)
		}
	}
	return activities.MaintenanceMirrorsInfo{
		Mirrors: onlyRunningMirrors,
	}, nil
}

func getAllMirrors(ctx workflow.Context) (activities.MaintenanceMirrorsInfo, error) {
	getMirrorsFuture := workflow.ExecuteActivity(ctx, maintenance.GetAllMirrors)
	var mirrorsList activities.MaintenanceMirrorsInfo
	err := getMirrorsFuture.Get(ctx, &mirrorsList)
	return mirrorsList, err
}

func EndMaintenanceWorkflow(ctx workflow.Context, input *protos.EndMaintenanceFlowInput) (*protos.EndMaintenanceFlowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting EndMaintenance workflow", "input", input)
	defer runBackgroundAlerter(ctx)()

	flowOutput, err := endMaintenance(ctx, logger)
	if err != nil {
		slog.Error("Error in EndMaintenance workflow", "error", err)
		return nil, err
	}
	return flowOutput, nil
}

func endMaintenance(ctx workflow.Context, logger log.Logger) (*protos.EndMaintenanceFlowOutput, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    1 * time.Minute,
	})

	mirrorsList, err := resumeBackedUpMirrors(ctx, logger)
	if err != nil {
		return nil, err
	}

	clearBackupsFuture := workflow.ExecuteActivity(ctx, maintenance.CleanupBackupedFlows)
	err = clearBackupsFuture.Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	logger.Info("Resumed backed up mirrors", "mirrors", mirrorsList)

	future := workflow.ExecuteActivity(ctx, maintenance.DisableMaintenanceMode)
	err = future.Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	logger.Info("EndMaintenance workflow completed")
	return &protos.EndMaintenanceFlowOutput{
		Version: peerdbenv.PeerDBVersionShaShort(),
	}, nil
}

func resumeBackedUpMirrors(ctx workflow.Context, logger log.Logger) (activities.MaintenanceMirrorsInfo, error) {
	future := workflow.ExecuteActivity(ctx, maintenance.GetBackedUpFlows)
	var mirrorsList activities.MaintenanceMirrorsInfo
	err := future.Get(ctx, &mirrorsList)
	if err != nil {
		return activities.MaintenanceMirrorsInfo{}, err
	}

	selector := workflow.NewSelector(ctx)
	for _, mirror := range mirrorsList.Mirrors {
		activityInput := mirror
		f := workflow.ExecuteActivity(
			ctx,
			maintenance.ResumeMirror,
			activityInput,
		)

		selector.AddFuture(f, func(f workflow.Future) {
			err := f.Get(ctx, nil)
			if err != nil {
				logger.Error("Error resuming mirror", "mirror", mirror, "error", err)
			} else {
				logger.Info("Finished resuming mirror", "mirror", mirror)
			}
			ctx.Done()
		})
	}

	for range mirrorsList.Mirrors {
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return activities.MaintenanceMirrorsInfo{}, err
		}
	}
	return mirrorsList, nil
}

// runBackgroundAlerter Alerts every few minutes regarding currently running maintenance workflows
func runBackgroundAlerter(ctx workflow.Context) workflow.CancelFunc {
	activityCtx, cancelActivity := workflow.WithCancel(ctx)
	alerterCtx := workflow.WithActivityOptions(activityCtx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    1 * time.Minute,
	})
	workflow.ExecuteActivity(alerterCtx, maintenance.BackgroundAlerter)
	return cancelActivity
}
