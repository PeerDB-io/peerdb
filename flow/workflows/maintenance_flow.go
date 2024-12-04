package peerflow

import (
	"context"
	"log/slog"
	"time"

	tEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

func getMaintenanceWorkflowOptions(workflowIDPrefix string, taskQueueId shared.TaskQueueID) client.StartWorkflowOptions {
	maintenanceWorkflowOptions := client.StartWorkflowOptions{
		WorkflowIDReusePolicy:    tEnums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: tEnums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		TaskQueue:                peerdbenv.PeerFlowTaskQueueName(taskQueueId),
		ID:                       workflowIDPrefix,
	}
	if deploymentUid := peerdbenv.PeerDBDeploymentUID(); deploymentUid != "" {
		maintenanceWorkflowOptions.ID += "-" + deploymentUid
	}
	return maintenanceWorkflowOptions
}

// RunStartMaintenanceWorkflow is a helper function to start the StartMaintenanceWorkflow with sane defaults
func RunStartMaintenanceWorkflow(
	ctx context.Context,
	temporalClient client.Client,
	input *protos.StartMaintenanceFlowInput,
	taskQueueId shared.TaskQueueID,
) (client.WorkflowRun, error) {
	workflowOptions := getMaintenanceWorkflowOptions("start-maintenance", taskQueueId)
	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, workflowOptions, StartMaintenanceWorkflow, input)
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
	taskQueueId shared.TaskQueueID,
) (client.WorkflowRun, error) {
	workflowOptions := getMaintenanceWorkflowOptions("end-maintenance", taskQueueId)
	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, workflowOptions, EndMaintenanceWorkflow, &protos.EndMaintenanceFlowInput{})
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

	enableCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	enableMaintenanceFuture := workflow.ExecuteActivity(enableCtx, maintenance.EnableMaintenanceMode)

	if err := enableMaintenanceFuture.Get(enableCtx, nil); err != nil {
		return nil, err
	}

	logger.Info("Waiting for all snapshot mirrors to finish snapshotting")
	waitSnapshotsPostEnableFuture := workflow.ExecuteActivity(snapshotWaitCtx,
		maintenance.WaitForRunningSnapshots,
	)

	if err := waitSnapshotsPostEnableFuture.Get(snapshotWaitCtx, nil); err != nil {
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

	backupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	})
	future := workflow.ExecuteActivity(backupCtx, maintenance.BackupAllPreviouslyRunningFlows, runningMirrors)

	if err := future.Get(backupCtx, nil); err != nil {
		return nil, err
	}
	version, err := GetPeerDBVersion(ctx)
	if err != nil {
		return nil, err
	}
	logger.Info("StartMaintenance workflow completed", "version", version)
	return &protos.StartMaintenanceFlowOutput{
		Version: version,
	}, nil
}

func pauseAndGetRunningMirrors(
	ctx workflow.Context,
	mirrorsList *protos.MaintenanceMirrors,
	logger log.Logger,
) (*protos.MaintenanceMirrors, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    1 * time.Minute,
	})
	selector := workflow.NewSelector(ctx)
	runningMirrors := make([]bool, len(mirrorsList.Mirrors))
	for i, mirror := range mirrorsList.Mirrors {
		f := workflow.ExecuteActivity(
			ctx,
			maintenance.PauseMirrorIfRunning,
			mirror,
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
		})
	}
	onlyRunningMirrors := make([]*protos.MaintenanceMirror, 0, len(mirrorsList.Mirrors))
	for range mirrorsList.Mirrors {
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	for i, mirror := range mirrorsList.Mirrors {
		if runningMirrors[i] {
			onlyRunningMirrors = append(onlyRunningMirrors, mirror)
		}
	}
	return &protos.MaintenanceMirrors{
		Mirrors: onlyRunningMirrors,
	}, nil
}

func getAllMirrors(ctx workflow.Context) (*protos.MaintenanceMirrors, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	})
	getMirrorsFuture := workflow.ExecuteActivity(ctx, maintenance.GetAllMirrors)
	var mirrorsList protos.MaintenanceMirrors
	err := getMirrorsFuture.Get(ctx, &mirrorsList)
	return &mirrorsList, err
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

	clearBackupsFuture := workflow.ExecuteActivity(ctx, maintenance.CleanBackedUpFlows)
	if err := clearBackupsFuture.Get(ctx, nil); err != nil {
		return nil, err
	}

	logger.Info("Resumed backed up mirrors", "mirrors", mirrorsList)

	disableCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	future := workflow.ExecuteActivity(disableCtx, maintenance.DisableMaintenanceMode)
	if err := future.Get(disableCtx, nil); err != nil {
		return nil, err
	}
	logger.Info("Disabled maintenance mode")
	version, err := GetPeerDBVersion(ctx)
	if err != nil {
		return nil, err
	}

	logger.Info("EndMaintenance workflow completed", "version", version)
	return &protos.EndMaintenanceFlowOutput{
		Version: version,
	}, nil
}

func resumeBackedUpMirrors(ctx workflow.Context, logger log.Logger) (*protos.MaintenanceMirrors, error) {
	future := workflow.ExecuteActivity(ctx, maintenance.GetBackedUpFlows)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	var mirrorsList *protos.MaintenanceMirrors
	err := future.Get(ctx, &mirrorsList)
	if err != nil {
		return nil, err
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
		})
	}

	for range mirrorsList.Mirrors {
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
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

func GetPeerDBVersion(wCtx workflow.Context) (string, error) {
	activityCtx := workflow.WithLocalActivityOptions(wCtx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	getVersionActivity := func(ctx context.Context) (string, error) {
		return peerdbenv.PeerDBVersionShaShort(), nil
	}
	var version string
	future := workflow.ExecuteLocalActivity(activityCtx, getVersionActivity)
	err := future.Get(activityCtx, &version)
	return version, err
}
