package activities

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

const (
	mirrorStateBackup   = "backup"
	mirrorStateRestored = "restore"
)

type MaintenanceActivity struct {
	CatalogPool    *pgxpool.Pool
	Alerter        *alerting.Alerter
	TemporalClient client.Client
}

type MaintenanceMirrorsInfo struct {
	Mirrors []MaintenanceMirrorInfoItem
}
type MaintenanceMirrorInfoItem struct {
	CreatedAt  time.Time
	Name       string
	WorkflowId string
	Id         int64
	IsCDC      bool
}

func (a *MaintenanceActivity) GetAllMirrors(ctx context.Context) (MaintenanceMirrorsInfo, error) {
	rows, err := a.CatalogPool.Query(ctx, `
	select distinct on(f.name)
	  f.id, f.name, f.workflow_id,
	  f.created_at, coalesce(f.query_string, '')='' is_cdc
	from flows f
	`)
	if err != nil {
		return MaintenanceMirrorsInfo{}, err
	}

	maintenanceMirrorItems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (MaintenanceMirrorInfoItem, error) {
		var info MaintenanceMirrorInfoItem
		err := row.Scan(&info.Id, &info.Name, &info.WorkflowId, &info.CreatedAt, &info.IsCDC)
		return info, err
	})
	return MaintenanceMirrorsInfo{
		Mirrors: maintenanceMirrorItems,
	}, err
}

func (a *MaintenanceActivity) getMirrorStatus(ctx context.Context, mirrorInfo MaintenanceMirrorInfoItem) (protos.FlowStatus, error) {
	encodedState, err := a.TemporalClient.QueryWorkflow(ctx, mirrorInfo.WorkflowId, "", shared.FlowStatusQuery)
	if err != nil {
		slog.Error("Error querying mirror status for maintenance", "mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "error", err)
		return protos.FlowStatus_STATUS_UNKNOWN, err
	}
	var state protos.FlowStatus
	if err = encodedState.Get(&state); err != nil {
		slog.Error("Error decoding mirror status for maintenance", "mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "error", err)
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("error decoding mirror status for maintenance: %w", err)
	}
	return state, nil
}

func (a *MaintenanceActivity) WaitForRunningSnapshots(ctx context.Context) (MaintenanceMirrorsInfo, error) {
	mirrors, err := a.GetAllMirrors(ctx)
	if err != nil {
		return MaintenanceMirrorsInfo{}, err
	}

	slog.Info("Found mirrors for snapshot check", "mirrors", mirrors, "len", len(mirrors.Mirrors))

	waitBeforeAlertingDuration := 30 * time.Minute

	for _, mirror := range mirrors.Mirrors {
		lastStatus, err := a.checkAndWaitIfSnapshot(ctx, mirror, 2*time.Minute, waitBeforeAlertingDuration)
		if err != nil {
			return MaintenanceMirrorsInfo{}, err
		}
		slog.Info("Finished checking and waiting for snapshot",
			"mirror", mirror.Name, "workflowId", mirror.WorkflowId, "lastStatus", lastStatus.String())
	}
	slog.Info("Finished checking and waiting for all mirrors to finish snapshot")
	return mirrors, nil
}

func (a *MaintenanceActivity) checkAndWaitIfSnapshot(
	ctx context.Context,
	mirrorInfo MaintenanceMirrorInfoItem,
	logEvery time.Duration,
	alertEvery time.Duration,
) (protos.FlowStatus, error) {
	// In case a mirror was just kicked off, it shows up in the running state, we wait for a bit before checking for snapshot
	if mirrorInfo.CreatedAt.After(time.Now().Add(-30 * time.Second)) {
		slog.Info("Mirror was created less than 30 seconds ago, waiting for it to be ready before checking for snapshot",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId)
		time.Sleep(30 * time.Second)
	}

	mirrorStatus, err := a.getMirrorStatus(ctx, mirrorInfo)
	if err != nil {
		return mirrorStatus, err
	}

	slog.Info("Checking if mirror is snapshot", "mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())
	if mirrorStatus != protos.FlowStatus_STATUS_SNAPSHOT && mirrorStatus != protos.FlowStatus_STATUS_SETUP {
		return mirrorStatus, nil
	}
	slog.Info("Waiting for mirror to finish snapshot",
		"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())
	defer shared.Interval(ctx, alertEvery, func() {
		slog.Warn("[Maintenance] Still waiting for mirror to finish snapshot",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())
		a.Alerter.LogNonFlowWarning(ctx, telemetry.MaintenanceWait, mirrorInfo.Name, fmt.Sprintf(
			"Maintenance mode is still waiting for mirror to finish snapshot, mirror=%s, workflowId=%s, status=%s",
			mirrorInfo.Name, mirrorInfo.WorkflowId, mirrorStatus))
	})()

	defer shared.Interval(ctx, logEvery, func() {
		slog.Info("[Maintenance] Waiting for mirror to finish snapshot",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())
	})()

	snapshotWaitSleepInterval := 10 * time.Second
	for mirrorStatus == protos.FlowStatus_STATUS_SNAPSHOT || mirrorStatus == protos.FlowStatus_STATUS_SETUP {
		time.Sleep(snapshotWaitSleepInterval)
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Waiting for mirror %s to finish snapshot", mirrorInfo.Name))
		mirrorStatus, err = a.getMirrorStatus(ctx, mirrorInfo)
		if err != nil {
			return mirrorStatus, err
		}
	}
	return mirrorStatus, nil
}

func (a *MaintenanceActivity) EnableMaintenanceMode(ctx context.Context) error {
	slog.Info("Enabling maintenance mode")
	return peerdbenv.UpdatePeerDBMaintenanceModeEnabled(ctx, a.CatalogPool, true)
}

func (a *MaintenanceActivity) BackupAllPreviouslyRunningFlows(ctx context.Context, mirrors MaintenanceMirrorsInfo) error {
	tx, err := a.CatalogPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer shared.RollbackTx(tx, slog.Default())

	for _, mirror := range mirrors.Mirrors {
		_, err := tx.Exec(ctx, `
		insert into maintenance.maintenance_flows
			(flow_id, flow_name, workflow_id, flow_created_at, is_cdc, state, from_version)
		values
			($1, $2, $3, $4, $5, $6, $7)
		`, mirror.Id, mirror.Name, mirror.WorkflowId, mirror.CreatedAt, mirror.IsCDC, mirrorStateBackup, peerdbenv.PeerDBVersionShaShort())
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *MaintenanceActivity) PauseMirrorIfRunning(ctx context.Context, mirrorInfo MaintenanceMirrorInfoItem) (bool, error) {
	mirrorStatus, err := a.getMirrorStatus(ctx, mirrorInfo)
	if err != nil {
		return false, err
	}

	slog.Info("Checking if mirror is running", "mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())

	if mirrorStatus != protos.FlowStatus_STATUS_RUNNING {
		return false, nil
	}

	slog.Info("Pausing mirror for maintenance", "mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId)

	err = model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirrorInfo.WorkflowId, "", model.PauseSignal)
	if err != nil {
		slog.Error("Error signaling mirror running to pause for maintenance",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "error", err)
		return false, err
	}
	defer shared.Interval(ctx, 30*time.Second, func() {
		slog.Info("Waiting for mirror to pause",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "currentStatus", mirrorStatus.String())
	})()

	for mirrorStatus != protos.FlowStatus_STATUS_PAUSED {
		time.Sleep(10 * time.Second)

		activity.RecordHeartbeat(ctx, fmt.Sprintf("Waiting for mirror %s to pause", mirrorInfo.Name))
		mirrorStatus, err = a.getMirrorStatus(ctx, mirrorInfo)
		if err != nil {
			slog.Error("Error querying mirror status for maintenance of previously running mirror",
				"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "error", err)
		}
	}
	return true, nil
}

func (a *MaintenanceActivity) CleanupBackupedFlows(ctx context.Context) error {
	_, err := a.CatalogPool.Exec(ctx, `
		update maintenance.maintenance_flows
			set state = $1,
			restored_at = now(),
			to_version = $2
		where state = $3
	`, mirrorStateRestored, peerdbenv.PeerDBVersionShaShort(), mirrorStateBackup)
	return err
}

func (a *MaintenanceActivity) GetBackedUpFlows(ctx context.Context) (MaintenanceMirrorsInfo, error) {
	rows, err := a.CatalogPool.Query(ctx, `
		select flow_id, flow_name, workflow_id, flow_created_at, is_cdc
		from maintenance.maintenance_flows
		where state = $1
	`, mirrorStateBackup)
	if err != nil {
		return MaintenanceMirrorsInfo{}, err
	}

	maintenanceMirrorItems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (MaintenanceMirrorInfoItem, error) {
		var info MaintenanceMirrorInfoItem
		err := row.Scan(&info.Id, &info.Name, &info.WorkflowId, &info.CreatedAt, &info.IsCDC)
		return info, err
	})
	if err != nil {
		return MaintenanceMirrorsInfo{}, err
	}

	return MaintenanceMirrorsInfo{
		Mirrors: maintenanceMirrorItems,
	}, nil
}

func (a *MaintenanceActivity) ResumeMirror(ctx context.Context, mirrorInfo MaintenanceMirrorInfoItem) error {
	mirrorStatus, err := a.getMirrorStatus(ctx, mirrorInfo)
	if err != nil {
		return err
	}

	if mirrorStatus != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Cannot resume mirror that is not paused",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "status", mirrorStatus.String())
		return nil
	}

	// There can also be "workflow already completed" errors, what should we do in that case?
	err = model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirrorInfo.WorkflowId, "", model.NoopSignal)
	if err != nil {
		slog.Error("Error signaling mirror to resume for maintenance",
			"mirror", mirrorInfo.Name, "workflowId", mirrorInfo.WorkflowId, "error", err)
		return err
	}
	return nil
}

func (a *MaintenanceActivity) DisableMaintenanceMode(ctx context.Context) error {
	slog.Info("Disabling maintenance mode")
	return peerdbenv.UpdatePeerDBMaintenanceModeEnabled(ctx, a.CatalogPool, false)
}

func (a *MaintenanceActivity) BackgroundAlerter(ctx context.Context) error {
	defer shared.Interval(ctx, 30*time.Second, func() {
		activity.RecordHeartbeat(ctx, "Maintenance Workflow is still running")
	})()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Duration(peerdbenv.PeerDBMaintenanceModeWaitAlertSeconds()) * time.Second):
			slog.Warn("Maintenance Workflow is still running")
			a.Alerter.LogNonFlowWarning(ctx, telemetry.MaintenanceWait, "", "Maintenance mode is still running")
		}
	}
}
