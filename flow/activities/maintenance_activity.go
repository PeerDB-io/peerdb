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

func (a *MaintenanceActivity) GetAllMirrors(ctx context.Context) (*protos.MaintenanceMirrors, error) {
	rows, err := a.CatalogPool.Query(ctx, `
	select distinct on(name)
	  id, name, workflow_id,
	  created_at, coalesce(query_string, '')='' is_cdc
	from flows
	`)
	if err != nil {
		return &protos.MaintenanceMirrors{}, err
	}

	maintenanceMirrorItems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.MaintenanceMirror, error) {
		var info protos.MaintenanceMirror
		err := row.Scan(&info.MirrorId, &info.MirrorName, &info.WorkflowId, &info.MirrorCreatedAt, &info.IsCdc)
		return &info, err
	})
	return &protos.MaintenanceMirrors{
		Mirrors: maintenanceMirrorItems,
	}, err
}

func (a *MaintenanceActivity) getMirrorStatus(ctx context.Context, mirror *protos.MaintenanceMirror) (protos.FlowStatus, error) {
	return shared.GetWorkflowStatus(ctx, a.TemporalClient, mirror.WorkflowId)
}

func (a *MaintenanceActivity) WaitForRunningSnapshots(ctx context.Context) (*protos.MaintenanceMirrors, error) {
	mirrors, err := a.GetAllMirrors(ctx)
	if err != nil {
		return &protos.MaintenanceMirrors{}, err
	}

	slog.Info("Found mirrors for snapshot check", "mirrors", mirrors, "len", len(mirrors.Mirrors))

	waitBeforeAlertingDuration := 30 * time.Minute

	for _, mirror := range mirrors.Mirrors {
		lastStatus, err := a.checkAndWaitIfSnapshot(ctx, mirror, 2*time.Minute, waitBeforeAlertingDuration)
		if err != nil {
			return &protos.MaintenanceMirrors{}, err
		}
		slog.Info("Finished checking and waiting for snapshot",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "lastStatus", lastStatus.String())
	}
	slog.Info("Finished checking and waiting for all mirrors to finish snapshot")
	return mirrors, nil
}

func (a *MaintenanceActivity) checkAndWaitIfSnapshot(
	ctx context.Context,
	mirror *protos.MaintenanceMirror,
	logEvery time.Duration,
	alertEvery time.Duration,
) (protos.FlowStatus, error) {
	// In case a mirror was just kicked off, it shows up in the running state, we wait for a bit before checking for snapshot
	if mirror.MirrorCreatedAt.AsTime().After(time.Now().Add(-30 * time.Second)) {
		slog.Info("Mirror was created less than 30 seconds ago, waiting for it to be ready before checking for snapshot",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId)
		time.Sleep(30 * time.Second)
	}

	mirrorStatus, err := a.getMirrorStatus(ctx, mirror)
	if err != nil {
		return mirrorStatus, err
	}
	defer shared.Interval(ctx, alertEvery, func() {
		slog.Warn("[Maintenance] Still waiting for mirror to finish snapshot",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "status", mirrorStatus.String())
		a.Alerter.LogNonFlowWarning(ctx, telemetry.MaintenanceWait, mirror.MirrorName, fmt.Sprintf(
			"Maintenance mode is still waiting for mirror to finish snapshot, mirror=%s, workflowId=%s, status=%s",
			mirror.MirrorName, mirror.WorkflowId, mirrorStatus))
	})()

	defer shared.Interval(ctx, logEvery, func() {
		slog.Info("[Maintenance] Waiting for mirror to finish snapshot",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "status", mirrorStatus.String())
	})()

	slog.Info("Checking and waiting if mirror is snapshot", "mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "status",
		mirrorStatus.String())
	snapshotWaitSleepInterval := 10 * time.Second
	for mirrorStatus == protos.FlowStatus_STATUS_SNAPSHOT || mirrorStatus == protos.FlowStatus_STATUS_SETUP {
		time.Sleep(snapshotWaitSleepInterval)
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Waiting for mirror %s to finish snapshot", mirror.MirrorName))
		mirrorStatus, err = a.getMirrorStatus(ctx, mirror)
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

func (a *MaintenanceActivity) BackupAllPreviouslyRunningFlows(ctx context.Context, mirrors *protos.MaintenanceMirrors) error {
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
		`, mirror.MirrorId, mirror.MirrorName, mirror.WorkflowId, mirror.MirrorCreatedAt, mirror.IsCdc, mirrorStateBackup,
			peerdbenv.PeerDBVersionShaShort())
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *MaintenanceActivity) PauseMirrorIfRunning(ctx context.Context, mirror *protos.MaintenanceMirror) (bool, error) {
	mirrorStatus, err := a.getMirrorStatus(ctx, mirror)
	if err != nil {
		return false, err
	}

	slog.Info("Checking if mirror is running", "mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "status", mirrorStatus.String())

	if mirrorStatus != protos.FlowStatus_STATUS_RUNNING {
		return false, nil
	}

	slog.Info("Pausing mirror for maintenance", "mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId)

	err = model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirror.WorkflowId, "", model.PauseSignal)
	if err != nil {
		slog.Error("Error signaling mirror running to pause for maintenance",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "error", err)
		return false, err
	}
	defer shared.Interval(ctx, 30*time.Second, func() {
		slog.Info("Waiting for mirror to pause",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "currentStatus", mirrorStatus.String())
	})()

	for mirrorStatus != protos.FlowStatus_STATUS_PAUSED {
		time.Sleep(10 * time.Second)

		activity.RecordHeartbeat(ctx, fmt.Sprintf("Waiting for mirror %s to pause", mirror.MirrorName))
		mirrorStatus, err = a.getMirrorStatus(ctx, mirror)
		if err != nil {
			slog.Error("Error querying mirror status for maintenance of previously running mirror",
				"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "error", err)
		}
	}
	return true, nil
}

func (a *MaintenanceActivity) CleanBackedUpFlows(ctx context.Context) error {
	_, err := a.CatalogPool.Exec(ctx, `
		update maintenance.maintenance_flows
			set state = $1,
			restored_at = now(),
			to_version = $2
		where state = $3
	`, mirrorStateRestored, peerdbenv.PeerDBVersionShaShort(), mirrorStateBackup)
	return err
}

func (a *MaintenanceActivity) GetBackedUpFlows(ctx context.Context) (*protos.MaintenanceMirrors, error) {
	rows, err := a.CatalogPool.Query(ctx, `
		select flow_id, flow_name, workflow_id, flow_created_at, is_cdc
		from maintenance.maintenance_flows
		where state = $1
	`, mirrorStateBackup)
	if err != nil {
		return nil, err
	}

	maintenanceMirrorItems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.MaintenanceMirror, error) {
		var info protos.MaintenanceMirror
		err := row.Scan(&info.MirrorId, &info.MirrorName, &info.WorkflowId, &info.MirrorCreatedAt, &info.IsCdc)
		return &info, err
	})
	if err != nil {
		return nil, err
	}

	return &protos.MaintenanceMirrors{
		Mirrors: maintenanceMirrorItems,
	}, nil
}

func (a *MaintenanceActivity) ResumeMirror(ctx context.Context, mirror *protos.MaintenanceMirror) error {
	mirrorStatus, err := a.getMirrorStatus(ctx, mirror)
	if err != nil {
		return err
	}

	if mirrorStatus != protos.FlowStatus_STATUS_PAUSED {
		slog.Error("Cannot resume mirror that is not paused",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "status", mirrorStatus.String())
		return nil
	}

	// There can also be "workflow already completed" errors, what should we do in that case?
	err = model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirror.WorkflowId, "", model.NoopSignal)
	if err != nil {
		slog.Error("Error signaling mirror to resume for maintenance",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "error", err)
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
	defer shared.Interval(ctx, time.Duration(peerdbenv.PeerDBMaintenanceModeWaitAlertSeconds())*time.Second, func() {
		slog.Warn("Maintenance Workflow is still running")
		a.Alerter.LogNonFlowWarning(ctx, telemetry.MaintenanceWait, "", "Maintenance mode is still running")
	})()
	<-ctx.Done()
	return nil
}
