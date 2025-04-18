package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

const (
	mirrorStateBackup   = "backup"
	mirrorStateRestored = "restore"
)

type MaintenanceActivity struct {
	CatalogPool    shared.CatalogPool
	Alerter        *alerting.Alerter
	TemporalClient client.Client
	OtelManager    *otel_metrics.OtelManager
}

func (a *MaintenanceActivity) GetAllMirrors(ctx context.Context) (*protos.MaintenanceMirrors, error) {
	rows, err := a.CatalogPool.Query(ctx, `
	select distinct on(name)
	  id, name, workflow_id,
	  created_at, updated_at, coalesce(query_string, '')='' is_cdc
	from flows
	`)
	if err != nil {
		return nil, err
	}

	maintenanceMirrorItems, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.MaintenanceMirror, error) {
		var info protos.MaintenanceMirror
		var createdAt, updatedAt time.Time
		err := row.Scan(&info.MirrorId, &info.MirrorName, &info.WorkflowId, &createdAt, &updatedAt, &info.IsCdc)
		info.MirrorCreatedAt = timestamppb.New(createdAt)
		info.MirrorUpdatedAt = timestamppb.New(updatedAt)
		return &info, err
	})
	return &protos.MaintenanceMirrors{
		Mirrors: maintenanceMirrorItems,
	}, err
}

func (a *MaintenanceActivity) getMirrorStatus(ctx context.Context, mirror *protos.MaintenanceMirror) (protos.FlowStatus, error) {
	return internal.GetWorkflowStatus(ctx, a.CatalogPool, a.TemporalClient, mirror.WorkflowId)
}

func (a *MaintenanceActivity) WaitForRunningSnapshots(
	ctx context.Context,
	skippedFlows map[string]struct{},
) (*protos.MaintenanceMirrors, error) {
	mirrors, err := a.GetAllMirrors(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Found mirrors for snapshot check", "mirrors", mirrors, "len", len(mirrors.Mirrors))

	for _, mirror := range mirrors.Mirrors {
		if _, shouldSkip := skippedFlows[mirror.MirrorName]; shouldSkip {
			slog.Warn("Skipping wait for mirror as it was in the skippedFlows", "mirror", mirror.MirrorName)
			continue
		}
		lastStatus, err := a.checkAndWaitIfSnapshot(ctx, mirror, 2*time.Minute)
		if err != nil {
			return nil, err
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
) (protos.FlowStatus, error) {
	// In case a mirror was just kicked off, it shows up in the running state, we wait for a bit before checking for snapshot
	targetCheckTime := mirror.MirrorCreatedAt.AsTime().Add(30 * time.Second)
	now := time.Now()
	if now.Before(targetCheckTime) {
		slog.Info("Mirror was created less than 30 seconds ago, waiting for it to be ready before checking for snapshot",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId)
		time.Sleep(targetCheckTime.Sub(now))
	}

	flowStatus, err := RunEveryIntervalUntilFinish(ctx, func() (bool, protos.FlowStatus, error) {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("Waiting for mirror %s to be ready", mirror.MirrorName))
		mirrorStatus, err := a.getMirrorStatus(ctx, mirror)
		if err != nil || mirrorStatus == protos.FlowStatus_STATUS_SNAPSHOT || mirrorStatus == protos.FlowStatus_STATUS_SETUP ||
			mirrorStatus == protos.FlowStatus_STATUS_RESYNC || mirrorStatus == protos.FlowStatus_STATUS_UNKNOWN {
			return false, mirrorStatus, err
		}
		return true, mirrorStatus, nil
	}, 10*time.Second, fmt.Sprintf("Waiting for mirror %s to be ready", mirror.MirrorName), logEvery, true)
	return flowStatus, err
}

func (a *MaintenanceActivity) EnableMaintenanceMode(ctx context.Context) error {
	slog.Info("Enabling maintenance mode")
	return internal.UpdatePeerDBMaintenanceModeEnabled(ctx, a.CatalogPool, true)
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
		`, mirror.MirrorId, mirror.MirrorName, mirror.WorkflowId, mirror.MirrorCreatedAt.AsTime(), mirror.IsCdc, mirrorStateBackup,
			internal.PeerDBVersionShaShort())
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

var workflowNotFoundMessageRe = regexp.MustCompile("workflow not found for ID: (.+)")

func (a *MaintenanceActivity) PauseMirrorIfRunning(ctx context.Context, mirror *protos.MaintenanceMirror) (bool, error) {
	logger := slog.With("mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId)
	mirrorStatus, err := a.getMirrorStatus(ctx, mirror)
	if err != nil {
		logger.Warn("Error getting mirror status", "error", err)
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) && workflowNotFoundMessageRe.MatchString(notFoundErr.Message) {
			logger.Warn("Received a workflow not found error, checking if the workflow is missing and if it is older than 90 days",
				"error", err, "temporalCertAuth", internal.PeerDBTemporalEnableCertAuth())
			// This is max temporal retention period, but this is mirror update time, not deletion time, so it is not accurate
			if mirror.MirrorUpdatedAt.AsTime().Before(time.Now().Add(-90*24*time.Hour)) &&
				// We are in Temporal Cloud
				internal.PeerDBTemporalEnableCertAuth() {
				// workflow not found for ID: mirror_d1e3f532__8adb__4f79__9d00__01e44b6bcbfb-peerflow-27144d2c-06ce-4552-87e5-696b3a909702
				logger.Warn("Workflow not found in Temporal Cloud and mirror update_at is older than 90 days, checking for existing workflows")
				response, wErr := a.TemporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Query: fmt.Sprintf("`MirrorName`=\"%s\"",
						mirror.MirrorName),
				})
				if wErr != nil {
					logger.Error("Error checking for ANY existing Workflows", "error", wErr)
					return false, wErr
				}
				logger.Info("Received response for ANY existing Workflows check", "len(executions)", len(response.Executions))
				if len(response.Executions) == 0 {
					logger.Warn("No existing workflows found, skipping pause")
					return false, nil
				}
				foundWorkflowIds := make([]string, len(response.Executions))
				for i, exec := range response.Executions {
					logger.Info("Found existing CDCFlow", "workflowId", exec.GetExecution().GetWorkflowId())
					foundWorkflowIds[i] = exec.GetExecution().GetWorkflowId()
				}
				logger.Warn("Found some existing CDCFlow, this is unexpected and should be investigated",
					"foundWorkflows", foundWorkflowIds)
			}
		}
		return false, err
	}

	logger.Info("Checking if mirror is running", "status", mirrorStatus.String())

	if mirrorStatus != protos.FlowStatus_STATUS_RUNNING {
		return false, nil
	}

	logger.Info("Pausing mirror for maintenance")

	if err := model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirror.WorkflowId, "", model.PauseSignal); err != nil {
		logger.Error("Error signaling mirror to pause for maintenance", "error", err)
		// Is the CDC flow missing?
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) && notFoundErr.Message == "workflow execution already completed" {
			logger.Info("Workflow execution already completed, checking for existing DropFlow")
			// Check if we are actively trying to drop the mirror
			response, wErr := a.TemporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Query: fmt.Sprintf("`MirrorName`=\"%s\" AND `WorkflowType`=\"DropFlowWorkflow\" AND `ExecutionStatus`=\"Running\"",
					mirror.MirrorName),
			})
			if wErr != nil {
				logger.Error("Error checking for existing DropFlow", "error", wErr)
				return false, wErr
			}
			logger.Info("Received response for DropFlow check", "len(executions)", len(response.Executions))
			if len(response.Executions) > 0 {
				// We can skip if we find a running DropFlow
				foundWorkflowIds := make([]string, len(response.Executions))
				for i, exec := range response.Executions {
					foundWorkflowIds[i] = exec.GetExecution().GetWorkflowId()
				}
				logger.Warn("Found existing DropFlow, skipping pause", "foundDropFlows", foundWorkflowIds,
					"len(foundDropFlows)", len(foundWorkflowIds),
				)
				return false, nil
			} else {
				// Maybe the drop flow is already completed, but relying on a completed state can be error-prone, so we check flows table
				logger.Warn("No running DropFlow found, checking if mirror exists in flows table")
				var exists bool
				err := a.CatalogPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM flows WHERE name = $1)", mirror.MirrorName).Scan(&exists)
				if err != nil {
					logger.Error("Error checking if flow exists", "error", err)
					return false, err
				}
				if !exists {
					logger.Warn("Mirror does not exist in flows table, skipping pause")
					return false, nil
				}
			}
		}
		return false, err
	}

	return RunEveryIntervalUntilFinish(ctx, func() (bool, bool, error) {
		updatedMirrorStatus, statusErr := a.getMirrorStatus(ctx, mirror)
		if statusErr != nil {
			return false, false, statusErr
		}
		activity.RecordHeartbeat(ctx, "Waiting for mirror to pause with current status "+updatedMirrorStatus.String())
		if statusErr := model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirror.WorkflowId, "",
			model.PauseSignal); statusErr != nil {
			return false, false, statusErr
		}
		if updatedMirrorStatus == protos.FlowStatus_STATUS_PAUSED {
			return true, true, nil
		}
		return false, false, nil
	}, 10*time.Second, "Waiting for mirror to pause", 30*time.Second, false)
}

func (a *MaintenanceActivity) CleanBackedUpFlows(ctx context.Context) error {
	_, err := a.CatalogPool.Exec(ctx, `
		update maintenance.maintenance_flows
			set state = $1,
			restored_at = now(),
			to_version = $2
		where state = $3
	`, mirrorStateRestored, internal.PeerDBVersionShaShort(), mirrorStateBackup)
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
		var createdAt time.Time
		err := row.Scan(&info.MirrorId, &info.MirrorName, &info.WorkflowId, &createdAt, &info.IsCdc)
		info.MirrorCreatedAt = timestamppb.New(createdAt)
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
	if err := model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, mirror.WorkflowId, "", model.NoopSignal); err != nil {
		slog.Error("Error signaling mirror to resume for maintenance",
			"mirror", mirror.MirrorName, "workflowId", mirror.WorkflowId, "error", err)
		return err
	}
	return nil
}

func (a *MaintenanceActivity) DisableMaintenanceMode(ctx context.Context) error {
	slog.Info("Disabling maintenance mode")
	return internal.UpdatePeerDBMaintenanceModeEnabled(ctx, a.CatalogPool, false)
}

func (a *MaintenanceActivity) BackgroundAlerter(ctx context.Context) error {
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()
	alertTicker := time.NewTicker(time.Duration(internal.PeerDBMaintenanceModeWaitAlertSeconds()) * time.Second)
	defer alertTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			activity.RecordHeartbeat(ctx, "Maintenance Workflow is still running")
		case <-alertTicker.C:
			slog.Warn("Maintenance Workflow is still running")
			a.Alerter.LogNonFlowWarning(ctx, telemetry.MaintenanceWait, "Waiting", "Maintenance mode is still running")
			a.OtelManager.Metrics.MaintenanceStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.WorkflowTypeKey, activity.GetInfo(ctx).WorkflowType.Name),
			)))
		}
	}
}

func RunEveryIntervalUntilFinish[T any](
	ctx context.Context,
	runFunc func() (finished bool, result T, err error),
	runInterval time.Duration,
	logMessage string,
	logInterval time.Duration,
	runBeforeFirstTick bool,
) (T, error) {
	if runBeforeFirstTick {
		finished, result, err := runFunc()
		if err != nil || finished {
			return result, err
		}
	}

	runTicker := time.NewTicker(runInterval)
	defer runTicker.Stop()

	logTicker := time.NewTicker(logInterval)
	defer logTicker.Stop()
	var lastResult T
	for {
		select {
		case <-ctx.Done():
			return lastResult, ctx.Err()
		case <-runTicker.C:
			finished, result, err := runFunc()
			lastResult = result
			if err != nil || finished {
				return lastResult, err
			}
		case <-logTicker.C:
			slog.Info(logMessage, "lastResult", lastResult)
		}
	}
}
