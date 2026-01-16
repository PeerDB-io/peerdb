package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	ActionCreateFlow            = "create_flow"
	ActionResyncFlow            = "resync_flow"
	ActionPauseFlow             = "pause_flow"
	ActionResumeFlow            = "resume_flow"
	ActionTerminateFlow         = "terminate_flow"
	ActionUpdateFlowConfig      = "update_flow_config"
	ActionStartMaintenance      = "start_maintenance"
	ActionEndMaintenance        = "end_maintenance"
	ActionSkipSnapshotWaitFlows = "skip_snapshot_wait_flows"
	ActionCreatePeer            = "create_peer"
	ActionDropPeer              = "drop_peer"
)

func LogActivityCreateFlow(ctx context.Context, flowName string) {
	logActivity(ctx, ActionCreateFlow, slog.String("flowName", flowName))
}

func LogActivityResyncFlow(ctx context.Context, flowName string) {
	logActivity(ctx, ActionResyncFlow, slog.String("flowName", flowName))
}

func LogActivityPauseFlow(ctx context.Context, flowName string) {
	logActivity(ctx, ActionPauseFlow, slog.String("flowName", flowName))
}

func LogActivityResumeFlow(ctx context.Context, flowName string) {
	logActivity(ctx, ActionResumeFlow, slog.String("flowName", flowName))
}

func LogActivityTerminateFlow(ctx context.Context, flowName string) {
	logActivity(ctx, ActionTerminateFlow, slog.String("flowName", flowName))
}

func LogActivityStartMaintenance(ctx context.Context) {
	logActivity(ctx, ActionStartMaintenance)
}

func LogActivityEndMaintenance(ctx context.Context) {
	logActivity(ctx, ActionEndMaintenance)
}

func LogActivitySkipSnapshotWaitFlows(ctx context.Context) {
	logActivity(ctx, ActionSkipSnapshotWaitFlows)
}

func LogActivityCreatePeer(ctx context.Context) {
	logActivity(ctx, ActionCreatePeer)
}

func LogActivityDropPeer(ctx context.Context) {
	logActivity(ctx, ActionDropPeer)
}

type OldCDCFlowValues struct {
	Env                           map[string]string
	IdleTimeout                   uint64
	BatchSize                     uint32
	SnapshotNumRowsPerPartition   uint32
	SnapshotNumPartitionsOverride uint32
	SnapshotMaxParallelWorkers    uint32
	SnapshotNumTablesInParallel   uint32
}

func LogActivityUpdateFlowConfig(ctx context.Context, flowName string, oldValues OldCDCFlowValues, update *protos.CDCFlowConfigUpdate) {
	var changes []string

	logIfChanged := func(name string, oldVal, newVal any) {
		if newVal != oldVal {
			changes = append(changes, fmt.Sprintf("%s: %v->%v", name, oldVal, newVal))
		}
	}

	if update.BatchSize > 0 {
		logIfChanged("batchSize", oldValues.BatchSize, update.BatchSize)
	}
	if update.IdleTimeout > 0 {
		logIfChanged("idleTimeout", oldValues.IdleTimeout, update.IdleTimeout)
	}
	if update.SnapshotNumRowsPerPartition > 0 {
		logIfChanged("snapshotNumRowsPerPartition", oldValues.SnapshotNumRowsPerPartition, update.SnapshotNumRowsPerPartition)
	}
	if update.SnapshotNumPartitionsOverride > 0 {
		logIfChanged("snapshotNumPartitionsOverride", oldValues.SnapshotNumPartitionsOverride, update.SnapshotNumPartitionsOverride)
	}
	if update.SnapshotMaxParallelWorkers > 0 {
		logIfChanged("snapshotMaxParallelWorkers", oldValues.SnapshotMaxParallelWorkers, update.SnapshotMaxParallelWorkers)
	}
	if update.SnapshotNumTablesInParallel > 0 {
		logIfChanged("snapshotNumTablesInParallel", oldValues.SnapshotNumTablesInParallel, update.SnapshotNumTablesInParallel)
	}

	if len(update.UpdatedEnv) > 0 {
		for key, newValue := range update.UpdatedEnv {
			if oldValue := oldValues.Env[key]; oldValue != newValue {
				changes = append(changes, fmt.Sprintf("env %s: %s->%s", key, oldValue, newValue))
			}
		}
	}

	if len(update.AdditionalTables) > 0 {
		var addedTables []string
		for _, t := range update.AdditionalTables {
			addedTables = append(addedTables, t.SourceTableIdentifier)
		}
		changes = append(changes, fmt.Sprintf("tables added: %v", addedTables))
	}

	if len(update.RemovedTables) > 0 {
		var removedTables []string
		for _, t := range update.RemovedTables {
			removedTables = append(removedTables, t.SourceTableIdentifier)
		}
		changes = append(changes, fmt.Sprintf("tables removed: %v", removedTables))
	}

	if len(changes) > 0 {
		logActivity(ctx, ActionUpdateFlowConfig,
			slog.String("flowName", flowName),
			slog.String("activityDetails", strings.Join(changes, ", ")))
	}
}

func logActivity(ctx context.Context, action string, attrs ...any) {
	baseAttrs := []any{slog.String("action", action)}
	if requestID, ok := ctx.Value(shared.RequestIdKey).(string); ok {
		baseAttrs = append(baseAttrs, slog.String("requestId", requestID))
	}
	baseAttrs = append(baseAttrs, attrs...)

	slog.InfoContext(ctx, "[flow activity] "+action, baseAttrs...)
}
