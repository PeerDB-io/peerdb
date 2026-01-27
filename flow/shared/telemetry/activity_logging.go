package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
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

func logActivity(ctx context.Context, action string, additionalAttrs ...any) {
	attrs := []any{slog.String("action", action)}
	if requestID, ok := ctx.Value(shared.RequestIdKey).(string); ok {
		attrs = append(attrs, slog.String("requestId", requestID))
	}
	attrs = append(attrs, additionalAttrs...)

	slog.InfoContext(ctx, "[flow activity] "+action, attrs...)
}

type FlowConfigForLogging struct {
	FlowName                    string `json:"flow_name"`
	PublicationName             string `json:"pg_publication_name"`
	ReplicationSlotName         string `json:"pg_replication_slot_name"`
	IdleTimeoutSeconds          uint64 `json:"sync_interval"`
	MaxBatchSize                uint32 `json:"max_batch_size"`
	SnapshotNumRowsPerPartition uint32 `json:"snapshot_num_rows_per_partition"`
	SnapshotMaxParallelWorkers  uint32 `json:"snapshot_max_parallel_workers"`
	SnapshotNumTablesInParallel uint32 `json:"snapshot_num_tables_in_parallel"`
	CdcOnly                     bool   `json:"cdc_only"`
	SnapshotOnly                bool   `json:"snapshot_only"`
	Resync                      bool   `json:"is_resync"`
	NumTables                   int    `json:"num_tables"`
}

type TableMappingForLogging struct {
	TableName     string   `json:"table_name"`
	DestTableName string   `json:"destination_table_name"`
	PartitionKey  string   `json:"partition_key"`
	Engine        string   `json:"engine"`
	Exclude       []string `json:"excluded_columns"`
}

func LogFlowConfigs(ctx context.Context, catalogPool shared.CatalogPool) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "LogFlowConfigs"))

	rows, err := catalogPool.Query(ctx,
		`SELECT DISTINCT ON (name) name, config_proto FROM flows WHERE config_proto IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("failed to query flow configs: %w", err)
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.FlowConnectionConfigsCore, error) {
		var name string
		var configProto []byte
		if err := row.Scan(&name, &configProto); err != nil {
			return nil, err
		}
		cfg := &protos.FlowConnectionConfigsCore{}
		if err := proto.Unmarshal(configProto, cfg); err != nil {
			return nil, err
		}
		return cfg, nil
	})
	if err != nil {
		return fmt.Errorf("failed to collect flow configs: %w", err)
	}

	for _, cfg := range configs {
		numTables := len(cfg.TableMappings)
		flowConfig := FlowConfigForLogging{
			FlowName:                    cfg.FlowJobName,
			MaxBatchSize:                cfg.MaxBatchSize,
			IdleTimeoutSeconds:          cfg.IdleTimeoutSeconds,
			PublicationName:             cfg.PublicationName,
			ReplicationSlotName:         cfg.ReplicationSlotName,
			CdcOnly:                     !cfg.DoInitialSnapshot,
			SnapshotOnly:                cfg.InitialSnapshotOnly,
			SnapshotNumRowsPerPartition: cfg.SnapshotNumRowsPerPartition,
			SnapshotMaxParallelWorkers:  cfg.SnapshotMaxParallelWorkers,
			SnapshotNumTablesInParallel: cfg.SnapshotNumTablesInParallel,
			Resync:                      cfg.Resync,
			NumTables:                   numTables,
		}
		configJSON, err := json.Marshal(flowConfig)
		if err != nil {
			logger.Error("failed to marshal flow configs", slog.Any("error", err))
			continue
		}
		logger.Info("[flow config]",
			slog.String("flowName", cfg.FlowJobName),
			slog.String("flowConfig", string(configJSON)))

		for _, tm := range cfg.TableMappings {
			tableConfig := TableMappingForLogging{
				TableName:     tm.SourceTableIdentifier,
				DestTableName: tm.DestinationTableIdentifier,
				PartitionKey:  tm.PartitionKey,
				Engine:        tm.Engine.String(),
				Exclude:       tm.Exclude,
			}
			tableJSON, err := json.Marshal(tableConfig)
			if err != nil {
				logger.Error("failed to marshal table config", slog.Any("error", err))
				continue
			}
			logger.Info("[table config]",
				slog.String("flowName", cfg.FlowJobName),
				slog.String("tableName", tm.SourceTableIdentifier),
				slog.String("tableConfig", string(tableJSON)))
		}
	}

	return nil
}
