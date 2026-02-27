package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
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
	ActionStartFlowConfigUpdate = "start_flow_config_update"
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

func LogActivityStartFlowConfigUpdate(ctx context.Context, flowName string, update *protos.CDCFlowConfigUpdate) {
	var changes []string

	// Only logging fields that don't need old values to compare to
	if len(update.AdditionalTables) > 0 {
		addedTables := make([]string, 0, len(update.AdditionalTables))
		for _, t := range update.AdditionalTables {
			addedTables = append(addedTables, t.SourceTableIdentifier)
		}
		changes = append(changes, fmt.Sprintf("tables added: %v", addedTables))
	}

	if len(update.RemovedTables) > 0 {
		removedTables := make([]string, 0, len(update.RemovedTables))
		for _, t := range update.RemovedTables {
			removedTables = append(removedTables, t.SourceTableIdentifier)
		}
		changes = append(changes, fmt.Sprintf("tables removed: %v", removedTables))
	}

	logActivity(ctx, ActionStartFlowConfigUpdate,
		slog.String("flowName", flowName),
		slog.String("activityDetails", strings.Join(changes, ", ")))
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
		addedTables := make([]string, 0, len(update.AdditionalTables))
		for _, t := range update.AdditionalTables {
			addedTables = append(addedTables, t.SourceTableIdentifier)
		}
		changes = append(changes, fmt.Sprintf("tables added: %v", addedTables))
	}

	if len(update.RemovedTables) > 0 {
		removedTables := make([]string, 0, len(update.RemovedTables))
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
	SourcePeerName              string `json:"source_peer_name"`
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
	TableName           string   `json:"table_name"`
	DestTableName       string   `json:"destination_table_name"`
	PartitionKey        string   `json:"partition_key"`
	Engine              string   `json:"engine"`
	Exclude             []string `json:"excluded_columns"`
	ReplicaIdentityFull bool     `json:"replica_identity_full"`
	UseCustomSortKey    bool     `json:"use_custom_sort_key"`
	TotalInserts        int64    `json:"total_inserts"`
	TotalUpdates        int64    `json:"total_updates"`
	TotalDeletes        int64    `json:"total_deletes"`
}

type SourcePeerInfoForLogging struct {
	PeerName        string `json:"peer_name"`
	PeerType        string `json:"peer_type"`
	DatabaseVersion string `json:"database_version,omitempty"`
	DatabaseVariant string `json:"database_variant,omitempty"`
	CustomTLS       bool   `json:"custom_tls"`
	SSHTunnel       bool   `json:"ssh_tunnel"`
}

type (
	LoadPeersFunc               func(ctx context.Context, catalogPool shared.CatalogPool, peerNames []string) (map[string]*protos.Peer, error)
	PopulateRuntimePeerInfoFunc func(ctx context.Context, peer *protos.Peer, info *SourcePeerInfoForLogging)
)

func LogFlowConfigs(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	loadPeers LoadPeersFunc,
	populateRuntimeInfo PopulateRuntimePeerInfoFunc,
) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "LogFlowConfigs"))

	batch := pgx.Batch{}
	batch.Queue(`SELECT DISTINCT ON (name) name, config_proto FROM flows WHERE config_proto IS NOT NULL`)
	batch.Queue(`SELECT flow_name, destination_table_name, inserts_count, updates_count, deletes_count
		FROM peerdb_stats.cdc_table_aggregate_counts`)
	batch.Queue(`SELECT flow_name, table_name, table_schema FROM table_schema_mapping`)

	batchResults := catalogPool.Pool.SendBatch(ctx, &batch)
	defer batchResults.Close()

	configRows, err := batchResults.Query()
	if err != nil {
		return fmt.Errorf("failed to query flow configs: %w", err)
	}
	configs, err := pgx.CollectRows(configRows, func(row pgx.CollectableRow) (*protos.FlowConnectionConfigsCore, error) {
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

	type tableCounts struct {
		inserts int64
		updates int64
		deletes int64
	}
	countsByFlow := make(map[string]map[string]tableCounts)

	countRows, err := batchResults.Query()
	if err != nil {
		logger.Warn("failed to query table aggregate counts", slog.Any("error", err))
	} else {
		var flowName, destTable string
		var counts tableCounts
		if _, err := pgx.ForEachRow(countRows, []any{&flowName, &destTable, &counts.inserts, &counts.updates, &counts.deletes}, func() error {
			if countsByFlow[flowName] == nil {
				countsByFlow[flowName] = make(map[string]tableCounts)
			}
			countsByFlow[flowName][destTable] = counts
			return nil
		}); err != nil {
			logger.Warn("failed to read table aggregate counts", slog.Any("error", err))
		}
	}

	replicaIdentityByFlow := make(map[string]map[string]bool)

	schemaRows, err := batchResults.Query()
	if err != nil {
		logger.Warn("failed to query table schema mapping", slog.Any("error", err))
	} else {
		var flowName, tableName string
		var tableSchemaBytes []byte
		if _, err := pgx.ForEachRow(schemaRows, []any{&flowName, &tableName, &tableSchemaBytes}, func() error {
			tableSchema := &protos.TableSchema{}
			if err := proto.Unmarshal(tableSchemaBytes, tableSchema); err != nil {
				return err
			}
			if replicaIdentityByFlow[flowName] == nil {
				replicaIdentityByFlow[flowName] = make(map[string]bool)
			}
			replicaIdentityByFlow[flowName][tableName] = tableSchema.IsReplicaIdentityFull
			return nil
		}); err != nil {
			logger.Warn("failed to read table schema mapping", slog.Any("error", err))
		}
	}

	sourcePeerNames := make(map[string]struct{})
	for _, cfg := range configs {
		sourcePeerNames[cfg.SourceName] = struct{}{}

		logAsJSON(logger, "[flow config]", FlowConfigForLogging{
			FlowName:                    cfg.FlowJobName,
			SourcePeerName:              cfg.SourceName,
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
			NumTables:                   len(cfg.TableMappings),
		}, slog.String("flowName", cfg.FlowJobName))

		flowReplicaIdentity := replicaIdentityByFlow[cfg.FlowJobName]
		flowCounts := countsByFlow[cfg.FlowJobName]
		for _, tm := range cfg.TableMappings {
			counts := flowCounts[tm.DestinationTableIdentifier]
			hasCustomSortKey := slices.ContainsFunc(tm.Columns, func(col *protos.ColumnSetting) bool {
				return col.Ordering > 0
			})
			logAsJSON(logger, "[table config]", TableMappingForLogging{
				TableName:           tm.SourceTableIdentifier,
				DestTableName:       tm.DestinationTableIdentifier,
				PartitionKey:        tm.PartitionKey,
				Engine:              tm.Engine.String(),
				Exclude:             tm.Exclude,
				ReplicaIdentityFull: flowReplicaIdentity[tm.DestinationTableIdentifier],
				UseCustomSortKey:    hasCustomSortKey,
				TotalInserts:        counts.inserts,
				TotalUpdates:        counts.updates,
				TotalDeletes:        counts.deletes,
			}, slog.String("flowName", cfg.FlowJobName), slog.String("tableName", tm.SourceTableIdentifier))
		}
	}

	nameList := make([]string, 0, len(sourcePeerNames))
	for name := range sourcePeerNames {
		nameList = append(nameList, name)
	}

	peers, err := loadPeers(ctx, catalogPool, nameList)
	if err != nil {
		return fmt.Errorf("failed to load source peers: %w", err)
	}

	for _, peer := range peers {
		info := sourcePeerInfoFromDB(peer)
		populateRuntimeInfo(ctx, peer, &info)
		logAsJSON(logger, "[source info]", info, slog.String("peerName", peer.Name))
	}

	return nil
}

func sourcePeerInfoFromDB(peer *protos.Peer) SourcePeerInfoForLogging {
	info := SourcePeerInfoForLogging{
		PeerName: peer.Name,
		PeerType: peer.Type.String(),
	}
	switch config := peer.Config.(type) {
	case *protos.Peer_PostgresConfig:
		info.CustomTLS = config.PostgresConfig.RootCa != nil
		info.SSHTunnel = config.PostgresConfig.SshConfig != nil
	case *protos.Peer_MysqlConfig:
		info.CustomTLS = !config.MysqlConfig.DisableTls && config.MysqlConfig.RootCa != nil
		info.SSHTunnel = config.MysqlConfig.SshConfig != nil
	case *protos.Peer_MongoConfig:
		info.CustomTLS = !config.MongoConfig.DisableTls && config.MongoConfig.RootCa != nil
		info.SSHTunnel = config.MongoConfig.SshConfig != nil
	}
	return info
}

func logAsJSON(logger log.Logger, msg string, data any, attrs ...any) {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		logger.Error("failed to marshal "+msg, "error", err)
		return
	}
	attrs = append(attrs, slog.String("data", string(dataJSON)))
	logger.Info(msg, attrs...)
}
