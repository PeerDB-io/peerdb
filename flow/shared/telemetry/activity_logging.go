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
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
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

	if update.SnapshotMaxParallelWorkers > 0 {
		changes = append(changes, fmt.Sprintf("snapshotMaxParallelWorkers: %v", update.SnapshotMaxParallelWorkers))
	}
	if update.SnapshotNumTablesInParallel > 0 {
		changes = append(changes, fmt.Sprintf("snapshotNumTablesInParallel: %v", update.SnapshotNumTablesInParallel))
	}
	if update.SnapshotNumRowsPerPartition > 0 {
		changes = append(changes, fmt.Sprintf("snapshotNumRowsPerPartition: %v", update.SnapshotNumRowsPerPartition))
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
	PipeId                      string `json:"pipe_id"`
	PipeName                    string `json:"pipe_name"`
	SourcePeerName              string `json:"source_peer_name"`
	PublicationName             string `json:"pg_publication_name"`          // postgres
	ReplicationSlotName         string `json:"pg_replication_slot_name"`     // postgres
	ReplicationMechanismInUse   string `json:"replication_mechanism_in_use"` // mysql
	IdleTimeoutSeconds          uint64 `json:"sync_interval"`
	NumTables                   int    `json:"num_tables"`
	MaxBatchSize                uint32 `json:"max_batch_size"`
	SnapshotNumRowsPerPartition uint32 `json:"snapshot_num_rows_per_partition"`
	SnapshotMaxParallelWorkers  uint32 `json:"snapshot_max_parallel_workers"`
	SnapshotNumTablesInParallel uint32 `json:"snapshot_num_tables_in_parallel"`
	CdcOnly                     bool   `json:"cdc_only"`
	SnapshotOnly                bool   `json:"snapshot_only"`
	Resync                      bool   `json:"is_resync"`
}

type tableMappingForLogging struct {
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

type FlowConfigLogEntry struct {
	TableMappings []tableMappingForLogging
	FlowConfig    FlowConfigForLogging
}

//nolint:govet // field order is intentional for readability
type sourcePeerInfoForLogging struct {
	PeerName             string `json:"peer_name"`
	PeerType             string `json:"peer_type"`
	Host                 string `json:"host"`
	DatabaseVersion      string `json:"database_version"`
	DatabaseVariant      string `json:"database_variant"`
	DisableTLS           bool   `json:"disable_tls"`
	TLSHost              string `json:"tls_host"`
	DatabaseName         string `json:"database_name"`
	SshHost              string `json:"ssh_host"`
	AuthType             string `json:"auth_type"`
	Flavor               string `json:"flavor"`                // mysql
	Compression          uint32 `json:"compression"`           // mysql
	ReplicationMechanism string `json:"replication_mechanism"` // mysql
	ReadPreference       string `json:"read_preference"`       // mongo
}

func GetFlowConfigLogEntries(
	ctx context.Context,
	catalogPool shared.CatalogPool,
) ([]FlowConfigLogEntry, error) {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "LogFlowConfigs"))

	type flowConfigWithTags struct {
		config *protos.FlowConnectionConfigsCore
		tags   map[string]string
	}

	batch := pgx.Batch{}
	batch.Queue(`SELECT DISTINCT ON (name) name, config_proto, tags FROM flows WHERE config_proto IS NOT NULL`)
	batch.Queue(`SELECT flow_name, destination_table_name, inserts_count, updates_count, deletes_count
		FROM peerdb_stats.cdc_table_aggregate_counts`)
	batch.Queue(`SELECT flow_name, table_name, table_schema FROM table_schema_mapping`)

	batchResults := catalogPool.Pool.SendBatch(ctx, &batch)
	defer batchResults.Close()

	configRows, err := batchResults.Query()
	if err != nil {
		return nil, fmt.Errorf("failed to query flow configs: %w", err)
	}
	configs, err := pgx.CollectRows(configRows, func(row pgx.CollectableRow) (flowConfigWithTags, error) {
		var name string
		var configProto []byte
		var tags map[string]string
		if err := row.Scan(&name, &configProto, &tags); err != nil {
			return flowConfigWithTags{}, err
		}
		cfg := &protos.FlowConnectionConfigsCore{}
		if err := proto.Unmarshal(configProto, cfg); err != nil {
			return flowConfigWithTags{}, err
		}
		return flowConfigWithTags{config: cfg, tags: tags}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to collect flow configs: %w", err)
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

	flowConfigs := make([]FlowConfigLogEntry, 0, len(configs))
	for _, entry := range configs {
		cfg := entry.config
		fc := FlowConfigForLogging{
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
			PipeId:                      entry.tags[common.PipeIdTag],
			PipeName:                    entry.tags[common.PipeNameTag],
		}
		flowReplicaIdentity := replicaIdentityByFlow[cfg.FlowJobName]
		flowCounts := countsByFlow[cfg.FlowJobName]
		tableMappings := make([]tableMappingForLogging, 0, len(cfg.TableMappings))
		for _, tm := range cfg.TableMappings {
			counts := flowCounts[tm.DestinationTableIdentifier]
			hasCustomSortKey := slices.ContainsFunc(tm.Columns, func(col *protos.ColumnSetting) bool {
				return col.Ordering > 0
			})
			tableMappings = append(tableMappings, tableMappingForLogging{
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
			})
		}
		flowConfigs = append(flowConfigs, FlowConfigLogEntry{
			FlowConfig:    fc,
			TableMappings: tableMappings,
		})
	}

	return flowConfigs, nil
}

func LogFlowConfigs(ctx context.Context, flowConfigs []FlowConfigLogEntry) {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "LogFlowConfigs"))

	for i := range flowConfigs {
		entry := &flowConfigs[i]
		logAsJSON(logger, "[flow config]", entry.FlowConfig, slog.String("flowName", entry.FlowConfig.FlowName))
		for _, tableMapping := range entry.TableMappings {
			logAsJSON(logger, "[table config]", tableMapping,
				slog.String("flowName", entry.FlowConfig.FlowName),
				slog.String("tableName", tableMapping.TableName))
		}
	}
}

func LogPeerInfo(ctx context.Context, peer *protos.Peer, version string, variant string) {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "LogFlowConfigs"))
	info := &sourcePeerInfoForLogging{
		PeerName:        peer.Name,
		PeerType:        peer.Type.String(),
		DatabaseVersion: version,
		DatabaseVariant: variant,
	}
	switch config := peer.Config.(type) {
	case *protos.Peer_PostgresConfig:
		info.Host = config.PostgresConfig.Host
		info.DisableTLS = !config.PostgresConfig.RequireTls
		info.TLSHost = config.PostgresConfig.TlsHost
		info.SshHost = config.PostgresConfig.SshConfig.GetHost()
		info.DatabaseName = config.PostgresConfig.Database
		info.AuthType = config.PostgresConfig.AuthType.String()
	case *protos.Peer_MysqlConfig:
		info.Host = config.MysqlConfig.Host
		info.DisableTLS = config.MysqlConfig.DisableTls
		info.TLSHost = config.MysqlConfig.TlsHost
		info.SshHost = config.MysqlConfig.SshConfig.GetHost()
		info.DatabaseName = config.MysqlConfig.Database
		info.AuthType = config.MysqlConfig.AuthType.String()
		info.Flavor = config.MysqlConfig.Flavor.String()
		info.ReplicationMechanism = config.MysqlConfig.ReplicationMechanism.String()
		info.Compression = config.MysqlConfig.Compression
	case *protos.Peer_MongoConfig:
		info.Host = config.MongoConfig.Uri
		info.DisableTLS = config.MongoConfig.DisableTls
		info.TLSHost = config.MongoConfig.TlsHost
		info.SshHost = config.MongoConfig.SshConfig.GetHost()
		info.ReadPreference = config.MongoConfig.ReadPreference.String()
	case *protos.Peer_BigqueryConfig:
		info.AuthType = config.BigqueryConfig.AuthType
	}
	logAsJSON(logger, "[peer info]", info, slog.String("peerName", peer.Name))
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
