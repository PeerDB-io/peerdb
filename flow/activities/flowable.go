package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	lua "github.com/yuin/gopher-lua"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors"
	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/otel_metrics"
	"github.com/PeerDB-io/peer-flow/otel_metrics/peerdb_guages"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	NeedsSetupMetadataTables bool
}

type CdcCacheEntry struct {
	connector connectors.CDCPullConnectorCore
	done      chan struct{}
}

type FlowableActivity struct {
	CatalogPool *pgxpool.Pool
	Alerter     *alerting.Alerter
	CdcCache    map[string]CdcCacheEntry
	OtelManager *otel_metrics.OtelManager
	CdcCacheRw  sync.RWMutex
}

func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.SetupInput,
) (*CheckConnectionResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	needsSetup := dstConn.NeedsSetupMetadataTables(ctx)

	return &CheckConnectionResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.SetupInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if err := dstConn.SetupMetadataTables(ctx); err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	output, err := srcConn.EnsurePullability(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	return output, nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	res, err := dstConn.CreateRawTable(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, err
	}
	err = monitoring.InitializeCDCFlow(ctx, a.CatalogPool, config.FlowJobName)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// GetTableSchema returns the schema of a table.
func (a *FlowableActivity) GetTableSchema(
	ctx context.Context,
	config *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	srcConn, err := connectors.GetAs[connectors.GetTableSchemaConnector](ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get GetTableSchemaConnector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	heartbeatRoutine(ctx, func() string {
		return "getting table schema"
	})

	return srcConn.GetTableSchema(ctx, config)
}

// CreateNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	logger := activity.GetLogger(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetAs[connectors.NormalizedTablesConnector](ctx, config.PeerConnectionConfig)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			logger.Info("Connector does not implement normalized tables")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	tx, err := conn.StartSetupNormalizedTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup normalized tables tx: %w", err)
	}
	defer conn.CleanupSetupNormalizedTables(ctx, tx)

	numTablesSetup := atomic.Uint32{}
	totalTables := uint32(len(config.TableNameSchemaMapping))
	shutdown := heartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("setting up normalized tables - %d of %d done",
			numTablesSetup.Load(), totalTables)
	})
	defer shutdown()

	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range config.TableNameSchemaMapping {
		var existing bool
		existing, err = conn.SetupNormalizedTable(
			ctx,
			tx,
			tableIdentifier,
			tableSchema,
			config.SoftDeleteColName,
			config.SyncedAtColName,
		)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowName, err)
			return nil, fmt.Errorf("failed to setup normalized table %s: %w", tableIdentifier, err)
		}
		tableExistsMapping[tableIdentifier] = existing

		numTablesSetup.Add(1)
		if !existing {
			logger.Info("created table " + tableIdentifier)
		} else {
			logger.Info("table already exists " + tableIdentifier)
		}
	}

	if err := conn.FinishSetupNormalizedTables(ctx, tx); err != nil {
		return nil, fmt.Errorf("failed to commit normalized tables tx: %w", err)
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (a *FlowableActivity) MaintainPull(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	sessionID string,
) error {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
	if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.SetupReplConn(ctx); err != nil {
		return err
	}

	done := make(chan struct{})
	a.CdcCacheRw.Lock()
	a.CdcCache[sessionID] = CdcCacheEntry{
		connector: srcConn,
		done:      done,
	}
	a.CdcCacheRw.Unlock()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "keep session alive")
			if err := srcConn.ReplPing(ctx); err != nil {
				a.CdcCacheRw.Lock()
				delete(a.CdcCache, sessionID)
				a.CdcCacheRw.Unlock()
				return temporal.NewNonRetryableApplicationError("connection to source down", "disconnect", err)
			}
		case <-done:
			return nil
		case <-ctx.Done():
			a.CdcCacheRw.Lock()
			delete(a.CdcCache, sessionID)
			a.CdcCacheRw.Unlock()
			return nil
		}
	}
}

func (a *FlowableActivity) UnmaintainPull(ctx context.Context, sessionID string) error {
	a.CdcCacheRw.Lock()
	if entry, ok := a.CdcCache[sessionID]; ok {
		close(entry.done)
		delete(a.CdcCache, sessionID)
	}
	a.CdcCacheRw.Unlock()
	return nil
}

func (a *FlowableActivity) SyncRecords(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	sessionID string,
) (*model.SyncResponse, error) {
	var adaptStream func(stream *model.CDCStream[model.RecordItems]) (*model.CDCStream[model.RecordItems], error)
	if config.Script != "" {
		var onErr context.CancelCauseFunc
		ctx, onErr = context.WithCancelCause(ctx)
		adaptStream = func(stream *model.CDCStream[model.RecordItems]) (*model.CDCStream[model.RecordItems], error) {
			ls, err := utils.LoadScript(ctx, config.Script, utils.LuaPrintFn(func(s string) {
				a.Alerter.LogFlowInfo(ctx, config.FlowJobName, s)
			}))
			if err != nil {
				a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
				return nil, err
			}
			if fn, ok := ls.Env.RawGetString("transformRecord").(*lua.LFunction); ok {
				return pua.AttachToCdcStream(ctx, ls, fn, stream, onErr), nil
			} else if fn, ok := ls.Env.RawGetString("transformRow").(*lua.LFunction); ok {
				return pua.AttachToCdcStream(ctx, ls, ls.NewFunction(func(ls *lua.LState) int {
					ud, _ := pua.LuaRecord.Check(ls, 1)
					for _, key := range []string{"old", "new"} {
						if row := ls.GetField(ud, key); row != lua.LNil {
							ls.Push(fn)
							ls.Push(row)
							ls.Call(1, 0)
						}
					}
					return 0
				}), stream, onErr), nil
			}
			return stream, nil
		}
	}
	return syncCore(ctx, a, config, options, sessionID, adaptStream,
		connectors.CDCPullConnector.PullRecords,
		connectors.CDCSyncConnector.SyncRecords)
}

func (a *FlowableActivity) SyncPg(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	sessionID string,
) (*model.SyncResponse, error) {
	return syncCore(ctx, a, config, options, sessionID, nil,
		connectors.CDCPullPgConnector.PullPg,
		connectors.CDCSyncPgConnector.SyncPg)
}

func (a *FlowableActivity) StartNormalize(
	ctx context.Context,
	input *protos.StartNormalizeInput,
) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs
	ctx = context.WithValue(ctx, shared.FlowNameKey, conn.FlowJobName)
	logger := activity.GetLogger(ctx)

	dstConn, err := connectors.GetCDCNormalizeConnector(ctx, conn.Destination)
	if errors.Is(err, errors.ErrUnsupported) {
		err = monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, input.FlowConnectionConfigs.FlowJobName,
			input.SyncBatchID)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := heartbeatRoutine(ctx, func() string {
		return "normalizing records from batch for job"
	})
	defer shutdown()

	res, err := dstConn.NormalizeRecords(ctx, &model.NormalizeRecordsRequest{
		FlowJobName:            input.FlowConnectionConfigs.FlowJobName,
		SyncBatchID:            input.SyncBatchID,
		SoftDelete:             input.FlowConnectionConfigs.SoftDelete,
		SoftDeleteColName:      input.FlowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:        input.FlowConnectionConfigs.SyncedAtColName,
		TableNameSchemaMapping: input.TableNameSchemaMapping,
	})
	if err != nil {
		a.Alerter.LogFlowError(ctx, input.FlowConnectionConfigs.FlowJobName, err)
		return nil, fmt.Errorf("failed to normalized records: %w", err)
	}

	// normalize flow did not run due to no records, no need to update end time.
	if res.Done {
		err = monitoring.UpdateEndTimeForCDCBatch(
			ctx,
			a.CatalogPool,
			input.FlowConnectionConfigs.FlowJobName,
			res.EndBatchID,
		)
		if err != nil {
			return nil, err
		}
	}

	// log the number of batches normalized
	logger.Info(fmt.Sprintf("normalized records from batch %d to batch %d",
		res.StartBatchID, res.EndBatchID))

	return res, nil
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	err = conn.SetupQRepMetadataTables(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	runUUID string,
) (*protos.QRepParitionResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	err := monitoring.InitializeQRepRun(ctx, a.CatalogPool, config, runUUID, nil)
	if err != nil {
		return nil, err
	}
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get qrep pull connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := heartbeatRoutine(ctx, func() string {
		return "getting partitions for job"
	})
	defer shutdown()
	partitions, err := srcConn.GetQRepPartitions(ctx, config, last)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to get partitions from source: %w", err)
	}
	if len(partitions) > 0 {
		err = monitoring.InitializeQRepRun(
			ctx,
			a.CatalogPool,
			config,
			runUUID,
			partitions,
		)
		if err != nil {
			return nil, err
		}
	}

	return &protos.QRepParitionResult{
		Partitions: partitions,
	}, nil
}

// ReplicateQRepPartitions spawns multiple ReplicateQRepPartition
func (a *FlowableActivity) ReplicateQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	partitions *protos.QRepPartitionBatch,
	runUUID string,
) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "replicating partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(activity.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))
	err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	if err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)

	logger.Info(fmt.Sprintf("replicating partitions for batch %d - size: %d",
		partitions.BatchId, numPartitions),
	)
	for _, p := range partitions.Partitions {
		logger.Info(fmt.Sprintf("batch-%d - replicating partition - %s", partitions.BatchId, p.PartitionId))
		var err error
		switch config.System {
		case protos.TypeSystem_Q:
			stream := model.NewQRecordStream(shared.FetchAndChannelSize)
			outstream := stream
			if config.Script != "" {
				ls, err := utils.LoadScript(ctx, config.Script, utils.LuaPrintFn(func(s string) {
					a.Alerter.LogFlowInfo(ctx, config.FlowJobName, s)
				}))
				if err != nil {
					a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
					return err
				}
				if fn, ok := ls.Env.RawGetString("transformRow").(*lua.LFunction); ok {
					outstream = pua.AttachToStream(ls, fn, stream)
				}
			}
			err = replicateQRepPartition(ctx, a, config, p, runUUID, stream, outstream,
				connectors.QRepPullConnector.PullQRepRecords,
				connectors.QRepSyncConnector.SyncQRepRecords,
			)
		case protos.TypeSystem_PG:
			read, write := connpostgres.NewPgCopyPipe()
			err = replicateQRepPartition(ctx, a, config, p, runUUID, write, read,
				connectors.QRepPullPgConnector.PullPgQRepRecords,
				connectors.QRepSyncPgConnector.SyncPgQRepRecords,
			)
		default:
			err = fmt.Errorf("unknown type system %d", config.System)
		}

		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return err
		}
	}

	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, errors.ErrUnsupported) {
		return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	} else if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := heartbeatRoutine(ctx, func() string {
		return "consolidating partitions for job"
	})
	defer shutdown()

	err = dstConn.ConsolidateQRepPartitions(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return err
	}

	return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dst, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	} else if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return err
	}
	defer dst.Close()

	return dst.CleanupQRepFlow(ctx, config)
}

func (a *FlowableActivity) DropFlowSource(ctx context.Context, config *protos.ShutdownRequest) error {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	return srcConn.PullFlowCleanup(ctx, config.FlowJobName)
}

func (a *FlowableActivity) DropFlowDestination(ctx context.Context, config *protos.ShutdownRequest) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	return dstConn.SyncFlowCleanup(ctx, config.FlowJobName)
}

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	logger := activity.GetLogger(ctx)
	walHeartbeatEnabled, err := peerdbenv.PeerDBEnableWALHeartbeat(ctx)
	if err != nil {
		logger.Warn("unable to fetch wal heartbeat config. Skipping walheartbeat send.", slog.Any("error", err))
		return err
	}
	if !walHeartbeatEnabled {
		logger.Info("wal heartbeat is disabled")
		return nil
	}
	walHeartbeatStatement, err := peerdbenv.PeerDBWALHeartbeatQuery(ctx)
	if err != nil {
		logger.Warn("unable to fetch wal heartbeat config. Skipping walheartbeat send.", slog.Any("error", err))
		return err
	}

	pgPeers, err := a.getPostgresPeerConfigs(ctx)
	if err != nil {
		logger.Warn("[sendwalheartbeat] unable to fetch peers. Skipping walheartbeat send.", slog.Any("error", err))
		return err
	}

	// run above command for each Postgres peer
	for _, pgPeer := range pgPeers {
		activity.RecordHeartbeat(ctx, pgPeer.Name)
		if ctx.Err() != nil {
			return nil
		}

		func() {
			pgConfig := pgPeer.GetPostgresConfig()
			pgConn, peerErr := connpostgres.NewPostgresConnector(ctx, pgConfig)
			if peerErr != nil {
				logger.Error(fmt.Sprintf("error creating connector for postgres peer %v with host %v: %v",
					pgPeer.Name, pgConfig.Host, peerErr))
				return
			}
			defer pgConn.Close()
			cmdErr := pgConn.ExecuteCommand(ctx, walHeartbeatStatement)
			if cmdErr != nil {
				logger.Warn(fmt.Sprintf("could not send walheartbeat to peer %v: %v", pgPeer.Name, cmdErr))
			}
			logger.Info(fmt.Sprintf("sent walheartbeat to peer %v", pgPeer.Name))
		}()
	}

	return nil
}

func (a *FlowableActivity) RecordSlotSizes(ctx context.Context) error {
	rows, err := a.CatalogPool.Query(ctx, "SELECT DISTINCT ON (name) name, config_proto FROM flows WHERE query_string IS NULL")
	if err != nil {
		return err
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.FlowConnectionConfigs, error) {
		var flowName string
		var configProto []byte
		err := rows.Scan(&flowName, &configProto)
		if err != nil {
			return nil, err
		}

		var config protos.FlowConnectionConfigs
		err = proto.Unmarshal(configProto, &config)
		if err != nil {
			return nil, err
		}

		return &config, nil
	})
	if err != nil {
		return err
	}

	logger := activity.GetLogger(ctx)
	for _, config := range configs {
		func() {
			srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
			if err != nil {
				if !errors.Is(err, errors.ErrUnsupported) {
					logger.Error("Failed to create connector to handle slot info", slog.Any("error", err))
				}
				return
			}
			defer connectors.CloseConnector(ctx, srcConn)

			slotName := "peerflow_slot_" + config.FlowJobName
			if config.ReplicationSlotName != "" {
				slotName = config.ReplicationSlotName
			}
			peerName := config.Source.Name

			activity.RecordHeartbeat(ctx, fmt.Sprintf("checking %s on %s", slotName, peerName))
			if ctx.Err() != nil {
				return
			}

			slotMetricGuages := peerdb_guages.SlotMetricGuages{}
			if a.OtelManager != nil {
				slotLagGauge, err := otel_metrics.GetOrInitFloat64SyncGauge(a.OtelManager.Meter,
					a.OtelManager.Float64GaugesCache,
					peerdb_guages.SlotLagGuageName,
					metric.WithUnit("MB"),
					metric.WithDescription("Postgres replication slot lag in MB"))
				if err != nil {
					logger.Error("Failed to get slot lag gauge", slog.Any("error", err))
					return
				}
				slotMetricGuages.SlotLagGuage = slotLagGauge

				openConnectionsGauge, err := otel_metrics.GetOrInitInt64SyncGauge(a.OtelManager.Meter,
					a.OtelManager.Int64GaugesCache,
					peerdb_guages.OpenConnectionsGuageName,
					metric.WithDescription("Current open connections for PeerDB user"))
				if err != nil {
					logger.Error("Failed to get open connections gauge", slog.Any("error", err))
					return
				}
				slotMetricGuages.OpenConnectionsGuage = openConnectionsGauge

				openReplicationConnectionsGauge, err := otel_metrics.GetOrInitInt64SyncGauge(a.OtelManager.Meter,
					a.OtelManager.Int64GaugesCache,
					peerdb_guages.OpenReplicationConnectionsGuageName,
					metric.WithDescription("Current open replication connections for PeerDB user"))
				if err != nil {
					logger.Error("Failed to get open replication connections gauge", slog.Any("error", err))
					return
				}
				slotMetricGuages.OpenReplicationConnectionsGuage = openReplicationConnectionsGauge
			}

			err = srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, slotName, peerName, slotMetricGuages)
			if err != nil {
				logger.Error("Failed to handle slot info", slog.Any("error", err))
			}
		}()
		if ctx.Err() != nil {
			return nil
		}
	}

	return nil
}

type QRepWaitUntilNewRowsResult struct {
	Found bool
}

func (a *FlowableActivity) QRepHasNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) (QRepWaitUntilNewRowsResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(activity.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	if config.SourcePeer.Type != protos.DBType_POSTGRES {
		return QRepWaitUntilNewRowsResult{Found: true}, nil
	}

	logger.Info(fmt.Sprintf("current last partition value is %v", last))

	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return QRepWaitUntilNewRowsResult{Found: false}, fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := heartbeatRoutine(ctx, func() string {
		return "scanning for new rows"
	})
	defer shutdown()

	pgSrcConn := srcConn.(*connpostgres.PostgresConnector)
	result, err := pgSrcConn.CheckForUpdatedMaxValue(ctx, config, last)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return QRepWaitUntilNewRowsResult{Found: false}, fmt.Errorf("failed to check for new rows: %w", err)
	}

	return QRepWaitUntilNewRowsResult{Found: result}, nil
}

func (a *FlowableActivity) RenameTables(ctx context.Context, config *protos.RenameTablesInput) (
	*protos.RenameTablesOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	conn, err := connectors.GetAs[connectors.RenameTablesConnector](ctx, config.Peer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	shutdown := heartbeatRoutine(ctx, func() string {
		return "renaming tables for job"
	})
	defer shutdown()

	return conn.RenameTables(ctx, config)
}

func (a *FlowableActivity) CreateTablesFromExisting(ctx context.Context, req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, req.Peer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if req.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			return nil, errors.New("failed to cast connector to snowflake connector")
		}
		return sfConn.CreateTablesFromExisting(ctx, req)
	} else if req.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			return nil, errors.New("failed to cast connector to bigquery connector")
		}
		return bqConn.CreateTablesFromExisting(ctx, req)
	}
	a.Alerter.LogFlowError(ctx, req.FlowJobName, err)
	return nil, errors.New("create tables from existing is only supported on snowflake and bigquery")
}

func (a *FlowableActivity) ReplicateXminPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
) (int64, error) {
	switch config.System {
	case protos.TypeSystem_Q:
		stream := model.NewQRecordStream(shared.FetchAndChannelSize)
		return replicateXminPartition(ctx, a, config, partition, runUUID,
			stream, stream,
			(*connpostgres.PostgresConnector).PullXminRecordStream,
			connectors.QRepSyncConnector.SyncQRepRecords)
	case protos.TypeSystem_PG:
		pgread, pgwrite := connpostgres.NewPgCopyPipe()
		return replicateXminPartition(ctx, a, config, partition, runUUID,
			pgwrite, pgread,
			(*connpostgres.PostgresConnector).PullXminPgRecordStream,
			connectors.QRepSyncPgConnector.SyncPgQRepRecords)
	default:
		return 0, fmt.Errorf("unknown type system %d", config.System)
	}
}

func (a *FlowableActivity) AddTablesToPublication(ctx context.Context, cfg *protos.FlowConnectionConfigs,
	additionalTableMappings []*protos.TableMapping,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, cfg.Source)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	err = srcConn.AddTablesToPublication(ctx, &protos.AddTablesToPublicationInput{
		FlowJobName:      cfg.FlowJobName,
		PublicationName:  cfg.PublicationName,
		AdditionalTables: additionalTableMappings,
	})
	if err != nil {
		a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}
	return err
}

func (a *FlowableActivity) LoadPeer(ctx context.Context, peerName string) (*protos.Peer, error) {
	row := a.CatalogPool.QueryRow(ctx, `
		SELECT name, type, options
		FROM peers
		WHERE name = $1`, peerName)

	var peer protos.Peer
	var peerOptions []byte
	if err := row.Scan(&peer.Name, &peer.Type, &peerOptions); err != nil {
		return nil, fmt.Errorf("failed to load peer: %w", err)
	}

	switch peer.Type {
	case protos.DBType_BIGQUERY:
		var config protos.BigqueryConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BigQuery config: %w", err)
		}
		peer.Config = &protos.Peer_BigqueryConfig{BigqueryConfig: &config}
	case protos.DBType_SNOWFLAKE:
		var config protos.SnowflakeConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Snowflake config: %w", err)
		}
		peer.Config = &protos.Peer_SnowflakeConfig{SnowflakeConfig: &config}
	case protos.DBType_MONGO:
		var config protos.MongoConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal MongoDB config: %w", err)
		}
		peer.Config = &protos.Peer_MongoConfig{MongoConfig: &config}
	case protos.DBType_POSTGRES:
		var config protos.PostgresConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Postgres config: %w", err)
		}
		peer.Config = &protos.Peer_PostgresConfig{PostgresConfig: &config}
	case protos.DBType_S3:
		var config protos.S3Config
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal S3 config: %w", err)
		}
		peer.Config = &protos.Peer_S3Config{S3Config: &config}
	case protos.DBType_SQLSERVER:
		var config protos.SqlServerConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal SQL Server config: %w", err)
		}
		peer.Config = &protos.Peer_SqlserverConfig{SqlserverConfig: &config}
	case protos.DBType_MYSQL:
		var config protos.MySqlConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal MySQL config: %w", err)
		}
		peer.Config = &protos.Peer_MysqlConfig{MysqlConfig: &config}
	case protos.DBType_CLICKHOUSE:
		var config protos.ClickhouseConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ClickHouse config: %w", err)
		}
		peer.Config = &protos.Peer_ClickhouseConfig{ClickhouseConfig: &config}
	case protos.DBType_KAFKA:
		var config protos.KafkaConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Kafka config: %w", err)
		}
		peer.Config = &protos.Peer_KafkaConfig{KafkaConfig: &config}
	case protos.DBType_PUBSUB:
		var config protos.PubSubConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Pub/Sub config: %w", err)
		}
		peer.Config = &protos.Peer_PubsubConfig{PubsubConfig: &config}
	case protos.DBType_EVENTHUBS:
		var config protos.EventHubGroupConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Event Hubs config: %w", err)
		}
		peer.Config = &protos.Peer_EventhubGroupConfig{EventhubGroupConfig: &config}
	case protos.DBType_ELASTICSEARCH:
		var config protos.ElasticsearchConfig
		if err := proto.Unmarshal(peerOptions, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Elasticsearch config: %w", err)
		}
		peer.Config = &protos.Peer_ElasticsearchConfig{ElasticsearchConfig: &config}
	default:
		return nil, fmt.Errorf("unsupported peer type: %s", peer.Type)
	}

	return &peer, nil
}
