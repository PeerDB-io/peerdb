package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	lua "github.com/yuin/gopher-lua"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pua"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

type CheckMetadataTablesResult struct {
	NeedsSetupMetadataTables bool
}

type FlowableActivity struct {
	CatalogPool    shared.CatalogPool
	Alerter        *alerting.Alerter
	OtelManager    *otel_metrics.OtelManager
	TemporalClient client.Client
}

type StreamCloser interface {
	Close(error)
}

func cdcIdleTimeout(value int) time.Duration {
	if value == 0 {
		value = 10
	}
	return time.Duration(value) * time.Second
}

func (a *FlowableActivity) Alert(
	ctx context.Context,
	alert *protos.AlertInput,
) error {
	_ = a.Alerter.LogFlowError(ctx, alert.FlowName, errors.New(alert.Message))
	return nil
}

func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.SetupInput,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, connClose, err := connectors.GetByNameAs[connectors.Connector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connClose(ctx)

	if err = conn.ConnectionActive(ctx); err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("connection not active: %w", err))
	}

	return nil
}

func (a *FlowableActivity) CheckMetadataTables(
	ctx context.Context,
	config *protos.SetupInput,
) (*CheckMetadataTablesResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, connClose, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connClose(ctx)

	needsSetup, err := conn.NeedsSetupMetadataTables(ctx)
	if err != nil {
		return nil, err
	}

	return &CheckMetadataTablesResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.SetupInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer dstClose(ctx)

	if err := dstConn.SetupMetadataTables(ctx); err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to setup metadata tables: %w", err))
	}

	return nil
}

func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer srcClose(ctx)

	output, err := srcConn.EnsurePullability(ctx, config)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to ensure pullability: %w", err))
	}

	return output, nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer dstClose(ctx)

	res, err := dstConn.CreateRawTable(ctx, config)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}
	if err := monitoring.InitializeCDCFlow(ctx, a.CatalogPool, config.FlowJobName); err != nil {
		return nil, err
	}

	return res, nil
}

// SetupTableSchema populates table_schema_mapping
func (a *FlowableActivity) SetupTableSchema(
	ctx context.Context,
	config *protos.SetupTableSchemaBatchInput,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "getting table schema"
	})
	defer shutdown()

	logger := internal.LoggerFromCtx(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get GetTableSchemaConnector: %w", err))
	}
	defer srcClose(ctx)

	tableNameSchemaMapping, err := srcConn.GetTableSchema(ctx, config.Env, config.Version, config.System, config.TableMappings)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get GetTableSchemaConnector: %w", err))
	}
	processed := internal.BuildProcessedSchemaMapping(config.TableMappings, tableNameSchemaMapping, logger)

	tx, err := a.CatalogPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer shared.RollbackTx(tx, logger)

	for k, v := range processed {
		processedBytes, err := proto.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			"insert into table_schema_mapping(flow_name, table_name, table_schema) values ($1, $2, $3) "+
				"on conflict (flow_name, table_name) do update set table_schema = $3",
			config.FlowName,
			k,
			processedBytes,
		); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// CreateNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	numTablesSetup := atomic.Uint32{}
	numTablesToSetup := atomic.Int32{}

	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("setting up normalized tables - %d of %d done", numTablesSetup.Load(), numTablesToSetup.Load())
	})
	defer shutdown()

	logger := internal.LoggerFromCtx(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	a.Alerter.LogFlowInfo(ctx, config.FlowName, "Setting up destination tables")
	conn, connClose, err := connectors.GetByNameAs[connectors.NormalizedTablesConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			logger.Info("Connector does not implement normalized tables")
			return nil, nil
		}
		return nil, a.Alerter.LogFlowError(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connClose(ctx)

	tx, err := conn.StartSetupNormalizedTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup normalized tables tx: %w", err)
	}
	defer conn.CleanupSetupNormalizedTables(ctx, tx)

	tableNameSchemaMapping, err := a.getTableNameSchemaMapping(ctx, config.FlowName)
	if err != nil {
		return nil, err
	}

	numTablesToSetup.Store(int32(len(tableNameSchemaMapping)))
	tableExistsMapping := make(map[string]bool, len(tableNameSchemaMapping))
	for _, tableMapping := range config.TableMappings {
		tableIdentifier := tableMapping.DestinationTableIdentifier
		tableSchema := tableNameSchemaMapping[tableIdentifier]
		existing, err := conn.SetupNormalizedTable(
			ctx,
			tx,
			config,
			tableIdentifier,
			tableSchema,
		)
		if err != nil {
			return nil, a.Alerter.LogFlowError(ctx, config.FlowName,
				fmt.Errorf("failed to setup normalized table %s: %w", tableIdentifier, err),
			)
		}
		tableExistsMapping[tableIdentifier] = existing

		numTablesSetup.Add(1)
		if !existing {
			a.Alerter.LogFlowInfo(ctx, config.FlowName, "created table "+tableIdentifier+" in destination")
		} else {
			logger.Info("table already exists " + tableIdentifier)
		}
	}

	if err := conn.FinishSetupNormalizedTables(ctx, tx); err != nil {
		return nil, fmt.Errorf("failed to commit normalized tables tx: %w", err)
	}

	a.Alerter.LogFlowInfo(ctx, config.FlowName, "All destination tables have been setup")

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (a *FlowableActivity) SyncFlow(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	options *protos.SyncFlowOptions,
) error {
	var currentSyncFlowNum atomic.Int32
	var totalRecordsSynced atomic.Int64
	var normalizingBatchID atomic.Int64
	var normalizeWaiting atomic.Bool
	var syncingBatchID atomic.Int64
	var syncState atomic.Pointer[string]
	syncState.Store(shared.Ptr("setup"))
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		// Must load Waiting after BatchID to avoid race saying we're waiting on currently processing batch
		sBatchID := syncingBatchID.Load()
		nBatchID := normalizingBatchID.Load()
		var nWaiting string
		if normalizeWaiting.Load() {
			nWaiting = " (W)"
		}
		return fmt.Sprintf(
			"currentSyncFlowNum:%d, totalRecordsSynced:%d, syncingBatchID:%d (%s), normalizingBatchID:%d%s",
			currentSyncFlowNum.Load(), totalRecordsSynced.Load(),
			sBatchID, *syncState.Load(), nBatchID, nWaiting,
		)
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	// This is kept here and not deeper as we can have errors during SetupReplConn
	ctx = internal.WithOperationContext(ctx, protos.FlowOperation_FLOW_OPERATION_SYNC)
	logger := internal.LoggerFromCtx(ctx)

	srcConn, srcClose, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	if err := srcConn.SetupReplConn(ctx, config.Env); err != nil {
		srcClose(ctx)
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	reconnectAfterBatches, err := internal.PeerDBReconnectAfterBatches(ctx, config.Env)
	if err != nil {
		srcClose(ctx)
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	// syncDone will be closed by SyncFlow,
	// whereas normalizeDone will be closed by normalizing goroutine
	// Wait on normalizeDone at end to not interrupt final normalize
	syncDone := make(chan struct{})
	normRequests := concurrency.NewLastChan()
	normResponses := concurrency.NewLastChan()
	normBufferHours, err := internal.PeerDBNormalizeBufferHours(ctx, config.Env)
	if err != nil {
		srcClose(ctx)
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}
	idleTimeout := cdcIdleTimeout(int(options.IdleTimeoutSeconds))
	// normBufferSize allows _approximately_ normBufferHours delay between pull/sync and normalize
	// under normal steady operation where the batch hits idle timeout every time it will match the hours very closely
	// effective hours will be longer if pull is idling, or there are waits on big transactions,
	// or the sync interval is so small that start/stop overhead starts being visible
	// will be shorter if the batches hit the size limit rather rather than idle timeout
	normBufferSize := normBufferHours * 3600 / int64(idleTimeout.Seconds())
	// Normalize is always 1 batch behind, allow 2 to still run in parallel with pull-sync
	normBufferSize = max(normBufferSize, 2)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		normalizeCtx := internal.WithOperationContext(groupCtx, protos.FlowOperation_FLOW_OPERATION_NORMALIZE)
		// returning error signals sync to stop, normalize can recover connections without interrupting sync, so never return error
		a.normalizeLoop(normalizeCtx, logger, config, syncDone, normRequests, normResponses, &normalizingBatchID, &normalizeWaiting)
		return nil
	})
	group.Go(func() error {
		defer srcClose(groupCtx)
		if err := a.maintainReplConn(groupCtx, config.FlowJobName, srcConn, syncDone); err != nil {
			return a.Alerter.LogFlowError(groupCtx, config.FlowJobName, err)
		}
		return nil
	})

	for groupCtx.Err() == nil {
		syncNum := currentSyncFlowNum.Add(1)
		logger.Info("executing sync flow", slog.Int64("count", int64(syncNum)))

		var syncResponse *model.SyncResponse
		var syncErr error
		if config.System == protos.TypeSystem_Q {
			syncResponse, syncErr = a.syncRecords(groupCtx, config, options, srcConn.(connectors.CDCPullConnector),
				normRequests, normResponses, normBufferSize, idleTimeout, &syncingBatchID, &syncState)
		} else {
			syncResponse, syncErr = a.syncPg(groupCtx, config, options, srcConn.(connectors.CDCPullPgConnector),
				normRequests, normResponses, normBufferSize, idleTimeout, &syncingBatchID, &syncState)
		}

		if syncErr != nil {
			if groupCtx.Err() != nil {
				// need to return ctx.Err(), avoid returning syncErr that's wrapped context canceled
				break
			}
			logger.Error("failed to sync records", slog.Any("error", syncErr))
			syncState.Store(shared.Ptr("cleanup"))
			close(syncDone)
			normRequests.Close()
			normResponses.Close()
			return errors.Join(syncErr, group.Wait())
		} else if syncResponse != nil {
			totalRecordsSynced.Add(syncResponse.NumRecordsSynced)
			logger.Info("synced records", slog.Int64("numRecordsSynced", syncResponse.NumRecordsSynced),
				slog.Int64("totalRecordsSynced", totalRecordsSynced.Load()))
			a.OtelManager.Metrics.RecordsSyncedGauge.Record(ctx, syncResponse.NumRecordsSynced, metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.BatchIdKey, strconv.FormatInt(syncResponse.CurrentSyncBatchID, 10)),
			)))
			a.OtelManager.Metrics.RecordsSyncedCounter.Add(ctx, syncResponse.NumRecordsSynced)
		}
		if reconnectAfterBatches > 0 && syncNum >= reconnectAfterBatches {
			break
		}
	}

	syncState.Store(shared.Ptr("cleanup"))
	close(syncDone)
	normRequests.Close()
	normResponses.Close()

	waitErr := group.Wait()
	if err := ctx.Err(); err != nil {
		logger.Info("sync canceled", slog.Any("error", err))
		return err
	} else if waitErr != nil {
		logger.Error("sync failed", slog.Any("error", waitErr))
		return waitErr
	}
	return nil
}

func (a *FlowableActivity) syncRecords(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	options *protos.SyncFlowOptions,
	srcConn connectors.CDCPullConnector,
	normRequests *concurrency.LastChan,
	normResponses *concurrency.LastChan,
	normBufferSize int64,
	idleTimeout time.Duration,
	syncingBatchID *atomic.Int64,
	syncWaiting *atomic.Pointer[string],
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
				return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
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
	return syncCore(ctx, a, config, options, srcConn,
		normRequests, normResponses, normBufferSize, idleTimeout,
		syncingBatchID, syncWaiting, adaptStream,
		connectors.CDCPullConnector.PullRecords,
		connectors.CDCSyncConnector.SyncRecords)
}

func (a *FlowableActivity) syncPg(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	options *protos.SyncFlowOptions,
	srcConn connectors.CDCPullPgConnector,
	normRequests *concurrency.LastChan,
	normResponses *concurrency.LastChan,
	normBufferSize int64,
	idleTimeout time.Duration,
	syncingBatchID *atomic.Int64,
	syncWaiting *atomic.Pointer[string],
) (*model.SyncResponse, error) {
	return syncCore(ctx, a, config, options, srcConn,
		normRequests, normResponses, normBufferSize, idleTimeout,
		syncingBatchID, syncWaiting, nil,
		connectors.CDCPullPgConnector.PullPg,
		connectors.CDCSyncPgConnector.SyncPg)
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, connClose, err := connectors.GetByNameAs[connectors.QRepSyncConnector](ctx, config.Env, a.CatalogPool, config.DestinationName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connClose(ctx)

	if err := conn.SetupQRepMetadataTables(ctx, config); err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to setup metadata tables: %w", err))
	}

	return nil
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	runUUID string,
) (*protos.QRepParitionResult, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "getting partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))
	if err := monitoring.InitializeQRepRun(ctx, logger, a.CatalogPool, config, runUUID, nil, config.ParentMirrorName); err != nil {
		return nil, err
	}
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.QRepPullConnectorCore](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep pull connector: %w", err))
	}
	defer srcClose(ctx)

	partitioned := config.WatermarkColumn != ""
	if tableSizeEstimatorConn, ok := srcConn.(connectors.TableSizeEstimatorConnector); ok && partitioned {
		// expect estimate query execution to be fast, set a short timeout defensively to avoid blocking workflow
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if bytes, connErr := tableSizeEstimatorConn.GetTableSizeEstimatedBytes(timeoutCtx, config.WatermarkTable); connErr == nil {
			if bytes > 100<<30 { // 100 GiB
				msg := fmt.Sprintf("large table detected: %s (%s). Counting/partitioning queries for parallel "+
					"snapshotting may take minutes to hours to execute. This is normal for tables over 100 GiB.",
					config.WatermarkTable, utils.FormatTableSize(bytes))
				a.Alerter.LogFlowInfo(ctx, config.FlowJobName, msg)
			}
		} else {
			logger.Warn("failed to get estimated table size", slog.Any("error", connErr))
		}
	}

	partitions, err := srcConn.GetQRepPartitions(ctx, config, last)
	if err != nil {
		return nil, a.Alerter.LogFlowWrappedError(ctx, config.FlowJobName, "failed to get partitions from source", err)
	}
	if len(partitions) > 0 {
		if err := monitoring.InitializeQRepRun(
			ctx,
			logger,
			a.CatalogPool,
			config,
			runUUID,
			partitions,
			config.ParentMirrorName,
		); err != nil {
			return nil, err
		}
	}

	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "obtained partitions for table "+config.WatermarkTable)

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
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "replicating partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	if err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID); err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)
	logger.Info("replicating partitions for batch",
		slog.Int64("batchID", int64(partitions.BatchId)), slog.Int("partitions", numPartitions))

	qRepPullCoreConn, qRepPullCoreClose, err := connectors.GetByNameAs[connectors.QRepPullConnectorCore](
		ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep source connector: %w", err))
	}
	defer qRepPullCoreClose(ctx)

	dstPeer, qRepSyncCoreConn, qRepSyncCoreClose, err := connectors.LoadPeerAndGetByNameAs[connectors.QRepSyncConnectorCore](
		ctx,
		config.Env,
		a.CatalogPool,
		config.DestinationName,
	)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep destination connector: %w", err))
	}
	defer qRepSyncCoreClose(ctx)

	var replicatePartition func(partition *protos.QRepPartition) error

	qRecordReplication := func() (func(partition *protos.QRepPartition) error, error) {
		srcConn, ok := qRepPullCoreConn.(connectors.QRepPullConnector)
		if !ok {
			return nil, fmt.Errorf("source connector is not QRepPullConnector, got %T", qRepPullCoreConn)
		}

		destConn, ok := qRepSyncCoreConn.(connectors.QRepSyncConnector)
		if !ok {
			return nil, fmt.Errorf(
				"source connector is QRepPullConnector but destination connector is not QRepSyncConnector, got %T",
				qRepSyncCoreConn,
			)
		}

		var luaScript *lua.LFunction
		var luaState *lua.LState

		if config.Script != "" {
			ls, err := utils.LoadScript(ctx, config.Script, utils.LuaPrintFn(func(s string) {
				a.Alerter.LogFlowInfo(ctx, config.FlowJobName, s)
			}))
			if err != nil {
				return nil, err
			}
			if fn, ok := ls.Env.RawGetString("transformRow").(*lua.LFunction); ok {
				luaState = ls
				luaScript = fn
			}
		}

		return func(partition *protos.QRepPartition) error {
			stream := model.NewQRecordStream(shared.QRepChannelSize)
			outstream := stream

			if luaScript != nil {
				outstream = pua.AttachToStream(luaState, luaScript, stream)
			}

			return replicateQRepPartition(ctx, a, srcConn, destConn, dstPeer.Type, config, partition, runUUID, stream, outstream,
				connectors.QRepPullConnector.PullQRepRecords,
				connectors.QRepSyncConnector.SyncQRepRecords,
			)
		}, nil
	}

	qObjectReplication := func() (func(partition *protos.QRepPartition) error, error) {
		srcConn, ok := qRepPullCoreConn.(connectors.QRepPullObjectsConnector)
		if !ok {
			return nil, fmt.Errorf("source connector is not QRepPullObjectsConnector, got %T", qRepPullCoreConn)
		}

		destConn, ok := qRepSyncCoreConn.(connectors.QRepSyncObjectsConnector)
		if !ok {
			return nil, fmt.Errorf(
				"source connector is QRepPullObjectsConnector but destination connector is not QRepSyncObjectsConnector, got %T",
				qRepSyncCoreConn,
			)
		}

		return func(partition *protos.QRepPartition) error {
			stream := model.NewQObjectStream(shared.QRepChannelSize)

			return replicateQRepPartition(ctx, a, srcConn, destConn, dstPeer.Type, config, partition, runUUID, stream, stream,
				connectors.QRepPullObjectsConnector.PullQRepObjects,
				connectors.QRepSyncObjectsConnector.SyncQRepObjects,
			)
		}, nil
	}

	pgReplication := func() (func(partition *protos.QRepPartition) error, error) {
		srcConn, ok := qRepPullCoreConn.(*connpostgres.PostgresConnector)
		if !ok {
			return nil, fmt.Errorf("source connector is not PostgresConnector, got %T", qRepPullCoreConn)
		}

		destConn, ok := qRepSyncCoreConn.(*connpostgres.PostgresConnector)
		if !ok {
			return nil, fmt.Errorf("source connector is PostgresConnector but destination connector is not, got %T", qRepSyncCoreConn)
		}

		return func(partition *protos.QRepPartition) error {
			read, write := connpostgres.NewPgCopyPipe()

			return replicateQRepPartition(ctx, a, srcConn, destConn, dstPeer.Type, config, partition, runUUID, write, read,
				(*connpostgres.PostgresConnector).PullPgQRepRecords,
				(*connpostgres.PostgresConnector).SyncPgQRepRecords,
			)
		}, nil
	}

	switch qRepPullCoreConn.(type) {
	case *connpostgres.PostgresConnector:
		switch config.System {
		case protos.TypeSystem_Q:
			replicatePartition, err = qRecordReplication()
		case protos.TypeSystem_PG:
			replicatePartition, err = pgReplication()
		default:
			err = fmt.Errorf("unknown type system %d", config.System)
		}
	case connectors.QRepPullConnector:
		replicatePartition, err = qRecordReplication()
	case connectors.QRepPullObjectsConnector:
		replicatePartition, err = qObjectReplication()
	default:
		err = fmt.Errorf("unsupported QRepSyncConnectorCore type %T", qRepPullCoreConn)
	}

	if err != nil {
		logger.Error("failed to initialize replication method", slog.Any("error", err))
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	for _, partition := range partitions.Partitions {
		logger.Info(fmt.Sprintf("batch-%d - replicating partition - %s", partitions.BatchId, partition.PartitionId))

		err := replicatePartition(partition)
		if err != nil {
			logger.Error("failed to replicate partition", slog.Any("error", err))
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		}
	}

	a.Alerter.LogFlowInfo(
		ctx,
		config.FlowJobName,
		fmt.Sprintf("replicated %d partitions to destination for table %s", numPartitions, config.DestinationTableIdentifier),
	)
	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "consolidating partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.QRepConsolidateConnector](
		ctx, config.Env, a.CatalogPool, config.DestinationName)
	if errors.Is(err, errors.ErrUnsupported) {
		return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	} else if err != nil {
		return err
	}
	defer dstClose(ctx)

	if err := dstConn.ConsolidateQRepPartitions(ctx, config); err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.QRepConsolidateConnector](
		ctx, config.Env, a.CatalogPool, config.DestinationName)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	} else if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}
	defer dstClose(ctx)

	return dstConn.CleanupQRepFlow(ctx, config)
}

func (a *FlowableActivity) DropFlowSource(ctx context.Context, req *protos.DropFlowActivityInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		var notFound *exceptions.NotFoundError
		if errors.As(err, &notFound) {
			logger := internal.LoggerFromCtx(ctx)
			logger.Warn("peer missing, skipping", slog.String("peer", req.PeerName))
			return nil
		}
		return a.Alerter.LogFlowError(ctx, req.FlowJobName,
			exceptions.NewDropFlowError(fmt.Errorf("[DropFlowSource] failed to get source connector: %w", err)),
		)
	}
	defer srcClose(ctx)

	if err := srcConn.PullFlowCleanup(ctx, req.FlowJobName); err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			a.Alerter.LogFlowWarning(ctx, req.FlowJobName, fmt.Errorf("[DropFlowSource] hostname not found, skipping: %w", err))
			return nil
		} else {
			pullCleanupErr := exceptions.NewDropFlowError(fmt.Errorf("[DropFlowSource] failed to clean up source: %w", err))
			if !shared.IsSQLStateError(err, pgerrcode.ObjectInUse) {
				// don't alert when PID active
				_ = a.Alerter.LogFlowError(ctx, req.FlowJobName, pullCleanupErr)
			}
			return pullCleanupErr
		}
	}

	a.Alerter.LogFlowInfo(ctx, req.FlowJobName, "Cleaned up source peer replication objects.")

	return nil
}

func (a *FlowableActivity) DropFlowDestination(ctx context.Context, req *protos.DropFlowActivityInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			a.Alerter.LogFlowWarning(ctx, req.FlowJobName, fmt.Errorf("[DropFlowDestination] hostname not found, skipping: %w", err))
			return nil
		} else {
			var notFound *exceptions.NotFoundError
			if errors.As(err, &notFound) {
				logger := internal.LoggerFromCtx(ctx)
				logger.Warn("peer missing, skipping", slog.String("peer", req.PeerName))
				return nil
			}
			return a.Alerter.LogFlowError(ctx, req.FlowJobName,
				exceptions.NewDropFlowError(fmt.Errorf("[DropFlowDestination] failed to get destination connector: %w", err)),
			)
		}
	}
	defer dstClose(ctx)

	if err := dstConn.SyncFlowCleanup(ctx, req.FlowJobName); err != nil {
		return a.Alerter.LogFlowError(ctx, req.FlowJobName,
			exceptions.NewDropFlowError(fmt.Errorf("[DropFlowDestination] failed to clean up destination: %w", err)),
		)
	}

	a.Alerter.LogFlowInfo(ctx, req.FlowJobName,
		"Cleaned up destination peer replication objects. Any metadata storage has been dropped.")

	return nil
}

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "SendWALHeartbeat"))
	walHeartbeatEnabled, err := internal.PeerDBEnableWALHeartbeat(ctx, nil)
	if err != nil {
		logger.Warn("unable to fetch wal heartbeat config, skipping wal heartbeat send", slog.Any("error", err))
		return err
	}
	if !walHeartbeatEnabled {
		logger.Info("wal heartbeat is disabled")
		return nil
	}
	walHeartbeatStatement, err := internal.PeerDBWALHeartbeatQuery(ctx, nil)
	if err != nil {
		logger.Warn("unable to fetch wal heartbeat config, skipping wal heartbeat send", slog.Any("error", err))
		return err
	}

	pgPeers, err := a.getPostgresPeerConfigs(ctx)
	if err != nil {
		logger.Warn("unable to fetch peers, skipping wal heartbeat send", slog.Any("error", err))
		return err
	}

	// run above command for each Postgres peer
	for _, pgPeer := range pgPeers {
		if err := ctx.Err(); err != nil {
			return err
		}

		func() {
			pgConfig := pgPeer.GetPostgresConfig()
			pgConn, peerErr := connpostgres.NewPostgresConnector(ctx, nil, pgConfig)
			if peerErr != nil {
				logger.Error("error creating connector for postgres peer",
					slog.String("peer", pgPeer.Name), slog.String("host", pgConfig.Host), slog.Any("error", err))
				return
			}
			defer pgConn.Close()
			if cmdErr := pgConn.ExecuteCommand(ctx, walHeartbeatStatement); cmdErr != nil {
				logger.Warn("could not send wal heartbeat to peer", slog.String("peer", pgPeer.Name), slog.Any("error", cmdErr))
			}
			logger.Info("sent wal heartbeat", slog.String("peer", pgPeer.Name))
		}()
	}

	return nil
}

func (a *FlowableActivity) ScheduledTasks(ctx context.Context) error {
	logger := internal.LoggerFromCtx(ctx)
	logger.Info("Starting scheduled tasks")
	defer common.Interval(ctx, 20*time.Second, func() {
		activity.RecordHeartbeat(ctx, "Running scheduled tasks")
	})()
	wrapWithLog := func(name string, fn func() error) func() {
		return func() {
			now := time.Now()
			logger.Info(name + " starting")
			if err := fn(); err != nil {
				logger.Error(name+" failed", slog.Any("error", err))
			}
			logger.Info(name+" completed", slog.Duration("duration", time.Since(now)))
		}
	}
	defer common.Interval(ctx, 10*time.Minute, wrapWithLog("SendWALHeartbeat", func() error {
		return a.SendWALHeartbeat(ctx)
	}))()
	defer common.Interval(ctx, 1*time.Minute, wrapWithLog("RecordMetricsCritical", func() error {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 50*time.Second)
		defer cancelFunc()
		return a.RecordMetricsCritical(timeoutCtx)
	}))()
	defer common.Interval(ctx, 2*time.Minute, wrapWithLog("RecordMetricsAggregates", func() error {
		if enabled, err := internal.PeerDBMetricsRecordAggregatesEnabled(ctx, nil); err == nil && enabled {
			return a.RecordMetricsAggregates(ctx)
		}
		logger.Info("metrics aggregates recording is disabled")
		return nil
	}))()
	defer common.Interval(ctx, 1*time.Hour, wrapWithLog("LogFlowConfigs", func() error {
		return telemetry.LogFlowConfigs(ctx, a.CatalogPool)
	}))()
	defer common.Interval(ctx, 1*time.Minute, wrapWithLog("RecordSlotSizes", func() error {
		return a.RecordSlotSizes(ctx)
	}))()
	<-ctx.Done()
	logger.Info("Stopping scheduled tasks due to context done", slog.Any("error", ctx.Err()))
	return nil
}

type flowInformation struct {
	config     *protos.FlowConnectionConfigsCore
	updatedAt  time.Time
	workflowID string
}

type metricsFlowMetadata struct {
	updatedAt           time.Time
	config              *protos.FlowConnectionConfigsCore
	sourcePeerConfig    *protos.Peer
	name                string
	workflowID          string
	sourcePeerName      string
	destinationPeerName string
	status              protos.FlowStatus
	sourcePeerType      protos.DBType
	destinationPeerType protos.DBType
}

func (m *metricsFlowMetadata) toFlowContextMetadata() *protos.FlowContextMetadata {
	return &protos.FlowContextMetadata{
		Source: &protos.PeerContextMetadata{
			Name:     m.sourcePeerName,
			Type:     m.sourcePeerType,
			Hostname: getPeerHostName(m.sourcePeerType, m.sourcePeerConfig),
		},
		Destination: &protos.PeerContextMetadata{
			Name: m.destinationPeerName,
			Type: m.destinationPeerType,
		},
		FlowName: m.config.FlowJobName,
		Status:   m.status,
	}
}

func (a *FlowableActivity) RecordMetricsAggregates(ctx context.Context) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "RecordMetricsAggregates"))
	logger.Info("Started RecordMetricsAggregates")
	flows, err := a.getFlowsForMetrics(ctx)
	if err != nil {
		logger.Error("Failed to get flows for metrics", slog.Any("error", err))
		return err
	}

	flowsMap := make(map[string]*metricsFlowMetadata, len(flows))
	flowNames := make([]string, 0, len(flows))
	for idx, flow := range flows {
		flowsMap[flow.name] = &flows[idx]
		flowNames = append(flowNames, flow.name)
	}
	rows, err := a.CatalogPool.Query(ctx, `
		SELECT
			destination_table_name,
			inserts_count,
			updates_count,
			deletes_count,
			flow_name
		FROM peerdb_stats.cdc_table_aggregate_counts
		WHERE flow_name = ANY($1)
		ORDER BY flow_name, destination_table_name`, flowNames)
	if err != nil {
		return fmt.Errorf("failed to query cdc table total counts: %w", err)
	}

	var scannedFlow string
	var tableName string
	operationValueMapping := [3]struct {
		op    string
		count int64
	}{
		{op: otel_metrics.RecordOperationTypeInsert},
		{op: otel_metrics.RecordOperationTypeUpdate},
		{op: otel_metrics.RecordOperationTypeDelete},
	}

	if _, err = pgx.ForEachRow(rows, []any{
		&tableName,
		&operationValueMapping[0].count,
		&operationValueMapping[1].count,
		&operationValueMapping[2].count,
		&scannedFlow,
	}, func() error {
		if flowData, ok := flowsMap[scannedFlow]; ok {
			eCtx := context.WithValue(ctx, internal.FlowMetadataKey, flowData.toFlowContextMetadata())
			for _, opAndValue := range operationValueMapping {
				a.OtelManager.Metrics.RecordsSyncedPerTableGauge.Record(eCtx, opAndValue.count, metric.WithAttributeSet(attribute.NewSet(
					attribute.String(otel_metrics.FlowNameKey, scannedFlow),
					attribute.String(otel_metrics.DestinationTableNameKey, tableName),
					attribute.String(otel_metrics.RecordOperationTypeKey, opAndValue.op),
				)))
			}
		} else {
			logger.Error("Flow not found for metrics",
				slog.String("flow", scannedFlow), slog.String("tableName", tableName))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over cdc table total counts: %w", err)
	}

	return nil
}

func (a *FlowableActivity) RecordMetricsCritical(ctx context.Context) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "RecordMetricsCritical"))
	logger.Info("Started RecordMetricsCritical")

	maintenanceEnabled, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	instanceStatus := otel_metrics.InstanceStatusReady
	if err != nil {
		logger.Error("Failed to get maintenance mode status", slog.Any("error", err))
		instanceStatus = otel_metrics.InstanceStatusUnknown
	}
	if maintenanceEnabled {
		instanceStatus = otel_metrics.InstanceStatusMaintenance
	}
	logger.Info("Emitting Instance Status")
	a.OtelManager.Metrics.InstanceStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.InstanceStatusKey, instanceStatus),
		attribute.String(otel_metrics.PeerDBVersionKey, internal.PeerDBVersionShaShort()),
		attribute.String(otel_metrics.DeploymentVersionKey, internal.PeerDBDeploymentVersion()),
	)))
	logger.Info("Querying for flows and statuses to emit metrics")
	queryCtx, cancelFunc := context.WithTimeout(ctx, 20*time.Second)
	defer cancelFunc()
	infos, err := a.getFlowsForMetrics(queryCtx)
	if err != nil {
		return err
	}
	logger.Info("Emitting metrics for flows", slog.Int("flows", len(infos)))
	activeFlows := make([]metricsFlowMetadata, 0, len(infos))
	currentTime := time.Now()
	for _, info := range infos {
		ctx := context.WithValue(ctx, internal.FlowMetadataKey, info.toFlowContextMetadata())
		_, isActive := activeFlowStatuses[info.status]
		if isActive {
			activeFlows = append(activeFlows, info)
		}
		a.OtelManager.Metrics.SyncedTablesGauge.Record(ctx, int64(len(info.config.TableMappings)))
		a.OtelManager.Metrics.FlowStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.FlowStatusKey, info.status.String()),
			attribute.Bool(otel_metrics.IsFlowActiveKey, isActive),
		)))
		a.OtelManager.Metrics.DurationSinceLastFlowUpdateGauge.Record(ctx, int64(currentTime.Sub(info.updatedAt).Seconds()),
			metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowStatusKey, info.status.String()),
			)))
	}
	logger.Info("Finished emitting Instance and Flow Status", slog.Int("flows", len(infos)))
	var totalCpuLimit float64
	if cpuLimitStr, ok := os.LookupEnv("CURRENT_CONTAINER_CPU_LIMIT"); ok {
		totalCpuLimit, err = strconv.ParseFloat(cpuLimitStr, 64)
		if err != nil {
			logger.Error("Failed to parse CPU limit", slog.Any("error", err), slog.String("cpuLimit", cpuLimitStr))
		}
	}

	var totalMemoryLimit float64
	if memLimitStr, ok := os.LookupEnv("CURRENT_CONTAINER_MEMORY_LIMIT"); ok {
		totalMemoryLimit, err = strconv.ParseFloat(memLimitStr, 64)
		if err != nil {
			logger.Error("Failed to parse Memory limit", slog.Any("error", err), slog.String("memLimit", memLimitStr))
		}
	}

	var workloadTotalReplicaCount int
	if workloadTotalReplicaCountStr, ok := os.LookupEnv("CURRENT_WORKLOAD_TOTAL_REPLICAS"); ok {
		workloadTotalReplicaCount, err = strconv.Atoi(workloadTotalReplicaCountStr)
		if err != nil {
			logger.Error("Failed to parse workloadTotalReplicaCount",
				slog.Any("error", err), slog.String("workloadTotalReplicaCount", workloadTotalReplicaCountStr))
		}
	}

	logger.Info("Emitting Workload Compute information")
	a.OtelManager.Metrics.TotalCPULimitsGauge.Record(ctx, totalCpuLimit)
	a.OtelManager.Metrics.TotalMemoryLimitsGauge.Record(ctx, totalMemoryLimit)
	a.OtelManager.Metrics.WorkloadTotalReplicasGauge.Record(ctx, int64(workloadTotalReplicaCount))
	logger.Info("Finished emitting Workload Compute information")
	logger.Info("Emitting Active Flow Info", slog.Int("flows", len(activeFlows)))
	a.OtelManager.Metrics.ActiveFlowsGauge.Record(ctx, int64(len(activeFlows)))
	if activeFlowCount := len(activeFlows); activeFlowCount > 0 {
		activeFlowCpuLimit := totalCpuLimit / float64(activeFlowCount)
		activeFlowMemoryLimit := totalMemoryLimit / float64(activeFlowCount)
		if activeFlowCpuLimit > 0 || activeFlowMemoryLimit > 0 {
			for _, info := range activeFlows {
				ctx := context.WithValue(ctx, internal.FlowMetadataKey, info.toFlowContextMetadata())
				if activeFlowMemoryLimit > 0 {
					a.OtelManager.Metrics.MemoryLimitsPerActiveFlowGauge.Record(ctx, activeFlowMemoryLimit)
				}
				if activeFlowCpuLimit > 0 {
					a.OtelManager.Metrics.CPULimitsPerActiveFlowGauge.Record(ctx, activeFlowCpuLimit)
				}
			}
		}
	}
	logger.Info("Finished emitting Active Flow Info", slog.Int("flows", len(activeFlows)))
	logger.Info("Finished RecordMetricsCritical")
	return nil
}

func (a *FlowableActivity) getFlowsForMetrics(ctx context.Context) ([]metricsFlowMetadata, error) {
	logger := internal.LoggerFromCtx(ctx)
	rows, err := a.CatalogPool.Query(ctx,
		`
			SELECT DISTINCT ON (f.name)
				f.name AS flow_name,
				f.status AS status,
				f.config_proto AS config_proto,
				f.workflow_id AS workflow_id,
				f.updated_at AS updated_at,
				COALESCE(sp.name, '') AS source_peer_name,
				COALESCE(sp.type, 0) AS source_peer_type,
				COALESCE(dp.name, '') AS destination_peer_name,
				COALESCE(dp.type, 0) AS destination_peer_type,
				COALESCE(sp.options, '') AS source_peer_config_proto,
				sp.enc_key_id AS source_enc_key_id
			FROM
				flows f
			LEFT JOIN peers sp ON f.source_peer = sp.id
			LEFT JOIN peers dp ON f.destination_peer = dp.id
		`)
	if err != nil {
		logger.Error("failed to query all flows", slog.Any("error", err))
		return nil, fmt.Errorf("failed to query all flows for metrics: %w", err)
	}

	infos, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (metricsFlowMetadata, error) {
		f := metricsFlowMetadata{
			config: &protos.FlowConnectionConfigsCore{},
		}
		var configProto []byte
		var sourcePeerConfig []byte
		var sourceEncKeyID string
		if err := rows.Scan(
			&f.name,
			&f.status,
			&configProto,
			&f.workflowID,
			&f.updatedAt,
			&f.sourcePeerName,
			&f.sourcePeerType,
			&f.destinationPeerName,
			&f.destinationPeerType,
			&sourcePeerConfig,
			&sourceEncKeyID,
		); err != nil {
			return metricsFlowMetadata{}, fmt.Errorf("failed to scan row: %w", err)
		}
		if f.sourcePeerName == "" || f.destinationPeerName == "" {
			logger.Error("flow has missing peer information", slog.String("flow_name", f.name))
			return metricsFlowMetadata{},
				a.Alerter.LogFlowError(ctx, f.name,
					exceptions.NewRecordMetricsError(fmt.Errorf("flow has missing peer information %s", f.name)))
		}
		if err := proto.Unmarshal(configProto, f.config); err != nil {
			return metricsFlowMetadata{}, err
		}
		config, err := connectors.BuildPeerConfig(ctx, sourceEncKeyID, sourcePeerConfig, f.sourcePeerName, f.sourcePeerType)
		if err != nil {
			return metricsFlowMetadata{}, err
		}
		f.sourcePeerConfig = config
		return f, nil
	})
	if err != nil {
		logger.Error("failed to collect rows", slog.Any("error", err))
		return nil, fmt.Errorf("failed to collect rows for metrics: %w", err)
	}
	return infos, nil
}

func (a *FlowableActivity) RecordSlotSizes(ctx context.Context) error {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("scheduledTask", "RecordSlotSizes"))
	logger.Info("Recording Slot Information")
	slotMetricGauges := otel_metrics.SlotMetricGauges{}
	slotMetricGauges.SlotLagGauge = a.OtelManager.Metrics.SlotLagGauge
	slotMetricGauges.RestartLSNGauge = a.OtelManager.Metrics.RestartLSNGauge
	slotMetricGauges.ConfirmedFlushLSNGauge = a.OtelManager.Metrics.ConfirmedFlushLSNGauge
	slotMetricGauges.SentLSNGauge = a.OtelManager.Metrics.SentLSNGauge
	slotMetricGauges.CurrentWalLSNGauge = a.OtelManager.Metrics.CurrentWalLSNGauge
	slotMetricGauges.RestartToConfirmedMBGauge = a.OtelManager.Metrics.RestartToConfirmedMBGauge
	slotMetricGauges.ConfirmedToCurrentMBGauge = a.OtelManager.Metrics.ConfirmedToCurrentMBGauge
	slotMetricGauges.SafeWalSizeGauge = a.OtelManager.Metrics.SafeWalSizeGauge
	slotMetricGauges.SlotActiveGauge = a.OtelManager.Metrics.SlotActiveGauge
	slotMetricGauges.WalSenderStateGauge = a.OtelManager.Metrics.WalSenderStateGauge
	slotMetricGauges.WalStatusGauge = a.OtelManager.Metrics.WalStatusGauge
	slotMetricGauges.LogicalDecodingWorkMemGauge = a.OtelManager.Metrics.LogicalDecodingWorkMemGauge
	slotMetricGauges.StatsResetGauge = a.OtelManager.Metrics.StatsResetGauge
	slotMetricGauges.SpillTxnsGauge = a.OtelManager.Metrics.SpillTxnsGauge
	slotMetricGauges.SpillCountGauge = a.OtelManager.Metrics.SpillCountGauge
	slotMetricGauges.SpillBytesGauge = a.OtelManager.Metrics.SpillBytesGauge
	slotMetricGauges.OpenConnectionsGauge = a.OtelManager.Metrics.OpenConnectionsGauge
	slotMetricGauges.OpenReplicationConnectionsGauge = a.OtelManager.Metrics.OpenReplicationConnectionsGauge
	slotMetricGauges.IntervalSinceLastNormalizeGauge = a.OtelManager.Metrics.IntervalSinceLastNormalizeGauge

	logger.Info("Querying for flows to emit slot metrics")
	rows, err := a.CatalogPool.Query(ctx,
		"SELECT DISTINCT ON (name) name, config_proto, workflow_id, updated_at FROM flows WHERE query_string IS NULL")
	if err != nil {
		logger.Error("failed to query all flows", slog.Any("error", err))
		return fmt.Errorf("failed to query all flows for metrics: %w", err)
	}

	infos, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (flowInformation, error) {
		var flowName string
		var configProto []byte
		var workflowID string
		var updatedAt time.Time
		if err := rows.Scan(&flowName, &configProto, &workflowID, &updatedAt); err != nil {
			return flowInformation{}, err
		}

		var config protos.FlowConnectionConfigsCore
		if err := proto.Unmarshal(configProto, &config); err != nil {
			return flowInformation{}, err
		}

		return flowInformation{
			config:     &config,
			workflowID: workflowID,
			updatedAt:  updatedAt,
		}, nil
	})
	if err != nil {
		logger.Error("failed to process result of all flows", slog.Any("error", err))
		return fmt.Errorf("failed to process result of all flows for metrics: %w", err)
	}

	logger.Info("Recording slot size and emitting log retention where applicable", slog.Int("flows", len(infos)))
	for _, info := range infos {
		if err := ctx.Err(); err != nil {
			return err
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := a.recordSlotInformation(timeoutCtx, info, slotMetricGauges); err != nil {
			logger.Error("Failed to record slot information", slog.Any("error", err))
		}
		if err := a.emitLogRetentionHours(timeoutCtx, info, a.OtelManager.Metrics.LogRetentionGauge); err != nil {
			logger.Error("Failed to emit log retention hours", slog.Any("error", err))
		}
		if err := a.recordServerSideCommitLag(timeoutCtx, info, a.OtelManager.Metrics.ServerSideCommitLagGauge); err != nil {
			logger.Error("Failed to record server-side commit lag", slog.Any("error", err))
		}
		cancel()
	}
	logger.Info("Finished emitting Slot Information", slog.Int("flows", len(infos)))
	return nil
}

func (a *FlowableActivity) recordSlotInformation(
	ctx context.Context,
	info flowInformation,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	logger := internal.LoggerFromCtx(ctx)
	flowMetadata, err := a.GetFlowMetadata(ctx, &protos.FlowContextMetadataInput{
		FlowName:           info.config.FlowJobName,
		SourceName:         info.config.SourceName,
		DestinationName:    info.config.DestinationName,
		FetchSourceVariant: true,
	})
	if err != nil {
		logger.Error("Failed to get flow metadata", slog.Any("error", err))
		return err
	}

	if flowMetadata.Source.Type != protos.DBType_POSTGRES {
		return nil
	}

	ctx = context.WithValue(ctx, internal.FlowMetadataKey, flowMetadata)
	srcConn, srcClose, err := connectors.GetPostgresConnectorByName(ctx, nil, a.CatalogPool, info.config.SourceName)
	if err != nil {
		if !errors.Is(err, errors.ErrUnsupported) {
			logger.Error("Failed to create connector to handle slot info", slog.Any("error", err))
		}
		return err
	}
	defer srcClose(ctx)

	slotName := connpostgres.GetDefaultSlotName(info.config.FlowJobName)
	if info.config.ReplicationSlotName != "" {
		slotName = info.config.ReplicationSlotName
	}
	peerName := info.config.SourceName

	if err := srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, &alerting.AlertKeys{
		FlowName: info.config.FlowJobName,
		PeerName: peerName,
		SlotName: slotName,
	}, slotMetricGauges); err != nil {
		logger.Error("Failed to handle slot info", slog.Any("error", err))
	}

	return nil
}

func (a *FlowableActivity) emitLogRetentionHours(
	ctx context.Context,
	info flowInformation,
	logRetentionGauge metric.Float64Gauge,
) error {
	logger := internal.LoggerFromCtx(ctx)
	flowMetadata, err := a.GetFlowMetadata(ctx, &protos.FlowContextMetadataInput{
		FlowName:           info.config.FlowJobName,
		SourceName:         info.config.SourceName,
		DestinationName:    info.config.DestinationName,
		FetchSourceVariant: true,
	})
	if err != nil {
		logger.Error("Failed to get flow metadata", slog.Any("error", err))
		return err
	}
	ctx = context.WithValue(ctx, internal.FlowMetadataKey, flowMetadata)
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.GetLogRetentionConnector](ctx, nil, a.CatalogPool, info.config.SourceName)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	} else if err != nil {
		logger.Error("Failed to create connector to emit log retention", slog.Any("error", err))
		return err
	}
	defer srcClose(ctx)

	peerName := info.config.SourceName
	logRetentionHours, err := srcConn.GetLogRetentionHours(ctx)
	if err != nil {
		logger.Error("Failed to get log retention hours", slog.Any("error", err))
	}

	if logRetentionHours > 0 {
		logRetentionGauge.Record(ctx, logRetentionHours)
		logger.Info("Emitted log retention hours", slog.String("peerName", peerName), slog.Float64("logRetentionHours", logRetentionHours))
		return nil
	}

	logger.Warn("Log retention hours is not set or is zero, skipping emission",
		slog.String("peerName", peerName), slog.Float64("logRetentionHours", logRetentionHours))
	return nil
}

func (a *FlowableActivity) recordServerSideCommitLag(
	ctx context.Context,
	info flowInformation,
	serverSideCommitLagGauge metric.Int64Gauge,
) error {
	logger := internal.LoggerFromCtx(ctx)
	flowMetadata, err := a.GetFlowMetadata(ctx, &protos.FlowContextMetadataInput{
		FlowName:           info.config.FlowJobName,
		SourceName:         info.config.SourceName,
		DestinationName:    info.config.DestinationName,
		FetchSourceVariant: true,
	})
	if err != nil {
		logger.Error("Failed to get flow metadata", slog.Any("error", err))
		return err
	}
	ctx = context.WithValue(ctx, internal.FlowMetadataKey, flowMetadata)
	srcConn, srcClose, err := connectors.GetByNameAs[connectors.GetServerSideCommitLagConnector](
		ctx, nil, a.CatalogPool, info.config.SourceName)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	} else if err != nil {
		logger.Error("Failed to get connector", slog.Any("error", err))
		return err
	}
	defer srcClose(ctx)

	flowName := info.config.FlowJobName
	lagMicroseconds, err := srcConn.GetServerSideCommitLagMicroseconds(ctx, flowName)
	if err != nil {
		logger.Error("Failed to get commit lag", slog.Any("error", err))
		return err
	}
	serverSideCommitLagGauge.Record(ctx, lagMicroseconds)
	return nil
}

var activeFlowStatuses = map[protos.FlowStatus]struct{}{
	protos.FlowStatus_STATUS_RUNNING:   {},
	protos.FlowStatus_STATUS_PAUSED:    {},
	protos.FlowStatus_STATUS_PAUSING:   {},
	protos.FlowStatus_STATUS_SETUP:     {},
	protos.FlowStatus_STATUS_SNAPSHOT:  {},
	protos.FlowStatus_STATUS_RESYNC:    {},
	protos.FlowStatus_STATUS_MODIFYING: {},
}

func (a *FlowableActivity) QRepHasNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) (bool, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "scanning for new rows"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	// TODO implement for other QRepPullConnector sources
	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return true, nil
		}
		return false, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep source connector: %w", err))
	}
	defer srcClose(ctx)

	logger.Info(fmt.Sprintf("current last partition value is %v", last))

	maxValue, err := srcConn.GetMaxValue(ctx, config, last)
	if err != nil {
		return false, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to check for new rows: %w", err))
	}

	if maxValue == nil || last == nil || last.Range == nil {
		return maxValue != nil, nil
	}

	switch x := last.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		if maxValue.(int64) > x.IntRange.End {
			return true, nil
		}
	case *protos.PartitionRange_UintRange:
		if maxValue.(uint64) > x.UintRange.End {
			return true, nil
		}
	case *protos.PartitionRange_TimestampRange:
		if maxValue.(time.Time).After(x.TimestampRange.End.AsTime()) {
			return true, nil
		}
	default:
		return false, fmt.Errorf("unknown range type: %v", x)
	}

	return false, nil
}

func (a *FlowableActivity) RenameTables(ctx context.Context, config *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "renaming tables for job"
	})
	defer shutdown()

	var renameOutput *protos.RenameTablesOutput
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	renameWithSoftDeleteConn, renameWithSoftDeleteClose, err := connectors.GetByNameAs[connectors.RenameTablesWithSoftDeleteConnector](
		ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		if err == errors.ErrUnsupported {
			// Rename without soft-delete
			renameConn, renameClose, renameErr := connectors.GetByNameAs[connectors.RenameTablesConnector](
				ctx, nil, a.CatalogPool, config.PeerName)
			if renameErr != nil {
				return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get rename connector: %w", renameErr))
			}
			defer renameClose(ctx)

			a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Renaming tables for resync")
			renameOutput, err = renameConn.RenameTables(ctx, config)
			if err != nil {
				return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to rename tables: %w", err))
			}

			err = a.updateTableSchemaMappingForResync(ctx, config.RenameTableOptions, config.FlowJobName)
			if err != nil {
				return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName,
					fmt.Errorf("failed to update table_schema_mapping after resync: %w", err))
			}

			a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Resync completed for all tables")
			return renameOutput, nil
		}
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get rename with soft-delete connector: %w", err))
	}
	defer renameWithSoftDeleteClose(ctx)

	// Rename with soft-delete
	tableNameSchemaMapping := make(map[string]*protos.TableSchema, len(config.RenameTableOptions))
	for _, option := range config.RenameTableOptions {
		schema, err := internal.LoadTableSchemaFromCatalog(
			ctx,
			a.CatalogPool,
			config.FlowJobName,
			option.CurrentName,
		)
		if err != nil {
			return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to load schema to rename tables: %w", err))
		}
		tableNameSchemaMapping[option.CurrentName] = schema
	}
	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Renaming tables for resync with soft-delete")
	renameOutput, err = renameWithSoftDeleteConn.RenameTables(ctx, config, tableNameSchemaMapping)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to rename tables: %w", err))
	}

	err = a.updateTableSchemaMappingForResync(ctx, config.RenameTableOptions, config.FlowJobName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName,
			fmt.Errorf("failed to update table_schema_mapping after resync with soft-delete: %w", err))
	}

	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Resync with soft-delete completed for all tables")
	return renameOutput, nil
}

func (a *FlowableActivity) updateTableSchemaMappingForResync(
	ctx context.Context,
	renameOptions []*protos.RenameTableOption,
	flowJobName string,
) error {
	tx, err := a.CatalogPool.Begin(ctx)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, flowJobName, fmt.Errorf("failed to begin updating table_schema_mapping: %w", err))
	}
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), flowJobName))
	defer shared.RollbackTx(tx, logger)

	for _, option := range renameOptions {
		if option.NewName != option.CurrentName {
			if _, err := tx.Exec(
				ctx,
				"delete from table_schema_mapping where flow_name = $1 and table_name = $2",
				flowJobName,
				option.NewName,
			); err != nil {
				return a.Alerter.LogFlowError(ctx, flowJobName, fmt.Errorf("failed to update table_schema_mapping: %w", err))
			}
		}
		if _, err := tx.Exec(
			ctx,
			"update table_schema_mapping set table_name = $3 where flow_name = $1 and table_name = $2",
			flowJobName,
			option.CurrentName,
			option.NewName,
		); err != nil {
			return a.Alerter.LogFlowError(ctx, flowJobName, fmt.Errorf("failed to update table_schema_mapping: %w", err))
		}
	}

	a.Alerter.LogFlowInfo(ctx, flowJobName, "Resync completed for all tables")

	if commitErr := tx.Commit(ctx); commitErr != nil {
		return a.Alerter.LogFlowError(ctx, flowJobName, fmt.Errorf("failed to commit updating table_schema_mapping: %w", commitErr))
	}
	return nil
}

func (a *FlowableActivity) DeleteMirrorStats(ctx context.Context, flowName string) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "deleting mirror stats"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, flowName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), flowName))
	if err := monitoring.DeleteMirrorStats(ctx, logger, a.CatalogPool, flowName); err != nil {
		logger.Warn("was not able to delete mirror stats", slog.Any("error", err))
		return err
	}

	return nil
}

func (a *FlowableActivity) CreateTablesFromExisting(ctx context.Context, req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dstConn, dstClose, err := connectors.GetByNameAs[connectors.CreateTablesFromExistingConnector](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, req.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer dstClose(ctx)

	return dstConn.CreateTablesFromExisting(ctx, req)
}

func (a *FlowableActivity) ReplicateXminPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
) (int64, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "syncing xmin"
	})
	defer shutdown()

	switch config.System {
	case protos.TypeSystem_Q:
		stream := model.NewQRecordStream(shared.QRepChannelSize)
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

func (a *FlowableActivity) AddTablesToPublication(ctx context.Context, cfg *protos.FlowConnectionConfigsCore,
	additionalTableMappings []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "adding tables to publication"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer srcClose(ctx)

	if err := srcConn.AddTablesToPublication(ctx, &protos.AddTablesToPublicationInput{
		FlowJobName:      cfg.FlowJobName,
		PublicationName:  cfg.PublicationName,
		AdditionalTables: additionalTableMappings,
	}); err != nil {
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	a.Alerter.LogFlowInfo(ctx, cfg.FlowJobName, fmt.Sprintf("ensured %d tables exist in publication %s",
		len(additionalTableMappings), cfg.PublicationName))
	return nil
}

func (a *FlowableActivity) RemoveTablesFromPublication(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	removedTablesMapping []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "removing tables from publication"
	})
	defer shutdown()
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, srcClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, fmt.Errorf("failed to get source connector: %w", err))
	}
	defer srcClose(ctx)

	if err := srcConn.RemoveTablesFromPublication(ctx, &protos.RemoveTablesFromPublicationInput{
		FlowJobName:     cfg.FlowJobName,
		PublicationName: cfg.PublicationName,
		TablesToRemove:  removedTablesMapping,
	}); err != nil {
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	a.Alerter.LogFlowInfo(ctx, cfg.FlowJobName, fmt.Sprintf("removed %d tables from publication %s",
		len(removedTablesMapping), cfg.PublicationName))
	return nil
}

func (a *FlowableActivity) RemoveTablesFromRawTable(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tablesToRemove []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "removing tables from raw table"
	})
	defer shutdown()
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), cfg.FlowJobName))
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)
	normBatchID, err := pgMetadata.GetLastNormalizeBatchID(ctx, cfg.FlowJobName)
	if err != nil {
		logger.Error("[RemoveTablesFromRawTable] failed to get last normalize batch id", slog.Any("error", err))
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	syncBatchID, err := pgMetadata.GetLastSyncBatchID(ctx, cfg.FlowJobName)
	if err != nil {
		logger.Error("[RemoveTablesFromRawTable] failed to get last sync batch id", slog.Any("error", err))
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	dstConn, dstClose, err := connectors.GetByNameAs[connectors.RawTableConnector](ctx, cfg.Env, a.CatalogPool, cfg.DestinationName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			// For connectors where raw table is not a concept,
			// we can ignore the error
			return nil
		}
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName,
			fmt.Errorf("[RemoveTablesFromRawTable] failed to get destination connector: %w", err),
		)
	}
	defer dstClose(ctx)

	tableNames := make([]string, 0, len(tablesToRemove))
	for _, table := range tablesToRemove {
		tableNames = append(tableNames, table.DestinationTableIdentifier)
	}
	if err := dstConn.RemoveTableEntriesFromRawTable(ctx, &protos.RemoveTablesFromRawTableInput{
		FlowJobName:           cfg.FlowJobName,
		DestinationTableNames: tableNames,
		SyncBatchId:           syncBatchID,
		NormalizeBatchId:      normBatchID,
	}); err != nil {
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}
	return nil
}

func (a *FlowableActivity) RemoveTablesFromCatalog(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigsCore,
	tablesToRemove []*protos.TableMapping,
) error {
	removedTables := make([]string, 0, len(tablesToRemove))
	for _, tm := range tablesToRemove {
		removedTables = append(removedTables, tm.DestinationTableIdentifier)
	}

	if _, err := a.CatalogPool.Exec(
		ctx,
		"delete from table_schema_mapping where flow_name = $1 and table_name = ANY($2)",
		cfg.FlowJobName,
		removedTables,
	); err != nil {
		return a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}

	return nil
}

func (a *FlowableActivity) RemoveFlowDetailsFromCatalog(
	ctx context.Context,
	req *model.RemoveFlowDetailsFromCatalogRequest,
) error {
	flowName := req.FlowName
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), req.FlowName))
	tx, err := a.CatalogPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction to remove flow details from catalog: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if _, err := tx.Exec(ctx, "DELETE FROM table_schema_mapping WHERE flow_name=$1", flowName); err != nil {
		return fmt.Errorf("unable to clear table_schema_mapping in catalog: %w", err)
	}

	if !req.Resync {
		ct, err := tx.Exec(ctx, "DELETE FROM flows WHERE name=$1", flowName)
		if err != nil {
			return fmt.Errorf("unable to remove flow entry in catalog: %w", err)
		}
		if ct.RowsAffected() == 0 {
			logger.Warn("flow entry not found in catalog, 0 records deleted")
		} else {
			logger.Info("flow entries removed from catalog",
				slog.Int64("rowsAffected", ct.RowsAffected()))
		}
	}

	if err := connmetadata.SyncFlowCleanupInTx(ctx, tx, flowName); err != nil {
		return fmt.Errorf("unable to clear metadata for flow cleanup: %w", err)
	}

	// only for ClickHouse, should be a no-op for other destination connectors
	if _, err := tx.Exec(ctx, `DELETE FROM ch_s3_stage WHERE flow_job_name = $1`, flowName); err != nil {
		return fmt.Errorf("failed to clear avro stage for flow %s: %w", flowName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction to remove flow details from catalog: %w", err)
	}

	return nil
}

func getPeerHostName(peerType protos.DBType, peer *protos.Peer) string {
	switch peerType {
	case protos.DBType_POSTGRES:
		return shared.Val(peer.GetPostgresConfig()).Host
	case protos.DBType_MYSQL:
		return shared.Val(peer.GetMysqlConfig()).Host
	case protos.DBType_MONGO:
		return shared.Val(peer.GetMongoConfig()).TlsHost
	case protos.DBType_CLICKHOUSE:
		return shared.Val(peer.GetClickhouseConfig()).Host
	}
	return ""
}

// NOTE: this activity is used on the path between CDCFlowWorkflow start and the signal handler for running state.
// If it's unable to progress for whatever reason, the upgrades will break and very unpleasant manual recovery will be needed.
// If you have to modify it, do it carefully and think through the edge cases.
func (a *FlowableActivity) GetFlowMetadata(
	ctx context.Context,
	input *protos.FlowContextMetadataInput,
) (*protos.FlowContextMetadata, error) {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), input.FlowName))
	peerNames := make([]string, 0, 2)
	if input.SourceName != "" {
		peerNames = append(peerNames, input.SourceName)
	}
	if input.DestinationName != "" {
		peerNames = append(peerNames, input.DestinationName)
	}
	peers, err := connectors.LoadPeers(ctx, a.CatalogPool, peerNames)
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, input.FlowName, err)
	}
	var sourcePeer, destinationPeer *protos.PeerContextMetadata
	if input.SourceName != "" {
		sourcePeer = &protos.PeerContextMetadata{
			Name:     input.SourceName,
			Type:     peers[input.SourceName].Type,
			Hostname: getPeerHostName(peers[input.SourceName].Type, peers[input.SourceName]),
		}
	}
	if input.DestinationName != "" {
		destinationPeer = &protos.PeerContextMetadata{
			Name:     input.DestinationName,
			Type:     peers[input.DestinationName].Type,
			Hostname: getPeerHostName(peers[input.DestinationName].Type, peers[input.DestinationName]),
		}
	}

	// Detect source database variant
	if input.FetchSourceVariant && input.SourceName != "" {
		// Use a short timeout for optional variant detection to avoid consuming entire activity timeout
		variantCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if srcConn, srcClose, err := connectors.GetByNameAs[connectors.DatabaseVariantConnector](
			variantCtx, nil, a.CatalogPool, input.SourceName,
		); err == nil {
			if variant, variantErr := srcConn.GetDatabaseVariant(variantCtx); variantErr == nil {
				sourcePeer.Variant = variant
			} else {
				logger.Warn("failed to get source database variant", slog.Any("error", variantErr))
			}
			srcClose(ctx)
		} else if !errors.Is(err, errors.ErrUnsupported) {
			logger.Warn("failed to get source connector to detect database variant", slog.Any("error", err))
		}
	}

	logger.Debug("loaded peer types for flow", slog.String("flowName", input.FlowName),
		slog.String("sourceName", input.SourceName), slog.String("destinationName", input.DestinationName),
		slog.Int("peerTypes", len(peers)))
	return &protos.FlowContextMetadata{
		FlowName:    input.FlowName,
		Source:      sourcePeer,
		Destination: destinationPeer,
		Status:      input.Status,
		IsResync:    input.IsResync,
	}, nil
}

func (a *FlowableActivity) UpdateCDCConfigInCatalogActivity(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	return internal.UpdateCDCConfigInCatalog(ctx, a.CatalogPool, internal.LoggerFromCtx(ctx), cfg)
}

func (a *FlowableActivity) PeerDBFullRefreshOverwriteMode(ctx context.Context, env map[string]string) (bool, error) {
	return internal.PeerDBFullRefreshOverwriteMode(ctx, env)
}

func (a *FlowableActivity) ReportStatusMetric(ctx context.Context, status protos.FlowStatus) error {
	_, isActive := activeFlowStatuses[status]
	a.OtelManager.Metrics.FlowStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowStatusKey, status.String()),
		attribute.Bool(otel_metrics.IsFlowActiveKey, isActive),
	)))
	return nil
}

/**
 * MigratePostgresTableOIDs migrates the OIDs for source Postgres tables to the catalog's table_schema_mapping
 */
func (a *FlowableActivity) MigratePostgresTableOIDs(
	ctx context.Context,
	flowName string,
	oidToTableNameMapping map[uint32]string,
	tableMappings []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "migrating oids to table schema"
	})
	defer shutdown()

	logger := internal.LoggerFromCtx(ctx)
	migrationName := shared.POSTGRES_TABLE_OID_MIGRATION

	if err := internal.RunMigrationOnce(ctx, a.CatalogPool, logger, flowName, migrationName, func(ctx context.Context) error {
		logger.Info("starting PostgreSQL table OIDs migration",
			slog.String("flowName", flowName),
			slog.Int("tableCount", len(oidToTableNameMapping)))

		sourceToDestTableMap := make(map[string]string, len(tableMappings))
		for _, tm := range tableMappings {
			sourceToDestTableMap[tm.SourceTableIdentifier] = tm.DestinationTableIdentifier
		}
		destinationTableOidMap := make(map[string]uint32, len(oidToTableNameMapping))
		for oid, tableName := range oidToTableNameMapping {
			destinationTableIdentifier, ok := sourceToDestTableMap[tableName]
			if !ok {
				return fmt.Errorf("destination table identifier not found for source table %s", tableName)
			}
			destinationTableOidMap[destinationTableIdentifier] = oid
		}

		err := internal.UpdateTableOIDsInTableSchemaInCatalog(
			ctx,
			a.CatalogPool,
			logger,
			flowName,
			destinationTableOidMap,
		)
		if err != nil {
			return fmt.Errorf("failed to update table OIDs in catalog: %w", err)
		}

		logger.Info("successfully completed PostgreSQL table OIDs migration",
			slog.String("flowName", flowName),
			slog.Int("tableCount", len(oidToTableNameMapping)))

		return nil
	}); err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, err)
	}

	return nil
}
