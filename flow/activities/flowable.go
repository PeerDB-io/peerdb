package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	"github.com/PeerDB-io/peerdb/flow/pua"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type CheckMetadataTablesResult struct {
	NeedsSetupMetadataTables bool
}

type NormalizeBatchRequest struct {
	Done    chan struct{}
	BatchID int64
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

func (a *FlowableActivity) Alert(
	ctx context.Context,
	alert *protos.AlertInput,
) error {
	_ = a.Alerter.LogFlowErrorNoStatus(ctx, alert.FlowName, errors.New(alert.Message))
	return nil
}

func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.SetupInput,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetByNameAs[connectors.Connector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, conn)

	return conn.ConnectionActive(ctx)
}

func (a *FlowableActivity) CheckMetadataTables(
	ctx context.Context,
	config *protos.SetupInput,
) (*CheckMetadataTablesResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, conn)

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
	dstConn, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, config.Env, a.CatalogPool, config.PeerName)
	if err != nil {
		return a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if err := dstConn.SetupMetadataTables(ctx); err != nil {
		return a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, fmt.Errorf("failed to setup metadata tables: %w", err))
	}

	return nil
}

func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, srcConn)

	output, err := srcConn.EnsurePullability(ctx, config)
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, fmt.Errorf("failed to ensure pullability: %w", err))
	}

	return output, nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, dstConn)

	res, err := dstConn.CreateRawTable(ctx, config)
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, err)
	}
	if err := monitoring.InitializeCDCFlow(ctx, a.CatalogPool, config.FlowJobName); err != nil {
		return nil, err
	}

	return res, nil
}

func (a *FlowableActivity) UpdateFlowStatusInCatalogActivity(
	ctx context.Context,
	workflowID string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	return internal.UpdateFlowStatusInCatalog(ctx, a.CatalogPool, workflowID, status)
}

func (a *FlowableActivity) SetupTableSchemaActivity(
	ctx context.Context,
	config *protos.SetupTableSchemaBatchInput,
) error {
	if err := setupTableSchema(ctx, a.CatalogPool, config); err != nil {
		setupErr := fmt.Errorf("failed to setup table schema: %w", err)
		return a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, setupErr)
	}
	return nil
}

func (a *FlowableActivity) SetupQRepTableSchemaActivity(
	ctx context.Context,
	config *protos.SetupTableSchemaBatchInput,
	snapshotID int32,
	runUUID string,
) error {
	if err := setupTableSchema(ctx, a.CatalogPool, config); err != nil {
		setupErr := fmt.Errorf("failed to setup table schema: %w", err)
		a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowName, snapshotID, runUUID, setupErr)
		return setupErr
	}
	return nil
}

// SetupTableSchema populates table_schema_mapping
func setupTableSchema(
	ctx context.Context,
	pool shared.CatalogPool,
	config *protos.SetupTableSchemaBatchInput,
) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "getting table schema"
	})
	defer shutdown()

	logger := internal.LoggerFromCtx(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	srcConn, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, config.Env, pool, config.PeerName)
	if err != nil {
		return fmt.Errorf("failed to get table schema connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	tableNameSchemaMapping, err := srcConn.GetTableSchema(ctx, config.Env, config.Version, config.System, config.TableMappings)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}
	processed := internal.BuildProcessedSchemaMapping(config.TableMappings, tableNameSchemaMapping, logger)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer shared.RollbackTx(tx, logger)

	for k, v := range processed {
		processedBytes, err := internal.ProtoMarshal(v)
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
			return fmt.Errorf("failed to set table schema: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// CreateNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateSetupNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	res, err := createNormalizedTable(ctx, a.CatalogPool, config, a.Alerter.LogFlowInfo)
	if err != nil {
		createErr := fmt.Errorf("failed to create normalized table: %w", err)
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowName, createErr)
	}
	return res, nil
}

// CreateQRepNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateQRepNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
	snapshotID int32,
	runUUID string,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	res, err := createNormalizedTable(ctx, a.CatalogPool, config, a.Alerter.LogFlowInfo)
	if err != nil {
		createErr := fmt.Errorf("failed to create normalized table: %w", err)
		a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowName, snapshotID, runUUID, createErr)
		return nil, err
	}
	return res, nil
}

func createNormalizedTable(
	ctx context.Context,
	pool shared.CatalogPool,
	config *protos.SetupNormalizedTableBatchInput,
	logFlowInfo func(ctx context.Context, flowName string, info string),
) (*protos.SetupNormalizedTableBatchOutput, error) {
	numTablesSetup := atomic.Uint32{}
	numTablesToSetup := atomic.Int32{}

	shutdown := heartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("setting up normalized tables - %d of %d done", numTablesSetup.Load(), numTablesToSetup.Load())
	})
	defer shutdown()

	logger := internal.LoggerFromCtx(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	logFlowInfo(ctx, config.FlowName, "Setting up destination tables")
	conn, err := connectors.GetByNameAs[connectors.NormalizedTablesConnector](ctx, config.Env, pool, config.PeerName)
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

	tableNameSchemaMapping, err := getTableNameSchemaMapping(ctx, pool, config.FlowName)
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
			return nil, fmt.Errorf("failed to setup normalized table %s: %w", tableIdentifier, err)
		}
		tableExistsMapping[tableIdentifier] = existing

		numTablesSetup.Add(1)
		if !existing {
			logFlowInfo(ctx, config.FlowName, "created table "+tableIdentifier+" in destination")
		} else {
			logger.Info("table already exists " + tableIdentifier)
		}
	}

	if err := conn.FinishSetupNormalizedTables(ctx, tx); err != nil {
		return nil, fmt.Errorf("failed to commit normalized tables tx: %w", err)
	}
	logFlowInfo(ctx, config.FlowName, "All destination tables have been setup")

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (a *FlowableActivity) SyncFlow(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
) error {
	var currentSyncFlowNum atomic.Int32
	var totalRecordsSynced atomic.Int64
	var normalizingBatchID atomic.Int64
	var normalizeWaiting atomic.Bool
	var syncingBatchID atomic.Int64
	var syncState atomic.Pointer[string]
	syncState.Store(shared.Ptr("setup"))
	shutdown := heartbeatRoutine(ctx, func() string {
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

	srcConn, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		getErr := fmt.Errorf("failed to get pull connector: %w", err)
		return a.Alerter.LogFlowSyncError(ctx, config.FlowJobName, syncingBatchID.Load(), getErr)
	}

	if err := srcConn.SetupReplConn(ctx); err != nil {
		connectors.CloseConnector(ctx, srcConn)
		replConnErr := fmt.Errorf("failed to setup repl connection: %w", err)
		return a.Alerter.LogFlowSyncError(ctx, config.FlowJobName, syncingBatchID.Load(), replConnErr)
	}

	normalizeBufferSize, err := internal.PeerDBNormalizeChannelBufferSize(ctx, config.Env)
	if err != nil {
		connectors.CloseConnector(ctx, srcConn)
		cfgErr := fmt.Errorf("failed to get normalize channel buffer size config: %w", err)
		return a.Alerter.LogFlowSyncError(ctx, config.FlowJobName, syncingBatchID.Load(), cfgErr)
	}

	reconnectAfterBatches, err := internal.PeerDBReconnectAfterBatches(ctx, config.Env)
	if err != nil {
		connectors.CloseConnector(ctx, srcConn)
		cfgErr := fmt.Errorf("failed to get reconnect after batches config: %w", err)
		return a.Alerter.LogFlowSyncError(ctx, config.FlowJobName, syncingBatchID.Load(), cfgErr)
	}

	// syncDone will be closed by SyncFlow,
	// whereas normalizeDone will be closed by normalizing goroutine
	// Wait on normalizeDone at end to not interrupt final normalize
	syncDone := make(chan struct{})
	normRequests := make(chan NormalizeBatchRequest, normalizeBufferSize)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		normalizeCtx := internal.WithOperationContext(groupCtx, protos.FlowOperation_FLOW_OPERATION_NORMALIZE)
		// returning error signals sync to stop, normalize can recover connections without interrupting sync, so never return error
		a.normalizeLoop(normalizeCtx, logger, config, syncDone, normRequests, &normalizingBatchID, &normalizeWaiting)
		return nil
	})
	group.Go(func() error {
		defer connectors.CloseConnector(groupCtx, srcConn)
		if err := a.maintainReplConn(groupCtx, srcConn, syncDone); err != nil {
			return a.Alerter.LogFlowErrorNoStatus(groupCtx, config.FlowJobName, err)
		}
		return nil
	})

	for groupCtx.Err() == nil {
		syncNum := currentSyncFlowNum.Add(1)
		logger.Info("executing sync flow", slog.Int64("count", int64(syncNum)))

		var syncResponse *model.SyncResponse
		var err error
		if config.System == protos.TypeSystem_Q {
			syncResponse, err = a.syncRecords(groupCtx, config, options, srcConn.(connectors.CDCPullConnector),
				normRequests, &syncingBatchID, &syncState)
		} else {
			syncResponse, err = a.syncPg(groupCtx, config, options, srcConn.(connectors.CDCPullPgConnector),
				normRequests, &syncingBatchID, &syncState)
		}

		if err != nil {
			if groupCtx.Err() != nil {
				// need to return ctx.Err(), avoid returning syncErr that's wrapped context canceled
				break
			}
			syncErr := fmt.Errorf("failed to sync records: %w", err)
			var skipLogFlowError *exceptions.SkipLogFlowError
			if !errors.As(syncErr, &skipLogFlowError) {
				a.Alerter.LogFlowSyncError(ctx, config.FlowJobName, syncingBatchID.Load(), syncErr)
			} else {
				logger.Error(syncErr.Error())
			}
			syncState.Store(shared.Ptr("cleanup"))
			close(syncDone)
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
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	srcConn connectors.CDCPullConnector,
	normRequests chan<- NormalizeBatchRequest,
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
				return nil, fmt.Errorf("failed to load script: %w", err)
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
	return syncCore(ctx, a, config, options, srcConn, normRequests,
		syncingBatchID, syncWaiting, adaptStream,
		connectors.CDCPullConnector.PullRecords,
		connectors.CDCSyncConnector.SyncRecords)
}

func (a *FlowableActivity) syncPg(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	srcConn connectors.CDCPullPgConnector,
	normRequests chan<- NormalizeBatchRequest,
	syncingBatchID *atomic.Int64,
	syncWaiting *atomic.Pointer[string],
) (*model.SyncResponse, error) {
	return syncCore(ctx, a, config, options, srcConn, normRequests,
		syncingBatchID, syncWaiting, nil,
		connectors.CDCPullPgConnector.PullPg,
		connectors.CDCSyncPgConnector.SyncPg)
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig, runUUID string) error {
	conn, err := connectors.GetByNameAs[connectors.QRepSyncConnector](ctx, config.Env, a.CatalogPool, config.DestinationName)
	if err != nil {
		getErr := fmt.Errorf("failed to get connector: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, getErr)
	}
	defer connectors.CloseConnector(ctx, conn)

	if err := conn.SetupQRepMetadataTables(ctx, config); err != nil {
		setupErr := fmt.Errorf("failed to setup metadata tables: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, setupErr)
	}

	return nil
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	runUUID string,
) (*protos.QRepParitionResult, error) {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "getting partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))
	if err := monitoring.InitializeQRepRun(ctx, logger, a.CatalogPool, config, runUUID, nil, config.ParentMirrorName); err != nil {
		return nil, err
	}
	srcConn, err := connectors.GetByNameAs[connectors.QRepPullConnector](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		getErr := fmt.Errorf("failed to get qrep pull connector: %w", err)
		return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, getErr)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	partitions, err := srcConn.GetQRepPartitions(ctx, config, last)
	if err != nil {
		getErr := fmt.Errorf("failed to get partitions from source: %w", err)
		return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, getErr)
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
			initErr := fmt.Errorf("failed to initialize qrep run: %w", err)
			return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, initErr)
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
	shutdown := heartbeatRoutine(ctx, func() string {
		return "replicating partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	if err != nil {
		// Ideally we should have granular status for partition batches.
		// However, those are identified by run uuid + batch id, need to be stored in an array,
		// and this is the only place a failure can happen on a batch level.
		// Let's assume a catalog failure will bubble up elsewhere too.
		return shared.LogError(logger, fmt.Errorf("failed to update start time for qrep run: %w", err))
	}

	numPartitions := len(partitions.Partitions)
	logger.Info("replicating partitions for batch",
		slog.Int64("batchID", int64(partitions.BatchId)), slog.Int("partitions", numPartitions))

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
					loadErr := fmt.Errorf("failed to load script: %w", err)
					return a.Alerter.LogFlowSnapshotPartitionError(
						ctx, config.FlowJobName, config.SnapshotId, p.PartitionId, loadErr,
					)
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
			replErr := fmt.Errorf("failed to replicate partition: %w", err)
			return a.Alerter.LogFlowSnapshotPartitionError(
				ctx, config.FlowJobName, config.SnapshotId, p.PartitionId, replErr,
			)
		}
	}

	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "replicated all rows to destination for table "+config.DestinationTableIdentifier)
	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string,
) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "consolidating partitions for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetByNameAs[connectors.QRepConsolidateConnector](ctx, config.Env, a.CatalogPool, config.DestinationName)
	if errors.Is(err, errors.ErrUnsupported) {
		if err := monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID); err != nil {
			updateErr := fmt.Errorf("failed to update end time for qrep run: %w", err)
			return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, updateErr)
		}
		return nil
	} else if err != nil {
		getErr := fmt.Errorf("failed to get consolidate connector for qrep run: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, getErr)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if err := dstConn.ConsolidateQRepPartitions(ctx, config); err != nil {
		consErr := fmt.Errorf("failed to consolidate partitions for qrep run: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, consErr)
	}

	if err := monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID); err != nil {
		updateErr := fmt.Errorf("failed to update end time for qrep run: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, updateErr)
	}
	return nil
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig, runUUID string) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dst, err := connectors.GetByNameAs[connectors.QRepConsolidateConnector](ctx, config.Env, a.CatalogPool, config.DestinationName)
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	} else if err != nil {
		getErr := fmt.Errorf("failed to get consolidate connector for qrep run: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, getErr)
	}
	defer connectors.CloseConnector(ctx, dst)

	if err := dst.CleanupQRepFlow(ctx, config); err != nil {
		cleanupErr := fmt.Errorf("failed to cleanup qrep flow: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, cleanupErr)
	}
	return nil
}

func (a *FlowableActivity) UpdateQRepStatusSuccess(
	ctx context.Context, config *protos.QRepConfig, runUUID string,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	if err := monitoring.UpdateQRepStatusSuccess(
		ctx, a.CatalogPool, config.FlowJobName, config.SnapshotId, runUUID,
	); err != nil {
		updateErr := fmt.Errorf("failed to update qrep status: %w", err)
		return a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, config.SnapshotId, runUUID, updateErr)
	}
	return nil
}

func (a *FlowableActivity) DropFlowSource(ctx context.Context, req *protos.DropFlowActivityInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	srcConn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		getErr := exceptions.NewDropFlowError("[DropFlowSource] failed to get source connector: %w", err)
		return a.Alerter.LogFlowErrorNoStatus(ctx, req.FlowJobName, getErr)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.PullFlowCleanup(ctx, req.FlowJobName); err != nil {
		pullCleanupErr := exceptions.NewDropFlowError("[DropFlowSource] failed to clean up source: %w", err)
		if !shared.IsSQLStateError(err, pgerrcode.ObjectInUse) {
			// don't alert when PID active
			_ = a.Alerter.LogFlowErrorNoStatus(ctx, req.FlowJobName, pullCleanupErr)
		} else {
			logger := internal.LoggerFromCtx(ctx)
			logger.Error(pullCleanupErr.Error())
		}
		return pullCleanupErr
	}

	a.Alerter.LogFlowInfo(ctx, req.FlowJobName, "Cleaned up source peer replication objects.")

	return nil
}

func (a *FlowableActivity) DropFlowDestination(ctx context.Context, req *protos.DropFlowActivityInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dstConn, err := connectors.GetByNameAs[connectors.CDCSyncConnector](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		getErr := exceptions.NewDropFlowError("[DropFlowDestination] failed to get destination connector: %w", err)
		return a.Alerter.LogFlowErrorNoStatus(ctx, req.FlowJobName, getErr)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if err := dstConn.SyncFlowCleanup(ctx, req.FlowJobName); err != nil {
		cleanupErr := exceptions.NewDropFlowError("[DropFlowDestination] failed to clean up destination: %w", err)
		return a.Alerter.LogFlowErrorNoStatus(ctx, req.FlowJobName, cleanupErr)
	}

	a.Alerter.LogFlowInfo(ctx, req.FlowJobName,
		"Cleaned up destination peer replication objects. Any metadata storage has been dropped.")

	return nil
}

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	logger := internal.LoggerFromCtx(ctx)
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
		activity.RecordHeartbeat(ctx, pgPeer.Name)
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
				logger.Warn(fmt.Sprintf("could not send wal heartbeat to peer %s: %v", pgPeer.Name, cmdErr))
			}
			logger.Info("sent wal heartbeat", slog.String("peer", pgPeer.Name))
		}()
	}

	return nil
}

type flowInformation struct {
	config     *protos.FlowConnectionConfigs
	workflowID string
	status     protos.FlowStatus
	isActive   bool
}

func (a *FlowableActivity) RecordSlotSizes(ctx context.Context) error {
	rows, err := a.CatalogPool.Query(ctx, "SELECT DISTINCT ON (name) name, config_proto, workflow_id FROM flows WHERE query_string IS NULL")
	if err != nil {
		return err
	}

	infos, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*flowInformation, error) {
		var flowName string
		var configProto []byte
		var workflowID string
		if err := rows.Scan(&flowName, &configProto, &workflowID); err != nil {
			return nil, err
		}

		var config protos.FlowConnectionConfigs
		if err := internal.ProtoUnmarshal(configProto, &config); err != nil {
			return nil, err
		}

		return &flowInformation{
			config:     &config,
			workflowID: workflowID,
		}, nil
	})
	if err != nil {
		return err
	}

	logger := internal.LoggerFromCtx(ctx)
	slotMetricGauges := otel_metrics.SlotMetricGauges{}
	slotMetricGauges.SlotLagGauge = a.OtelManager.Metrics.SlotLagGauge
	slotMetricGauges.RestartLSNGauge = a.OtelManager.Metrics.RestartLSNGauge
	slotMetricGauges.ConfirmedFlushLSNGauge = a.OtelManager.Metrics.ConfirmedFlushLSNGauge

	slotMetricGauges.OpenConnectionsGauge = a.OtelManager.Metrics.OpenConnectionsGauge

	slotMetricGauges.OpenReplicationConnectionsGauge = a.OtelManager.Metrics.OpenReplicationConnectionsGauge

	slotMetricGauges.IntervalSinceLastNormalizeGauge = a.OtelManager.Metrics.IntervalSinceLastNormalizeGauge

	maintenanceEnabled, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	instanceStatus := otel_metrics.InstanceStatusReady
	if err != nil {
		logger.Error("Failed to get maintenance mode status", slog.Any("error", err))
		instanceStatus = otel_metrics.InstanceStatusUnknown
	}
	if maintenanceEnabled {
		instanceStatus = otel_metrics.InstanceStatusMaintenance
	}

	a.OtelManager.Metrics.InstanceStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.InstanceStatusKey, instanceStatus),
		attribute.String(otel_metrics.PeerDBVersionKey, internal.PeerDBVersionShaShort()),
		attribute.String(otel_metrics.DeploymentVersionKey, internal.PeerDBDeploymentVersion()),
	)))
	activeFlows := make([]*flowInformation, 0, len(infos))
	for _, info := range infos {
		func(ctx context.Context) {
			flowMetadata, err := a.GetFlowMetadata(ctx, &protos.FlowContextMetadataInput{
				FlowName:        info.config.FlowJobName,
				SourceName:      info.config.SourceName,
				DestinationName: info.config.DestinationName,
			})
			if err != nil {
				logger.Error("Failed to get flow metadata", slog.Any("error", err))
			}
			ctx = context.WithValue(ctx, internal.FlowMetadataKey, flowMetadata)
			logger = internal.LoggerFromCtx(ctx)
			status, sErr := internal.GetWorkflowStatus(ctx, a.CatalogPool, a.TemporalClient, info.workflowID)
			if sErr != nil {
				logger.Error("Failed to get workflow status", slog.Any("error", sErr), slog.String("status", status.String()))
			}
			info.status = status
			if _, info.isActive = activeFlowStatuses[status]; info.isActive {
				activeFlows = append(activeFlows, info)
			}
			a.OtelManager.Metrics.SyncedTablesGauge.Record(ctx, int64(len(info.config.TableMappings)))
			a.OtelManager.Metrics.FlowStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowStatusKey, status.String()),
				attribute.Bool(otel_metrics.IsFlowActiveKey, info.isActive),
			)))

			if flowMetadata.Status == protos.FlowStatus_STATUS_COMPLETED ||
				flowMetadata.Status == protos.FlowStatus_STATUS_TERMINATED {
				return
			}

			srcConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, a.CatalogPool, info.config.SourceName)
			if err != nil {
				if !errors.Is(err, errors.ErrUnsupported) {
					logger.Error("Failed to create connector to handle slot info", slog.Any("error", err))
				}
				return
			}
			defer connectors.CloseConnector(ctx, srcConn)

			slotName := "peerflow_slot_" + info.config.FlowJobName
			if info.config.ReplicationSlotName != "" {
				slotName = info.config.ReplicationSlotName
			}
			peerName := info.config.SourceName

			activity.RecordHeartbeat(ctx, fmt.Sprintf("checking %s on %s", slotName, peerName))
			if ctx.Err() != nil {
				return
			}
			if err := srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, &alerting.AlertKeys{
				FlowName: info.config.FlowJobName,
				PeerName: peerName,
				SlotName: slotName,
			}, slotMetricGauges); err != nil {
				logger.Error("Failed to handle slot info", slog.Any("error", err))
			}
		}(ctx)
	}
	if activeFlowCount := len(activeFlows); activeFlowCount > 0 {
		var activeFlowCpuLimit float64
		var totalCpuLimit float64
		if cpuLimitStr, ok := os.LookupEnv("CURRENT_CONTAINER_CPU_LIMIT"); ok {
			totalCpuLimit, err = strconv.ParseFloat(cpuLimitStr, 64)
			if err != nil {
				logger.Error("Failed to parse CPU limit", slog.Any("error", err), slog.String("cpuLimit", cpuLimitStr))
			}
		}

		var activeFlowMemoryLimit float64
		var totalMemoryLimit float64
		if memLimitStr, ok := os.LookupEnv("CURRENT_CONTAINER_MEMORY_LIMIT"); ok {
			totalMemoryLimit, err = strconv.ParseFloat(memLimitStr, 64)
			if err != nil {
				logger.Error("Failed to parse Memory limit", slog.Any("error", err), slog.String("memLimit", memLimitStr))
			}
		}

		activeFlowCpuLimit = totalCpuLimit / float64(activeFlowCount)
		activeFlowMemoryLimit = totalMemoryLimit / float64(activeFlowCount)
		a.OtelManager.Metrics.ActiveFlowsGauge.Record(ctx, int64(activeFlowCount))
		if activeFlowCpuLimit > 0 || activeFlowMemoryLimit > 0 {
			for _, info := range activeFlows {
				func(ctx context.Context) {
					flowMetadata, err := a.GetFlowMetadata(ctx, &protos.FlowContextMetadataInput{
						FlowName:        info.config.FlowJobName,
						SourceName:      info.config.SourceName,
						DestinationName: info.config.DestinationName,
					})
					if err != nil {
						logger.Error("Failed to get flow metadata", slog.Any("error", err))
					}
					ctx = context.WithValue(ctx, internal.FlowMetadataKey, flowMetadata)
					logger = internal.LoggerFromCtx(ctx)

					if activeFlowMemoryLimit > 0 {
						a.OtelManager.Metrics.MemoryLimitsPerActiveFlowGauge.Record(ctx, activeFlowMemoryLimit)
					}
					if activeFlowCpuLimit > 0 {
						a.OtelManager.Metrics.CPULimitsPerActiveFlowGauge.Record(ctx, activeFlowCpuLimit)
					}
				}(ctx)
			}
		}
	}

	return nil
}

var activeFlowStatuses = map[protos.FlowStatus]struct{}{
	protos.FlowStatus_STATUS_RUNNING:  {},
	protos.FlowStatus_STATUS_PAUSING:  {},
	protos.FlowStatus_STATUS_SETUP:    {},
	protos.FlowStatus_STATUS_SNAPSHOT: {},
	protos.FlowStatus_STATUS_RESYNC:   {},
}

func (a *FlowableActivity) QRepHasNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) (bool, error) {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "scanning for new rows"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	// TODO implement for other QRepPullConnector sources
	srcConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, config.Env, a.CatalogPool, config.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return true, nil
		}
		getErr := fmt.Errorf("failed to get qrep source connector: %w", err)
		return false, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, getErr)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	logger.Info(fmt.Sprintf("current last partition value is %v", last))

	maxValue, err := srcConn.GetMaxValue(ctx, config, last)
	if err != nil {
		checkErr := fmt.Errorf("failed to check for new rows: %w", err)
		return false, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, checkErr)
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
	res, err := renameTables(ctx, config, a.CatalogPool)
	if err != nil {
		renameErr := fmt.Errorf("failed to rename tables: %w", err)
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, config.FlowJobName, renameErr)
	}
	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Resync completed for all tables")
	return res, nil
}

func (a *FlowableActivity) QRepRenameTables(
	ctx context.Context, config *protos.RenameTablesInput, snapshotID int32, runUUID string,
) (*protos.RenameTablesOutput, error) {
	res, err := renameTables(ctx, config, a.CatalogPool)
	if err != nil {
		renameErr := fmt.Errorf("failed to rename tables: %w", err)
		return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, config.FlowJobName, snapshotID, runUUID, renameErr)
	}
	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Resync completed for all tables")
	return res, nil
}

func renameTables(ctx context.Context, config *protos.RenameTablesInput, pool shared.CatalogPool) (*protos.RenameTablesOutput, error) {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "renaming tables for job"
	})
	defer shutdown()

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	conn, err := connectors.GetByNameAs[connectors.RenameTablesConnector](ctx, nil, pool, config.PeerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	tableNameSchemaMapping := make(map[string]*protos.TableSchema, len(config.RenameTableOptions))
	for _, option := range config.RenameTableOptions {
		schema, err := internal.LoadTableSchemaFromCatalog(
			ctx,
			pool,
			config.FlowJobName,
			option.CurrentName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load schema to rename tables: %w", err)
		}
		tableNameSchemaMapping[option.CurrentName] = schema
	}

	renameOutput, err := conn.RenameTables(ctx, config, tableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to rename tables: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin updating table_schema_mapping: %w", err)
	}
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))
	defer shared.RollbackTx(tx, logger)

	for _, option := range config.RenameTableOptions {
		if option.NewName != option.CurrentName {
			if _, err := tx.Exec(
				ctx,
				"delete from table_schema_mapping where flow_name = $1 and table_name = $2",
				config.FlowJobName,
				option.NewName,
			); err != nil {
				return nil, fmt.Errorf("failed to update table_schema_mapping: %w", err)
			}
		}
		if _, err := tx.Exec(
			ctx,
			"update table_schema_mapping set table_name = $3 where flow_name = $1 and table_name = $2",
			config.FlowJobName,
			option.CurrentName,
			option.NewName,
		); err != nil {
			return nil, fmt.Errorf("failed to update table_schema_mapping: %w", err)
		}
	}

	return renameOutput, tx.Commit(ctx)
}

func (a *FlowableActivity) DeleteMirrorStats(ctx context.Context, flowName string) error {
	shutdown := heartbeatRoutine(ctx, func() string {
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
	dstConn, err := connectors.GetByNameAs[connectors.CreateTablesFromExistingConnector](ctx, nil, a.CatalogPool, req.PeerName)
	if err != nil {
		getErr := fmt.Errorf("failed to get connector: %w", err)
		return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, req.FlowJobName, req.SnapshotId, req.RunUuid, getErr)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	output, err := dstConn.CreateTablesFromExisting(ctx, req)
	if err != nil {
		createErr := fmt.Errorf("failed to create tables: %w", err)
		return nil, a.Alerter.LogFlowSnapshotQRepError(ctx, req.FlowJobName, req.SnapshotId, req.RunUuid, createErr)
	}
	return output, nil
}

func (a *FlowableActivity) ReplicateXminPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
) (int64, error) {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "syncing xmin"
	})
	defer shutdown()

	var lastPartition int64
	var err error
	switch config.System {
	case protos.TypeSystem_Q:
		stream := model.NewQRecordStream(shared.FetchAndChannelSize)
		lastPartition, err = replicateXminPartition(ctx, a, config, partition, runUUID,
			stream, stream,
			(*connpostgres.PostgresConnector).PullXminRecordStream,
			connectors.QRepSyncConnector.SyncQRepRecords)
	case protos.TypeSystem_PG:
		pgread, pgwrite := connpostgres.NewPgCopyPipe()
		lastPartition, err = replicateXminPartition(ctx, a, config, partition, runUUID,
			pgwrite, pgread,
			(*connpostgres.PostgresConnector).PullXminPgRecordStream,
			connectors.QRepSyncPgConnector.SyncPgQRepRecords)
	default:
		return 0, fmt.Errorf("unknown type system %d", config.System)
	}

	if err != nil {
		replErr := fmt.Errorf("failed to replicate partition: %w", err)
		return 0, a.Alerter.LogFlowSnapshotPartitionError(ctx, config.FlowJobName, config.SnapshotId, partition.PartitionId, replErr)
	}
	return lastPartition, nil
}

func (a *FlowableActivity) AddTablesToPublication(ctx context.Context, cfg *protos.FlowConnectionConfigs,
	additionalTableMappings []*protos.TableMapping,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.AddTablesToPublication(ctx, &protos.AddTablesToPublicationInput{
		FlowJobName:      cfg.FlowJobName,
		PublicationName:  cfg.PublicationName,
		AdditionalTables: additionalTableMappings,
	}); err != nil {
		return a.Alerter.LogFlowErrorNoStatus(ctx, cfg.FlowJobName, err)
	}

	a.Alerter.LogFlowInfo(ctx, cfg.FlowJobName, fmt.Sprintf("ensured %d tables exist in publication %s",
		len(additionalTableMappings), cfg.PublicationName))
	return nil
}

func (a *FlowableActivity) RemoveTablesFromPublication(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigs,
	removedTablesMapping []*protos.TableMapping,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, cfg.Env, a.CatalogPool, cfg.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil
		}
		return a.Alerter.LogFlowErrorNoStatus(ctx, cfg.FlowJobName, fmt.Errorf("failed to get source connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.RemoveTablesFromPublication(ctx, &protos.RemoveTablesFromPublicationInput{
		FlowJobName:     cfg.FlowJobName,
		PublicationName: cfg.PublicationName,
		TablesToRemove:  removedTablesMapping,
	}); err != nil {
		return a.Alerter.LogFlowErrorNoStatus(ctx, cfg.FlowJobName, err)
	}

	a.Alerter.LogFlowInfo(ctx, cfg.FlowJobName, fmt.Sprintf("removed %d tables from publication %s",
		len(removedTablesMapping), cfg.PublicationName))
	return nil
}

func (a *FlowableActivity) RemoveTablesFromRawTable(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigs,
	tablesToRemove []*protos.TableMapping,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), cfg.FlowJobName))
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)
	normBatchID, err := pgMetadata.GetLastNormalizeBatchID(ctx, cfg.FlowJobName)
	if err != nil {
		logger.Error("[RemoveTablesFromRawTable] failed to get last normalize batch id", slog.Any("error", err))
		return err
	}

	syncBatchID, err := pgMetadata.GetLastSyncBatchID(ctx, cfg.FlowJobName)
	if err != nil {
		logger.Error("[RemoveTablesFromRawTable] failed to get last sync batch id", slog.Any("error", err))
		return err
	}

	dstConn, err := connectors.GetByNameAs[connectors.RawTableConnector](ctx, cfg.Env, a.CatalogPool, cfg.DestinationName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			// For connectors where raw table is not a concept,
			// we can ignore the error
			return nil
		}
		return a.Alerter.LogFlowErrorNoStatus(ctx, cfg.FlowJobName,
			fmt.Errorf("[RemoveTablesFromRawTable] failed to get destination connector: %w", err),
		)
	}
	defer connectors.CloseConnector(ctx, dstConn)

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
		return a.Alerter.LogFlowErrorNoStatus(ctx, cfg.FlowJobName, err)
	}
	return nil
}

func (a *FlowableActivity) RemoveTablesFromCatalog(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigs,
	tablesToRemove []*protos.TableMapping,
) error {
	removedTables := make([]string, 0, len(tablesToRemove))
	for _, tm := range tablesToRemove {
		removedTables = append(removedTables, tm.DestinationTableIdentifier)
	}

	_, err := a.CatalogPool.Exec(
		ctx,
		"delete from table_schema_mapping where flow_name = $1 and table_name = ANY($2)",
		cfg.FlowJobName,
		removedTables,
	)

	return err
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

func (a *FlowableActivity) GetFlowMetadata(
	ctx context.Context,
	input *protos.FlowContextMetadataInput,
) (*protos.FlowContextMetadata, error) {
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), input.FlowName))
	peerTypes, err := connectors.LoadPeerTypes(ctx, a.CatalogPool, []string{input.SourceName, input.DestinationName})
	if err != nil {
		return nil, a.Alerter.LogFlowErrorNoStatus(ctx, input.FlowName, err)
	}
	logger.Debug("loaded peer types for flow", slog.String("flowName", input.FlowName),
		slog.String("sourceName", input.SourceName), slog.String("destinationName", input.DestinationName),
		slog.Any("peerTypes", peerTypes))
	return &protos.FlowContextMetadata{
		FlowName: input.FlowName,
		Source: &protos.PeerContextMetadata{
			Name: input.SourceName,
			Type: peerTypes[input.SourceName],
		},
		Destination: &protos.PeerContextMetadata{
			Name: input.DestinationName,
			Type: peerTypes[input.DestinationName],
		},
		Status:   input.Status,
		IsResync: input.IsResync,
	}, nil
}
