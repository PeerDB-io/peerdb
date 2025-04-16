// internal methods for flowable.go
package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type PeerType string

const (
	Source      PeerType = "source"
	Destination PeerType = "destination"
)

func heartbeatRoutine(
	ctx context.Context,
	message func() string,
) func() {
	counter := 0
	return shared.Interval(
		ctx,
		15*time.Second,
		func() {
			counter += 1
			activity.RecordHeartbeat(ctx, fmt.Sprintf("heartbeat #%d: %s", counter, message()))
		},
	)
}

func (a *FlowableActivity) getTableNameSchemaMapping(ctx context.Context, flowName string) (map[string]*protos.TableSchema, error) {
	rows, err := a.CatalogPool.Query(ctx, "select table_name, table_schema from table_schema_mapping where flow_name = $1", flowName)
	if err != nil {
		return nil, err
	}

	var tableName string
	var tableSchemaBytes []byte
	tableNameSchemaMapping := make(map[string]*protos.TableSchema)
	if _, err := pgx.ForEachRow(rows, []any{&tableName, &tableSchemaBytes}, func() error {
		tableSchema := &protos.TableSchema{}
		if err := proto.Unmarshal(tableSchemaBytes, tableSchema); err != nil {
			return err
		}
		tableNameSchemaMapping[tableName] = tableSchema
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to deserialize table schema proto: %w", err)
	}
	return tableNameSchemaMapping, nil
}

func (a *FlowableActivity) applySchemaDeltas(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	filteredTableMappings := make([]*protos.TableMapping, 0, len(schemaDeltas))
	for _, tableMapping := range options.TableMappings {
		if slices.ContainsFunc(schemaDeltas, func(schemaDelta *protos.TableSchemaDelta) bool {
			return schemaDelta.SrcTableName == tableMapping.SourceTableIdentifier &&
				schemaDelta.DstTableName == tableMapping.DestinationTableIdentifier
		}) {
			filteredTableMappings = append(filteredTableMappings, tableMapping)
		}
	}

	if len(schemaDeltas) > 0 {
		if err := a.SetupTableSchema(ctx, &protos.SetupTableSchemaBatchInput{
			PeerName:      config.SourceName,
			TableMappings: filteredTableMappings,
			FlowName:      config.FlowJobName,
			System:        config.System,
			Env:           config.Env,
		}); err != nil {
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to execute schema update at source: %w", err))
		}
	}
	return nil
}

func syncCore[TPull connectors.CDCPullConnectorCore, TSync connectors.CDCSyncConnectorCore, Items model.Items](
	ctx context.Context,
	a *FlowableActivity,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	srcConn TPull,
	normRequests chan<- NormalizeBatchRequest,
	syncingBatchID *atomic.Int64,
	syncState *atomic.Pointer[string],
	adaptStream func(*model.CDCStream[Items]) (*model.CDCStream[Items], error),
	pull func(TPull, context.Context, shared.CatalogPool, *otel_metrics.OtelManager, *model.PullRecordsRequest[Items]) error,
	sync func(TSync, context.Context, *model.SyncRecordsRequest[Items]) (*model.SyncResponse, error),
) (*model.SyncResponse, error) {
	flowName := config.FlowJobName
	ctx = context.WithValue(ctx, shared.FlowNameKey, flowName)
	logger := internal.LoggerFromCtx(ctx)

	tblNameMapping := make(map[string]model.NameAndExclude, len(options.TableMappings))
	for _, v := range options.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = model.NewNameAndExclude(v.DestinationTableIdentifier, v.Exclude)
	}

	if err := srcConn.ConnectionActive(ctx); err != nil {
		return nil, temporal.NewNonRetryableApplicationError("connection to source down", "disconnect", nil)
	}

	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = 250_000
	}

	lastOffset, err := func() (model.CdcCheckpoint, error) {
		if myConn, isMy := any(srcConn).(*connmysql.MySqlConnector); isMy {
			return myConn.GetLastOffset(ctx, config.FlowJobName)
		} else {
			dstConn, err := connectors.GetByNameAs[TSync](ctx, config.Env, a.CatalogPool, config.DestinationName)
			if err != nil {
				return model.CdcCheckpoint{}, fmt.Errorf("failed to get destination connector: %w", err)
			}
			defer connectors.CloseConnector(ctx, dstConn)

			return dstConn.GetLastOffset(ctx, config.FlowJobName)
		}
	}()
	if err != nil {
		return nil, a.Alerter.LogFlowError(ctx, flowName, err)
	}

	logger.Info("pulling records...", slog.Any("LastOffset", lastOffset))
	consumedOffset := atomic.Int64{}
	consumedOffset.Store(lastOffset.ID)

	channelBufferSize, err := internal.PeerDBCDCChannelBufferSize(ctx, config.Env)
	if err != nil {
		return nil, fmt.Errorf("failed to get CDC channel buffer size: %w", err)
	}
	recordBatchPull := model.NewCDCStream[Items](channelBufferSize)
	recordBatchSync := recordBatchPull
	if adaptStream != nil {
		var err error
		if recordBatchSync, err = adaptStream(recordBatchPull); err != nil {
			return nil, err
		}
	}

	tableNameSchemaMapping, err := a.getTableNameSchemaMapping(ctx, flowName)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	syncState.Store(shared.Ptr("syncing"))
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return pull(srcConn, errCtx, a.CatalogPool, a.OtelManager, &model.PullRecordsRequest[Items]{
			FlowJobName:           flowName,
			SrcTableIDNameMapping: options.SrcTableIdNameMapping,
			TableNameMapping:      tblNameMapping,
			LastOffset:            lastOffset,
			ConsumedOffset:        &consumedOffset,
			MaxBatchSize:          batchSize,
			IdleTimeout: internal.PeerDBCDCIdleTimeoutSeconds(
				int(options.IdleTimeoutSeconds),
			),
			TableNameSchemaMapping:      tableNameSchemaMapping,
			OverridePublicationName:     config.PublicationName,
			OverrideReplicationSlotName: config.ReplicationSlotName,
			RecordStream:                recordBatchPull,
			Env:                         config.Env,
		})
	})

	hasRecords := !recordBatchSync.WaitAndCheckEmpty()
	logger.Info("current sync flow has records?", slog.Bool("hasRecords", hasRecords))

	if !hasRecords {
		// wait for the pull goroutine to finish
		if err := errGroup.Wait(); err != nil {
			// don't log flow error for "replState changed" and "slot is already active"
			if !(temporal.IsApplicationError(err) || shared.IsSQLStateError(err, pgerrcode.ObjectInUse)) {
				_ = a.Alerter.LogFlowError(ctx, flowName, err)
			}
			if temporal.IsApplicationError(err) {
				return nil, err
			} else {
				return nil, fmt.Errorf("failed in pull records when: %w", err)
			}
		}
		logger.Info("no records to push")

		dstConn, err := connectors.GetByNameAs[TSync](ctx, config.Env, a.CatalogPool, config.DestinationName)
		if err != nil {
			return nil, fmt.Errorf("failed to recreate destination connector: %w", err)
		}
		defer connectors.CloseConnector(ctx, dstConn)

		syncState.Store(shared.Ptr("updating schema"))
		if err := dstConn.ReplayTableSchemaDeltas(ctx, config.Env, flowName, recordBatchSync.SchemaDeltas); err != nil {
			return nil, fmt.Errorf("failed to sync schema: %w", err)
		}

		return nil, a.applySchemaDeltas(ctx, config, options, recordBatchSync.SchemaDeltas)
	}

	var res *model.SyncResponse
	errGroup.Go(func() error {
		dstConn, err := connectors.GetByNameAs[TSync](ctx, config.Env, a.CatalogPool, config.DestinationName)
		if err != nil {
			return fmt.Errorf("failed to recreate destination connector: %w", err)
		}
		defer connectors.CloseConnector(ctx, dstConn)

		syncBatchID, err := dstConn.GetLastSyncBatchID(errCtx, flowName)
		if err != nil {
			return err
		}
		syncBatchID += 1
		syncingBatchID.Store(syncBatchID)
		logger.Info("begin pulling records for batch", slog.Int64("SyncBatchID", syncBatchID))

		if err := monitoring.AddCDCBatchForFlow(errCtx, a.CatalogPool, flowName, monitoring.CDCBatchInfo{
			BatchID:     syncBatchID,
			RowsInBatch: 0,
			BatchEndlSN: 0,
			StartTime:   startTime,
		}); err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}

		res, err = sync(dstConn, errCtx, &model.SyncRecordsRequest[Items]{
			SyncBatchID:            syncBatchID,
			Records:                recordBatchSync,
			ConsumedOffset:         &consumedOffset,
			FlowJobName:            flowName,
			TableMappings:          options.TableMappings,
			StagingPath:            config.CdcStagingPath,
			Script:                 config.Script,
			TableNameSchemaMapping: tableNameSchemaMapping,
		})
		if err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("failed to push records: %w", err))
		}

		logger.Info("finished pulling records for batch", slog.Int64("SyncBatchID", syncBatchID))
		return nil
	})

	syncStartTime := time.Now()
	if err := errGroup.Wait(); err != nil {
		// don't log flow error for "replState changed" and "slot is already active"
		var applicationError *temporal.ApplicationError
		if !((errors.As(err, &applicationError) && applicationError.Type() == "desync") || shared.IsSQLStateError(err, pgerrcode.ObjectInUse)) {
			_ = a.Alerter.LogFlowError(ctx, flowName, err)
		}
		if temporal.IsApplicationError(err) {
			return nil, err
		} else {
			return nil, fmt.Errorf("[cdc] failed to pull records: %w", err)
		}
	}
	syncState.Store(shared.Ptr("bookkeeping"))

	syncDuration := time.Since(syncStartTime)
	lastCheckpoint := recordBatchSync.GetLastCheckpoint()
	logger.Info("batch synced", slog.Any("checkpoint", lastCheckpoint))
	if err := srcConn.UpdateReplStateLastOffset(ctx, lastCheckpoint); err != nil {
		return nil, a.Alerter.LogFlowError(ctx, flowName, err)
	}

	if err := monitoring.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx, a.CatalogPool, flowName, res.CurrentSyncBatchID, uint32(res.NumRecordsSynced), lastCheckpoint,
	); err != nil {
		return nil, a.Alerter.LogFlowError(ctx, flowName, err)
	}

	if err := monitoring.UpdateLatestLSNAtTargetForCDCFlow(ctx, a.CatalogPool, flowName, lastCheckpoint.ID); err != nil {
		return nil, a.Alerter.LogFlowError(ctx, flowName, err)
	}
	if res.TableNameRowsMapping != nil {
		if err := monitoring.AddCDCBatchTablesForFlow(
			ctx, a.CatalogPool, flowName, res.CurrentSyncBatchID, res.TableNameRowsMapping,
		); err != nil {
			return nil, err
		}
	}

	pushedRecordsWithCount := fmt.Sprintf("stored %d records into intermediate storage for batch %d in %v",
		res.NumRecordsSynced, res.CurrentSyncBatchID, syncDuration.Truncate(time.Second))
	a.Alerter.LogFlowInfo(ctx, flowName, pushedRecordsWithCount)

	a.OtelManager.Metrics.CurrentBatchIdGauge.Record(ctx, res.CurrentSyncBatchID)

	syncState.Store(shared.Ptr("updating schema"))
	if err := a.applySchemaDeltas(ctx, config, options, res.TableSchemaDeltas); err != nil {
		return nil, err
	}

	if recordBatchSync.NeedsNormalize() {
		parallel, err := internal.PeerDBEnableParallelSyncNormalize(ctx, config.Env)
		if err != nil {
			return nil, err
		}
		var done chan struct{}
		if !parallel {
			done = make(chan struct{})
		}
		syncState.Store(shared.Ptr("normalizing"))
		select {
		case normRequests <- NormalizeBatchRequest{BatchID: res.CurrentSyncBatchID, Done: done}:
		case <-ctx.Done():
			return res, nil
		}
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return res, nil
			}
		}
	}

	return res, nil
}

func (a *FlowableActivity) getPostgresPeerConfigs(ctx context.Context) ([]*protos.Peer, error) {
	optionRows, err := a.CatalogPool.Query(ctx, `
		SELECT p.name, p.options, p.enc_key_id
		FROM peers p
		WHERE p.type = $1 AND EXISTS(SELECT * FROM flows f WHERE p.id = f.source_peer)`, protos.DBType_POSTGRES)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(optionRows, func(row pgx.CollectableRow) (*protos.Peer, error) {
		var peerName string
		var encPeerOptions []byte
		var encKeyID string
		if err := optionRows.Scan(&peerName, &encPeerOptions, &encKeyID); err != nil {
			return nil, err
		}

		peerOptions, err := internal.Decrypt(ctx, encKeyID, encPeerOptions)
		if err != nil {
			return nil, err
		}

		var pgPeerConfig protos.PostgresConfig
		unmarshalErr := proto.Unmarshal(peerOptions, &pgPeerConfig)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return &protos.Peer{
			Name:   peerName,
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: &pgPeerConfig},
		}, nil
	})
}

// replicateQRepPartition replicates a QRepPartition from the source to the destination.
func replicateQRepPartition[TRead any, TWrite StreamCloser, TSync connectors.QRepSyncConnectorCore, TPull connectors.QRepPullConnectorCore](
	ctx context.Context,
	a *FlowableActivity,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
	stream TWrite,
	outstream TRead,
	pullRecords func(
		TPull,
		context.Context, *protos.QRepConfig,
		*protos.QRepPartition,
		TWrite,
	) (int64, int64, error),
	syncRecords func(TSync, context.Context, *protos.QRepConfig, *protos.QRepPartition, TRead) (int, error),
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	dstConn, err := connectors.GetByNameAs[TSync](ctx, config.Env, a.CatalogPool, config.DestinationName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep destination connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, dstConn)

	done, err := dstConn.IsQRepPartitionSynced(ctx, &protos.IsQRepPartitionSyncedInput{
		FlowJobName: config.FlowJobName,
		PartitionId: partition.PartitionId,
	})
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get fetch status of partition: %w", err))
	}
	if done {
		logger.Info("no records to push for partition " + partition.PartitionId)
		activity.RecordHeartbeat(ctx, "no records to push for partition "+partition.PartitionId)
		return nil
	}

	if err := monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, time.Now()); err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to update start time for partition: %w", err))
	}

	logger.Info("replicating partition " + partition.PartitionId)

	var rowsSynced int
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		srcConn, err := connectors.GetByNameAs[TPull](ctx, config.Env, a.CatalogPool, config.SourceName)
		if err != nil {
			stream.Close(err)
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get qrep source connector: %w", err))
		}
		defer connectors.CloseConnector(ctx, srcConn)

		numRecords, numBytes, err := pullRecords(srcConn, errCtx, config, partition, stream)
		if err != nil {
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("[qrep] failed to pull records: %w", err))
		}

		a.OtelManager.Metrics.FetchedBytesCounter.Add(ctx, numBytes)

		if err := monitoring.UpdatePullEndTimeAndRowsForPartition(
			errCtx, a.CatalogPool, runUUID, partition, numRecords,
		); err != nil {
			logger.Error(err.Error())
		}
		return nil
	})

	errGroup.Go(func() error {
		var err error
		rowsSynced, err = syncRecords(dstConn, errCtx, config, partition, outstream)
		if err != nil {
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to sync records: %w", err))
		}
		return context.Canceled
	})

	if err := errGroup.Wait(); err != nil && err != context.Canceled {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	if rowsSynced > 0 {
		logger.Info(fmt.Sprintf("pushed %d records", rowsSynced))
		err := monitoring.UpdateRowsSyncedForPartition(ctx, a.CatalogPool, rowsSynced, runUUID, partition)
		if err != nil {
			return err
		}
	}

	return monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
}

// replicateXminPartition replicates a XminPartition from the source to the destination.
func replicateXminPartition[TRead any, TWrite any, TSync connectors.QRepSyncConnectorCore](
	ctx context.Context,
	a *FlowableActivity,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
	stream TWrite,
	outstream TRead,
	pullRecords func(
		*connpostgres.PostgresConnector,
		context.Context, *protos.QRepConfig,
		*protos.QRepPartition,
		TWrite,
	) (int64, int64, int64, error),
	syncRecords func(TSync, context.Context, *protos.QRepConfig, *protos.QRepPartition, TRead) (int, error),
) (int64, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := internal.LoggerFromCtx(ctx)
	logger.Info("replicating xmin")
	errGroup, errCtx := errgroup.WithContext(ctx)
	startTime := time.Now()

	var currentSnapshotXmin int64
	var rowsSynced int
	errGroup.Go(func() error {
		srcConn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, config.Env, a.CatalogPool, config.SourceName)
		if err != nil {
			return fmt.Errorf("failed to get qrep source connector: %w", err)
		}
		defer connectors.CloseConnector(ctx, srcConn)

		var pullErr error
		var numRecords int64
		var numBytes int64
		numRecords, numBytes, currentSnapshotXmin, pullErr = pullRecords(srcConn, ctx, config, partition, stream)
		if pullErr != nil {
			logger.Warn(fmt.Sprintf("[xmin] failed to pull records: %v", pullErr))
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, pullErr)
		}

		// The first sync of an XMIN mirror will have a partition without a range
		// A nil range is not supported by the catalog mirror monitor functions below
		// So I'm creating a partition with a range of 0 to numRecords
		partitionForMetrics := partition
		if partition.Range == nil {
			partitionForMetrics = &protos.QRepPartition{
				PartitionId: partition.PartitionId,
				Range: &protos.PartitionRange{
					Range: &protos.PartitionRange_IntRange{
						IntRange: &protos.IntPartitionRange{Start: 0, End: numRecords},
					},
				},
			}
		}
		if err := monitoring.InitializeQRepRun(
			ctx, logger, a.CatalogPool, config, runUUID, []*protos.QRepPartition{partitionForMetrics}, config.ParentMirrorName,
		); err != nil {
			return err
		}

		if err := monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, startTime); err != nil {
			return fmt.Errorf("failed to update start time for partition: %w", err)
		}

		a.OtelManager.Metrics.FetchedBytesCounter.Add(ctx, numBytes)

		if err := monitoring.UpdatePullEndTimeAndRowsForPartition(
			errCtx, a.CatalogPool, runUUID, partition, numRecords,
		); err != nil {
			logger.Error(err.Error())
			return err
		}

		return nil
	})

	errGroup.Go(func() error {
		dstConn, err := connectors.GetByNameAs[TSync](ctx, config.Env, a.CatalogPool, config.DestinationName)
		if err != nil {
			return fmt.Errorf("failed to get qrep destination connector: %w", err)
		}
		defer connectors.CloseConnector(ctx, dstConn)

		rowsSynced, err = syncRecords(dstConn, ctx, config, partition, outstream)
		if err != nil {
			return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to sync records: %w", err))
		}
		return context.Canceled
	})

	if err := errGroup.Wait(); err != nil && err != context.Canceled {
		return 0, a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
	}

	if rowsSynced > 0 {
		err := monitoring.UpdateRowsSyncedForPartition(ctx, a.CatalogPool, rowsSynced, runUUID, partition)
		if err != nil {
			return 0, err
		}

		logger.Info(fmt.Sprintf("pushed %d records", rowsSynced))
	}

	if err := monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition); err != nil {
		return 0, err
	}

	return currentSnapshotXmin, nil
}

func (a *FlowableActivity) maintainReplConn(
	ctx context.Context, flowName string, srcConn connectors.CDCPullConnectorCore, syncDone <-chan struct{},
) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := srcConn.ReplPing(ctx); err != nil {
				return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("connection to source down: %w", err))
			}
		case <-syncDone:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (a *FlowableActivity) startNormalize(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	batchID int64,
) error {
	logger := internal.LoggerFromCtx(ctx)

	dstConn, err := connectors.GetByNameAs[connectors.CDCNormalizeConnector](
		ctx,
		config.Env,
		a.CatalogPool,
		config.DestinationName,
	)
	if errors.Is(err, errors.ErrUnsupported) {
		return monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, config.FlowJobName, batchID)
	} else if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get normalize connector: %w", err))
	}
	defer connectors.CloseConnector(ctx, dstConn)

	tableNameSchemaMapping, err := a.getTableNameSchemaMapping(ctx, config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to get table name schema mapping: %w", err)
	}

	logger.Info("normalizing batch", slog.Int64("SyncBatchID", batchID))
	res, err := dstConn.NormalizeRecords(ctx, &model.NormalizeRecordsRequest{
		FlowJobName:            config.FlowJobName,
		Env:                    config.Env,
		TableNameSchemaMapping: tableNameSchemaMapping,
		TableMappings:          config.TableMappings,
		SoftDeleteColName:      config.SoftDeleteColName,
		SyncedAtColName:        config.SyncedAtColName,
		SyncBatchID:            batchID,
	})
	if err != nil {
		return a.Alerter.LogFlowError(ctx, config.FlowJobName,
			exceptions.NewNormalizationError(fmt.Errorf("failed to normalize records: %w", err)))
	}
	if _, dstPg := dstConn.(*connpostgres.PostgresConnector); dstPg {
		if err := monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, config.FlowJobName, batchID); err != nil {
			return fmt.Errorf("failed to update end time for cdc batch: %w", err)
		}
	}

	logger.Info("normalized batches", slog.Int64("StartBatchID", res.StartBatchID), slog.Int64("EndBatchID", res.EndBatchID))

	return nil
}

// Suitable to be run as goroutine
func (a *FlowableActivity) normalizeLoop(
	ctx context.Context,
	logger log.Logger,
	config *protos.FlowConnectionConfigs,
	syncDone <-chan struct{},
	normalizeRequests <-chan NormalizeBatchRequest,
	normalizingBatchID *atomic.Int64,
	normalizeWaiting *atomic.Bool,
) {
	defer normalizeWaiting.Store(false)

	for {
		normalizeWaiting.Store(true)
		select {
		case req := <-normalizeRequests:
			normalizeWaiting.Store(false)
			retryInterval := time.Minute
		retryLoop:
			for {
				normalizingBatchID.Store(req.BatchID)
				if err := a.startNormalize(ctx, config, req.BatchID); err != nil {
					_ = a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
					for {
						// update req to latest normalize request & retry
						select {
						case req = <-normalizeRequests:
						case <-syncDone:
							logger.Info("[normalize-loop] syncDone closed before retry")
							return
						case <-ctx.Done():
							logger.Info("[normalize-loop] context closed before retry")
							return
						default:
							time.Sleep(retryInterval)
							retryInterval = min(retryInterval*2, 5*time.Minute)
							continue retryLoop
						}
					}
				} else if req.Done != nil {
					close(req.Done)
				}
				a.OtelManager.Metrics.LastNormalizedBatchIdGauge.Record(ctx, req.BatchID, metric.WithAttributeSet(attribute.NewSet(
					attribute.String(otel_metrics.FlowNameKey, config.FlowJobName),
				)))
				break
			}
		case <-syncDone:
			logger.Info("[normalize-loop] syncDone closed")
			return
		case <-ctx.Done():
			logger.Info("[normalize-loop] context closed")
			return
		}
	}
}
