package activities

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
)

// syncFlowIsolatedTables replaces pullAndSync/normalizeLoop for the isolated
// per-table CDC path: each source table gets its own sync loop (pull +
// stage-to-S3) and its own normalize loop (staged batches -> final table),
// coupled only by that table's own synced/normalized batch counters. A
// lagging or failing table never blocks its siblings, and a slow normalize
// backpressures only that table's own sync loop. There is no shared
// batch/backpressure state across tables at all.
func (a *FlowableActivity) syncFlowIsolatedTables(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	options *protos.SyncFlowOptions,
	srcConn connectors.TableCDCPullConnector,
) error {
	flowName := config.FlowJobName
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)

	if err := srcConn.ConnectionActive(ctx); err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("connection to source down: %w", err))
	}

	idleTimeout := cdcIdleTimeout(int(options.IdleTimeoutSeconds))
	channelBufferSize, err := internal.PeerDBCDCChannelBufferSize(ctx, config.Env)
	if err != nil {
		return fmt.Errorf("failed to get CDC channel buffer size: %w", err)
	}

	parallelism := int(config.GetQueryCdcTablesParallelism())
	if parallelism <= 0 {
		parallelism, err = internal.PeerDBCDCTableParallelism(ctx, config.Env)
		if err != nil {
			return fmt.Errorf("failed to get CDC table parallelism: %w", err)
		}
	}
	// Bounds concurrent pull+sync work only; each table's normalize loop runs
	// independently of this limit.
	var pullSem chan struct{}
	if parallelism > 0 {
		pullSem = make(chan struct{}, parallelism)
	}

	normBufferHours, err := internal.PeerDBNormalizeBufferHours(ctx, config.Env)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, err)
	}
	// Same approximation pullAndSyncCore uses: normBufferHours worth of
	// idleTimeout-cadence polls, at least 2 so a table's own sync can run
	// ahead of its own normalize by a couple of batches under steady state.
	normBufferSize := normBufferHours * 3600 / int64(idleTimeout.Seconds())
	normBufferSize = max(normBufferSize, 2)

	sourceTables := make([]string, 0, len(options.TableMappings))
	for _, tm := range options.TableMappings {
		sourceTables = append(sourceTables, tm.SourceTableIdentifier)
	}
	// Prune removed mirror tables, otherwise the old state might be used if they are re-added later
	if err := pgMetadata.PruneTableReplicationState(ctx, flowName, sourceTables); err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, err)
	}

	tableNameSchemaMapping, err := a.getTableNameSchemaMapping(ctx, flowName)
	if err != nil {
		return err
	}

	// Seeded from the highest batch_id ever recorded
	maxBatchID, err := monitoring.GetMaxCDCBatchID(ctx, a.CatalogPool, flowName)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, err)
	}
	batchIDCounter := &atomic.Int64{}
	batchIDCounter.Store(maxBatchID)

	var totalRecordsSynced atomic.Int64
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("isolated CDC: %d tables, totalRecordsSynced:%d", len(sourceTables), totalRecordsSynced.Load())
	})
	defer shutdown()

	group, groupCtx := errgroup.WithContext(ctx)
	for _, tableMapping := range options.TableMappings {
		normRequests := concurrency.NewLastChan()
		normResponses := concurrency.NewLastChan()

		group.Go(func() error {
			return a.isolatedTableNormalizeLoop(groupCtx, config, tableMapping, tableNameSchemaMapping, normRequests, normResponses)
		})
		group.Go(func() error {
			return a.isolatedTablePullSyncLoop(groupCtx, config, srcConn, pgMetadata, tableMapping, tableNameSchemaMapping,
				channelBufferSize, idleTimeout, normBufferSize, pullSem, batchIDCounter, &totalRecordsSynced, normRequests, normResponses)
		})
	}
	return group.Wait()
}

// isolatedTablePollWait mirrors bigquery/cdc.go's checkpoint.nextPollWait,
// generalized to the activity level: a table is due once idleTimeout has
// passed since its last poll attempt.
func isolatedTablePollWait(lastAttemptAt time.Time, now time.Time, idleTimeout time.Duration) time.Duration {
	if lastAttemptAt.IsZero() {
		return 0
	}
	nextPollAt := lastAttemptAt.Add(idleTimeout)
	if !nextPollAt.After(now) {
		return 0
	}
	return nextPollAt.Sub(now)
}

func waitOrDone(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquire blocks until sem has room (a nil sem, meaning unlimited, never
// blocks) or ctx is done, returning a release func to call when done. It's a
// no-op once ctx is already done, since nothing was acquired.
func acquire(ctx context.Context, sem chan struct{}) (func(), error) {
	if sem == nil {
		return func() {}, nil
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}

// isolatedTablePullSyncLoop pulls and stages (but does not normalize) one
// source table's records until ctx is done. Backpressures itself, without
// affecting any other table, once this table's own sync/normalize gap
// reaches normBufferSize. A poll failure is alerted once per new lagging
// episode and retried on the same idle-timeout cadence as a normal poll.
func (a *FlowableActivity) isolatedTablePullSyncLoop(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	srcConn connectors.TableCDCPullConnector,
	pgMetadata *connmetadata.PostgresMetadata,
	tableMapping *protos.TableMapping,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	channelBufferSize int,
	idleTimeout time.Duration,
	normBufferSize int64,
	pullSem chan struct{},
	batchIDCounter *atomic.Int64,
	totalRecordsSynced *atomic.Int64,
	normRequests *concurrency.LastChan,
	normResponses *concurrency.LastChan,
) error {
	flowName := config.FlowJobName
	sourceTable := tableMapping.SourceTableIdentifier
	destTable := tableMapping.DestinationTableIdentifier
	nameAndExclude := model.NewNameAndExclude(destTable, tableMapping.Exclude)
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("table", sourceTable))

	wasLagging := false
	for ctx.Err() == nil {
		// captured before the state read so a concurrent normalize commit can't land in the
		// gap between reading a stale state and waiting, which would wait on a channel that
		// already fired (or never will) and stall this table's replication forever
		normWaitCh := normResponses.Wait()
		state, err := pgMetadata.GetTableReplicationState(ctx, flowName, sourceTable)
		if err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}

		if state.SyncedBatchID-state.NormalizedBatchID >= normBufferSize {
			logger.Warn("isolated CDC table waiting on its own normalize backpressure",
				slog.Int64("syncedBatchID", state.SyncedBatchID),
				slog.Int64("normalizedBatchID", state.NormalizedBatchID), slog.Int64("normBufferSize", normBufferSize))
			select {
			case <-normWaitCh:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		if wait := isolatedTablePollWait(state.LastAttemptAt, time.Now(), idleTimeout); wait > 0 {
			logger.Info("[cdc] waiting before next poll", slog.Duration("wait", wait))
			if err := waitOrDone(ctx, wait); err != nil {
				return err
			}
			continue
		}

		// bounded parallelism: only pull+sync for up to parallelism tables at once
		release, err := acquire(ctx, pullSem)
		if err != nil {
			return err
		}
		attemptedAt := time.Now()
		logger.Info("[cdc] starting poll")
		if err := pgMetadata.RecordTableReplicationAttempt(ctx, flowName, sourceTable, attemptedAt); err != nil {
			release()
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}

		nextBatchID := state.SyncedBatchID + 1
		stream := model.NewCDCStream[model.RecordItems](channelBufferSize)
		pollGroup, pollCtx := errgroup.WithContext(ctx)
		var pullResult model.PullTableRecordsResult
		pollGroup.Go(func() error {
			var pullErr error
			pullResult, pullErr = srcConn.PullTableRecords(pollCtx, a.CatalogPool, a.OtelManager, &model.PullTableRecordsRequest{
				Env:                   config.Env,
				FlowJobName:           flowName,
				SourceTableIdentifier: sourceTable,
				NameAndExclude:        nameAndExclude,
				Cursor:                state.CursorText,
				Stream:                stream,
			})
			stream.Close()
			if pullErr == nil {
				logger.Info("[cdc] poll done", slog.Int64("bytesProcessed", pullResult.BytesProcessed))
			}
			return pullErr
		})

		hasRecords := !stream.WaitAndCheckEmpty()
		var rowCounts *model.RecordTypeCounts
		if hasRecords {
			pollGroup.Go(func() error {
				dstConn, dstClose, syncErr := connectors.GetByNameAs[connectors.TableCDCSyncConnector](
					pollCtx, config.Env, a.CatalogPool, config.DestinationName)
				if syncErr != nil {
					return fmt.Errorf("failed to get destination connector: %w", syncErr)
				}
				defer dstClose(pollCtx)

				logger.Info("[cdc] starting sync")
				rowCounts, syncErr = dstConn.SyncTableCDC(pollCtx, &model.SyncTableCDCRequest{
					Env:               config.Env,
					FlowJobName:       flowName,
					TableMapping:      tableMapping,
					TableSchema:       tableNameSchemaMapping[destTable],
					Records:           stream.GetRecords(),
					Version:           config.Version,
					Flags:             config.Flags,
					SchemaDeltas:      stream.SchemaDeltas,
					BatchID:           nextBatchID,
					SoftDeleteColName: config.SoftDeleteColName,
				})
				if syncErr == nil {
					logger.Info("[cdc] sync done",
						slog.Int("inserts", int(rowCounts.InsertCount.Load())),
						slog.Int("updates", int(rowCounts.UpdateCount.Load())),
						slog.Int("deletes", int(rowCounts.DeleteCount.Load())))
				}
				return syncErr
			})
		}

		pollErr := pollGroup.Wait()
		release()
		if pollErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("[cdc] table poll failed; will retry", slog.Any("error", pollErr))
			if !wasLagging {
				_ = a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf(
					"isolated CDC failed to poll source table %s; replication for other tables will continue: %w",
					sourceTable, pollErr))
				wasLagging = true
			}
			continue
		}
		wasLagging = false

		if len(stream.SchemaDeltas) > 0 {
			if err := a.applySchemaDeltas(ctx, config, stream.SchemaDeltas); err != nil {
				return a.Alerter.LogFlowError(ctx, flowName, err)
			}
		}

		a.OtelManager.Metrics.FetchedBytesCounter.Add(ctx, pullResult.BytesProcessed)
		a.OtelManager.Metrics.AllFetchedBytesCounter.Add(ctx, pullResult.BytesProcessed)

		var numSynced int64
		if rowCounts != nil {
			numSynced = int64(rowCounts.InsertCount.Load() + rowCounts.UpdateCount.Load() + rowCounts.DeleteCount.Load())
		}
		newBatchID := int64(0)
		if numSynced > 0 {
			newBatchID = nextBatchID
		}
		if err := pgMetadata.RecordTableReplicationSync(
			ctx, flowName, sourceTable, pullResult.NextCursor, time.Now(), newBatchID,
		); err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}

		if numSynced > 0 {
			totalRecordsSynced.Add(numSynced)
			if err := a.recordIsolatedTableBatch(
				ctx, flowName, destTable, batchIDCounter, attemptedAt, numSynced, rowCounts, pullResult.NextCursor,
			); err != nil {
				return a.Alerter.LogFlowError(ctx, flowName, err)
			}
			normRequests.Update(newBatchID)
		}
	}
	return ctx.Err()
}

// isolatedTableNormalizeLoop inserts one source table's staged batches
// straight into its final destination table as they become available,
// independent of every other table's normalize progress. A normalize failure
// is alerted once per new lagging episode and retried with backoff.
func (a *FlowableActivity) isolatedTableNormalizeLoop(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	tableMapping *protos.TableMapping,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	normRequests *concurrency.LastChan,
	normResponses *concurrency.LastChan,
) error {
	flowName := config.FlowJobName
	sourceTable := tableMapping.SourceTableIdentifier
	destTable := tableMapping.DestinationTableIdentifier
	logger := log.With(internal.LoggerFromCtx(ctx), slog.String("table", sourceTable))
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)

	state, err := pgMetadata.GetTableReplicationState(ctx, flowName, sourceTable)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, err)
	}
	normResponses.Update(state.NormalizedBatchID)
	if state.SyncedBatchID > state.NormalizedBatchID {
		// Resume any batches synced but not normalized before a prior activity attempt stopped.
		normRequests.Update(state.SyncedBatchID)
	}

	wasLagging := false
	retryInterval := time.Minute
	for {
		reqBatchID := normRequests.Load()
		lastNormalized := normResponses.Load()
		if reqBatchID <= lastNormalized {
			logger.Info("[cdc] waiting for next normalize request", slog.Int64("lastNormalizedBatchID", lastNormalized))
			ch := normRequests.Wait()
			if ch == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
				continue
			}
		}

		dstConn, dstClose, err := connectors.GetByNameAs[connectors.TableCDCSyncConnector](ctx, config.Env,
			a.CatalogPool, config.DestinationName)
		if err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("failed to get destination connector: %w", err))
		}
		logger.Info("[cdc] starting normalize",
			slog.Int64("startBatchID", lastNormalized), slog.Int64("endBatchID", reqBatchID))
		normErr := dstConn.NormalizeTableCDC(ctx, &model.NormalizeTableCDCRequest{
			Env:               config.Env,
			FlowJobName:       flowName,
			TableMapping:      tableMapping,
			TableSchema:       tableNameSchemaMapping[destTable],
			Version:           config.Version,
			Flags:             config.Flags,
			StartBatchID:      lastNormalized,
			EndBatchID:        reqBatchID,
			SoftDeleteColName: config.SoftDeleteColName,
		})
		dstClose(ctx)

		if normErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("[cdc] table normalize failed; will retry", slog.Any("error", normErr))
			if !wasLagging {
				_ = a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf(
					"isolated CDC failed to normalize table %s; replication for other tables will continue: %w",
					sourceTable, normErr))
				wasLagging = true
			}
			if err := waitOrDone(ctx, retryInterval); err != nil {
				return err
			}
			retryInterval = min(retryInterval*2, 5*time.Minute)
			continue
		}
		wasLagging = false
		retryInterval = time.Minute

		if err := pgMetadata.RecordTableReplicationNormalize(ctx, flowName, sourceTable, reqBatchID, time.Now()); err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}
		normResponses.Update(reqBatchID)
		logger.Info("[cdc] normalize done", slog.Int64("normalizedBatchID", reqBatchID))
	}
}

// recordIsolatedTableBatch records one table's completed sync as its own tiny
// CDC batch, reusing the same catalog bookkeeping the shared-stream pipeline
// uses, so the mirror status UI and lag metrics keep working for this path too.
func (a *FlowableActivity) recordIsolatedTableBatch(
	ctx context.Context,
	flowName string,
	destTable string,
	batchIDCounter *atomic.Int64,
	attemptedAt time.Time,
	numSynced int64,
	rowCounts *model.RecordTypeCounts,
	nextCursor string,
) error {
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(internal.LoggerFromCtx(ctx), a.CatalogPool)
	batchID := batchIDCounter.Add(1)

	if err := monitoring.AddCDCBatchForFlow(ctx, a.CatalogPool, flowName, monitoring.CDCBatchInfo{
		BatchID:     batchID,
		RowsInBatch: uint32(numSynced),
		StartTime:   attemptedAt,
	}); err != nil {
		return err
	}
	checkpoint := model.CdcCheckpoint{Text: nextCursor}
	if err := monitoring.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx, a.CatalogPool, flowName, batchID, uint32(numSynced), checkpoint, &attemptedAt, &attemptedAt,
	); err != nil {
		return err
	}
	if err := monitoring.AddCDCBatchTablesForFlow(
		ctx, a.CatalogPool, flowName, batchID, map[string]*model.RecordTypeCounts{destTable: rowCounts}, a.OtelManager,
	); err != nil {
		return err
	}
	if err := monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, flowName, batchID); err != nil {
		return err
	}
	return pgMetadata.FinishBatch(ctx, flowName, batchID, checkpoint)
}
