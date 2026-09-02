package activities

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
)

// syncFlowIsolatedTables replaces pullAndSync/normalizeLoop for the isolated
// per-table CDC path: each source table gets its own sync loop (pull +
// stage-to-S3) and its own normalize loop (staged batches -> final table),
// coupled only by that table's own synced/normalized batch counters. A
// lagging or failing table never blocks its siblings, and a slow normalize
// backpressures only that table's own sync loop. There is no shared
// batch/backpressure state across tables at all, beyond the two semaphores
// bounding how many tables may pull+sync and normalize concurrently.
func (a *FlowableActivity) syncFlowIsolatedTables(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	options *protos.SyncFlowOptions,
	srcConn connectors.TableCDCPullConnector,
	shutDown *atomic.Bool,
) error {
	flowName := config.FlowJobName
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata := connmetadata.NewPostgresMetadataFromCatalog(logger, a.CatalogPool)

	var totalRecordsSynced atomic.Int64
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("isolated CDC: %d tables, totalRecordsSynced:%d",
			len(options.TableMappings), totalRecordsSynced.Load())
	})
	defer shutdown()

	if err := srcConn.ConnectionActive(ctx); err != nil {
		return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("connection to source down: %w", err))
	}

	idleTimeout := cdcIdleTimeout(int(options.IdleTimeoutSeconds))
	channelBufferSize, err := internal.PeerDBCDCChannelBufferSize(ctx, config.Env)
	if err != nil {
		return fmt.Errorf("failed to get CDC channel buffer size: %w", err)
	}

	pullSyncParallelism := int(config.GetQueryCdcTablesParallelism())
	if pullSyncParallelism <= 0 {
		pullSyncParallelism, err = internal.PeerDBCDCTablePullSyncParallelism(ctx, config.Env)
		if err != nil {
			return fmt.Errorf("failed to get CDC table pull-sync parallelism: %w", err)
		}
	}
	// Bounds concurrent pull+sync work only; normalize is bounded separately by
	// normSem below, so a stalled destination can't be starved by pull work and
	// vice versa.
	var pullSyncSem chan struct{}
	if pullSyncParallelism > 0 {
		pullSyncSem = make(chan struct{}, pullSyncParallelism)
	}

	normParallelism, err := internal.PeerDBCDCTableNormalizeParallelism(ctx, config.Env)
	if err != nil {
		return fmt.Errorf("failed to get CDC table normalize parallelism: %w", err)
	}
	// Bounds concurrent normalize work to ensure we don't overload the destination
	// Especially when normalize needs to catch up after a downtime
	var normSem chan struct{}
	if normParallelism > 0 {
		normSem = make(chan struct{}, normParallelism)
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

	group, groupCtx := errgroup.WithContext(ctx)
	for _, tableMapping := range options.TableMappings {
		normRequests := concurrency.NewLastChan()
		normResponses := concurrency.NewLastChan()

		group.Go(func() error {
			return a.isolatedTableNormalizeLoop(groupCtx, config, tableMapping, tableNameSchemaMapping,
				normSem, normRequests, normResponses)
		})
		group.Go(func() error {
			return a.isolatedTablePullSyncLoop(groupCtx, config, srcConn, pgMetadata, tableMapping, tableNameSchemaMapping,
				channelBufferSize, idleTimeout, normBufferSize, pullSyncSem, &totalRecordsSynced, normRequests, normResponses)
		})
	}

	if err := group.Wait(); err != nil {
		// mirrors syncFlowSharedStream: a worker shutdown cancels ctx ourselves, so the
		// resulting context.Canceled is expected teardown rather than an activity failure
		if shutDown.Load() {
			logger.Info("isolated CDC SyncFlow shutdown")
			return nil
		}
		// errgroup cancels groupCtx, not ctx, on a table error, so ctx being done here
		// means an outside cancel rather than a replication failure
		if ctx.Err() != nil {
			logger.Info("isolated CDC SyncFlow canceled", slog.Any("error", ctx.Err()))
			return ctx.Err()
		}
		logger.Error("isolated CDC SyncFlow failed", slog.Any("error", err))
		return err
	}
	return nil
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
func acquire(ctx context.Context, sem chan struct{}, logger log.Logger, name string) (func(), error) {
	if sem == nil {
		return func() {}, nil
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	default:
	}

	logger.Info("[cdc] waiting for a slot", slog.String("semaphore", name), slog.Int("parallelism", cap(sem)))
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
	pullSyncSem chan struct{},
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
		release, err := acquire(ctx, pullSyncSem, logger, "pull-sync")
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
			a.recordSyncMetrics(ctx, destTable, rowCounts, pullResult.BytesProcessed)
			normRequests.Update(newBatchID)
		}
	}
	return ctx.Err()
}

func (a *FlowableActivity) recordSyncMetrics(ctx context.Context, destTable string, rowCounts *model.RecordTypeCounts, bytesProcessed int64) {
	opAndCount := []struct {
		op    string
		count int64
	}{
		{count: int64(rowCounts.InsertCount.Load()), op: "insert"},
		{count: int64(rowCounts.UpdateCount.Load()), op: "update"},
		{count: int64(rowCounts.DeleteCount.Load()), op: "delete"},
	}
	for _, oc := range opAndCount {
		a.OtelManager.Metrics.RecordsSyncedPerTableCounter.Add(ctx, oc.count, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.DestinationTableNameKey, destTable),
			attribute.String(otel_metrics.RecordOperationTypeKey, oc.op),
		)))
		a.OtelManager.Metrics.RecordsSyncedPerTableGauge.Record(ctx, oc.count, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.DestinationTableNameKey, destTable),
			attribute.String(otel_metrics.RecordOperationTypeKey, oc.op),
		)))
	}

	a.OtelManager.Metrics.FetchedBytesCounter.Add(ctx, bytesProcessed)
	a.OtelManager.Metrics.AllFetchedBytesCounter.Add(ctx, bytesProcessed)
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
	normSem chan struct{},
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

		// bounded parallelism: only normalize into the destination for up to
		// normParallelism tables at once.
		release, err := acquire(ctx, normSem, logger, "normalize")
		if err != nil {
			return err
		}

		dstConn, dstClose, err := connectors.GetByNameAs[connectors.TableCDCSyncConnector](ctx, config.Env,
			a.CatalogPool, config.DestinationName)
		if err != nil {
			release()
			return a.Alerter.LogFlowError(ctx, flowName, fmt.Errorf("failed to get destination connector: %w", err))
		}
		logger.Info("[cdc] starting normalize",
			slog.Int64("startBatchID", lastNormalized), slog.Int64("endBatchID", reqBatchID))
		normCounts, normErr := dstConn.NormalizeTableCDC(ctx, &model.NormalizeTableCDCRequest{
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
		release()

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

		if err := pgMetadata.RecordTableReplicationNormalize(ctx, flowName, sourceTable, reqBatchID, normCounts, time.Now()); err != nil {
			return a.Alerter.LogFlowError(ctx, flowName, err)
		}
		normResponses.Update(reqBatchID)
		logger.Info("[cdc] normalize done", slog.Int64("normalizedBatchID", reqBatchID))
	}
}
