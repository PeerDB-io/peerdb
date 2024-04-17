// internal methods for flowable.go
package activities

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

func waitForCdcCache[TPull connectors.CDCPullConnectorCore](ctx context.Context, a *FlowableActivity, sessionID string) (TPull, error) {
	var none TPull
	logger := activity.GetLogger(ctx)
	attempt := 0
	for {
		a.CdcCacheRw.RLock()
		entry, ok := a.CdcCache[sessionID]
		a.CdcCacheRw.RUnlock()
		if ok {
			if conn, ok := entry.connector.(TPull); ok {
				return conn, nil
			}
			return none, fmt.Errorf("expected %s, cache held %T", reflect.TypeFor[TPull]().Name(), entry.connector)
		}
		activity.RecordHeartbeat(ctx, "wait another second for source connector")
		attempt += 1
		if attempt > 2 {
			logger.Info("waiting on source connector setup", slog.Int("attempt", attempt))
		}
		if err := ctx.Err(); err != nil {
			return none, err
		}
		time.Sleep(time.Second)
	}
}

func syncCore[TPull connectors.CDCPullConnectorCore, TSync connectors.CDCSyncConnectorCore, Items model.Items](
	ctx context.Context,
	a *FlowableActivity,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	sessionID string,
	pull func(TPull, context.Context, *pgxpool.Pool, *model.PullRecordsRequest[Items]) error,
	sync func(TSync, context.Context, *model.SyncRecordsRequest[Items]) (*model.SyncResponse, error),
) (*model.SyncResponse, error) {
	flowName := config.FlowJobName
	ctx = context.WithValue(ctx, shared.FlowNameKey, flowName)
	logger := activity.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "starting flow...")
	dstConn, err := connectors.GetAs[TSync](ctx, config.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	tblNameMapping := make(map[string]model.NameAndExclude, len(options.TableMappings))
	for _, v := range options.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = model.NewNameAndExclude(v.DestinationTableIdentifier, v.Exclude)
	}

	var srcConn TPull
	if sessionID == "" {
		srcConn, err = connectors.GetAs[TPull](ctx, config.Source)
		if err != nil {
			return nil, err
		}
		defer connectors.CloseConnector(ctx, srcConn)

		if err := srcConn.SetupReplConn(ctx); err != nil {
			return nil, err
		}
	} else {
		srcConn, err = waitForCdcCache[TPull](ctx, a, sessionID)
		if err != nil {
			return nil, err
		}
		if err := srcConn.ConnectionActive(ctx); err != nil {
			return nil, temporal.NewNonRetryableApplicationError("connection to source down", "disconnect", nil)
		}
	}

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "transferring records for job"
	})
	defer shutdown()

	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = 1_000_000
	}

	lastOffset, err := dstConn.GetLastOffset(ctx, config.FlowJobName)
	if err != nil {
		return nil, err
	}
	logger.Info("pulling records...", slog.Int64("LastOffset", lastOffset))
	consumedOffset := atomic.Int64{}
	consumedOffset.Store(lastOffset)

	recordBatch := model.NewCDCStream[Items]()
	startTime := time.Now()

	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return pull(srcConn, errCtx, a.CatalogPool, &model.PullRecordsRequest[Items]{
			FlowJobName:           flowName,
			SrcTableIDNameMapping: options.SrcTableIdNameMapping,
			TableNameMapping:      tblNameMapping,
			LastOffset:            lastOffset,
			ConsumedOffset:        &consumedOffset,
			MaxBatchSize:          batchSize,
			IdleTimeout: peerdbenv.PeerDBCDCIdleTimeoutSeconds(
				int(options.IdleTimeoutSeconds),
			),
			TableNameSchemaMapping:      options.TableNameSchemaMapping,
			OverridePublicationName:     config.PublicationName,
			OverrideReplicationSlotName: config.ReplicationSlotName,
			RecordStream:                recordBatch,
		})
	})

	hasRecords := !recordBatch.WaitAndCheckEmpty()
	logger.Info("current sync flow has records?", slog.Bool("hasRecords", hasRecords))

	if !hasRecords {
		// wait for the pull goroutine to finish
		err = errGroup.Wait()
		if err != nil {
			a.Alerter.LogFlowError(ctx, flowName, err)
			if temporal.IsApplicationError(err) {
				return nil, err
			} else {
				return nil, fmt.Errorf("failed in pull records when: %w", err)
			}
		}
		logger.Info("no records to push")

		err := dstConn.ReplayTableSchemaDeltas(ctx, flowName, recordBatch.SchemaDeltas)
		if err != nil {
			return nil, fmt.Errorf("failed to sync schema: %w", err)
		}

		return &model.SyncResponse{
			CurrentSyncBatchID: -1,
			TableSchemaDeltas:  recordBatch.SchemaDeltas,
		}, nil
	}

	var syncStartTime time.Time
	var res *model.SyncResponse
	errGroup.Go(func() error {
		syncBatchID, err := dstConn.GetLastSyncBatchID(errCtx, flowName)
		if err != nil && config.Destination.Type != protos.DBType_EVENTHUBS {
			return err
		}
		syncBatchID += 1

		err = monitoring.AddCDCBatchForFlow(errCtx, a.CatalogPool, flowName,
			monitoring.CDCBatchInfo{
				BatchID:     syncBatchID,
				RowsInBatch: 0,
				BatchEndlSN: 0,
				StartTime:   startTime,
			})
		if err != nil {
			a.Alerter.LogFlowError(ctx, flowName, err)
			return err
		}

		syncStartTime = time.Now()
		res, err = sync(dstConn, errCtx, &model.SyncRecordsRequest[Items]{
			SyncBatchID:            syncBatchID,
			Records:                recordBatch,
			ConsumedOffset:         &consumedOffset,
			FlowJobName:            flowName,
			TableMappings:          options.TableMappings,
			StagingPath:            config.CdcStagingPath,
			Script:                 config.Script,
			TableNameSchemaMapping: options.TableNameSchemaMapping,
		})
		if err != nil {
			a.Alerter.LogFlowError(ctx, flowName, err)
			return fmt.Errorf("failed to push records: %w", err)
		}

		return nil
	})

	err = errGroup.Wait()
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		if temporal.IsApplicationError(err) {
			return nil, err
		} else {
			return nil, fmt.Errorf("failed to pull records: %w", err)
		}
	}

	numRecords := res.NumRecordsSynced
	syncDuration := time.Since(syncStartTime)

	logger.Info(fmt.Sprintf("pushed %d records in %d seconds", numRecords, int(syncDuration.Seconds())))

	lastCheckpoint := recordBatch.GetLastCheckpoint()
	srcConn.UpdateReplStateLastOffset(lastCheckpoint)

	err = monitoring.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx,
		a.CatalogPool,
		flowName,
		res.CurrentSyncBatchID,
		uint32(numRecords),
		lastCheckpoint,
	)
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}

	err = monitoring.UpdateLatestLSNAtTargetForCDCFlow(ctx, a.CatalogPool, flowName, lastCheckpoint)
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}
	if res.TableNameRowsMapping != nil {
		err = monitoring.AddCDCBatchTablesForFlow(ctx, a.CatalogPool, flowName,
			res.CurrentSyncBatchID, res.TableNameRowsMapping)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}

	pushedRecordsWithCount := fmt.Sprintf("pushed %d records", numRecords)
	activity.RecordHeartbeat(ctx, pushedRecordsWithCount)
	a.Alerter.LogFlowInfo(ctx, flowName, pushedRecordsWithCount)

	return res, nil
}

func (a *FlowableActivity) getPostgresPeerConfigs(ctx context.Context) ([]*protos.Peer, error) {
	optionRows, err := a.CatalogPool.Query(ctx, `
			SELECT DISTINCT p.name, p.options
			FROM peers p
			JOIN flows f ON p.id = f.source_peer
			WHERE p.type = $1`, protos.DBType_POSTGRES)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(optionRows, func(row pgx.CollectableRow) (*protos.Peer, error) {
		var peerName string
		var peerOptions []byte
		err := optionRows.Scan(&peerName, &peerOptions)
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
func (a *FlowableActivity) replicateQRepPartition(ctx context.Context,
	config *protos.QRepConfig,
	idx int,
	total int,
	partition *protos.QRepPartition,
	runUUID string,
) error {
	msg := fmt.Sprintf("replicating partition - %s: %d of %d total.", partition.PartitionId, idx, total)
	activity.RecordHeartbeat(ctx, msg)

	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(activity.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	done, err := dstConn.IsQRepPartitionSynced(ctx, &protos.IsQRepPartitionSyncedInput{
		FlowJobName: config.FlowJobName,
		PartitionId: partition.PartitionId,
	})
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to get fetch status of partition: %w", err)
	}
	if done {
		logger.Info("no records to push for partition " + partition.PartitionId)
		activity.RecordHeartbeat(ctx, "no records to push for partition "+partition.PartitionId)
		return nil
	}

	err = monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, time.Now())
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to update start time for partition: %w", err)
	}

	logger.Info("replicating partition " + partition.PartitionId)
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("syncing partition - %s: %d of %d total.", partition.PartitionId, idx, total)
	})
	defer shutdown()

	bufferSize := shared.FetchAndChannelSize
	stream := model.NewQRecordStream(bufferSize)
	outstream := stream
	if config.Script != "" {
		ls, err := utils.LoadScript(ctx, config.Script, func(ls *lua.LState) int {
			top := ls.GetTop()
			ss := make([]string, top)
			for i := range top {
				ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
			}
			a.Alerter.LogFlowInfo(ctx, config.FlowJobName, strings.Join(ss, "\t"))
			return 0
		})
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return err
		}
		lfn := ls.Env.RawGetString("transformRow")
		fn, ok := lfn.(*lua.LFunction)
		if !ok {
			err := fmt.Errorf("script should define `transformRow` as function, not %s", lfn)
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return err
		}
		outstream = pua.AttachToStream(ls, fn, stream)
	}

	var rowsSynced int
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		tmp, err := srcConn.PullQRepRecords(errCtx, config, partition, stream)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to pull records: %w", err)
		}
		numRecords := int64(tmp)
		err = monitoring.UpdatePullEndTimeAndRowsForPartition(errCtx,
			a.CatalogPool, runUUID, partition, numRecords)
		if err != nil {
			logger.Error(err.Error())
		}
		return nil
	})

	errGroup.Go(func() error {
		rowsSynced, err = dstConn.SyncQRepRecords(errCtx, config, partition, outstream)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to sync records: %w", err)
		}
		return context.Canceled
	})

	if err := errGroup.Wait(); err != nil && err != context.Canceled {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return err
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
