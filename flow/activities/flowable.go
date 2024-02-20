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
	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/connectors"
	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	NeedsSetupMetadataTables bool
}

type SlotSnapshotSignal struct {
	signal       connpostgres.SlotSignal
	snapshotName string
	connector    connectors.CDCPullConnector
}

type FlowableActivity struct {
	CatalogPool *pgxpool.Pool
	Alerter     *alerting.Alerter
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
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	return srcConn.GetTableSchema(ctx, config)
}

// CreateNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	logger := activity.GetLogger(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetConnectorAs[connectors.NormalizedTablesConnector](ctx, config.PeerConnectionConfig)
	if err != nil {
		if err == connectors.ErrUnsupportedFunctionality {
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
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("setting up normalized tables - %d of %d done",
			numTablesSetup.Load(), totalTables)
	})
	defer shutdown()

	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range config.TableNameSchemaMapping {
		existing, err := conn.SetupNormalizedTable(
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
			logger.Info(fmt.Sprintf("created table %s", tableIdentifier))
		} else {
			logger.Info(fmt.Sprintf("table already exists %s", tableIdentifier))
		}
	}

	err = conn.FinishSetupNormalizedTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit normalized tables tx: %w", err)
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (a *FlowableActivity) StartFlow(ctx context.Context,
	input *protos.StartFlowInput,
) (*model.SyncResponse, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, input.FlowConnectionConfigs.FlowJobName)
	logger := activity.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "starting flow...")
	config := input.FlowConnectionConfigs
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	logger.Info("pulling records...")
	tblNameMapping := make(map[string]model.NameAndExclude, len(input.SyncFlowOptions.TableMappings))
	for _, v := range input.SyncFlowOptions.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = model.NewNameAndExclude(v.DestinationTableIdentifier, v.Exclude)
	}

	srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		jobName := input.FlowConnectionConfigs.FlowJobName
		return fmt.Sprintf("transferring records for job - %s", jobName)
	})
	defer shutdown()

	errGroup, errCtx := errgroup.WithContext(ctx)

	batchSize := input.SyncFlowOptions.BatchSize
	if batchSize <= 0 {
		batchSize = 1_000_000
	}

	lastOffset, err := dstConn.GetLastOffset(ctx, input.FlowConnectionConfigs.FlowJobName)
	if err != nil {
		return nil, err
	}

	// start a goroutine to pull records from the source
	recordBatch := model.NewCDCRecordStream()
	startTime := time.Now()
	flowName := input.FlowConnectionConfigs.FlowJobName
	errGroup.Go(func() error {
		return srcConn.PullRecords(errCtx, a.CatalogPool, &model.PullRecordsRequest{
			FlowJobName:           flowName,
			SrcTableIDNameMapping: input.SrcTableIdNameMapping,
			TableNameMapping:      tblNameMapping,
			LastOffset:            lastOffset,
			MaxBatchSize:          batchSize,
			IdleTimeout: peerdbenv.PeerDBCDCIdleTimeoutSeconds(
				int(input.SyncFlowOptions.IdleTimeoutSeconds),
			),
			TableNameSchemaMapping:      input.TableNameSchemaMapping,
			OverridePublicationName:     input.FlowConnectionConfigs.PublicationName,
			OverrideReplicationSlotName: input.FlowConnectionConfigs.ReplicationSlotName,
			RelationMessageMapping:      input.RelationMessageMapping,
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
			return nil, fmt.Errorf("failed in pull records when: %w", err)
		}
		logger.Info("no records to push")

		err := dstConn.ReplayTableSchemaDeltas(ctx, flowName, recordBatch.SchemaDeltas)
		if err != nil {
			return nil, fmt.Errorf("failed to sync schema: %w", err)
		}

		return &model.SyncResponse{
			CurrentSyncBatchID:     -1,
			TableSchemaDeltas:      recordBatch.SchemaDeltas,
			RelationMessageMapping: input.RelationMessageMapping,
		}, nil
	}

	var syncStartTime time.Time
	var res *model.SyncResponse
	errGroup.Go(func() error {
		syncBatchID, err := dstConn.GetLastSyncBatchID(errCtx, flowName)
		if err != nil && config.Destination.Type != protos.DBType_EVENTHUB {
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
		res, err = dstConn.SyncRecords(errCtx, &model.SyncRecordsRequest{
			SyncBatchID:   syncBatchID,
			Records:       recordBatch,
			FlowJobName:   input.FlowConnectionConfigs.FlowJobName,
			TableMappings: input.SyncFlowOptions.TableMappings,
			StagingPath:   input.FlowConnectionConfigs.CdcStagingPath,
		})
		if err != nil {
			logger.Warn("failed to push records", slog.Any("error", err))
			a.Alerter.LogFlowError(ctx, flowName, err)
			return fmt.Errorf("failed to push records: %w", err)
		}
		res.RelationMessageMapping = input.RelationMessageMapping

		return nil
	})

	err = errGroup.Wait()
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, fmt.Errorf("failed to pull records: %w", err)
	}

	numRecords := res.NumRecordsSynced
	syncDuration := time.Since(syncStartTime)

	logger.Info(fmt.Sprintf("pushed %d records in %d seconds", numRecords, int(syncDuration.Seconds())))

	lastCheckpoint := recordBatch.GetLastCheckpoint()

	err = monitoring.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx,
		a.CatalogPool,
		input.FlowConnectionConfigs.FlowJobName,
		res.CurrentSyncBatchID,
		uint32(numRecords),
		lastCheckpoint,
	)
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}

	err = monitoring.UpdateLatestLSNAtTargetForCDCFlow(
		ctx,
		a.CatalogPool,
		input.FlowConnectionConfigs.FlowJobName,
		lastCheckpoint,
	)
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}
	if res.TableNameRowsMapping != nil {
		err = monitoring.AddCDCBatchTablesForFlow(ctx, a.CatalogPool, input.FlowConnectionConfigs.FlowJobName,
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

func (a *FlowableActivity) StartNormalize(
	ctx context.Context,
	input *protos.StartNormalizeInput,
) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs
	ctx = context.WithValue(ctx, shared.FlowNameKey, conn.FlowJobName)
	logger := activity.GetLogger(ctx)

	dstConn, err := connectors.GetCDCNormalizeConnector(ctx, conn.Destination)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		err = monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, input.FlowConnectionConfigs.FlowJobName,
			input.SyncBatchID)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("normalizing records from batch for job - %s", input.FlowConnectionConfigs.FlowJobName)
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
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get qrep pull connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("getting partitions for job - %s", config.FlowJobName)
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
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	if err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)

	logger.Info(fmt.Sprintf("replicating partitions for batch %d - size: %d",
		partitions.BatchId, numPartitions),
	)
	for i, p := range partitions.Partitions {
		logger.Info(fmt.Sprintf("batch-%d - replicating partition - %s", partitions.BatchId, p.PartitionId))
		err := a.replicateQRepPartition(ctx, config, i+1, numPartitions, p, runUUID)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return err
		}
	}

	return nil
}

// ReplicateQRepPartition replicates a QRepPartition from the source to the destination.
func (a *FlowableActivity) replicateQRepPartition(ctx context.Context,
	config *protos.QRepConfig,
	idx int,
	total int,
	partition *protos.QRepPartition,
	runUUID string,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	err := monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, time.Now())
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to update start time for partition: %w", err)
	}

	pullCtx, pullCancel := context.WithCancel(ctx)
	defer pullCancel()
	srcConn, err := connectors.GetQRepPullConnector(pullCtx, config.SourcePeer)
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

	logger.Info(fmt.Sprintf("replicating partition %s", partition.PartitionId))
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("syncing partition - %s: %d of %d total.", partition.PartitionId, idx, total)
	})
	defer shutdown()

	var stream *model.QRecordStream
	bufferSize := shared.FetchAndChannelSize
	var wg sync.WaitGroup

	var goroutineErr error = nil
	if config.SourcePeer.Type == protos.DBType_POSTGRES {
		stream = model.NewQRecordStream(bufferSize)
		wg.Add(1)

		go func() {
			pgConn := srcConn.(*connpostgres.PostgresConnector)
			tmp, err := pgConn.PullQRepRecordStream(ctx, config, partition, stream)
			numRecords := int64(tmp)
			if err != nil {
				a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
				logger.Error("failed to pull records", slog.Any("error", err))
				goroutineErr = err
			} else {
				err = monitoring.UpdatePullEndTimeAndRowsForPartition(ctx,
					a.CatalogPool, runUUID, partition, numRecords)
				if err != nil {
					logger.Error(err.Error())
					goroutineErr = err
				}
			}
			wg.Done()
		}()
	} else {
		recordBatch, err := srcConn.PullQRepRecords(ctx, config, partition)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to pull qrep records: %w", err)
		}
		logger.Info(fmt.Sprintf("pulled %d records", len(recordBatch.Records)))

		err = monitoring.UpdatePullEndTimeAndRowsForPartition(ctx, a.CatalogPool, runUUID, partition, int64(len(recordBatch.Records)))
		if err != nil {
			return err
		}

		stream, err = recordBatch.ToQRecordStream(bufferSize)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to convert to qrecord stream: %w", err)
		}
	}

	rowsSynced, err := dstConn.SyncQRepRecords(ctx, config, partition, stream)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to sync records: %w", err)
	}

	if rowsSynced == 0 {
		logger.Info(fmt.Sprintf("no records to push for partition %s", partition.PartitionId))
		pullCancel()
	} else {
		wg.Wait()
		if goroutineErr != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, goroutineErr)
			return goroutineErr
		}

		err := monitoring.UpdateRowsSyncedForPartition(ctx, a.CatalogPool, rowsSynced, runUUID, partition)
		if err != nil {
			return err
		}

		logger.Info(fmt.Sprintf("pushed %d records", rowsSynced))
	}

	err = monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
	return err
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string,
) error {
	dstConn, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	} else if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("consolidating partitions for job - %s", config.FlowJobName)
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
	dst, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
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
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	return dstConn.SyncFlowCleanup(ctx, config.FlowJobName)
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

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	logger := activity.GetLogger(ctx)
	if !peerdbenv.PeerDBEnableWALHeartbeat() {
		logger.Info("wal heartbeat is disabled")
		return nil
	}

	pgPeers, err := a.getPostgresPeerConfigs(ctx)
	if err != nil {
		logger.Warn("[sendwalheartbeat] unable to fetch peers. " +
			"Skipping walheartbeat send. Error: " + err.Error())
		return err
	}

	command := `
		BEGIN;
		DROP AGGREGATE IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
		CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
		DROP AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4);
		END;
		`
	// run above command for each Postgres peer
	for _, pgPeer := range pgPeers {
		activity.RecordHeartbeat(ctx, pgPeer.Name)
		if ctx.Err() != nil {
			return nil
		}

		func() {
			pgConfig := pgPeer.GetPostgresConfig()
			peerConn, peerErr := pgx.Connect(ctx, utils.GetPGConnectionString(pgConfig))
			if peerErr != nil {
				logger.Error(fmt.Sprintf("error creating pool for postgres peer %v with host %v: %v",
					pgPeer.Name, pgConfig.Host, peerErr))
				return
			}
			defer peerConn.Close(ctx)

			_, err := peerConn.Exec(ctx, command)
			if err != nil {
				logger.Warn(fmt.Sprintf("could not send walheartbeat to peer %v: %v", pgPeer.Name, err))
			}

			logger.Info(fmt.Sprintf("sent walheartbeat to peer %v", pgPeer.Name))
		}()
	}

	return nil
}

func (a *FlowableActivity) RecordSlotSizes(ctx context.Context) error {
	rows, err := a.CatalogPool.Query(ctx, "SELECT flows.name, flows.config_proto FROM flows")
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
				if err != connectors.ErrUnsupportedFunctionality {
					logger.Error("Failed to create connector to handle slot info", slog.Any("error", err))
				}
				return
			}
			defer connectors.CloseConnector(ctx, srcConn)

			slotName := fmt.Sprintf("peerflow_slot_%s", config.FlowJobName)
			if config.ReplicationSlotName != "" {
				slotName = config.ReplicationSlotName
			}
			peerName := config.Source.Name

			activity.RecordHeartbeat(ctx, fmt.Sprintf("checking %s on %s", slotName, peerName))
			if ctx.Err() != nil {
				return
			}
			err = srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, slotName, peerName)
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

func (a *FlowableActivity) QRepWaitUntilNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	if config.SourcePeer.Type != protos.DBType_POSTGRES || last.Range == nil {
		return nil
	}
	waitBetweenBatches := 5 * time.Second
	if config.WaitBetweenBatchesSeconds > 0 {
		waitBetweenBatches = time.Duration(config.WaitBetweenBatchesSeconds) * time.Second
	}

	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)
	pgSrcConn := srcConn.(*connpostgres.PostgresConnector)
	logger.Info(fmt.Sprintf("current last partition value is %v", last))
	attemptCount := 1
	for {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("no new rows yet, attempt #%d", attemptCount))
		waitUntil := time.Now().Add(waitBetweenBatches)
		for {
			sleep := time.Until(waitUntil)
			if sleep > 15*time.Second {
				sleep = 15 * time.Second
			}
			time.Sleep(sleep)

			activity.RecordHeartbeat(ctx, "heartbeat while waiting before next batch")
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("cancelled while waiting for new rows: %w", err)
			} else if time.Now().After(waitUntil) {
				break
			}
		}

		result, err := pgSrcConn.CheckForUpdatedMaxValue(ctx, config, last)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to check for new rows: %w", err)
		}
		if result {
			break
		}

		attemptCount += 1
	}

	return nil
}

func (a *FlowableActivity) RenameTables(ctx context.Context, config *protos.RenameTablesInput) (
	*protos.RenameTablesOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("renaming tables for job - %s", config.FlowJobName)
	})
	defer shutdown()

	if config.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return nil, fmt.Errorf("failed to cast connector to snowflake connector")
		}
		return sfConn.RenameTables(ctx, config)
	} else if config.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return nil, fmt.Errorf("failed to cast connector to bigquery connector")
		}
		return bqConn.RenameTables(ctx, config)
	}
	return nil, fmt.Errorf("rename tables is only supported on snowflake and bigquery")
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
			return nil, fmt.Errorf("failed to cast connector to snowflake connector")
		}
		return sfConn.CreateTablesFromExisting(ctx, req)
	} else if req.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			return nil, fmt.Errorf("failed to cast connector to bigquery connector")
		}
		return bqConn.CreateTablesFromExisting(ctx, req)
	}
	a.Alerter.LogFlowError(ctx, req.FlowJobName, err)
	return nil, fmt.Errorf("create tables from existing is only supported on snowflake and bigquery")
}

// ReplicateXminPartition replicates a XminPartition from the source to the destination.
func (a *FlowableActivity) ReplicateXminPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
) (int64, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	startTime := time.Now()
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	logger.Info("replicating xmin")

	bufferSize := shared.FetchAndChannelSize
	errGroup, errCtx := errgroup.WithContext(ctx)

	stream := model.NewQRecordStream(bufferSize)

	var currentSnapshotXmin int64
	errGroup.Go(func() error {
		pgConn := srcConn.(*connpostgres.PostgresConnector)
		var pullErr error
		var numRecords int
		numRecords, currentSnapshotXmin, pullErr = pgConn.PullXminRecordStream(ctx, config, partition, stream)
		if pullErr != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			logger.Warn(fmt.Sprintf("[xmin] failed to pull records: %v", err))
			return err
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
						IntRange: &protos.IntPartitionRange{Start: 0, End: int64(numRecords)},
					},
				},
			}
		}
		updateErr := monitoring.InitializeQRepRun(
			ctx, a.CatalogPool, config, runUUID, []*protos.QRepPartition{partitionForMetrics})
		if updateErr != nil {
			return updateErr
		}

		err := monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, startTime)
		if err != nil {
			return fmt.Errorf("failed to update start time for partition: %w", err)
		}

		err = monitoring.UpdatePullEndTimeAndRowsForPartition(
			errCtx, a.CatalogPool, runUUID, partition, int64(numRecords))
		if err != nil {
			logger.Error(err.Error())
			return err
		}

		return nil
	})

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "syncing xmin."
	})
	defer shutdown()

	rowsSynced, err := dstConn.SyncQRepRecords(ctx, config, partition, stream)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return 0, fmt.Errorf("failed to sync records: %w", err)
	}

	if rowsSynced == 0 {
		logger.Info("no records to push for xmin")
	} else {
		err := errGroup.Wait()
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return 0, err
		}

		err = monitoring.UpdateRowsSyncedForPartition(ctx, a.CatalogPool, rowsSynced, runUUID, partition)
		if err != nil {
			return 0, err
		}

		logger.Info(fmt.Sprintf("pushed %d records", rowsSynced))
	}

	err = monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
	if err != nil {
		return 0, err
	}

	return currentSnapshotXmin, nil
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
