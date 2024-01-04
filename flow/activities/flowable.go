package activities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	// True of metadata tables need to be set up.
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

// CheckConnection implements CheckConnection.
func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.SetupInput,
) (*CheckConnectionResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	needsSetup := dstConn.NeedsSetupMetadataTables()

	return &CheckConnectionResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

// SetupMetadataTables implements SetupMetadataTables.
func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.SetupInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	if err := dstConn.SetupMetadataTables(); err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetLastSyncedID implements GetLastSyncedID.
func (a *FlowableActivity) GetLastSyncedID(
	ctx context.Context,
	config *protos.GetLastSyncedIDInput,
) (*protos.LastSyncState, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	var lastOffset int64
	lastOffset, err = dstConn.GetLastOffset(config.FlowJobName)
	if err != nil {
		return nil, err
	}
	return &protos.LastSyncState{Checkpoint: lastOffset}, nil
}

// EnsurePullability implements EnsurePullability.
func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	output, err := srcConn.EnsurePullability(config)
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
	defer connectors.CloseConnector(dstConn)

	res, err := dstConn.CreateRawTable(config)
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
	defer connectors.CloseConnector(srcConn)

	return srcConn.GetTableSchema(config)
}

// CreateNormalizedTable creates a normalized table in the destination flowable.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	setupNormalizedTablesOutput, err := conn.SetupNormalizedTables(config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return nil, fmt.Errorf("failed to setup normalized tables: %w", err)
	}

	return setupNormalizedTablesOutput, nil
}

func (a *FlowableActivity) StartFlow(ctx context.Context,
	input *protos.StartFlowInput,
) (*model.SyncResponse, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, input.FlowConnectionConfigs.FlowJobName)
	activity.RecordHeartbeat(ctx, "starting flow...")
	conn := input.FlowConnectionConfigs
	dstConn, err := connectors.GetCDCSyncConnector(ctx, conn.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	slog.InfoContext(ctx, "initializing table schema...")
	err = dstConn.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}
	activity.RecordHeartbeat(ctx, "initialized table schema")
	slog.InfoContext(ctx, "pulling records...")
	tblNameMapping := make(map[string]model.NameAndExclude)
	for _, v := range input.FlowConnectionConfigs.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = model.NewNameAndExclude(v.DestinationTableIdentifier, v.Exclude)
	}

	errGroup, errCtx := errgroup.WithContext(ctx)
	srcConn, err := connectors.GetCDCPullConnector(errCtx, conn.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	slotNameForMetrics := fmt.Sprintf("peerflow_slot_%s", input.FlowConnectionConfigs.FlowJobName)
	if input.FlowConnectionConfigs.ReplicationSlotName != "" {
		slotNameForMetrics = input.FlowConnectionConfigs.ReplicationSlotName
	}

	go a.recordSlotSizePeriodically(errCtx, srcConn, slotNameForMetrics, input.FlowConnectionConfigs.Source.Name)

	shutdown := utils.HeartbeatRoutine(ctx, 10*time.Second, func() string {
		jobName := input.FlowConnectionConfigs.FlowJobName
		return fmt.Sprintf("transferring records for job - %s", jobName)
	})
	defer shutdown()

	// start a goroutine to pull records from the source
	recordBatch := model.NewCDCRecordStream()
	startTime := time.Now()
	flowName := input.FlowConnectionConfigs.FlowJobName
	errGroup.Go(func() error {
		return srcConn.PullRecords(a.CatalogPool, &model.PullRecordsRequest{
			FlowJobName:           flowName,
			SrcTableIDNameMapping: input.FlowConnectionConfigs.SrcTableIdNameMapping,
			TableNameMapping:      tblNameMapping,
			LastOffset:            input.LastSyncState.Checkpoint,
			MaxBatchSize:          uint32(input.SyncFlowOptions.BatchSize),
			IdleTimeout: peerdbenv.PeerDBCDCIdleTimeoutSeconds(
				int(input.FlowConnectionConfigs.IdleTimeoutSeconds),
			),
			TableNameSchemaMapping:      input.FlowConnectionConfigs.TableNameSchemaMapping,
			OverridePublicationName:     input.FlowConnectionConfigs.PublicationName,
			OverrideReplicationSlotName: input.FlowConnectionConfigs.ReplicationSlotName,
			RelationMessageMapping:      input.RelationMessageMapping,
			RecordStream:                recordBatch,
			SetLastOffset: func(lastOffset int64) error {
				return dstConn.SetLastOffset(flowName, lastOffset)
			},
		})
	})

	hasRecords := !recordBatch.WaitAndCheckEmpty()
	slog.InfoContext(ctx, fmt.Sprintf("the current sync flow has records: %v", hasRecords))
	if a.CatalogPool != nil && hasRecords {
		syncBatchID, err := dstConn.GetLastSyncBatchID(flowName)
		if err != nil && conn.Destination.Type != protos.DBType_EVENTHUB {
			return nil, err
		}

		err = monitoring.AddCDCBatchForFlow(ctx, a.CatalogPool, flowName,
			monitoring.CDCBatchInfo{
				BatchID:     syncBatchID + 1,
				RowsInBatch: 0,
				BatchEndlSN: 0,
				StartTime:   startTime,
			})
		if err != nil {
			a.Alerter.LogFlowError(ctx, flowName, err)
			return nil, err
		}
	}

	if !hasRecords {
		// wait for the pull goroutine to finish
		err = errGroup.Wait()
		if err != nil {
			a.Alerter.LogFlowError(ctx, flowName, err)
			return nil, fmt.Errorf("failed in pull records when: %w", err)
		}
		slog.InfoContext(ctx, "no records to push")
		syncResponse := &model.SyncResponse{}
		syncResponse.RelationMessageMapping = <-recordBatch.RelationMessageMapping
		syncResponse.TableSchemaDeltas = recordBatch.WaitForSchemaDeltas(input.FlowConnectionConfigs.TableMappings)
		return syncResponse, nil
	}

	syncStartTime := time.Now()
	res, err := dstConn.SyncRecords(&model.SyncRecordsRequest{
		Records:         recordBatch,
		FlowJobName:     input.FlowConnectionConfigs.FlowJobName,
		StagingPath:     input.FlowConnectionConfigs.CdcStagingPath,
		PushBatchSize:   input.FlowConnectionConfigs.PushBatchSize,
		PushParallelism: input.FlowConnectionConfigs.PushParallelism,
	})
	if err != nil {
		slog.Warn("failed to push records", slog.Any("error", err))
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, fmt.Errorf("failed to push records: %w", err)
	}

	err = errGroup.Wait()
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, fmt.Errorf("failed to pull records: %w", err)
	}

	numRecords := res.NumRecordsSynced
	syncDuration := time.Since(syncStartTime)

	slog.InfoContext(ctx, fmt.Sprintf("pushed %d records in %d seconds\n",
		numRecords, int(syncDuration.Seconds())),
	)

	lastCheckpoint, err := recordBatch.GetLastCheckpoint()
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = monitoring.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx,
		a.CatalogPool,
		input.FlowConnectionConfigs.FlowJobName,
		res.CurrentSyncBatchID,
		uint32(numRecords),
		pglogrepl.LSN(lastCheckpoint),
	)
	if err != nil {
		a.Alerter.LogFlowError(ctx, flowName, err)
		return nil, err
	}

	err = monitoring.UpdateLatestLSNAtTargetForCDCFlow(
		ctx,
		a.CatalogPool,
		input.FlowConnectionConfigs.FlowJobName,
		pglogrepl.LSN(lastCheckpoint),
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
	res.TableSchemaDeltas = recordBatch.WaitForSchemaDeltas(input.FlowConnectionConfigs.TableMappings)
	res.RelationMessageMapping = <-recordBatch.RelationMessageMapping

	pushedRecordsWithCount := fmt.Sprintf("pushed %d records", numRecords)
	activity.RecordHeartbeat(ctx, pushedRecordsWithCount)

	return res, nil
}

func (a *FlowableActivity) StartNormalize(
	ctx context.Context,
	input *protos.StartNormalizeInput,
) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs
	ctx = context.WithValue(ctx, shared.FlowNameKey, conn.FlowJobName)
	dstConn, err := connectors.GetCDCNormalizeConnector(ctx, conn.Destination)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		dstConn, err := connectors.GetCDCSyncConnector(ctx, conn.Destination)
		if err != nil {
			return nil, fmt.Errorf("failed to get connector: %v", err)
		}
		defer connectors.CloseConnector(dstConn)

		lastSyncBatchID, err := dstConn.GetLastSyncBatchID(input.FlowConnectionConfigs.FlowJobName)
		if err != nil {
			return nil, fmt.Errorf("failed to get last sync batch ID: %v", err)
		}

		err = monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, input.FlowConnectionConfigs.FlowJobName,
			lastSyncBatchID)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("normalizing records from batch for job - %s", input.FlowConnectionConfigs.FlowJobName)
	})
	defer shutdown()

	slog.InfoContext(ctx, "initializing table schema...")
	err = dstConn.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	res, err := dstConn.NormalizeRecords(&model.NormalizeRecordsRequest{
		FlowJobName:       input.FlowConnectionConfigs.FlowJobName,
		SoftDelete:        input.FlowConnectionConfigs.SoftDelete,
		SoftDeleteColName: input.FlowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:   input.FlowConnectionConfigs.SyncedAtColName,
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
	if res != nil {
		slog.InfoContext(ctx, fmt.Sprintf("normalized records from batch %d to batch %d\n",
			res.StartBatchID, res.EndBatchID))
	}

	return res, nil
}

func (a *FlowableActivity) ReplayTableSchemaDeltas(
	ctx context.Context,
	input *protos.ReplayTableSchemaDeltaInput,
) error {
	dest, err := connectors.GetCDCNormalizeConnector(ctx, input.FlowConnectionConfigs.Destination)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return nil
	} else if err != nil {
		return err
	}
	defer connectors.CloseConnector(dest)

	err = dest.ReplayTableSchemaDeltas(input.FlowConnectionConfigs.FlowJobName, input.TableSchemaDeltas)
	if err != nil {
		a.Alerter.LogFlowError(ctx, input.FlowConnectionConfigs.FlowJobName, err)
		return fmt.Errorf("failed to replay table schema deltas: %w", err)
	}

	return nil
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	err = conn.SetupQRepMetadataTables(config)
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
	defer connectors.CloseConnector(srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("getting partitions for job - %s", config.FlowJobName)
	})
	defer shutdown()

	partitions, err := srcConn.GetQRepPartitions(config, last)
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
	err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	if err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)

	slog.InfoContext(ctx, fmt.Sprintf("replicating partitions for batch %d - size: %d\n",
		partitions.BatchId, numPartitions),
	)
	for i, p := range partitions.Partitions {
		slog.InfoContext(ctx, fmt.Sprintf("batch-%d - replicating partition - %s\n", partitions.BatchId, p.PartitionId))
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
	defer connectors.CloseConnector(srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	slog.InfoContext(ctx, fmt.Sprintf("replicating partition %s\n", partition.PartitionId))

	var stream *model.QRecordStream
	bufferSize := shared.FetchAndChannelSize
	var wg sync.WaitGroup

	var goroutineErr error = nil
	if config.SourcePeer.Type == protos.DBType_POSTGRES {
		stream = model.NewQRecordStream(bufferSize)
		wg.Add(1)

		go func() {
			pgConn := srcConn.(*connpostgres.PostgresConnector)
			tmp, err := pgConn.PullQRepRecordStream(config, partition, stream)
			numRecords := int64(tmp)
			if err != nil {
				a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
				slog.ErrorContext(ctx, "failed to pull records", slog.Any("error", err))
				goroutineErr = err
			} else {
				err = monitoring.UpdatePullEndTimeAndRowsForPartition(ctx,
					a.CatalogPool, runUUID, partition, numRecords)
				if err != nil {
					slog.ErrorContext(ctx, fmt.Sprintf("%v", err))
					goroutineErr = err
				}
			}
			wg.Done()
		}()
	} else {
		recordBatch, err := srcConn.PullQRepRecords(config, partition)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to pull qrep records: %w", err)
		}
		numRecords := int64(recordBatch.NumRecords)
		slog.InfoContext(ctx, fmt.Sprintf("pulled %d records\n", len(recordBatch.Records)))

		err = monitoring.UpdatePullEndTimeAndRowsForPartition(ctx, a.CatalogPool, runUUID, partition, numRecords)
		if err != nil {
			return err
		}

		stream, err = recordBatch.ToQRecordStream(bufferSize)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return fmt.Errorf("failed to convert to qrecord stream: %w", err)
		}
	}

	shutdown := utils.HeartbeatRoutine(ctx, 5*time.Minute, func() string {
		return fmt.Sprintf("syncing partition - %s: %d of %d total.", partition.PartitionId, idx, total)
	})
	defer shutdown()

	rowsSynced, err := dstConn.SyncQRepRecords(config, partition, stream)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to sync records: %w", err)
	}

	if rowsSynced == 0 {
		slog.InfoContext(ctx, fmt.Sprintf("no records to push for partition %s\n", partition.PartitionId))
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

		slog.InfoContext(ctx, fmt.Sprintf("pushed %d records\n", rowsSynced))
	}

	err = monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
	if err != nil {
		return err
	}

	return nil
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

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("consolidating partitions for job - %s", config.FlowJobName)
	})
	defer shutdown()

	err = dstConn.ConsolidateQRepPartitions(config)
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

	return dst.CleanupQRepFlow(config)
}

func (a *FlowableActivity) DropFlow(ctx context.Context, config *protos.ShutdownRequest) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	err = srcConn.PullFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup source: %w", err)
	}
	err = dstConn.SyncFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup destination: %w", err)
	}
	return nil
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
	defer optionRows.Close()
	var peerName pgtype.Text
	var postgresPeers []*protos.Peer
	var peerOptions sql.RawBytes
	for optionRows.Next() {
		err := optionRows.Scan(&peerName, &peerOptions)
		if err != nil {
			return nil, err
		}
		var pgPeerConfig protos.PostgresConfig
		unmarshalErr := proto.Unmarshal(peerOptions, &pgPeerConfig)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}
		postgresPeers = append(postgresPeers, &protos.Peer{
			Name:   peerName.String,
			Type:   protos.DBType_POSTGRES,
			Config: &protos.Peer_PostgresConfig{PostgresConfig: &pgPeerConfig},
		})
	}
	return postgresPeers, nil
}

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	if !peerdbenv.PeerDBEnableWALHeartbeat() {
		slog.Info("wal heartbeat is disabled")
		return nil
	}

	sendTimeout := 10 * time.Minute
	ticker := time.NewTicker(sendTimeout)
	defer ticker.Stop()
	activity.RecordHeartbeat(ctx, "sending walheartbeat every 10 minutes")
	for {
		select {
		case <-ctx.Done():
			slog.Info("context is done, exiting wal heartbeat send loop")
			return nil
		case <-ticker.C:
			pgPeers, err := a.getPostgresPeerConfigs(ctx)
			if err != nil {
				slog.Warn("[sendwalheartbeat]: warning: unable to fetch peers." +
					"Skipping walheartbeat send. error encountered: " + err.Error())
				continue
			}

			command := `
				BEGIN;
				DROP aggregate IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
				CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
				DROP aggregate PEERDB_EPHEMERAL_HEARTBEAT(float4);
				END;
				`
			// run above command for each Postgres peer
			for _, pgPeer := range pgPeers {
				pgConfig := pgPeer.GetPostgresConfig()
				peerConn, peerErr := pgx.Connect(ctx, utils.GetPGConnectionString(pgConfig))
				if peerErr != nil {
					return fmt.Errorf("error creating pool for postgres peer %v with host %v: %w",
						pgPeer.Name, pgConfig.Host, peerErr)
				}

				_, err := peerConn.Exec(ctx, command)
				if err != nil {
					slog.Warn(fmt.Sprintf("warning: could not send walheartbeat to peer %v: %v", pgPeer.Name, err))
				}

				closeErr := peerConn.Close(ctx)
				if closeErr != nil {
					return fmt.Errorf("error closing postgres connection for peer %v with host %v: %w",
						pgPeer.Name, pgConfig.Host, closeErr)
				}
				slog.InfoContext(ctx, fmt.Sprintf("sent walheartbeat to peer %v", pgPeer.Name))
			}
		}
		ticker.Reset(sendTimeout)
	}
}

func (a *FlowableActivity) QRepWaitUntilNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
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
	defer connectors.CloseConnector(srcConn)
	pgSrcConn := srcConn.(*connpostgres.PostgresConnector)
	slog.InfoContext(ctx, fmt.Sprintf("current last partition value is %v\n", last))
	attemptCount := 1
	for {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("no new rows yet, attempt #%d", attemptCount))
		time.Sleep(waitBetweenBatches)

		result, err := pgSrcConn.CheckForUpdatedMaxValue(config, last)
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
	defer connectors.CloseConnector(dstConn)

	if config.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return nil, fmt.Errorf("failed to cast connector to snowflake connector")
		}
		return sfConn.RenameTables(config)
	} else if config.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return nil, fmt.Errorf("failed to cast connector to bigquery connector")
		}
		return bqConn.RenameTables(config)
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
	defer connectors.CloseConnector(dstConn)

	if req.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			return nil, fmt.Errorf("failed to cast connector to snowflake connector")
		}
		return sfConn.CreateTablesFromExisting(req)
	} else if req.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			return nil, fmt.Errorf("failed to cast connector to bigquery connector")
		}
		return bqConn.CreateTablesFromExisting(req)
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
	startTime := time.Now()
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	slog.InfoContext(ctx, "replicating xmin\n")

	bufferSize := shared.FetchAndChannelSize
	errGroup, errCtx := errgroup.WithContext(ctx)

	stream := model.NewQRecordStream(bufferSize)

	var currentSnapshotXmin int64
	errGroup.Go(func() error {
		pgConn := srcConn.(*connpostgres.PostgresConnector)
		var pullErr error
		var numRecords int
		numRecords, currentSnapshotXmin, pullErr = pgConn.PullXminRecordStream(config, partition, stream)
		if pullErr != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			slog.InfoContext(ctx, fmt.Sprintf("[xmin] failed to pull records: %v", err))
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
			slog.Error(fmt.Sprintf("%v", err))
			return err
		}

		return nil
	})

	shutdown := utils.HeartbeatRoutine(ctx, 5*time.Minute, func() string {
		return "syncing xmin."
	})
	defer shutdown()

	rowsSynced, err := dstConn.SyncQRepRecords(config, partition, stream)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return 0, fmt.Errorf("failed to sync records: %w", err)
	}

	if rowsSynced == 0 {
		slog.InfoContext(ctx, "no records to push for xmin\n")
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

		slog.InfoContext(ctx, fmt.Sprintf("pushed %d records\n", rowsSynced))
	}

	err = monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
	if err != nil {
		return 0, err
	}

	return currentSnapshotXmin, nil
}
