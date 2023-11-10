package activities

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pglogrepl"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	// True of metadata tables need to be set up.
	NeedsSetupMetadataTables bool
}

type SlotSnapshotSignal struct {
	signal       *connpostgres.SlotSignal
	snapshotName string
	connector    connectors.CDCPullConnector
}

type FlowableActivity struct {
	EnableMetrics        bool
	CatalogMirrorMonitor *monitoring.CatalogMirrorMonitor
}

// CheckConnection implements CheckConnection.
func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.Peer,
) (*CheckConnectionResult, error) {
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config)
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
func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.Peer) error {
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	if err := dstConn.SetupMetadataTables(); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetLastSyncedID implements GetLastSyncedID.
func (a *FlowableActivity) GetLastSyncedID(
	ctx context.Context,
	config *protos.GetLastSyncedIDInput,
) (*protos.LastSyncState, error) {
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	return dstConn.GetLastOffset(config.FlowJobName)
}

// EnsurePullability implements EnsurePullability.
func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	output, err := srcConn.EnsurePullability(config)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	return output, nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	ctx = context.WithValue(ctx, shared.CDCMirrorMonitorKey, a.CatalogMirrorMonitor)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	res, err := dstConn.CreateRawTable(config)
	if err != nil {
		return nil, err
	}
	err = a.CatalogMirrorMonitor.InitializeCDCFlow(ctx, config.FlowJobName)
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
	conn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	return conn.SetupNormalizedTables(config)
}

// StartFlow implements StartFlow.
func (a *FlowableActivity) StartFlow(ctx context.Context,
	input *protos.StartFlowInput) (*model.SyncResponse, error) {
	activity.RecordHeartbeat(ctx, "starting flow...")
	conn := input.FlowConnectionConfigs

	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	ctx = context.WithValue(ctx, shared.CDCMirrorMonitorKey, a.CatalogMirrorMonitor)

	dstConn, err := connectors.GetCDCSyncConnector(ctx, conn.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("initializing table schema...")
	err = dstConn.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}
	activity.RecordHeartbeat(ctx, "initialized table schema")

	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Info("pulling records...")

	tblNameMapping := make(map[string]model.NameAndExclude)
	for _, v := range input.FlowConnectionConfigs.TableMappings {
		exclude := make(map[string]struct{}, len(v.Exclude))
		for _, col := range v.Exclude {
			exclude[col] = struct{}{}
		}
		tblNameMapping[v.SourceTableIdentifier] = model.NameAndExclude{
			Name:    v.DestinationTableIdentifier,
			Exclude: exclude,
		}
	}

	idleTimeout := utils.GetEnvInt("PEERDB_CDC_IDLE_TIMEOUT_SECONDS", 10)

	recordBatch := model.NewCDCRecordStream()

	startTime := time.Now()

	errGroup, errCtx := errgroup.WithContext(ctx)
	srcConn, err := connectors.GetCDCPullConnector(errCtx, conn.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	// start a goroutine to pull records from the source
	errGroup.Go(func() error {
		return srcConn.PullRecords(&model.PullRecordsRequest{
			FlowJobName:                 input.FlowConnectionConfigs.FlowJobName,
			SrcTableIDNameMapping:       input.FlowConnectionConfigs.SrcTableIdNameMapping,
			TableNameMapping:            tblNameMapping,
			LastSyncState:               input.LastSyncState,
			MaxBatchSize:                uint32(input.SyncFlowOptions.BatchSize),
			IdleTimeout:                 time.Duration(idleTimeout) * time.Second,
			TableNameSchemaMapping:      input.FlowConnectionConfigs.TableNameSchemaMapping,
			OverridePublicationName:     input.FlowConnectionConfigs.PublicationName,
			OverrideReplicationSlotName: input.FlowConnectionConfigs.ReplicationSlotName,
			RelationMessageMapping:      input.RelationMessageMapping,
			RecordStream:                recordBatch,
		})
	})

	hasRecords := !recordBatch.WaitAndCheckEmpty()
	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("the current sync flow has records: %v", hasRecords)

	if a.CatalogMirrorMonitor.IsActive() && hasRecords {
		syncBatchID, err := dstConn.GetLastSyncBatchID(input.FlowConnectionConfigs.FlowJobName)
		if err != nil && conn.Destination.Type != protos.DBType_EVENTHUB {
			return nil, err
		}

		err = a.CatalogMirrorMonitor.AddCDCBatchForFlow(ctx, input.FlowConnectionConfigs.FlowJobName,
			monitoring.CDCBatchInfo{
				BatchID:       syncBatchID + 1,
				RowsInBatch:   0,
				BatchStartLSN: pglogrepl.LSN(recordBatch.GetFirstCheckpoint()),
				BatchEndlSN:   0,
				StartTime:     startTime,
			})
		if err != nil {
			return nil, err
		}
	}

	if !hasRecords {
		// wait for the pull goroutine to finish
		err = errGroup.Wait()
		if err != nil {
			return nil, fmt.Errorf("failed to pull records: %w", err)
		}

		log.WithFields(log.Fields{"flowName": input.FlowConnectionConfigs.FlowJobName}).Info("no records to push")
		syncResponse := &model.SyncResponse{}
		syncResponse.RelationMessageMapping = <-recordBatch.RelationMessageMapping
		syncResponse.TableSchemaDeltas = recordBatch.WaitForSchemaDeltas()
		return syncResponse, nil
	}

	shutdown := utils.HeartbeatRoutine(ctx, 10*time.Second, func() string {
		jobName := input.FlowConnectionConfigs.FlowJobName
		return fmt.Sprintf("pushing records for job - %s", jobName)
	})

	defer func() {
		shutdown <- true
	}()

	syncStartTime := time.Now()
	res, err := dstConn.SyncRecords(&model.SyncRecordsRequest{
		Records:         recordBatch,
		FlowJobName:     input.FlowConnectionConfigs.FlowJobName,
		SyncMode:        input.FlowConnectionConfigs.CdcSyncMode,
		StagingPath:     input.FlowConnectionConfigs.CdcStagingPath,
		PushBatchSize:   input.FlowConnectionConfigs.PushBatchSize,
		PushParallelism: input.FlowConnectionConfigs.PushParallelism,
	})
	if err != nil {
		log.Warnf("failed to push records: %v", err)
		return nil, fmt.Errorf("failed to push records: %w", err)
	}

	err = errGroup.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to pull records: %w", err)
	}

	numRecords := res.NumRecordsSynced
	syncDuration := time.Since(syncStartTime)
	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("pushed %d records in %d seconds\n", numRecords, int(syncDuration.Seconds()))

	lastCheckpoint, err := recordBatch.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = a.CatalogMirrorMonitor.UpdateNumRowsAndEndLSNForCDCBatch(
		ctx,
		input.FlowConnectionConfigs.FlowJobName,
		res.CurrentSyncBatchID,
		uint32(numRecords),
		pglogrepl.LSN(lastCheckpoint),
	)
	if err != nil {
		return nil, err
	}

	err = a.CatalogMirrorMonitor.
		UpdateLatestLSNAtTargetForCDCFlow(ctx, input.FlowConnectionConfigs.FlowJobName, pglogrepl.LSN(lastCheckpoint))
	if err != nil {
		return nil, err
	}
	if res.TableNameRowsMapping != nil {
		err = a.CatalogMirrorMonitor.AddCDCBatchTablesForFlow(ctx, input.FlowConnectionConfigs.FlowJobName,
			res.CurrentSyncBatchID, res.TableNameRowsMapping)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	res.TableSchemaDeltas = recordBatch.WaitForSchemaDeltas()
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

	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
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

		err = a.CatalogMirrorMonitor.UpdateEndTimeForCDCBatch(ctx, input.FlowConnectionConfigs.FlowJobName,
			lastSyncBatchID)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("normalizing records from batch for job - %s", input.FlowConnectionConfigs.FlowJobName)
	})
	defer func() {
		shutdown <- true
	}()

	log.Info("initializing table schema...")
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
		return nil, fmt.Errorf("failed to normalized records: %w", err)
	}

	// normalize flow did not run due to no records, no need to update end time.
	if res.Done {
		err = a.CatalogMirrorMonitor.UpdateEndTimeForCDCBatch(ctx, input.FlowConnectionConfigs.FlowJobName,
			res.EndBatchID)
		if err != nil {
			return nil, err
		}
	}

	// log the number of batches normalized
	if res != nil {
		log.Infof("normalized records from batch %d to batch %d\n", res.StartBatchID, res.EndBatchID)
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

	return dest.ReplayTableSchemaDeltas(input.FlowConnectionConfigs.FlowJobName, input.TableSchemaDeltas)
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	return conn.SetupQRepMetadataTables(config)
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	runUUID string,
) (*protos.QRepParitionResult, error) {
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get qrep pull connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("getting partitions for job - %s", config.FlowJobName)
	})

	defer func() {
		shutdown <- true
	}()

	partitions, err := srcConn.GetQRepPartitions(config, last)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions from source: %w", err)
	}
	if len(partitions) > 0 {
		err = a.CatalogMirrorMonitor.InitializeQRepRun(
			ctx,
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

// ReplicateQRepPartition replicates a QRepPartition from the source to the destination.
func (a *FlowableActivity) ReplicateQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	partitions *protos.QRepPartitionBatch,
	runUUID string,
) error {
	err := a.CatalogMirrorMonitor.UpdateStartTimeForQRepRun(ctx, runUUID)
	if err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)
	log.Infof("replicating partitions for job - %s - batch %d - size: %d\n",
		config.FlowJobName, partitions.BatchId, numPartitions)
	for i, p := range partitions.Partitions {
		log.Infof("batch-%d - replicating partition - %s\n", partitions.BatchId, p.PartitionId)
		err := a.replicateQRepPartition(ctx, config, i+1, numPartitions, p, runUUID)
		if err != nil {
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
	err := a.CatalogMirrorMonitor.UpdateStartTimeForPartition(ctx, runUUID, partition)
	if err != nil {
		return fmt.Errorf("failed to update start time for partition: %w", err)
	}

	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	log.Infof("replicating partition %s\n", partition.PartitionId)

	var stream *model.QRecordStream
	bufferSize := shared.FetchAndChannelSize
	var wg sync.WaitGroup

	var goroutineErr error = nil
	if config.SourcePeer.Type == protos.DBType_POSTGRES {
		stream = model.NewQRecordStream(bufferSize)
		wg.Add(1)

		pullPgRecords := func() {
			pgConn := srcConn.(*connpostgres.PostgresConnector)
			tmp, err := pgConn.PullQRepRecordStream(config, partition, stream)
			numRecords := int64(tmp)
			if err != nil {
				log.WithFields(log.Fields{
					"flowName": config.FlowJobName,
				}).Errorf("failed to pull records: %v", err)
				goroutineErr = err
			}
			err = a.CatalogMirrorMonitor.UpdatePullEndTimeAndRowsForPartition(ctx, runUUID, partition, numRecords)
			if err != nil {
				log.Errorf("%v", err)
				goroutineErr = err
			}
			wg.Done()
		}

		go pullPgRecords()
	} else {
		recordBatch, err := srcConn.PullQRepRecords(config, partition)
		if err != nil {
			return fmt.Errorf("failed to pull records: %w", err)
		}
		numRecords := int64(recordBatch.NumRecords)
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("pulled %d records\n", len(recordBatch.Records))

		err = a.CatalogMirrorMonitor.UpdatePullEndTimeAndRowsForPartition(ctx, runUUID, partition, numRecords)
		if err != nil {
			return err
		}

		stream, err = recordBatch.ToQRecordStream(bufferSize)
		if err != nil {
			return fmt.Errorf("failed to convert to qrecord stream: %w", err)
		}
	}

	shutdown := utils.HeartbeatRoutine(ctx, 5*time.Minute, func() string {
		return fmt.Sprintf("syncing partition - %s: %d of %d total.", partition.PartitionId, idx, total)
	})

	defer func() {
		shutdown <- true
	}()

	res, err := dstConn.SyncQRepRecords(config, partition, stream)
	if err != nil {
		return fmt.Errorf("failed to sync records: %w", err)
	}

	if res == 0 {
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("no records to push for partition %s\n", partition.PartitionId)
	} else {
		wg.Wait()
		if goroutineErr != nil {
			return goroutineErr
		}
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("pushed %d records\n", res)
	}

	err = a.CatalogMirrorMonitor.UpdateEndTimeForPartition(ctx, runUUID, partition)
	if err != nil {
		return err
	}

	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string) error {
	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	dstConn, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return a.CatalogMirrorMonitor.UpdateEndTimeForQRepRun(ctx, runUUID)
	} else if err != nil {
		return err
	}

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("consolidating partitions for job - %s", config.FlowJobName)
	})

	defer func() {
		shutdown <- true
	}()

	err = dstConn.ConsolidateQRepPartitions(config)
	if err != nil {
		return err
	}

	return a.CatalogMirrorMonitor.UpdateEndTimeForQRepRun(ctx, runUUID)
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	dst, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return nil
	} else if err != nil {
		return err
	}

	return dst.CleanupQRepFlow(config)
}

func (a *FlowableActivity) DropFlow(ctx context.Context, config *protos.ShutdownRequest) error {
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

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context, config *protos.FlowConnectionConfigs) error {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	err = srcConn.SendWALHeartbeat()
	if err != nil {
		return fmt.Errorf("failed to send WAL heartbeat: %w", err)
	}

	return nil
}

func (a *FlowableActivity) QRepWaitUntilNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition) error {
	if config.SourcePeer.Type != protos.DBType_POSTGRES || last.Range == nil {
		return nil
	}
	waitBetweenBatches := 5 * time.Second
	if config.WaitBetweenBatchesSeconds > 0 {
		waitBetweenBatches = time.Duration(config.WaitBetweenBatchesSeconds) * time.Second
	}

	if config.WatermarkColumn == "xmin" {
		// for xmin we ignore the wait between batches, as seq scan time is
		// extremely slow.
		waitBetweenBatches = 10 * time.Second
	}

	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)
	pgSrcConn := srcConn.(*connpostgres.PostgresConnector)

	log.WithFields(log.Fields{
		"flowName": config.FlowJobName,
	}).Infof("current last partition value is %v\n", last)
	attemptCount := 1
	for {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("no new rows yet, attempt #%d", attemptCount))
		time.Sleep(waitBetweenBatches)

		result, err := pgSrcConn.CheckForUpdatedMaxValue(config, last)
		if err != nil {
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
	*protos.RenameTablesOutput, error) {
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(dstConn)

	if config.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			return nil, fmt.Errorf("failed to cast connector to snowflake connector")
		}
		return sfConn.RenameTables(config)
	} else if config.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			return nil, fmt.Errorf("failed to cast connector to bigquery connector")
		}
		return bqConn.RenameTables(config)
	}
	return nil, fmt.Errorf("rename tables is only supported on snowflake and bigquery")
}

func (a *FlowableActivity) CreateTablesFromExisting(ctx context.Context, req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error) {
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
	return nil, fmt.Errorf("create tables from existing is only supported on snowflake and bigquery")
}
