package activities

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pglogrepl"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	// True of metadata tables need to be set up.
	NeedsSetupMetadataTables bool
}

type SlotSnapshotSignal struct {
	signal       *connpostgres.SlotSignal
	snapshotName string
	connector    connectors.Connector
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
	conn, err := connectors.GetConnector(ctx, config)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	needsSetup := conn.NeedsSetupMetadataTables()

	return &CheckConnectionResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

// SetupMetadataTables implements SetupMetadataTables.
func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.Peer) error {
	conn, err := connectors.GetConnector(ctx, config)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}

	if err := conn.SetupMetadataTables(); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetLastSyncedID implements GetLastSyncedID.
func (a *FlowableActivity) GetLastSyncedID(
	ctx context.Context,
	config *protos.GetLastSyncedIDInput,
) (*protos.LastSyncState, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.GetLastOffset(config.FlowJobName)
}

// EnsurePullability implements EnsurePullability.
func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	output, err := conn.EnsurePullability(config)
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
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	res, err := conn.CreateRawTable(config)
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
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.GetTableSchema(config)
}

// CreateNormalizedTable creates a normalized table in the destination flowable.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.SetupNormalizedTables(config)
}

// StartFlow implements StartFlow.
func (a *FlowableActivity) StartFlow(ctx context.Context, input *protos.StartFlowInput) (*model.SyncResponse, error) {
	conn := input.FlowConnectionConfigs

	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	ctx = context.WithValue(ctx, shared.CDCMirrorMonitorKey, a.CatalogMirrorMonitor)
	src, err := connectors.GetConnector(ctx, conn.Source)
	defer connectors.CloseConnector(src)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, conn.Destination)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}

	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("initializing table schema...")
	err = dest.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Info("pulling records...")

	startTime := time.Now()
	records, err := src.PullRecords(&model.PullRecordsRequest{
		FlowJobName:                 input.FlowConnectionConfigs.FlowJobName,
		SrcTableIDNameMapping:       input.FlowConnectionConfigs.SrcTableIdNameMapping,
		TableNameMapping:            input.FlowConnectionConfigs.TableNameMapping,
		LastSyncState:               input.LastSyncState,
		MaxBatchSize:                uint32(input.SyncFlowOptions.BatchSize),
		IdleTimeout:                 10 * time.Second,
		TableNameSchemaMapping:      input.FlowConnectionConfigs.TableNameSchemaMapping,
		OverridePublicationName:     input.FlowConnectionConfigs.PublicationName,
		OverrideReplicationSlotName: input.FlowConnectionConfigs.ReplicationSlotName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to pull records: %w", err)
	}
	if a.CatalogMirrorMonitor.IsActive() && len(records.Records) > 0 {
		syncBatchID, err := dest.GetLastSyncBatchID(input.FlowConnectionConfigs.FlowJobName)
		if err != nil && conn.Destination.Type != protos.DBType_EVENTHUB {
			return nil, err
		}

		err = a.CatalogMirrorMonitor.AddCDCBatchForFlow(ctx, input.FlowConnectionConfigs.FlowJobName,
			monitoring.CDCBatchInfo{
				BatchID:       syncBatchID + 1,
				RowsInBatch:   uint32(len(records.Records)),
				BatchStartLSN: pglogrepl.LSN(records.FirstCheckPointID),
				BatchEndlSN:   pglogrepl.LSN(records.LastCheckPointID),
				StartTime:     startTime,
			})
		if err != nil {
			return nil, err
		}
	}

	pullDuration := time.Since(startTime)
	numRecords := len(records.Records)
	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("pulled %d records in %d seconds\n", numRecords, int(pullDuration.Seconds()))
	activity.RecordHeartbeat(ctx, fmt.Sprintf("pulled %d records", numRecords))

	if numRecords == 0 {
		log.WithFields(log.Fields{
			"flowName": input.FlowConnectionConfigs.FlowJobName,
		}).Info("no records to push")
		return nil, nil
	}

	syncStartTime := time.Now()
	res, err := dest.SyncRecords(&model.SyncRecordsRequest{
		Records:         records,
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

	syncDuration := time.Since(syncStartTime)
	log.WithFields(log.Fields{
		"flowName": input.FlowConnectionConfigs.FlowJobName,
	}).Infof("pushed %d records in %d seconds\n", numRecords, int(syncDuration.Seconds()))

	err = a.CatalogMirrorMonitor.
		UpdateLatestLSNAtTargetForCDCFlow(ctx, input.FlowConnectionConfigs.FlowJobName,
			pglogrepl.LSN(records.LastCheckPointID))
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

	activity.RecordHeartbeat(ctx, "pushed records")

	return res, nil
}

func (a *FlowableActivity) StartNormalize(
	ctx context.Context,
	input *protos.StartNormalizeInput,
) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs

	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	src, err := connectors.GetConnector(ctx, conn.Source)
	defer connectors.CloseConnector(src)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, conn.Destination)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("normalizing records from batch for job - %s", input.FlowConnectionConfigs.FlowJobName)
	})
	defer func() {
		shutdown <- true
	}()

	log.Info("initializing table schema...")
	err = dest.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	res, err := dest.NormalizeRecords(&model.NormalizeRecordsRequest{
		FlowJobName: input.FlowConnectionConfigs.FlowJobName,
		SoftDelete:  input.FlowConnectionConfigs.SoftDelete,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to normalized records: %w", err)
	}

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

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetConnector(ctx, config.DestinationPeer)
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
	conn, err := connectors.GetConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("getting partitions for job - %s", config.FlowJobName)
	})

	defer func() {
		shutdown <- true
	}()

	startTime := time.Now()
	partitions, err := conn.GetQRepPartitions(config, last)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions from source: %w", err)
	}
	if len(partitions) > 0 {
		err = a.CatalogMirrorMonitor.InitializeQRepRun(ctx, config.FlowJobName,
			runUUID, startTime)
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
	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	srcConn, err := connectors.GetConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	destConn, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(destConn)

	log.Infof("replicating partition %s\n", partition.PartitionId)

	var stream *model.QRecordStream
	bufferSize := shared.FetchAndChannelSize
	var wg sync.WaitGroup
	var numRecords int64

	err = a.CatalogMirrorMonitor.AddPartitionToQRepRun(ctx, config.FlowJobName, runUUID, partition)
	if err != nil {
		return err
	}
	var goroutineErr error = nil
	if config.SourcePeer.Type == protos.DBType_POSTGRES {
		stream = model.NewQRecordStream(bufferSize)
		wg.Add(1)

		pullPgRecords := func() {
			pgConn := srcConn.(*connpostgres.PostgresConnector)
			tmp, err := pgConn.PullQRepRecordStream(config, partition, stream)
			numRecords = int64(tmp)
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
		numRecords = int64(recordBatch.NumRecords)
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

	res, err := destConn.SyncQRepRecords(config, partition, stream)
	if err != nil {
		return fmt.Errorf("failed to sync records: %w", err)
	}

	if res == 0 {
		log.WithFields(log.Fields{
			"flowName": config.FlowJobName,
		}).Infof("no records to push for partition %s\n", partition.PartitionId)
		return nil
	}

	wg.Wait()
	if goroutineErr != nil {
		return goroutineErr
	}
	log.WithFields(log.Fields{
		"flowName": config.FlowJobName,
	}).Infof("pushed %d records\n", res)
	err = a.CatalogMirrorMonitor.UpdateEndTimeForPartition(ctx, runUUID, partition)
	if err != nil {
		return err
	}

	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string) error {
	ctx = context.WithValue(ctx, shared.EnableMetricsKey, a.EnableMetrics)
	dst, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	shutdown := utils.HeartbeatRoutine(ctx, 2*time.Minute, func() string {
		return fmt.Sprintf("consolidating partitions for job - %s", config.FlowJobName)
	})

	defer func() {
		shutdown <- true
	}()

	err = dst.ConsolidateQRepPartitions(config)
	if err != nil {
		return err
	}
	err = a.CatalogMirrorMonitor.UpdateEndTimeForQRepRun(ctx, runUUID)
	return err
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	dst, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	return dst.CleanupQRepFlow(config)
}

func (a *FlowableActivity) DropFlow(ctx context.Context, config *protos.ShutdownRequest) error {
	src, err := connectors.GetConnector(ctx, config.SourcePeer)
	defer connectors.CloseConnector(src)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, config.DestinationPeer)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	err = src.PullFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup source: %w", err)
	}
	err = dest.SyncFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup destination: %w", err)
	}
	return nil
}
