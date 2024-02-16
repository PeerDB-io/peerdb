package conneventhub

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type EventHubConnector struct {
	config     *protos.EventHubGroupConfig
	pgMetadata *metadataStore.PostgresMetadataStore
	creds      *azidentity.DefaultAzureCredential
	hubManager *EventHubManager
	logger     log.Logger
}

// NewEventHubConnector creates a new EventHubConnector.
func NewEventHubConnector(
	ctx context.Context,
	config *protos.EventHubGroupConfig,
) (*EventHubConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		logger.Error("failed to get default azure credentials", "error", err)
		return nil, err
	}

	hubManager := NewEventHubManager(defaultAzureCreds, config)
	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	return &EventHubConnector{
		config:     config,
		pgMetadata: pgMetadata,
		creds:      defaultAzureCreds,
		hubManager: hubManager,
	}, nil
}

func (c *EventHubConnector) Close() error {
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		err := c.hubManager.Close(timeout)
		if err != nil {
			c.logger.Error("failed to close event hub manager", slog.Any("error", err))
			return err
		}
	}

	return nil
}

func (c *EventHubConnector) ConnectionActive(_ context.Context) error {
	return nil
}

func (c *EventHubConnector) NeedsSetupMetadataTables(_ context.Context) bool {
	return false
}

func (c *EventHubConnector) SetupMetadataTables(_ context.Context) error {
	return nil
}

func (c *EventHubConnector) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(ctx, jobName)
}

func (c *EventHubConnector) GetLastOffset(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(ctx, jobName)
}

func (c *EventHubConnector) SetLastOffset(ctx context.Context, jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(ctx, jobName, offset)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to update last offset: %v", err))
		return err
	}

	return nil
}

// returns the number of records synced
func (c *EventHubConnector) processBatch(
	ctx context.Context,
	flowJobName string,
	batch *model.CDCRecordStream,
) (uint32, error) {
	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns, false)

	ticker := time.NewTicker(peerdbenv.PeerDBEventhubFlushTimeoutSeconds())
	defer ticker.Stop()

	lastSeenLSN := int64(0)
	lastUpdatedOffset := int64(0)

	numRecords := atomic.Uint32{}
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("processed %d records for flow %s", numRecords.Load(), flowJobName)
	})
	defer shutdown()

	for {
		select {
		case record, ok := <-batch.GetRecords():
			if !ok {
				c.logger.Info("flushing batches because no more records")
				err := batchPerTopic.flushAllBatches(ctx, flowJobName)
				if err != nil {
					return 0, err
				}

				currNumRecords := numRecords.Load()

				c.logger.Info("processBatch", slog.Int("Total records sent to event hub", int(currNumRecords)))
				return currNumRecords, nil
			}

			numRecords.Add(1)

			recordLSN := record.GetCheckpointID()
			if recordLSN > lastSeenLSN {
				lastSeenLSN = recordLSN
			}

			json, err := record.GetItems().ToJSONWithOpts(toJSONOpts)
			if err != nil {
				c.logger.Info("failed to convert record to json: %v", err)
				return 0, err
			}

			destination, err := NewScopedEventhub(record.GetDestinationTableName())
			if err != nil {
				c.logger.Error("failed to get topic name", slog.Any("error", err))
				return 0, err
			}

			ehConfig, ok := c.hubManager.peerConfig.Get(destination.PeerName)
			if !ok {
				c.logger.Error("failed to get eventhub config", slog.Any("error", err))
				return 0, err
			}

			numPartitions := ehConfig.PartitionCount
			// Scoped eventhub is of the form peer_name.eventhub_name.partition_column
			// partition_column is the column in the table that is used to determine
			// the partition key for the eventhub.
			partitionColumn := destination.PartitionKeyColumn
			partitionValue := record.GetItems().GetColumnValue(partitionColumn).Value
			var partitionKey string
			if partitionValue != nil {
				partitionKey = fmt.Sprint(partitionValue)
			}
			partitionKey = utils.HashedPartitionKey(partitionKey, numPartitions)
			destination.SetPartitionValue(partitionKey)
			err = batchPerTopic.AddEvent(ctx, destination, json, false)
			if err != nil {
				c.logger.Error("failed to add event to batch", slog.Any("error", err))
				return 0, err
			}

			curNumRecords := numRecords.Load()
			if curNumRecords%1000 == 0 {
				c.logger.Info("processBatch", slog.Int("number of records processed for sending", int(curNumRecords)))
			}

		case <-ctx.Done():
			return 0, fmt.Errorf("[eventhub] context cancelled %w", ctx.Err())

		case <-ticker.C:
			err := batchPerTopic.flushAllBatches(ctx, flowJobName)
			if err != nil {
				return 0, err
			}

			if lastSeenLSN > lastUpdatedOffset {
				err = c.SetLastOffset(ctx, flowJobName, lastSeenLSN)
				lastUpdatedOffset = lastSeenLSN
				c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeenLSN))
				if err != nil {
					return 0, fmt.Errorf("failed to update last offset: %w", err)
				}
			}
		}
	}
}

func (c *EventHubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	batch := req.Records

	numRecords, err := c.processBatch(ctx, req.FlowJobName, batch)
	if err != nil {
		c.logger.Error("failed to process batch", slog.Any("error", err))
		return nil, err
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err = c.pgMetadata.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		TableNameRowsMapping:   make(map[string]uint32),
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *EventHubConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	// create topics for each table
	// key is the source table and value is the "eh_peer.eh_topic" that ought to be used.
	tableMap := req.GetTableNameMapping()

	for _, destinationTable := range tableMap {
		// parse peer name and topic name.
		name, err := NewScopedEventhub(destinationTable)
		if err != nil {
			c.logger.Error("failed to parse scoped eventhub name",
				slog.Any("error", err), slog.String("destinationTable", destinationTable))
			return nil, err
		}

		err = c.hubManager.EnsureEventHubExists(ctx, name)
		if err != nil {
			c.logger.Error("failed to ensure eventhub exists",
				slog.Any("error", err), slog.String("destinationTable", destinationTable))
			return nil, err
		}
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: "n/a",
	}, nil
}

func (c *EventHubConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	c.logger.Info("ReplayTableSchemaDeltas for event hub is a no-op")
	return nil
}

func (c *EventHubConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	return c.pgMetadata.DropMetadata(ctx, jobName)
}
