package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type EventHubConnector struct {
	ctx        context.Context
	config     *protos.EventHubGroupConfig
	pgMetadata *metadataStore.PostgresMetadataStore
	creds      *azidentity.DefaultAzureCredential
	hubManager *EventHubManager
	logger     slog.Logger
}

// NewEventHubConnector creates a new EventHubConnector.
func NewEventHubConnector(
	ctx context.Context,
	config *protos.EventHubGroupConfig,
) (*EventHubConnector, error) {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to get default azure credentials",
			slog.Any("error", err))
		return nil, err
	}

	hubManager := NewEventHubManager(defaultAzureCreds, config)
	metadataSchemaName := "peerdb_eventhub_metadata" // #nosec G101
	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx, config.GetMetadataDb(),
		metadataSchemaName)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create postgres metadata store",
			slog.Any("error", err))
		return nil, err
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &EventHubConnector{
		ctx:        ctx,
		config:     config,
		pgMetadata: pgMetadata,
		creds:      defaultAzureCreds,
		hubManager: hubManager,
		logger:     *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

func (c *EventHubConnector) Close() error {
	var allErrors error

	// close the postgres metadata store.
	err := c.pgMetadata.Close()
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to close postgres metadata store: %v", err))
		allErrors = errors.Join(allErrors, err)
	}

	err = c.hubManager.Close(context.Background())
	if err != nil {
		c.logger.Error("failed to close event hub manager", slog.Any("error", err))
		allErrors = errors.Join(allErrors, err)
	}

	return allErrors
}

func (c *EventHubConnector) ConnectionActive() error {
	return nil
}

func (c *EventHubConnector) NeedsSetupMetadataTables() bool {
	return c.pgMetadata.NeedsSetupMetadata()
}

func (c *EventHubConnector) SetupMetadataTables() error {
	err := c.pgMetadata.SetupMetadata()
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to setup metadata tables: %v", err))
		return err
	}

	return nil
}

func (c *EventHubConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(jobName)
}

func (c *EventHubConnector) GetLastOffset(jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(jobName)
}

func (c *EventHubConnector) SetLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to update last offset: %v", err))
		return err
	}

	return nil
}

// returns the number of records synced
func (c *EventHubConnector) processBatch(
	flowJobName string,
	batch *model.CDCRecordStream,
) (uint32, error) {
	ctx := context.Background()
	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns)

	eventHubFlushTimeout := peerdbenv.PeerDBEventhubFlushTimeoutSeconds()

	ticker := time.NewTicker(eventHubFlushTimeout)
	defer ticker.Stop()

	lastSeenLSN := int64(0)
	lastUpdatedOffset := int64(0)

	numRecords := atomic.Uint32{}
	shutdown := utils.HeartbeatRoutine(c.ctx, 10*time.Second, func() string {
		return fmt.Sprintf(
			"processed %d records for flow %s",
			numRecords.Load(), flowJobName,
		)
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

			recordLSN := record.GetCheckPointID()
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

			numPartitions, err := c.hubManager.GetNumPartitions(ctx, destination)
			if err != nil {
				c.logger.Error("failed to get number of partitions", slog.Any("error", err))
				return 0, err
			}

			// Scoped eventhub is of the form peer_name.eventhub_name.partition_column
			// partition_column is the column in the table that is used to determine
			// the partition key for the eventhub.
			partitionColumn := destination.PartitionKeyColumn
			partitionValue := record.GetItems().GetColumnValue(partitionColumn).Value
			var partitionKey string
			if partitionValue == nil {
				partitionKey = ""
			} else {
				partitionKey = fmt.Sprintf("%v", partitionValue)
			}
			partitionKey = utils.HashedPartitionKey(partitionKey, uint32(numPartitions))
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

		case <-ticker.C:
			err := batchPerTopic.flushAllBatches(ctx, flowJobName)
			if err != nil {
				return 0, err
			}

			if lastSeenLSN > lastUpdatedOffset {
				err = c.SetLastOffset(flowJobName, lastSeenLSN)
				lastUpdatedOffset = lastSeenLSN
				c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeenLSN))
				if err != nil {
					return 0, fmt.Errorf("failed to update last offset: %v", err)
				}
			}

			ticker.Reset(eventHubFlushTimeout)
		}
	}
}

func (c *EventHubConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	batch := req.Records

	numRecords, err := c.processBatch(req.FlowJobName, batch)
	if err != nil {
		c.logger.Error("failed to process batch", slog.Any("error", err))
		return nil, err
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		c.logger.Error("failed to get last checkpoint", slog.Any("error", err))
		return nil, err
	}

	err = c.SetLastOffset(req.FlowJobName, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to update last offset", slog.Any("error", err))
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	rowsSynced := int64(numRecords)
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		c.logger.Error("failed to get last sync batch id", slog.Any("error", err))
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     syncBatchID,
		LastSyncedCheckPointID: lastCheckpoint,
		NumRecordsSynced:       rowsSynced,
		TableNameRowsMapping:   make(map[string]uint32),
		TableSchemaDeltas:      req.Records.WaitForSchemaDeltas(req.TableMappings),
		RelationMessageMapping: <-req.Records.RelationMessageMapping,
	}, nil
}

func (c *EventHubConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
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

		err = c.hubManager.EnsureEventHubExists(c.ctx, name)
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

func (c *EventHubConnector) ReplayTableSchemaDeltas(flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	c.logger.Info("ReplayTableSchemaDeltas for event hub is a no-op")
	return nil
}

func (c *EventHubConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error,
) {
	c.logger.Info("normalization for event hub is a no-op")
	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: nil,
	}, nil
}

func (c *EventHubConnector) SyncFlowCleanup(jobName string) error {
	err := c.pgMetadata.DropMetadata(jobName)
	if err != nil {
		return err
	}
	return nil
}
