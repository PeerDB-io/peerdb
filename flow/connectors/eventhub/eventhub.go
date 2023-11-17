package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	cmap "github.com/orcaman/concurrent-map/v2"
	log "github.com/sirupsen/logrus"
)

type EventHubConnector struct {
	ctx          context.Context
	config       *protos.EventHubGroupConfig
	pgMetadata   *metadataStore.PostgresMetadataStore
	tableSchemas map[string]*protos.TableSchema
	creds        *azidentity.DefaultAzureCredential
	hubManager   *EventHubManager
}

// NewEventHubConnector creates a new EventHubConnector.
func NewEventHubConnector(
	ctx context.Context,
	config *protos.EventHubGroupConfig,
) (*EventHubConnector, error) {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Errorf("failed to get default azure credentials: %v", err)
		return nil, err
	}

	hubManager := NewEventHubManager(defaultAzureCreds, config)
	metadataSchemaName := "peerdb_eventhub_metadata" // #nosec G101
	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx, config.GetMetadataDb(),
		metadataSchemaName)
	if err != nil {
		log.Errorf("failed to create postgres metadata store: %v", err)
		return nil, err
	}

	return &EventHubConnector{
		ctx:        ctx,
		config:     config,
		pgMetadata: pgMetadata,
		creds:      defaultAzureCreds,
		hubManager: hubManager,
	}, nil
}

func (c *EventHubConnector) Close() error {
	var allErrors error

	// close the postgres metadata store.
	err := c.pgMetadata.Close()
	if err != nil {
		log.Errorf("failed to close postgres metadata store: %v", err)
		allErrors = errors.Join(allErrors, err)
	}

	return allErrors
}

func (c *EventHubConnector) ConnectionActive() bool {
	return true
}

func (c *EventHubConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableSchemas = req
	return nil
}

func (c *EventHubConnector) NeedsSetupMetadataTables() bool {
	return c.pgMetadata.NeedsSetupMetadata()
}

func (c *EventHubConnector) SetupMetadataTables() error {
	err := c.pgMetadata.SetupMetadata()
	if err != nil {
		log.Errorf("failed to setup metadata tables: %v", err)
		return err
	}

	return nil
}

func (c *EventHubConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	syncBatchID, err := c.pgMetadata.GetLastBatchID(jobName)
	if err != nil {
		return 0, err
	}

	return syncBatchID, nil
}

func (c *EventHubConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	res, err := c.pgMetadata.FetchLastOffset(jobName)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *EventHubConnector) updateLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return err
	}

	return nil
}

// returns the number of records synced
func (c *EventHubConnector) processBatch(
	flowJobName string,
	batch *model.CDCRecordStream,
	maxParallelism int64,
) (uint32, error) {
	ctx := context.Background()

	tableNameRowsMapping := cmap.New[uint32]()
	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns)

	numRecords := 0
	for record := range batch.GetRecords() {
		numRecords++
		json, err := record.GetItems().ToJSONWithOpts(toJSONOpts)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to convert record to json: %v", err)
			return 0, err
		}

		topicName, err := NewScopedEventhub(record.GetTableName())
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to get topic name: %v", err)
			return 0, err
		}

		err = batchPerTopic.AddEvent(ctx, topicName, json)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to add event to batch: %v", err)
			return 0, err
		}

		if numRecords%1000 == 0 {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("processed %d records for sending", numRecords)
		}
	}

	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("processed %d records for sending", numRecords)

	err := batchPerTopic.flushAllBatches(ctx, batchPerTopic, maxParallelism, flowJobName, tableNameRowsMapping)
	if err != nil {
		return 0, err
	}
	batchPerTopic.Clear()

	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("[total] successfully sent %d records to event hub", numRecords)
	return uint32(numRecords), nil
}

func (c *EventHubConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	shutdown := utils.HeartbeatRoutine(c.ctx, 10*time.Second, func() string {
		return fmt.Sprintf("syncing records to eventhub with"+
			" push parallelism %d and push batch size %d",
			req.PushParallelism, req.PushBatchSize)
	})
	defer func() {
		shutdown <- true
	}()

	maxParallelism := req.PushParallelism
	if maxParallelism <= 0 {
		maxParallelism = 10
	}

	var err error
	batch := req.Records
	var numRecords uint32

	// if env var PEERDB_BETA_EVENTHUB_PUSH_ASYNC=true
	// we kick off processBatch in a goroutine and return immediately.
	// otherwise, we block until processBatch is done.
	if utils.GetEnvBool("PEERDB_BETA_EVENTHUB_PUSH_ASYNC", false) {
		go func() {
			numRecords, err = c.processBatch(req.FlowJobName, batch, maxParallelism)
			if err != nil {
				log.Errorf("[async] failed to process batch: %v", err)
			}
		}()
	} else {
		numRecords, err = c.processBatch(req.FlowJobName, batch, maxParallelism)
		if err != nil {
			log.Errorf("failed to process batch: %v", err)
			return nil, err
		}
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		log.Errorf("failed to get last checkpoint: %v", err)
		return nil, err
	}

	err = c.updateLastOffset(req.FlowJobName, lastCheckpoint)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	rowsSynced := int64(numRecords)
	return &model.SyncResponse{
		FirstSyncedCheckPointID: batch.GetFirstCheckpoint(),
		LastSyncedCheckPointID:  lastCheckpoint,
		NumRecordsSynced:        rowsSynced,
		TableNameRowsMapping:    make(map[string]uint32),
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
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
				"table":    destinationTable,
			}).Errorf("failed to parse peer and topic name: %v", err)
			return nil, err
		}

		err = c.hubManager.EnsureEventHubExists(c.ctx, name)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
				"table":    destinationTable,
			}).Errorf("failed to ensure event hub exists: %v", err)
			return nil, err
		}
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: "n/a",
	}, nil
}

func (c *EventHubConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error) {
	log.Infof("normalization for event hub is a no-op")
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
