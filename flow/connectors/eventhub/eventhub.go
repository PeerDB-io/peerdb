package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	cmap "github.com/orcaman/concurrent-map/v2"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
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

	hubManager := NewEventHubManager(ctx, defaultAzureCreds, config)
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

func (c *EventHubConnector) processBatch(
	flowJobName string,
	batch *model.RecordBatch,
	eventsPerBatch int,
	maxParallelism int64,
) error {
	ctx := context.Background()

	tableNameRowsMapping := cmap.New[uint32]()
	eventsPerHeartBeat := 1000

	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns)

	for i, record := range batch.Records {
		json, err := record.GetItems().ToJSONWithOpts(toJSONOpts)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to convert record to json: %v", err)
			return err
		}

		flushBatch := func() error {
			err := c.sendEventBatch(ctx, batchPerTopic, maxParallelism, flowJobName, tableNameRowsMapping)
			if err != nil {
				log.WithFields(log.Fields{
					"flowName": flowJobName,
				}).Infof("failed to send event batch: %v", err)
				return err
			}
			batchPerTopic.Clear()
			return nil
		}

		topicName, err := NewScopedEventhub(record.GetTableName())
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to get topic name: %v", err)
			return err
		}

		err = batchPerTopic.AddEvent(ctx, topicName, json)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": flowJobName,
			}).Infof("failed to add event to batch: %v", err)
			return err
		}

		if i%eventsPerHeartBeat == 0 {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("sent %d records to hub: %s", i, topicName.ToString()))
		}

		if (i+1)%eventsPerBatch == 0 {
			err := flushBatch()
			if err != nil {
				return err
			}
		}
	}

	if batchPerTopic.Len() > 0 {
		err := c.sendEventBatch(ctx, batchPerTopic, maxParallelism, flowJobName, tableNameRowsMapping)
		if err != nil {
			return err
		}
	}

	rowsSynced := len(batch.Records)
	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("[total] successfully sent %d records to event hub", rowsSynced)
	return nil
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

	eventsPerBatch := int(req.PushBatchSize)
	if eventsPerBatch <= 0 {
		eventsPerBatch = 10000
	}
	maxParallelism := req.PushParallelism
	if maxParallelism <= 0 {
		maxParallelism = 10
	}

	var err error
	startTime := time.Now()

	batch := req.Records

	// if env var PEERDB_BETA_EVENTHUB_PUSH_ASYNC=true
	// we kick off processBatch in a goroutine and return immediately.
	// otherwise, we block until processBatch is done.
	if utils.GetEnvBool("PEERDB_BETA_EVENTHUB_PUSH_ASYNC", false) {
		go func() {
			err = c.processBatch(req.FlowJobName, batch, eventsPerBatch, maxParallelism)
			if err != nil {
				log.Errorf("[async] failed to process batch: %v", err)
			}
		}()
	} else {
		err = c.processBatch(req.FlowJobName, batch, eventsPerBatch, maxParallelism)
		if err != nil {
			log.Errorf("failed to process batch: %v", err)
			return nil, err
		}
	}

	err = c.updateLastOffset(req.FlowJobName, batch.LastCheckPointID)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	rowsSynced := int64(len(batch.Records))
	metrics.LogSyncMetrics(c.ctx, req.FlowJobName, rowsSynced, time.Since(startTime))
	metrics.LogNormalizeMetrics(c.ctx, req.FlowJobName, rowsSynced, time.Since(startTime), rowsSynced)
	return &model.SyncResponse{
		FirstSyncedCheckPointID: batch.FirstCheckPointID,
		LastSyncedCheckPointID:  batch.LastCheckPointID,
		NumRecordsSynced:        rowsSynced,
		TableNameRowsMapping:    make(map[string]uint32),
	}, nil
}

func (c *EventHubConnector) sendEventBatch(
	ctx context.Context,
	events *HubBatches,
	maxParallelism int64,
	flowName string,
	tableNameRowsMapping cmap.ConcurrentMap[string, uint32]) error {
	if events.Len() == 0 {
		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("no events to send")
		return nil
	}

	var numEventsPushed int32
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	// Limiting concurrent sends
	guard := make(chan struct{}, maxParallelism)

	events.ForEach(func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
		guard <- struct{}{}
		wg.Add(1)
		go func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
			defer func() {
				<-guard
				wg.Done()
			}()

			numEvents := eventBatch.NumEvents()
			err := c.sendBatch(ctx, tblName, eventBatch)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s", numEvents, tblName)
			rowCount, ok := tableNameRowsMapping.Get(tblName.ToString())
			if !ok {
				rowCount = uint32(0)
			}
			rowCount += uint32(numEvents)
			tableNameRowsMapping.Set(tblName.ToString(), rowCount)
		}(tblName, eventBatch)
	})

	wg.Wait()

	if firstErr != nil {
		log.Error(firstErr)
		return firstErr
	}

	log.Infof("successfully sent %d events to event hub", numEventsPushed)
	return nil
}

func (c *EventHubConnector) sendBatch(
	ctx context.Context,
	tblName ScopedEventhub,
	events *azeventhubs.EventDataBatch,
) error {
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	hub, err := c.hubManager.GetOrCreateHubClient(tblName)
	if err != nil {
		return err
	}

	opts := &azeventhubs.SendEventDataBatchOptions{}
	err = hub.SendEventDataBatch(subCtx, events, opts)
	if err != nil {
		return err
	}

	log.Infof("successfully sent %d events to event hub topic - %s", events.NumEvents(), tblName.ToString())
	return nil
}

func (c *EventHubConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	// create topics for each table
	// key is the source table and value is the "eh_peer.eh_topic" that ought to be used.
	tableMap := req.GetTableNameMapping()

	for _, table := range tableMap {
		// parse peer name and topic name.
		name, err := NewScopedEventhub(table)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
				"table":    table,
			}).Errorf("failed to parse peer and topic name: %v", err)
			return nil, err
		}

		err = c.hubManager.EnsureEventHubExists(c.ctx, name)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
				"table":    table,
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
