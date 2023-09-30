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
	pgMetadata   *PostgresMetadataStore
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
	pgMetadata, err := NewPostgresMetadataStore(ctx, config.GetMetadataDb())
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

	// close all the eventhub connections.
	err := c.hubManager.Close()
	if err != nil {
		log.Errorf("failed to close eventhub connections: %v", err)
		allErrors = errors.Join(allErrors, err)
	}

	// close the postgres metadata store.
	err = c.pgMetadata.Close()
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

func (c *EventHubConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	shutdown := utils.HeartbeatRoutine(c.ctx, 10*time.Second, func() string {
		return fmt.Sprintf("syncing records to eventhub with"+
			" push parallelism %d and push batch size %d",
			req.PushParallelism, req.PushBatchSize)
	})
	defer func() {
		shutdown <- true
	}()
	tableNameRowsMapping := cmap.New[uint32]()
	batch := req.Records
	eventsPerHeartBeat := 1000
	eventsPerBatch := int(req.PushBatchSize)
	if eventsPerBatch <= 0 {
		eventsPerBatch = 10000
	}
	maxParallelism := req.PushParallelism
	if maxParallelism <= 0 {
		maxParallelism = 10
	}

	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns)

	startTime := time.Now()
	for i, record := range batch.Records {
		json, err := record.GetItems().ToJSONWithOpts(toJSONOpts)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Infof("failed to convert record to json: %v", err)
			return nil, err
		}

		flushBatch := func() error {
			err := c.sendEventBatch(batchPerTopic, maxParallelism,
				req.FlowJobName, tableNameRowsMapping)
			if err != nil {
				log.WithFields(log.Fields{
					"flowName": req.FlowJobName,
				}).Infof("failed to send event batch: %v", err)
				return err
			}
			batchPerTopic.Clear()
			return nil
		}

		topicName, err := NewScopedEventhub(record.GetTableName())
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Infof("failed to get topic name: %v", err)
			return nil, err
		}

		err = batchPerTopic.AddEvent(topicName, json)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Infof("failed to add event to batch: %v", err)
			return nil, err
		}

		if i%eventsPerHeartBeat == 0 {
			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("sent %d records to hub: %s", i, topicName.ToString()))
		}

		if (i+1)%eventsPerBatch == 0 {
			err := flushBatch()
			if err != nil {
				return nil, err
			}
		}
	}

	// send the remaining events.
	if batchPerTopic.Len() > 0 {
		err := c.sendEventBatch(batchPerTopic, maxParallelism,
			req.FlowJobName, tableNameRowsMapping)
		if err != nil {
			return nil, err
		}
	}
	rowsSynced := len(batch.Records)
	log.WithFields(log.Fields{
		"flowName": req.FlowJobName,
	}).Infof("[total] successfully sent %d records to event hub", rowsSynced)

	err := c.updateLastOffset(req.FlowJobName, batch.LastCheckPointID)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return nil, err
	}
	err = c.incrementSyncBatchID(req.FlowJobName)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	metrics.LogSyncMetrics(c.ctx, req.FlowJobName, int64(rowsSynced), time.Since(startTime))
	metrics.LogNormalizeMetrics(c.ctx, req.FlowJobName, int64(rowsSynced),
		time.Since(startTime), int64(rowsSynced))
	return &model.SyncResponse{
		FirstSyncedCheckPointID: batch.FirstCheckPointID,
		LastSyncedCheckPointID:  batch.LastCheckPointID,
		NumRecordsSynced:        int64(len(batch.Records)),
		TableNameRowsMapping:    tableNameRowsMapping.Items(),
	}, nil
}

func (c *EventHubConnector) sendEventBatch(
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
			err := c.sendBatch(tblName, eventBatch)
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

func (c *EventHubConnector) sendBatch(tblName ScopedEventhub, events *azeventhubs.EventDataBatch) error {
	subCtx, cancel := context.WithTimeout(c.ctx, 5*time.Minute)
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
