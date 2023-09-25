package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
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
	config       *protos.EventHubConfig
	pgMetadata   *PostgresMetadataStore
	tableSchemas map[string]*protos.TableSchema
	creds        *azidentity.DefaultAzureCredential
	hubManager   *EventHubManager
}

// NewEventHubConnector creates a new EventHubConnector.
func NewEventHubConnector(
	ctx context.Context,
	config *protos.EventHubConfig,
) (*EventHubConnector, error) {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Errorf("failed to get default azure credentials: %v", err)
		return nil, err
	}

	hubManager := NewEventHubManager(ctx, defaultAzureCreds, config.GetNamespace())

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

	batchPerTopic := make(map[string]*azeventhubs.EventDataBatch)
	startTime := time.Now()
	for i, record := range batch.Records {
		json, err := record.GetItems().ToJSON()
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
			batchPerTopic = make(map[string]*azeventhubs.EventDataBatch)
			return nil
		}

		// TODO (kaushik): this is a hack to get the table name.
		topicName := record.GetTableName()

		addRecord := func() error {
			if _, ok := batchPerTopic[topicName]; !ok {
				batch, err := c.hubManager.CreateEventDataBatch(topicName)
				if err != nil {
					log.WithFields(log.Fields{
						"flowName": req.FlowJobName,
					}).Infof("failed to create event data batch: %v", err)
					return err
				}
				batchPerTopic[topicName] = batch
			}

			opts := &azeventhubs.AddEventDataOptions{}
			eventData := eventDataFromString(json)
			return batchPerTopic[topicName].AddEventData(eventData, opts)
		}

		err = addRecord()
		if err != nil {
			// if the error contains `EventData could not be added because it is too large for the batch`
			// then flush the batch and try again.
			if strings.Contains(err.Error(), "too large for the batch") {
				err := flushBatch()
				if err != nil {
					return nil, err
				}

				err = addRecord()
				if err != nil {
					log.WithFields(log.Fields{
						"flowName": req.FlowJobName,
					}).Infof("failed to add event data to batch (retried): %v", err)
					return nil, err
				}
			} else {
				log.WithFields(log.Fields{
					"flowName": req.FlowJobName,
				}).Infof("failed to add event data to batch: %v", err)
				return nil, err
			}
		}

		if i%eventsPerHeartBeat == 0 {
			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("sent %d records to hub: %s", i, topicName))
		}

		if (i+1)%eventsPerBatch == 0 {
			err := flushBatch()
			if err != nil {
				return nil, err
			}
		}
	}

	// send the remaining events.
	if len(batchPerTopic) > 0 {
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
	events map[string]*azeventhubs.EventDataBatch,
	maxParallelism int64,
	flowName string,
	tableNameRowsMapping cmap.ConcurrentMap[string, uint32]) error {
	if len(events) == 0 {
		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("no events to send")
		return nil
	}

	subCtx, cancel := context.WithTimeout(c.ctx, 5*time.Minute)
	defer cancel()

	var numEventsPushed int32
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	// Limiting concurrent sends
	guard := make(chan struct{}, maxParallelism)

	for tblName, eventBatch := range events {
		guard <- struct{}{}
		wg.Add(1)
		go func(tblName string, eventBatch *azeventhubs.EventDataBatch) {
			defer func() {
				<-guard
				wg.Done()
			}()

			hub, err := c.hubManager.GetOrCreateHub(tblName)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}

			numEvents := eventBatch.NumEvents()
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("obtained hub connection and now sending %d events to event hub: %s",
				numEvents, tblName)

			opts := &azeventhubs.SendEventDataBatchOptions{}
			err = hub.SendEventDataBatch(subCtx, eventBatch, opts)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s",
				numEventsPushed, tblName)
			rowCount, ok := tableNameRowsMapping.Get(tblName)
			if !ok {
				rowCount = uint32(0)
			}
			rowCount += uint32(numEvents)
			tableNameRowsMapping.Set(tblName, rowCount)
		}(tblName, eventBatch)
	}

	wg.Wait()

	if firstErr != nil {
		log.Error(firstErr)
		return firstErr
	}

	log.Infof("successfully sent %d events to event hub", numEventsPushed)
	return nil
}

func (c *EventHubConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	// create topics for each table
	// key is the source table and value is the destination topic name.
	tableMap := req.GetTableNameMapping()

	for _, table := range tableMap {
		err := c.ensureEventHub(c.ctx, table, req.FlowJobName)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
				"table":    table,
			}).Errorf("failed to get event hub properties: %v", err)
			return nil, err
		}
	}

	return nil, nil
}

func (c *EventHubConnector) ensureEventHub(ctx context.Context, name string, flowName string) error {
	hubClient, err := c.getEventHubMgmtClient()
	if err != nil {
		return err
	}

	namespace := c.config.GetNamespace()
	resourceGroup := c.config.GetResourceGroup()
	_, err = hubClient.Get(ctx, resourceGroup, namespace, name, nil)

	// TODO (kaushik): make these configurable.
	partitionCount := int64(3)
	retention := int64(1)
	if err != nil {
		opts := armeventhub.Eventhub{
			Properties: &armeventhub.Properties{
				PartitionCount:         &partitionCount,
				MessageRetentionInDays: &retention,
			},
		}

		_, err := hubClient.CreateOrUpdate(ctx, resourceGroup, namespace, name, opts, nil)
		if err != nil {
			log.Errorf("failed to create event hub: %v", err)
			return err
		}

		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("event hub %s created", name)
	} else {
		log.Infof("event hub %s already exists", name)
	}

	return nil
}

func (c *EventHubConnector) getEventHubMgmtClient() (*armeventhub.EventHubsClient, error) {
	subID, err := utils.GetAzureSubscriptionID()
	if err != nil {
		log.Errorf("failed to get azure subscription id: %v", err)
		return nil, err
	}

	hubClient, err := armeventhub.NewEventHubsClient(subID, c.creds, nil)
	if err != nil {
		log.Errorf("failed to get event hub client: %v", err)
		return nil, err
	}

	return hubClient, nil
}

func (c *EventHubConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error) {
	log.Infof("normalization for event hub is a no-op")
	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: nil,
	}, nil
}

func eventDataFromString(s string) *azeventhubs.EventData {
	return &azeventhubs.EventData{
		Body: []byte(s),
	}
}
