package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-amqp-common-go/v4/aad"
	"github.com/Azure/azure-amqp-common-go/v4/auth"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

type EventHubConnector struct {
	ctx           context.Context
	config        *protos.EventHubConfig
	pgMetadata    *PostgresMetadataStore
	tableSchemas  map[string]*protos.TableSchema
	creds         *azidentity.DefaultAzureCredential
	tokenProvider auth.TokenProvider
	hubs          map[string]*eventhub.Hub
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

	jwtTokenProvider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		log.Errorf("failed to get jwt token provider: %v", err)
		return nil, err
	}

	pgMetadata, err := NewPostgresMetadataStore(ctx, config.GetMetadataDb())
	if err != nil {
		log.Errorf("failed to create postgres metadata store: %v", err)
		return nil, err
	}

	return &EventHubConnector{
		ctx:           ctx,
		config:        config,
		pgMetadata:    pgMetadata,
		creds:         defaultAzureCreds,
		tokenProvider: jwtTokenProvider,
		hubs:          make(map[string]*eventhub.Hub),
	}, nil
}

func (c *EventHubConnector) Close() error {
	var allErrors error

	// close all the event hub connections.
	for _, hub := range c.hubs {
		err := hub.Close(c.ctx)
		if err != nil {
			log.Errorf("failed to close event hub connection: %v", err)
			allErrors = errors.Join(allErrors, err)
		}
	}

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

func (c *EventHubConnector) EnsurePullability(
	req *protos.EnsurePullabilityBatchInput) (*protos.EnsurePullabilityBatchOutput, error) {
	panic("ensure pullability not implemented for event hub")
}

func (c *EventHubConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableSchemas = req
	return nil
}

func (c *EventHubConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	panic("pull records not implemented for event hub")
}

func (c *EventHubConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	shutdown := utils.HeartbeatRoutine(c.ctx, 1*time.Minute, func() string {
		return fmt.Sprintf("syncing records to eventhub with"+
			" push parallelism %d and push batch size %d",
			req.PushParallelism, req.PushBatchSize)
	})
	defer func() {
		shutdown <- true
	}()

	batch := req.Records
	eventsPerHeartBeat := 1000
	eventsPerBatch := int(req.PushBatchSize)
	maxParallelism := req.PushParallelism

	batchPerTopic := make(map[string][]*eventhub.Event)
	for i, record := range batch.Records {
		json, err := record.GetItems().ToJSON()
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Infof("failed to convert record to json: %v", err)
			return nil, err
		}

		// TODO (kaushik): this is a hack to get the table name.
		topicName := record.GetTableName()

		if _, ok := batchPerTopic[topicName]; !ok {
			batchPerTopic[topicName] = make([]*eventhub.Event, 0)
		}

		batchPerTopic[topicName] = append(batchPerTopic[topicName], eventhub.NewEventFromString(json))

		if i%eventsPerHeartBeat == 0 {
			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("sent %d records to hub: %s", i, topicName))
		}

		if (i+1)%eventsPerBatch == 0 {
			err := c.sendEventBatch(batchPerTopic, maxParallelism,
				req.FlowJobName)
			if err != nil {
				return nil, err
			}

			batchPerTopic = make(map[string][]*eventhub.Event)
		}
	}

	// send the remaining events.
	if len(batchPerTopic) > 0 {
		err := c.sendEventBatch(batchPerTopic, maxParallelism,
			req.FlowJobName)
		if err != nil {
			return nil, err
		}
	}

	log.WithFields(log.Fields{
		"flowName": req.FlowJobName,
	}).Infof("[total] successfully sent %d records to event hub", len(batch.Records))

	err := c.UpdateLastOffset(req.FlowJobName, batch.LastCheckPointID)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return nil, err
	}

	return &model.SyncResponse{
		FirstSyncedCheckPointID: batch.FirstCheckPointID,
		LastSyncedCheckPointID:  batch.LastCheckPointID,
		NumRecordsSynced:        int64(len(batch.Records)),
	}, nil
}

func (c *EventHubConnector) sendEventBatch(events map[string][]*eventhub.Event,
	maxParallelism int64,
	flowName string) error {
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
		go func(tblName string, eventBatch []*eventhub.Event) {
			defer func() {
				<-guard
				wg.Done()
			}()

			hub, err := c.getOrCreateHubConnection(tblName)
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("obtained hub connection and now sending %d events to event hub: %s",
				len(eventBatch), tblName)
			err = hub.SendBatch(subCtx, eventhub.NewEventBatchIterator(eventBatch...))
			if err != nil {
				once.Do(func() { firstErr = err })
				return
			}

			atomic.AddInt32(&numEventsPushed, int32(len(eventBatch)))
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s",
				numEventsPushed, tblName)
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

func (c *EventHubConnector) getOrCreateHubConnection(name string) (*eventhub.Hub, error) {
	hub, ok := c.hubs[name]
	if !ok {
		hub, err := eventhub.NewHub(c.config.GetNamespace(), name, c.tokenProvider)
		if err != nil {
			log.Errorf("failed to create event hub connection: %v", err)
			return nil, err
		}
		c.hubs[name] = hub
		return hub, nil
	}

	return hub, nil
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

func (c *EventHubConnector) GetTableSchema(
	req *protos.GetTableSchemaBatchInput) (*protos.GetTableSchemaBatchOutput, error) {
	panic("get table schema not implemented for event hub")
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

// Normalization

func (c *EventHubConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error) {
	log.Infof("normalization for event hub is a no-op")
	return nil, nil
}

func (c *EventHubConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	log.Infof("normalization for event hub is a no-op")
	return &model.NormalizeResponse{
		EndBatchID:   0,
		StartBatchID: 0,
		Done:         true,
	}, nil
}

// cleanup

func (c *EventHubConnector) PullFlowCleanup(jobName string) error {
	panic("pull flow cleanup not implemented for event hub")
}

func (c *EventHubConnector) SyncFlowCleanup(jobName string) error {
	// TODO (kaushik): this has to be implemented for DROP PEER support.
	panic("sync flow cleanup not implemented for event hub")
}
