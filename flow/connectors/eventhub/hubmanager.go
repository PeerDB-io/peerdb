package conneventhub

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	cmap "github.com/orcaman/concurrent-map/v2"
	log "github.com/sirupsen/logrus"
)

type EventHubManager struct {
	creds *azidentity.DefaultAzureCredential
	// eventhub peer name -> config
	peerConfig cmap.ConcurrentMap[string, *protos.EventHubConfig]
	// eventhub name -> client
	hubs sync.Map
}

func NewEventHubManager(
	creds *azidentity.DefaultAzureCredential,
	groupConfig *protos.EventHubGroupConfig,
) *EventHubManager {
	peerConfig := cmap.New[*protos.EventHubConfig]()

	for name, config := range groupConfig.Eventhubs {
		peerConfig.Set(name, config)
	}

	return &EventHubManager{
		creds:      creds,
		peerConfig: peerConfig,
	}
}

func (m *EventHubManager) GetOrCreateHubClient(name ScopedEventhub) (*azeventhubs.ProducerClient, error) {
	ehConfig, ok := m.peerConfig.Get(name.PeerName)
	if !ok {
		return nil, fmt.Errorf("eventhub '%s' not configured", name)
	}

	namespace := ehConfig.Namespace
	// if the namespace isn't fully qualified, add the `.servicebus.windows.net`
	// check by counting the number of '.' in the namespace
	if strings.Count(namespace, ".") < 2 {
		namespace = fmt.Sprintf("%s.servicebus.windows.net", namespace)
	}

	hub, ok := m.hubs.Load(name)
	if !ok {
		opts := &azeventhubs.ProducerClientOptions{}
		hub, err := azeventhubs.NewProducerClient(namespace, name.Eventhub, m.creds, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create eventhub client: %v", err)
		}
		m.hubs.Store(name, hub)
		return hub, nil
	}

	return hub.(*azeventhubs.ProducerClient), nil
}

// func (m *EventHubManager) Close() error {
// 	var globalErr error
// 	m.hubs.Range(func(key, value interface{}) bool {
// 		hub := value.(*azeventhubs.ProducerClient)
// 		err := hub.Close(m.ctx)
// 		if err != nil {
// 			log.Errorf("failed to close eventhub client: %v", err)
// 			globalErr = fmt.Errorf("failed to close eventhub client: %v", err)
// 			return false
// 		}
// 		return true
// 	})

// 	return globalErr
// }

func (m *EventHubManager) CreateEventDataBatch(ctx context.Context, name ScopedEventhub) (*azeventhubs.EventDataBatch, error) {
	hub, err := m.GetOrCreateHubClient(name)
	if err != nil {
		return nil, err
	}

	opts := &azeventhubs.EventDataBatchOptions{}
	batch, err := hub.NewEventDataBatch(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create event data batch: %v", err)
	}

	return batch, nil
}

// EnsureEventHubExists ensures that the eventhub exists.
func (m *EventHubManager) EnsureEventHubExists(ctx context.Context, name ScopedEventhub) error {
	cfg, ok := m.peerConfig.Get(name.PeerName)
	if !ok {
		return fmt.Errorf("eventhub peer '%s' not configured", name.PeerName)
	}

	hubClient, err := m.getEventHubMgmtClient(cfg.SubscriptionId)
	if err != nil {
		return fmt.Errorf("failed to get event hub client: %v", err)
	}

	namespace := cfg.Namespace
	resourceGroup := cfg.ResourceGroup

	_, err = hubClient.Get(ctx, resourceGroup, namespace, name.Eventhub, nil)

	partitionCount := int64(cfg.PartitionCount)
	retention := int64(cfg.MessageRetentionInDays)
	if err != nil {
		opts := armeventhub.Eventhub{
			Properties: &armeventhub.Properties{
				PartitionCount:         &partitionCount,
				MessageRetentionInDays: &retention,
			},
		}

		_, err := hubClient.CreateOrUpdate(ctx, resourceGroup, namespace, name.Eventhub, opts, nil)
		if err != nil {
			log.Errorf("failed to create event hub: %v", err)
			return err
		}

		log.Infof("event hub %s created", name)
	} else {
		log.Infof("event hub %s already exists", name)
	}

	return nil
}

func (m *EventHubManager) getEventHubMgmtClient(subID string) (*armeventhub.EventHubsClient, error) {
	if subID == "" {
		envSubID, err := utils.GetAzureSubscriptionID()
		if err != nil {
			log.Errorf("failed to get azure subscription id: %v", err)
			return nil, err
		}
		subID = envSubID
	}

	hubClient, err := armeventhub.NewEventHubsClient(subID, m.creds, nil)
	if err != nil {
		log.Errorf("failed to get event hub client: %v", err)
		return nil, err
	}

	return hubClient, nil
}
