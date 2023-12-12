package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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

func (m *EventHubManager) GetOrCreateHubClient(ctx context.Context, name ScopedEventhub) (
	*azeventhubs.ProducerClient, error) {
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

	var hubConnectOK bool
	var hub any
	hub, hubConnectOK = m.hubs.Load(name)
	if hubConnectOK {
		hubTmp := hub.(*azeventhubs.ProducerClient)
		_, err := hubTmp.GetEventHubProperties(ctx, nil)
		if err != nil {
			log.Infof("eventhub %s not reachable. Will re-establish connection and re-create it. Err: %v", name, err)
			closeError := m.closeProducerClient(ctx, hubTmp)
			if closeError != nil {
				log.Errorf("failed to close producer client: %v", closeError)
			}
			m.hubs.Delete(name)
			hubConnectOK = false
		}
	}

	if !hubConnectOK {
		opts := &azeventhubs.ProducerClientOptions{
			RetryOptions: azeventhubs.RetryOptions{
				MaxRetries:    32,
				RetryDelay:    2 * time.Second,
				MaxRetryDelay: 16 * time.Second,
			},
		}
		hub, err := azeventhubs.NewProducerClient(namespace, name.Eventhub, m.creds, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create eventhub client: %v", err)
		}
		m.hubs.Store(name, hub)
		return hub, nil
	}

	return hub.(*azeventhubs.ProducerClient), nil
}

func (m *EventHubManager) closeProducerClient(ctx context.Context, pc *azeventhubs.ProducerClient) error {
	if pc != nil {
		return pc.Close(ctx)
	}
	return nil
}

func (m *EventHubManager) Close(ctx context.Context) error {
	var allErrors error

	m.hubs.Range(func(key any, value any) bool {
		name := key.(ScopedEventhub)
		hub := value.(*azeventhubs.ProducerClient)
		err := m.closeProducerClient(ctx, hub)
		if err != nil {
			log.Errorf("failed to close eventhub client for %s: %v", name, err)
			allErrors = errors.Join(allErrors, err)
		}
		return true
	})

	return allErrors
}

func (m *EventHubManager) CreateEventDataBatch(ctx context.Context, name ScopedEventhub) (
	*azeventhubs.EventDataBatch, error) {
	hub, err := m.GetOrCreateHubClient(ctx, name)
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
