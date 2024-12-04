package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type EventHubManager struct {
	creds *azidentity.DefaultAzureCredential
	// eventhub namespace name -> config
	namespaceToEventhubMap cmap.ConcurrentMap[string, *protos.EventHubConfig]
	// eventhub name -> client
	hubs sync.Map
}

func NewEventHubManager(
	creds *azidentity.DefaultAzureCredential,
	groupConfig *protos.EventHubGroupConfig,
) *EventHubManager {
	namespaceToEventhubMap := cmap.New[*protos.EventHubConfig]()

	for name, config := range groupConfig.Eventhubs {
		namespaceToEventhubMap.Set(name, config)
	}

	return &EventHubManager{
		creds:                  creds,
		namespaceToEventhubMap: namespaceToEventhubMap,
	}
}

func (m *EventHubManager) GetOrCreateHubClient(ctx context.Context, name ScopedEventhub) (
	*azeventhubs.ProducerClient, error,
) {
	ehConfig, ok := m.namespaceToEventhubMap.Get(name.NamespaceName)
	if !ok {
		return nil, fmt.Errorf("eventhub '%s' not configured", name.Eventhub)
	}

	namespace := ehConfig.Namespace
	// if the namespace isn't fully qualified, add the `.servicebus.windows.net`
	// check by counting the number of '.' in the namespace
	if strings.Count(namespace, ".") < 2 {
		namespace += ".servicebus.windows.net"
	}

	var hubConnectOK bool
	var hub any
	hub, hubConnectOK = m.hubs.Load(name)
	if hubConnectOK {
		hubTmp := hub.(*azeventhubs.ProducerClient)
		_, err := hubTmp.GetEventHubProperties(ctx, nil)
		if err != nil {
			logger := shared.LoggerFromCtx(ctx)
			logger.Info(
				fmt.Sprintf("eventhub %s not reachable. Will re-establish connection and re-create it.", name),
				slog.Any("error", err))
			closeError := m.closeProducerClient(ctx, hubTmp)
			if closeError != nil {
				logger.Error("failed to close producer client", slog.Any("error", closeError))
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
			shared.LoggerFromCtx(ctx).Error(fmt.Sprintf("failed to close eventhub client for %v", name), slog.Any("error", err))
			allErrors = errors.Join(allErrors, err)
		}
		return true
	})

	return allErrors
}

func (m *EventHubManager) CreateEventDataBatch(ctx context.Context, destination ScopedEventhub) (
	*azeventhubs.EventDataBatch, error,
) {
	hub, err := m.GetOrCreateHubClient(ctx, destination)
	if err != nil {
		return nil, err
	}

	opts := &azeventhubs.EventDataBatchOptions{
		// Eventhubs internally does the routing
		// to partition based on hash of the partition key.
		// Same partition key is guaranteed to map to same partition.
		PartitionKey: &destination.PartitionKeyValue,
	}
	batch, err := hub.NewEventDataBatch(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create event data batch: %v", err)
	}

	return batch, nil
}

// EnsureEventHubExists ensures that the eventhub exists.
func (m *EventHubManager) EnsureEventHubExists(ctx context.Context, name ScopedEventhub) error {
	cfg, ok := m.namespaceToEventhubMap.Get(name.NamespaceName)
	if !ok {
		return fmt.Errorf("eventhub namespace '%s' not registered", name.NamespaceName)
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
	logger := shared.LoggerFromCtx(ctx)
	if err != nil {
		opts := armeventhub.Eventhub{
			Properties: &armeventhub.Properties{
				PartitionCount:         &partitionCount,
				MessageRetentionInDays: &retention,
			},
		}

		_, err := hubClient.CreateOrUpdate(ctx, resourceGroup, namespace, name.Eventhub, opts, nil)
		if err != nil {
			slog.Error("failed to create event hub", slog.Any("error", err))
			return err
		}

		logger.Info("event hub created", slog.Any("name", name))
	} else {
		logger.Info("event hub exists already", slog.Any("name", name))
	}

	return nil
}

func (m *EventHubManager) getEventHubMgmtClient(subID string) (*armeventhub.EventHubsClient, error) {
	if subID == "" {
		envSubID := peerdbenv.GetEnvString("AZURE_SUBSCRIPTION_ID", "")
		if envSubID == "" {
			slog.Error("couldn't find AZURE_SUBSCRIPTION_ID in environment")
			return nil, errors.New("couldn't find AZURE_SUBSCRIPTION_ID in environment")
		}
		subID = envSubID
	}

	hubClient, err := armeventhub.NewEventHubsClient(subID, m.creds, nil)
	if err != nil {
		slog.Error("failed to get event hub client", slog.Any("error", err))
		return nil, err
	}

	return hubClient, nil
}
