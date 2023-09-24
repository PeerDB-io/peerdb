package conneventhub

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type EventHubManager struct {
	ctx       context.Context
	creds     *azidentity.DefaultAzureCredential
	namespace string
	hubs      cmap.ConcurrentMap[string, *azeventhubs.ProducerClient]
}

func NewEventHubManager(
	ctx context.Context,
	creds *azidentity.DefaultAzureCredential,
	namespace string,
) *EventHubManager {
	hubs := cmap.New[*azeventhubs.ProducerClient]()
	return &EventHubManager{
		ctx:       ctx,
		creds:     creds,
		namespace: namespace,
		hubs:      hubs,
	}
}

func (m *EventHubManager) GetOrCreateHub(name string) (*azeventhubs.ProducerClient, error) {
	hub, ok := m.hubs.Get(name)

	namespace := m.namespace
	// if the namespace isn't fully qualified, add the `.servicebus.windows.net`
	// check by counting the number of '.' in the namespace
	if strings.Count(namespace, ".") < 2 {
		namespace = fmt.Sprintf("%s.servicebus.windows.net", namespace)
	}

	if !ok {
		opts := &azeventhubs.ProducerClientOptions{}
		hub, err := azeventhubs.NewProducerClient(namespace, name, m.creds, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create eventhub client: %v", err)
		}
		m.hubs.Set(name, hub)
		return hub, nil
	}

	return hub, nil
}

func (m *EventHubManager) Close() error {
	for hub := range m.hubs.IterBuffered() {
		err := hub.Val.Close(m.ctx)
		if err != nil {
			return fmt.Errorf("failed to close eventhub client: %v", err)
		}
	}
	return nil
}

func (m *EventHubManager) CreateEventDataBatch(name string) (*azeventhubs.EventDataBatch, error) {
	hub, err := m.GetOrCreateHub(name)
	if err != nil {
		return nil, err
	}

	opts := &azeventhubs.EventDataBatchOptions{}
	batch, err := hub.NewEventDataBatch(m.ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create event data batch: %v", err)
	}

	return batch, nil
}
