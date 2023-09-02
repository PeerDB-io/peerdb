package e2e_eventhub

import (
	"context"
	"sync"

	"github.com/Azure/azure-amqp-common-go/v4/aad"
	"github.com/Azure/azure-amqp-common-go/v4/auth"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	log "github.com/sirupsen/logrus"
)

type EventHubTestHelper struct {
	creds         *azidentity.DefaultAzureCredential
	ehConfig      *protos.EventHubConfig
	tokenProvider auth.TokenProvider
}

func NewEventHubTestHelper(pgConf *protos.PostgresConfig) (*EventHubTestHelper, error) {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Errorf("failed to get default azure credentials: %v", err)
		return nil, err
	}
	log.Info("got default azure credentials")

	jwtTokenProvider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		log.Errorf("failed to get jwt token provider: %v", err)
		return nil, err
	}
	log.Info("got jwt token provider")

	ehConfig := &protos.EventHubConfig{
		Namespace:     "peerdb-dev",
		ResourceGroup: "peerdb-resource",
		Location:      "eastus",
		MetadataDb:    pgConf,
	}

	return &EventHubTestHelper{
		creds:         defaultAzureCreds,
		tokenProvider: jwtTokenProvider,
		ehConfig:      ehConfig,
	}, nil
}

func (h *EventHubTestHelper) GetPeer() *protos.Peer {
	return &protos.Peer{
		Name: "test_eh_peer",
		Type: protos.DBType_EVENTHUB,
		Config: &protos.Peer_EventhubConfig{
			EventhubConfig: h.ehConfig,
		},
	}
}

// consume all messages from the eventhub with the given name.
// returns as a list of strings.
func (h *EventHubTestHelper) ConsumeAllMessages(
	ctx context.Context,
	name string,
	expectedNum int,
) ([]string, error) {
	hub, err := eventhub.NewHub(h.ehConfig.Namespace, name, h.tokenProvider)
	if err != nil {
		log.Errorf("failed to create eventhub hub [%s]: %v", name, err)
		return nil, err
	}

	var messages []string

	// create a WaitGroup to wait for all messages to be consumed
	wg := sync.WaitGroup{}
	wg.Add(expectedNum)

	handler := func(c context.Context, event *eventhub.Event) error {
		messages = append(messages, string(event.Data))
		log.Infof("received message: %s", string(event.Data))
		wg.Done()
		return nil
	}

	// listen to each partition of the Event Hub
	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		log.Errorf("failed to get runtime info for eventhub [%s]: %v", name, err)
		return nil, err
	}

	var listenerHandles []*eventhub.ListenerHandle

	for _, partitionID := range runtimeInfo.PartitionIDs {
		// Start receiving messages
		//
		// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
		// <- listenerHandle.Done() signals listener has stopped
		// listenerHandle.Err() provides the last error the receiver encountered
		listenerHandle, err := hub.Receive(ctx, partitionID, handler)
		if err != nil {
			log.Errorf("failed to receive messages from eventhub [%s]: %v", name, err)
			return nil, err
		}

		listenerHandles = append(listenerHandles, listenerHandle)
	}

	// wait for all messages to be consumed
	wg.Wait()

	// close all the listeners
	for _, listenerHandle := range listenerHandles {
		listenerHandle.Close(ctx)
	}

	err = hub.Close(ctx)
	if err != nil {
		log.Errorf("failed to close eventhub [%s]: %v", name, err)
		return nil, err
	}

	return messages, nil
}

func (h *EventHubTestHelper) CleanUp() error {
	return nil
}
