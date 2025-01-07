package e2e_eventhubs

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// consume all messages from the eventhub with the given name“.
// returns as a list of strings.
func ConsumeAllMessages(
	ctx context.Context,
	namespaceName string,
	eventhubName string,
	expectedNum int,
	config *protos.EventHubGroupConfig,
) ([]string, error) {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure creds: %w", err)
	}

	opts := &azeventhubs.ConsumerClientOptions{
		RetryOptions: azeventhubs.RetryOptions{
			MaxRetries:    32,
			RetryDelay:    2 * time.Second,
			MaxRetryDelay: 16 * time.Second,
		},
	}

	ehClient, err := azeventhubs.NewConsumerClient(
		namespaceName+".servicebus.windows.net",
		eventhubName,
		azeventhubs.DefaultConsumerGroup,
		defaultAzureCreds,
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create eventhub client: %w", err)
	}

	messages := make([]string, 0, expectedNum)
	partitionClient, err := ehClient.NewPartitionClient("0", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition client: %w", err)
	}

	defer func() {
		partitionClient.Close(ctx)
		ehClient.Close(ctx)
	}()

	events, receiveErr := partitionClient.ReceiveEvents(ctx, 1, nil)
	if receiveErr != nil {
		return nil, fmt.Errorf("failed to receive events: %w", receiveErr)
	}

	for _, event := range events {
		messages = append(messages, string(event.Body))
	}

	return messages, nil
}

func DeleteEventhub(
	ctx context.Context,
	eventhubName string,
	eventhubConfig *protos.EventHubConfig,
) error {
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create azure creds: %w", err)
	}

	hubClient, err := armeventhub.NewEventHubsClient(eventhubConfig.SubscriptionId, defaultAzureCreds, nil)
	if err != nil {
		return fmt.Errorf("failed to create eventhub client: %w", err)
	}

	_, deleteErr := hubClient.Delete(ctx,
		eventhubConfig.ResourceGroup,
		eventhubConfig.Namespace,
		eventhubName,
		nil,
	)

	return deleteErr
}
