package connkafka

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/twmb/franz-go/pkg/kerr"
)

func (c *KafkaConnector) StartSetupNormalizedTables(_ context.Context) (interface{}, error) {
	return nil, nil
}

func (c *KafkaConnector) FinishSetupNormalizedTables(_ context.Context, _ interface{}) error {
	return nil
}

func (c *KafkaConnector) CleanupSetupNormalizedTables(_ context.Context, _ interface{}) {
}

func (c *KafkaConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) (bool, error) {
	topicExists := true
	res, err := c.adminClient.DescribeTopicConfigs(ctx, tableIdentifier)
	if err == kerr.UnknownTopicOrPartition || res[0].Err == kerr.UnknownTopicOrPartition {
		c.logger.Info("topic does not exist, creating", "topic", tableIdentifier)
		topicExists = false
	} else if err != nil {
		return false, fmt.Errorf("failed to check topic existence: %w", err)
	} else if res[0].Err != nil {
		return false, fmt.Errorf("[topicErr]failed to check topic existence: %w", res[0].Err)
	}

	if !topicExists {
		res, err := c.adminClient.CreateTopics(ctx, c.partitions, 3, nil, tableIdentifier)
		if err != nil {
			return false, fmt.Errorf("failed to create topic: %w", err)
		}
		if res.Error() != nil {
			return false, fmt.Errorf("failed to create topic: %w", res.Error())
		}
	} else {
		c.logger.Info("topic exists, skipping creation", "topic", tableIdentifier)
	}

	return false, nil
}
