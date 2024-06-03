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
	_, err := c.adminClient.DescribeTopicConfigs(ctx, tableIdentifier)
	if err != nil && err != kerr.UnknownTopicOrPartition {
		return false, fmt.Errorf("failed to check topic existence: %w", err)
	}

	res, err := c.adminClient.CreateTopics(ctx, c.partitions, 1, nil, tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("failed to create topic: %w", err)
	}
	if res.Error() != nil {
		return false, fmt.Errorf("failed to create topic: %w", res.Error())
	}

	return false, nil
}
