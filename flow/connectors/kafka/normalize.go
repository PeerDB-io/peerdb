package connkafka

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/twmb/franz-go/pkg/kadm"
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
	kafkaAdminClient := kadm.NewClient(c.client)
	_, err := kafkaAdminClient.DescribeTopicConfigs(ctx, tableIdentifier)
	if err != nil && err != kerr.UnknownTopicOrPartition {
		return false, fmt.Errorf("failed to check topic existence: %w", err)
	}

	res, err := kafkaAdminClient.CreateTopic(ctx, c.partitions, 1, nil, tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("failed to create topic: %w", err)
	}
	if res.Err != nil {
		return false, fmt.Errorf("create topic response has error: %w", res.Err)
	}

	return false, nil
}
