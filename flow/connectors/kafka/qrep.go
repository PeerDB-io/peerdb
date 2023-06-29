package connkafka

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

func (c *KafkaConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *KafkaConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *KafkaConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {
	panic("not implemented")
}

func (c *KafkaConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	return nil
}

func (c *KafkaConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("not implemented")
}
