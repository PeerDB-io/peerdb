package conneventhub

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

func (c *EventHubConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("setup qrep metadata tables not implemented for eventhub")
}

func (c *EventHubConnector) GetQRepPartitions(
	config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	panic("get qrep partitions not implemented for eventhub")
}

func (c *EventHubConnector) PullQRepRecords(
	config *protos.QRepConfig, partition *protos.QRepPartition) (*model.QRecordBatch, error) {
	panic("pull qrep records not implemented for eventhub")
}

func (c *EventHubConnector) SyncQRepRecords(
	config *protos.QRepConfig, partition *protos.QRepPartition, records *model.QRecordStream) (int, error) {
	panic("sync qrep records not implemented for eventhub")
}

func (c *EventHubConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	panic("consolidate qrep partitions not implemented for eventhub")
}

func (c *EventHubConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	panic("cleanup qrep flow not implemented for eventhub")
}
