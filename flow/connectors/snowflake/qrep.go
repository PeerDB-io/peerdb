package connsnowflake

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

func (c *SnowflakeConnector) GetQRepPartitions(last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) PullQRepRecords(partition *protos.QRepPartition) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) SyncQRepRecords(
	partition *protos.QRepPartition,
	records *model.QRecordBatch) (int, error) {
	panic("not implemented")
}
