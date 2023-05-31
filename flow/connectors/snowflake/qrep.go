package connsnowflake

import (
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

func (c *SnowflakeConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("SetupQRepMetadataTables not implemented for snowflake connector")
}
