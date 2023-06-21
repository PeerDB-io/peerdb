package connkafka

import (
	"database/sql"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

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

func (c *KafkaConnector) createMetadataInsertStatement(
	partition *protos.QRepPartition,
	jobName string,
	startTime time.Time,
) (string, error) {
	panic("not implemented")
}

func (c *KafkaConnector) getTableSchema(tableName string) ([]*sql.ColumnType, error) {
	panic("not implemented")
}

func (c *KafkaConnector) isPartitionSynced(partitionID string) (bool, error) {
	panic("not implemented")
}

func (c *KafkaConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	panic("not implemented")
}
