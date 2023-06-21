package connbigquery

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
)

func (c *BigQueryConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	bqTable := c.client.Dataset(c.datasetID).Table(destTable)

	tblMetadata, err := bqTable.Metadata(c.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata of table %s: %w", destTable, err)
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition %s is synced: %w", partition.PartitionId, err)
	}

	if done {
		log.Infof("Partition %s has already been synced", partition.PartitionId)
		return 0, nil
	}

	syncMode := config.SyncMode
	switch syncMode {
	case protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT:
		stagingTableSync := &QRepStagingTableSync{connector: c}
		return stagingTableSync.SyncQRepRecords(config.FlowJobName, destTable, partition, tblMetadata, records)
	case protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO:
		avroSync := &QRepAvroSyncMethod{connector: c, gcsBucket: "peerdb_staging"}
		return avroSync.SyncQRepRecords(config.FlowJobName, destTable, partition, tblMetadata, records)
	default:
		return 0, fmt.Errorf("unsupported sync mode: %s", syncMode)
	}
}

func (c *BigQueryConnector) createMetadataInsertStatement(
	partition *protos.QRepPartition,
	jobName string,
	startTime time.Time,
) (string, error) {
	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return "", fmt.Errorf("failed to marshal partition to json: %v", err)
	}

	// convert the bytes to string
	partitionJSON := string(pbytes)

	insertMetadataStmt := fmt.Sprintf(
		"INSERT INTO %s._peerdb_query_replication_metadata"+
			"(flowJobName, partitionID, syncPartition, syncStartTime, syncFinishTime) "+
			"VALUES ('%s', '%s', JSON '%s', TIMESTAMP('%s'), CURRENT_TIMESTAMP());",
		c.datasetID, jobName, partition.PartitionId,
		partitionJSON, startTime.Format(time.RFC3339))

	return insertMetadataStmt, nil
}

func (c *BigQueryConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	qRepMetadataTableName := "_peerdb_query_replication_metadata"

	// define the schema
	qRepMetadataSchema := bigquery.Schema{
		{Name: "flowJobName", Type: bigquery.StringFieldType},
		{Name: "partitionID", Type: bigquery.StringFieldType},
		{Name: "syncPartition", Type: bigquery.JSONFieldType},
		{Name: "syncStartTime", Type: bigquery.TimestampFieldType},
		{Name: "syncFinishTime", Type: bigquery.TimestampFieldType},
	}

	// reference the table
	table := c.client.Dataset(c.datasetID).Table(qRepMetadataTableName)

	// check if the table exists
	meta, err := table.Metadata(c.ctx)
	if err == nil {
		// table exists, check if the schema matches
		if !reflect.DeepEqual(meta.Schema, qRepMetadataSchema) {
			return fmt.Errorf("table %s.%s already exists with different schema", c.datasetID, qRepMetadataTableName)
		} else {
			return nil
		}
	}

	// table does not exist, create it
	err = table.Create(c.ctx, &bigquery.TableMetadata{
		Schema: qRepMetadataSchema,
	})
	if err != nil {
		return fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, qRepMetadataTableName, err)
	}

	return nil
}

func (c *BigQueryConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	log.Infof("Consolidating partitions for flow job %s", config.FlowJobName)
	log.Infof("This is a no-op for BigQuery")
	return nil
}

func (c *BigQueryConnector) isPartitionSynced(partitionID string) (bool, error) {
	queryString := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s._peerdb_query_replication_metadata WHERE partitionID = '%s';",
		c.datasetID, partitionID,
	)

	query := c.client.Query(queryString)
	it, err := query.Read(c.ctx)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	var values []bigquery.Value
	err = it.Next(&values)
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to iterate query results: %w", err)
	}

	if len(values) != 1 {
		return false, fmt.Errorf("expected 1 value, got %d", len(values))
	}

	count, ok := values[0].(int64)
	if !ok {
		return false, fmt.Errorf("failed to convert %v to int64", reflect.TypeOf(values[0]))
	}

	return count > 0, nil
}
