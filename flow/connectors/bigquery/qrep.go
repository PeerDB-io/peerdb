package connbigquery

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *BigQueryConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	srcSchema, err := stream.Schema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema of source table %s: %w", config.WatermarkTable, err)
	}
	tblMetadata, err := c.replayTableSchemaDeltasQRep(config, partition, srcSchema)
	if err != nil {
		return 0, err
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition %s is synced: %w", partition.PartitionId, err)
	}

	if done {
		c.logger.InfoContext(c.ctx, fmt.Sprintf("Partition %s has already been synced", partition.PartitionId))
		return 0, nil
	}
	c.logger.Info(fmt.Sprintf("QRep sync function called and partition existence checked for"+
		" partition %s of destination table %s",
		partition.PartitionId, destTable))

	avroSync := NewQRepAvroSyncMethod(c, config.StagingPath, config.FlowJobName)
	return avroSync.SyncQRepRecords(config.FlowJobName, destTable, partition,
		tblMetadata, stream, config.SyncedAtColName, config.SoftDeleteColName)
}

func (c *BigQueryConnector) replayTableSchemaDeltasQRep(config *protos.QRepConfig, partition *protos.QRepPartition,
	srcSchema *model.QRecordSchema,
) (*bigquery.TableMetadata, error) {
	destDatasetTable, _ := c.convertToDatasetTable(config.DestinationTableIdentifier)
	bqTable := c.client.DatasetInProject(c.projectID, destDatasetTable.dataset).Table(destDatasetTable.table)
	dstTableMetadata, err := bqTable.Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of table %s: %w", destDatasetTable, err)
	}

	tableSchemaDelta := &protos.TableSchemaDelta{
		SrcTableName: config.WatermarkTable,
		DstTableName: config.DestinationTableIdentifier,
	}

	for _, col := range srcSchema.Fields {
		hasColumn := false
		// check ignoring case
		for _, dstCol := range dstTableMetadata.Schema {
			if strings.EqualFold(col.Name, dstCol.Name) {
				hasColumn = true
				break
			}
		}

		if !hasColumn {
			c.logger.Info(fmt.Sprintf("adding column %s to destination table %s",
				col.Name, config.DestinationTableIdentifier),
				slog.String(string(shared.PartitionIDKey), partition.PartitionId))
			tableSchemaDelta.AddedColumns = append(tableSchemaDelta.AddedColumns, &protos.DeltaAddedColumn{
				ColumnName: col.Name,
				ColumnType: string(col.Type),
			})
		}
	}

	err = c.ReplayTableSchemaDeltas(config.FlowJobName, []*protos.TableSchemaDelta{tableSchemaDelta})
	if err != nil {
		return nil, fmt.Errorf("failed to add columns to destination table: %w", err)
	}
	dstTableMetadata, err = bqTable.Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of table %s: %w", destDatasetTable, err)
	}
	return dstTableMetadata, nil
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
		"INSERT INTO _peerdb_query_replication_metadata"+
			"(flowJobName, partitionID, syncPartition, syncStartTime, syncFinishTime) "+
			"VALUES ('%s', '%s', JSON '%s', TIMESTAMP('%s'), CURRENT_TIMESTAMP());",
		jobName, partition.PartitionId,
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
	table := c.client.DatasetInProject(c.projectID, c.datasetID).Table(qRepMetadataTableName)

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

	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		query := c.client.Query(fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		query.DefaultDatasetID = c.datasetID
		query.DefaultProjectID = c.projectID
		_, err = query.Read(c.ctx)
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
}

func (c *BigQueryConnector) isPartitionSynced(partitionID string) (bool, error) {
	queryString := fmt.Sprintf(
		"SELECT COUNT(*) FROM _peerdb_query_replication_metadata WHERE partitionID = '%s';",
		partitionID,
	)

	query := c.client.Query(queryString)
	query.DefaultDatasetID = c.datasetID
	query.DefaultProjectID = c.projectID
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
