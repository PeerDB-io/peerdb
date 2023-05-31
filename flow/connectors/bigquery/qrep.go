package connbigquery

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
)

type QRecordValueSaver struct {
	Record      *model.QRecord
	PartitionID string
	RunID       int64
}

func (q QRecordValueSaver) Save() (map[string]bigquery.Value, string, error) {
	bqValues := make(map[string]bigquery.Value, len(*q.Record))

	for k, v := range *q.Record {
		switch v.Kind {
		case model.QValueKindFloat:
			val, ok := v.Value.(float64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to float64", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindInteger:
			val, ok := v.Value.(int64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to int64", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindBoolean:
			val, ok := v.Value.(bool)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to bool", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindString:
			val, ok := v.Value.(string)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to string", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindETime:
			val, ok := v.Value.(*model.ExtendedTime)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to ExtendedTime", v.Value)
			}
			bqValues[k] = val.Time

		default:
			// Skip invalid QValueKind
		}
	}

	// add partition id to the map
	bqValues["PartitionID"] = q.PartitionID

	// add run id to the map
	bqValues["RunID"] = q.RunID

	// log the bigquery values
	// fmt.Printf("BigQuery Values: %v\n", bqValues)

	return bqValues, "", nil
}

func (c *BigQueryConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	bqTable := c.client.Dataset(c.datasetID).Table(destTable)
	tblMetadata, err := bqTable.Metadata(c.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata of table %s: %w", destTable, err)
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to check if partition %s is synced: %w",
			partition.PartitionId,
			err,
		)
	}

	if done {
		log.Infof("Partition %s has already been synced", partition.PartitionId)
		return 0, nil
	}

	startTime := time.Now()

	// generate a 128 bit random runID for this run
	runID := rand.Int63()

	// create a staging table with the same schema as the destination table if it doesn't exist
	stagingTable := fmt.Sprintf("%s_staging", destTable)
	stagingBQTable := c.client.Dataset(c.datasetID).Table(stagingTable)
	if _, err := stagingBQTable.Metadata(c.ctx); err != nil {
		metadata := &bigquery.TableMetadata{
			Name:   stagingTable,
			Schema: tblMetadata.Schema,
		}

		// add partitionID column with string type
		metadata.Schema = append(metadata.Schema, &bigquery.FieldSchema{
			Name: "partitionID",
			Type: bigquery.StringFieldType,
		})

		// add runID column with integer type
		metadata.Schema = append(metadata.Schema, &bigquery.FieldSchema{
			Name: "runID",
			Type: bigquery.IntegerFieldType,
		})

		// create the staging table
		if err := stagingBQTable.Create(c.ctx, metadata); err != nil {
			return 0, fmt.Errorf("failed to create staging table %s: %w", stagingTable, err)
		}
	}

	// get an inserter for the staging table and insert the records
	inserter := stagingBQTable.Inserter()

	// Step 2: Insert records into the staging table.
	numRowsInserted := 0
	for _, qRecord := range records.Records {
		toPut := QRecordValueSaver{
			Record:      qRecord,
			PartitionID: partition.PartitionId,
			RunID:       runID,
		}

		var vs bigquery.ValueSaver = toPut
		err = inserter.Put(c.ctx, vs)
		if err != nil {
			return -1, fmt.Errorf("failed to insert record into staging table: %v", err)
		}

		numRowsInserted++
	}

	// Copy the records into the destination table in a transaction.
	// append all the statements to one list
	stmts := []string{}
	stmts = append(stmts, "BEGIN TRANSACTION;")

	// col names for the destination table joined by comma
	colNames := []string{}
	for _, col := range tblMetadata.Schema {
		colNames = append(colNames, col.Name)
	}
	colNamesStr := strings.Join(colNames, ", ")

	paritionSelect := fmt.Sprintf("SELECT %s FROM %s.%s WHERE partitionID = '%s' AND runID = %d;",
		colNamesStr, c.datasetID, stagingTable, partition.PartitionId, runID)
	appendStmt := fmt.Sprintf("INSERT INTO %s.%s %s", c.datasetID, destTable, paritionSelect)
	stmts = append(stmts, appendStmt)

	insertMetadataStmt, err := c.createMetadataInsertStatement(
		partition,
		config.FlowJobName,
		startTime,
	)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}
	stmts = append(stmts, insertMetadataStmt)

	stmts = append(stmts, "COMMIT TRANSACTION;")

	// execute the statements in a transaction
	_, err = c.client.Query(strings.Join(stmts, "\n")).Read(c.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	log.Printf("pushed %d records to %s.%s", numRowsInserted, c.datasetID, destTable)
	return numRowsInserted, nil
}

func (c *BigQueryConnector) createMetadataInsertStatement(
	partition *protos.QRepPartition,
	jobName string,
	startTime time.Time) (string, error) {
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
			return fmt.Errorf(
				"table %s.%s already exists with different schema",
				c.datasetID,
				qRepMetadataTableName,
			)
		} else {
			return nil
		}
	}

	// table does not exist, create it
	err = table.Create(c.ctx, &bigquery.TableMetadata{
		Schema: qRepMetadataSchema,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to create table %s.%s: %w",
			c.datasetID,
			qRepMetadataTableName,
			err,
		)
	}

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
