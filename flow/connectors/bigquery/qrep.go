package connbigquery

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

type QRecordValueSaver struct {
	Record      *model.QRecord
	PartitionID string
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

	paritionSelect := fmt.Sprintf("SELECT %s FROM %s.%s WHERE partitionID = '%s';",
		colNamesStr, c.datasetID, stagingTable, partition.PartitionId)
	appendStmt := fmt.Sprintf("INSERT INTO %s.%s %s", c.datasetID, destTable, paritionSelect)
	stmts = append(stmts, appendStmt)
	stmts = append(stmts, "COMMIT TRANSACTION;")

	// execute the statements in a transaction
	_, err = c.client.Query(strings.Join(stmts, "\n")).Read(c.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	log.Printf("pushed %d records to %s.%s", numRowsInserted, c.datasetID, destTable)
	return numRowsInserted, nil
}
