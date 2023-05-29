package connbigquery

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

func (c *BigQueryConnector) GetQRepPartitions(last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) PullQRepRecords(partition *protos.QRepPartition) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *BigQueryConnector) SyncQRepRecords(
	partition *protos.QRepPartition,
	records *model.QRecordBatch) (int, error) {
	// Ensure the destination table is available.
	destTable := partition.Config.DestinationTableIdentifier
	bqTable := c.client.Dataset(c.datasetID).Table(destTable)
	tblMetadata, err := bqTable.Metadata(c.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata of table %s: %w", destTable, err)
	}

	// create a staging table with the same schema as the destination table if it doesn't exist
	stagingTable := fmt.Sprintf("%s_staging", destTable)
	stagingBQTable := c.client.Dataset(c.datasetID).Table(stagingTable)
	if _, err := stagingBQTable.Metadata(c.ctx); err != nil {
		metadata := tblMetadata
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
		toPut := make(map[string]interface{})
		for k, v := range *qRecord {
			// TODO: handle complex types.
			toPut[k] = v.Value
		}
		toPut["partitionID"] = partition.PartitionId
		err := inserter.Put(c.ctx, toPut)
		if err != nil {
			return -1, fmt.Errorf("failed to insert record into staging table: %v", err)
		}

		numRowsInserted++
	}

	// Copy the records into the destination table in a transaction.
	// append all the statements to one list
	stmts := []string{}
	stmts = append(stmts, "BEGIN TRANSACTION;")
	paritionSelect := fmt.Sprintf("SELECT * FROM %s WHERE partitionID = '%s';", stagingTable, partition.PartitionId)
	appendStmt := fmt.Sprintf("INSERT INTO %s %s", destTable, paritionSelect)
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
