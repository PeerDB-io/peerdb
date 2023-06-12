package connbigquery

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

type QRepSyncMethod interface {
	SyncQRepRecords(
		flowJobName string,
		dstTableName string,
		partition *protos.QRepPartition,
		dstTableMetadata *bigquery.TableMetadata,
		records *model.QRecordBatch) (int, error)
}

type QRepStagingTableSync struct {
	connector *BigQueryConnector
}

func (s *QRepStagingTableSync) SyncQRepRecords(
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableMetadata *bigquery.TableMetadata,
	records *model.QRecordBatch) (int, error) {
	partitionID := partition.PartitionId

	startTime := time.Now()

	// generate a 128 bit random runID for this run
	runID := rand.Int63()

	// create a staging table with the same schema as the destination table if it doesn't exist
	stagingTable := fmt.Sprintf("%s_staging", dstTableName)
	stagingBQTable := s.connector.client.Dataset(s.connector.datasetID).Table(stagingTable)
	if _, err := stagingBQTable.Metadata(s.connector.ctx); err != nil {
		metadata := &bigquery.TableMetadata{
			Name:   stagingTable,
			Schema: dstTableMetadata.Schema,
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
		if err := stagingBQTable.Create(s.connector.ctx, metadata); err != nil {
			return 0, fmt.Errorf("failed to create staging table %s: %w", stagingTable, err)
		}
	}

	// get an inserter for the staging table and insert the records
	inserter := stagingBQTable.Inserter()

	// Step 2: Insert records into the staging table.
	numRowsInserted := 0
	for _, qRecord := range records.Records {
		toPut := QRecordValueSaver{
			ColumnNames: records.Schema.GetColumnNames(),
			Record:      qRecord,
			PartitionID: partitionID,
			RunID:       runID,
		}

		var vs bigquery.ValueSaver = toPut
		err := inserter.Put(s.connector.ctx, vs)
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
	for _, col := range dstTableMetadata.Schema {
		colNames = append(colNames, col.Name)
	}
	colNamesStr := strings.Join(colNames, ", ")

	paritionSelect := fmt.Sprintf("SELECT %s FROM %s.%s WHERE partitionID = '%s' AND runID = %d;",
		colNamesStr, s.connector.datasetID, stagingTable, partitionID, runID)
	appendStmt := fmt.Sprintf("INSERT INTO %s.%s %s", s.connector.datasetID, dstTableName, paritionSelect)
	stmts = append(stmts, appendStmt)

	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}
	stmts = append(stmts, insertMetadataStmt)

	stmts = append(stmts, "COMMIT TRANSACTION;")

	// execute the statements in a transaction
	_, err = s.connector.client.Query(strings.Join(stmts, "\n")).Read(s.connector.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	log.Printf("pushed %d records to %s.%s", numRowsInserted, s.connector.datasetID, dstTableName)
	return numRowsInserted, nil
}
