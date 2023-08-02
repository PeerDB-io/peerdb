package connbigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
)

type QRepAvroSyncMethod struct {
	connector *BigQueryConnector
	gcsBucket string
}

func NewQRepAvroSyncMethod(connector *BigQueryConnector, gcsBucket string) *QRepAvroSyncMethod {
	return &QRepAvroSyncMethod{
		connector: connector,
		gcsBucket: gcsBucket,
	}
}

func (s *QRepAvroSyncMethod) SyncQRepRecords(
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableMetadata *bigquery.TableMetadata,
	stream *model.QRecordStream,
) (int, error) {
	bqClient := s.connector.client
	datasetID := s.connector.datasetID
	startTime := time.Now()

	// You will need to define your Avro schema as a string
	avroSchema, nullable, err := DefineAvroSchema(dstTableName, dstTableMetadata)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	fmt.Printf("Avro schema: %s\n", avroSchema)

	ctx := context.Background()
	bucket := s.connector.storageClient.Bucket(s.gcsBucket)

	// Create a GCS object name with flowJobName and partitionID
	gcsObjectName := fmt.Sprintf("%s/%s.avro", flowJobName, partition.PartitionId)
	obj := bucket.Object(gcsObjectName)
	w := obj.NewWriter(ctx)

	// Create OCF Writer
	var ocfFileContents bytes.Buffer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &ocfFileContents,
		Schema: avroSchema,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
	}

	schema, err := stream.Schema()
	if err != nil {
		log.Errorf("failed to get schema from stream: %v", err)
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	numRecords := 0

	// Write each QRecord to the OCF file
	for qRecordOrErr := range stream.Records {
		if qRecordOrErr.Err != nil {
			log.Errorf("failed to get record from stream: %v", qRecordOrErr.Err)
			return 0, fmt.Errorf("failed to get record from stream: %w", qRecordOrErr.Err)
		}

		qRecord := qRecordOrErr.Record
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			qvalue.QDWHTypeBigQuery,
			&nullable,
			schema.GetColumnNames(),
		)
		avroMap, err := avroConverter.Convert()
		if err != nil {
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			return 0, fmt.Errorf("failed to write record to OCF file: %w", err)
		}

		numRecords++
	}

	// Write OCF contents to GCS
	if _, err = w.Write(ocfFileContents.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write OCF file to GCS: %w", err)
	}

	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("failed to close GCS object writer: %w", err)
	}

	// write this file to bigquery
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", s.gcsBucket, gcsObjectName))
	gcsRef.SourceFormat = bigquery.Avro

	// create a staging table name with partitionID replace hyphens with underscores
	stagingTable := fmt.Sprintf("%s_%s_staging", dstTableName, strings.ReplaceAll(partition.PartitionId, "-", "_"))

	loader := bqClient.Dataset(datasetID).Table(stagingTable).LoaderFrom(gcsRef)
	loader.UseAvroLogicalTypes = true

	job, err := loader.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to run BigQuery load job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to wait for BigQuery load job: %w", err)
	}

	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("failed to load Avro file into BigQuery table: %w", err)
	}

	// Start a transaction
	stmts := []string{"BEGIN TRANSACTION;"}

	// Insert the records from the staging table into the destination table
	insertStmt := fmt.Sprintf("INSERT INTO `%s.%s` SELECT * FROM `%s.%s`;",
		datasetID, dstTableName, datasetID, stagingTable)

	stmts = append(stmts, insertStmt)

	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}
	stmts = append(stmts, insertMetadataStmt)

	stmts = append(stmts, "COMMIT TRANSACTION;")

	// Execute the statements in a transaction
	syncRecordsStartTime := time.Now()
	_, err = bqClient.Query(strings.Join(stmts, "\n")).Read(s.connector.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}
	metrics.LogQRepSyncMetrics(s.connector.ctx, flowJobName,
		int64(numRecords), time.Since(syncRecordsStartTime))

	// drop the staging table
	if err := bqClient.Dataset(datasetID).Table(stagingTable).Delete(s.connector.ctx); err != nil {
		// just log the error this isn't fatal.
		log.Errorf("failed to delete staging table %s: %v", stagingTable, err)
	}

	log.Printf("pushed %d records to GCS %s/%s and loaded into %s.%s",
		numRecords, s.gcsBucket, gcsObjectName, datasetID, dstTableName)
	return numRecords, nil
}

type AvroField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

type AvroSchema struct {
	Type   string      `json:"type"`
	Name   string      `json:"name"`
	Fields []AvroField `json:"fields"`
}

func DefineAvroSchema(dstTableName string, dstTableMetadata *bigquery.TableMetadata) (string, map[string]bool, error) {
	avroFields := []AvroField{}
	nullableFields := map[string]bool{}

	for _, bqField := range dstTableMetadata.Schema {
		avroType, err := GetAvroType(bqField)
		if err != nil {
			return "", nil, err
		}

		// If a field is nullable, its Avro type should be ["null", actualType]
		if !bqField.Required {
			avroType = []interface{}{"null", avroType}
			nullableFields[bqField.Name] = true
		}

		avroFields = append(avroFields, AvroField{
			Name: bqField.Name,
			Type: avroType,
		})
	}

	avroSchema := AvroSchema{
		Type:   "record",
		Name:   dstTableName,
		Fields: avroFields,
	}

	avroSchemaJSON, err := json.Marshal(avroSchema)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal Avro schema to JSON: %v", err)
	}

	return string(avroSchemaJSON), nullableFields, nil
}

func GetAvroType(bqField *bigquery.FieldSchema) (interface{}, error) {
	switch bqField.Type {
	case bigquery.StringFieldType:
		return "string", nil
	case bigquery.BytesFieldType:
		return "bytes", nil
	case bigquery.IntegerFieldType:
		return "long", nil
	case bigquery.FloatFieldType:
		return "double", nil
	case bigquery.BooleanFieldType:
		return "boolean", nil
	case bigquery.TimestampFieldType:
		return map[string]string{
			"type":        "long",
			"logicalType": "timestamp-micros",
		}, nil
	case bigquery.DateFieldType:
		return map[string]string{
			"type":        "long",
			"logicalType": "timestamp-micros",
		}, nil
	case bigquery.TimeFieldType:
		return map[string]string{
			"type":        "long",
			"logicalType": "timestamp-micros",
		}, nil
	case bigquery.DateTimeFieldType:
		return map[string]interface{}{
			"type": "record",
			"name": "datetime",
			"fields": []map[string]string{
				{
					"name":        "date",
					"type":        "int",
					"logicalType": "date",
				},
				{
					"name":        "time",
					"type":        "long",
					"logicalType": "time-micros",
				},
			},
		}, nil
	case bigquery.NumericFieldType:
		return map[string]interface{}{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   38,
			"scale":       9,
		}, nil
	case bigquery.RecordFieldType:
		avroFields := []map[string]interface{}{}
		for _, bqSubField := range bqField.Schema {
			avroType, err := GetAvroType(bqSubField)
			if err != nil {
				return nil, err
			}
			avroFields = append(avroFields, map[string]interface{}{
				"name": bqSubField.Name,
				"type": avroType,
			})
		}
		return map[string]interface{}{
			"type":   "record",
			"name":   bqField.Name,
			"fields": avroFields,
		}, nil
	// TODO(kaushik/sai): Add other field types as needed
	default:
		return nil, fmt.Errorf("unsupported BigQuery field type: %s", bqField.Type)
	}
}
