package connbigquery

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

type QRepAvroSyncMethod struct {
	connector   *BigQueryConnector
	gcsBucket   string
	flowJobName string
}

func NewQRepAvroSyncMethod(connector *BigQueryConnector, gcsBucket string,
	flowJobName string) *QRepAvroSyncMethod {
	return &QRepAvroSyncMethod{
		connector:   connector,
		gcsBucket:   gcsBucket,
		flowJobName: flowJobName,
	}
}

func (s *QRepAvroSyncMethod) SyncRecords(
	dstTableName string,
	flowJobName string,
	lastCP int64,
	dstTableMetadata *bigquery.TableMetadata,
	syncBatchID int64,
	stream *model.QRecordStream,
) (int, error) {
	activity.RecordHeartbeat(s.connector.ctx, time.Minute,
		fmt.Sprintf("Flow job %s: Obtaining Avro schema"+
			" for destination table %s and sync batch ID %d",
			flowJobName, dstTableName, syncBatchID),
	)
	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(dstTableName, dstTableMetadata)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	stagingTable := fmt.Sprintf("%s_%s_staging", dstTableName, fmt.Sprint(syncBatchID))
	numRecords, err := s.writeToStage(fmt.Sprint(syncBatchID), dstTableName, avroSchema, stagingTable, stream)
	if err != nil {
		return -1, fmt.Errorf("failed to push to avro stage: %v", err)
	}

	bqClient := s.connector.client
	datasetID := s.connector.datasetID
	insertStmt := fmt.Sprintf("INSERT INTO `%s.%s` SELECT * FROM `%s.%s`;",
		datasetID, dstTableName, datasetID, stagingTable)
	updateMetadataStmt, err := s.connector.getUpdateMetadataStmt(flowJobName, lastCP, syncBatchID)
	if err != nil {
		return -1, fmt.Errorf("failed to update metadata: %v", err)
	}

	activity.RecordHeartbeat(s.connector.ctx, time.Minute,
		fmt.Sprintf("Flow job %s: performing insert and update transaction"+
			" for destination table %s and sync batch ID %d",
			flowJobName, dstTableName, syncBatchID),
	)

	// execute the statements in a transaction
	stmts := []string{}
	stmts = append(stmts, "BEGIN TRANSACTION;")
	stmts = append(stmts, insertStmt)
	stmts = append(stmts, updateMetadataStmt)
	stmts = append(stmts, "COMMIT TRANSACTION;")
	_, err = bqClient.Query(strings.Join(stmts, "\n")).Read(s.connector.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	// drop the staging table
	if err := bqClient.Dataset(datasetID).Table(stagingTable).Delete(s.connector.ctx); err != nil {
		// just log the error this isn't fatal.
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"syncBatchID":      syncBatchID,
			"destinationTable": dstTableName,
		}).Errorf("failed to delete staging table %s: %v", stagingTable, err)
	}

	log.Printf("loaded stage into %s.%s", datasetID, dstTableName)

	return numRecords, nil
}

func (s *QRepAvroSyncMethod) SyncQRepRecords(
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableMetadata *bigquery.TableMetadata,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()

	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(dstTableName, dstTableMetadata)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("Obtained Avro schema for destination table %s and partition ID %s",
		dstTableName, partition.PartitionId)
	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("Avro schema: %v\n", avroSchema)
	// create a staging table name with partitionID replace hyphens with underscores
	stagingTable := fmt.Sprintf("%s_%s_staging", dstTableName, strings.ReplaceAll(partition.PartitionId, "-", "_"))
	numRecords, err := s.writeToStage(partition.PartitionId, flowJobName, avroSchema, stagingTable, stream)
	if err != nil {
		return -1, fmt.Errorf("failed to push to avro stage: %v", err)
	}
	activity.RecordHeartbeat(s.connector.ctx, fmt.Sprintf(
		"Flow job %s: running insert-into-select transaction for"+
			" destination table %s and partition ID %s",
		flowJobName, dstTableName, partition.PartitionId),
	)
	bqClient := s.connector.client
	datasetID := s.connector.datasetID
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
	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("Performing transaction inside QRep sync function for partition ID %s",
		partition.PartitionId)
	stmts = append(stmts, insertMetadataStmt)
	stmts = append(stmts, "COMMIT TRANSACTION;")
	// Execute the statements in a transaction
	_, err = bqClient.Query(strings.Join(stmts, "\n")).Read(s.connector.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	// drop the staging table
	if err := bqClient.Dataset(datasetID).Table(stagingTable).Delete(s.connector.ctx); err != nil {
		// just log the error this isn't fatal.
		log.WithFields(log.Fields{
			"flowName":         flowJobName,
			"partitionID":      partition.PartitionId,
			"destinationTable": dstTableName,
		}).Errorf("failed to delete staging table %s: %v", stagingTable, err)
	}

	log.WithFields(log.Fields{
		"flowName":    flowJobName,
		"partitionID": partition.PartitionId,
	}).Infof("loaded stage into %s.%s",
		datasetID, dstTableName)
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

func DefineAvroSchema(dstTableName string,
	dstTableMetadata *bigquery.TableMetadata) (*model.QRecordAvroSchemaDefinition, error) {
	avroFields := []AvroField{}
	nullableFields := make(map[string]struct{})

	for _, bqField := range dstTableMetadata.Schema {
		avroType, err := GetAvroType(bqField)
		if err != nil {
			return nil, err
		}

		// If a field is nullable, its Avro type should be ["null", actualType]
		if !bqField.Required {
			avroType = []interface{}{"null", avroType}
			nullableFields[bqField.Name] = struct{}{}
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
		return nil, fmt.Errorf("failed to marshal Avro schema to JSON: %v", err)
	}

	return &model.QRecordAvroSchemaDefinition{
		Schema:         string(avroSchemaJSON),
		NullableFields: nullableFields,
	}, nil
}

func GetAvroType(bqField *bigquery.FieldSchema) (interface{}, error) {
	considerRepeated := func(typ string, repeated bool) interface{} {
		if repeated {
			return map[string]interface{}{
				"type":  "array",
				"items": typ,
			}
		} else {
			return typ
		}
	}

	switch bqField.Type {
	case bigquery.StringFieldType:
		return considerRepeated("string", bqField.Repeated), nil
	case bigquery.BytesFieldType:
		return "bytes", nil
	case bigquery.IntegerFieldType:
		return considerRepeated("long", bqField.Repeated), nil
	case bigquery.FloatFieldType:
		return considerRepeated("double", bqField.Repeated), nil
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

func (s *QRepAvroSyncMethod) writeToStage(
	syncID string,
	objectFolder string,
	avroSchema *model.QRecordAvroSchemaDefinition,
	stagingTable string,
	stream *model.QRecordStream,
) (int, error) {
	shutdown := utils.HeartbeatRoutine(s.connector.ctx, time.Minute,
		func() string {
			return fmt.Sprintf("writing to avro stage for objectFolder %s and staging table %s",
				objectFolder, stagingTable)
		},
	)
	defer func() {
		shutdown <- struct{}{}
	}()

	var avroFile *avro.AvroFile
	ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema,
		avro.CompressNone, qvalue.QDWHTypeBigQuery)
	if s.gcsBucket != "" {
		bucket := s.connector.storageClient.Bucket(s.gcsBucket)
		avroFilePath := fmt.Sprintf("%s/%s.avro", objectFolder, syncID)
		obj := bucket.Object(avroFilePath)
		w := obj.NewWriter(s.connector.ctx)

		numRecords, err := ocfWriter.WriteOCF(w)
		if err != nil {
			return 0, fmt.Errorf("failed to write records to Avro file on GCS: %w", err)
		}
		avroFile = &avro.AvroFile{
			NumRecords:      numRecords,
			StorageLocation: avro.AvroGCSStorage,
			FilePath:        avroFilePath,
		}
	} else {
		tmpDir := fmt.Sprintf("%s/peerdb-avro-%s", os.TempDir(), s.flowJobName)
		err := os.MkdirAll(tmpDir, os.ModePerm)
		if err != nil {
			return 0, fmt.Errorf("failed to create temp dir: %w", err)
		}

		avroFilePath := fmt.Sprintf("%s/%s.avro", tmpDir, syncID)
		log.WithFields(log.Fields{
			"batchOrPartitionID": syncID,
		}).Infof("writing records to local file %s", avroFilePath)
		avroFile, err = ocfWriter.WriteRecordsToAvroFile(avroFilePath)
		if err != nil {
			return 0, fmt.Errorf("failed to write records to local Avro file: %w", err)
		}
	}
	defer avroFile.Cleanup()

	if avroFile.NumRecords == 0 {
		return 0, nil
	}
	log.WithFields(log.Fields{
		"batchOrPartitionID": syncID,
	}).Infof("wrote %d records to file %s", avroFile.NumRecords, avroFile.FilePath)

	bqClient := s.connector.client
	datasetID := s.connector.datasetID
	var avroRef bigquery.LoadSource
	if s.gcsBucket != "" {
		gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", s.gcsBucket, avroFile.FilePath))
		gcsRef.SourceFormat = bigquery.Avro
		gcsRef.Compression = bigquery.Deflate
		avroRef = gcsRef
	} else {
		fh, err := os.Open(avroFile.FilePath)
		if err != nil {
			return 0, fmt.Errorf("failed to read local Avro file: %w", err)
		}
		localRef := bigquery.NewReaderSource(fh)
		localRef.SourceFormat = bigquery.Avro
		avroRef = localRef
	}

	loader := bqClient.Dataset(datasetID).Table(stagingTable).LoaderFrom(avroRef)
	loader.UseAvroLogicalTypes = true
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(s.connector.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to run BigQuery load job: %w", err)
	}

	status, err := job.Wait(s.connector.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to wait for BigQuery load job: %w", err)
	}

	if len(status.Errors) > 0 {
		return 0, fmt.Errorf("failed to load Avro file into BigQuery table: %v", status.Errors)
	}

	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("failed to load Avro file into BigQuery table: %w", err)
	}

	log.Infof("Pushed into %s/%s", avroFile.FilePath, syncID)
	return avroFile.NumRecords, nil
}
