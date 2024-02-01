package connbigquery

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type QRepAvroSyncMethod struct {
	connector   *BigQueryConnector
	gcsBucket   string
	flowJobName string
}

func NewQRepAvroSyncMethod(connector *BigQueryConnector, gcsBucket string,
	flowJobName string,
) *QRepAvroSyncMethod {
	return &QRepAvroSyncMethod{
		connector:   connector,
		gcsBucket:   gcsBucket,
		flowJobName: flowJobName,
	}
}

func (s *QRepAvroSyncMethod) SyncRecords(
	req *model.SyncRecordsRequest,
	rawTableName string,
	dstTableMetadata *bigquery.TableMetadata,
	syncBatchID int64,
	stream *model.QRecordStream,
	tableNameRowsMapping map[string]uint32,
) (*model.SyncResponse, error) {
	activity.RecordHeartbeat(s.connector.ctx,
		fmt.Sprintf("Flow job %s: Obtaining Avro schema"+
			" for destination table %s and sync batch ID %d",
			req.FlowJobName, rawTableName, syncBatchID),
	)
	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(rawTableName, dstTableMetadata, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	stagingTable := fmt.Sprintf("%s_%s_staging", rawTableName, strconv.FormatInt(syncBatchID, 10))
	numRecords, err := s.writeToStage(strconv.FormatInt(syncBatchID, 10), rawTableName, avroSchema,
		&datasetTable{
			project: s.connector.projectID,
			dataset: s.connector.datasetID,
			table:   stagingTable,
		}, stream, req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to push to avro stage: %w", err)
	}

	bqClient := s.connector.client
	datasetID := s.connector.datasetID
	insertStmt := fmt.Sprintf("INSERT INTO `%s` SELECT * FROM `%s`;",
		rawTableName, stagingTable)

	lastCP, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	activity.RecordHeartbeat(s.connector.ctx,
		fmt.Sprintf("Flow job %s: performing insert and update transaction"+
			" for destination table %s and sync batch ID %d",
			req.FlowJobName, rawTableName, syncBatchID),
	)

	err = s.connector.ReplayTableSchemaDeltas(req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	query := bqClient.Query(insertStmt)
	query.DefaultDatasetID = s.connector.datasetID
	query.DefaultProjectID = s.connector.projectID
	_, err = query.Read(s.connector.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statements in a transaction: %w", err)
	}

	err = s.connector.pgMetadata.FinishBatch(req.FlowJobName, syncBatchID, lastCP)
	if err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}

	// drop the staging table
	if err := bqClient.DatasetInProject(s.connector.projectID, datasetID).Table(stagingTable).Delete(s.connector.ctx); err != nil {
		// just log the error this isn't fatal.
		slog.Error("failed to delete staging table "+stagingTable,
			slog.Any("error", err),
			slog.Int64("syncBatchID", syncBatchID),
			slog.String("destinationTable", rawTableName))
	}

	s.connector.logger.Info(fmt.Sprintf("loaded stage into %s.%s", datasetID, rawTableName),
		slog.String(string(shared.FlowNameKey), req.FlowJobName),
		slog.String("dstTableName", rawTableName))

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCP,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func getTransformedColumns(dstSchema *bigquery.Schema, syncedAtCol string, softDeleteCol string) []string {
	transformedColumns := make([]string, 0, len(*dstSchema))
	for _, col := range *dstSchema {
		if col.Name == syncedAtCol || col.Name == softDeleteCol {
			continue
		}
		switch col.Type {
		case bigquery.GeographyFieldType:
			transformedColumns = append(transformedColumns,
				fmt.Sprintf("ST_GEOGFROMTEXT(`%s`) AS `%s`", col.Name, col.Name))
		case bigquery.JSONFieldType:
			transformedColumns = append(transformedColumns,
				fmt.Sprintf("PARSE_JSON(`%s`,wide_number_mode=>'round') AS `%s`", col.Name, col.Name))
		default:
			transformedColumns = append(transformedColumns, fmt.Sprintf("`%s`", col.Name))
		}
	}
	return transformedColumns
}

func (s *QRepAvroSyncMethod) SyncQRepRecords(
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableMetadata *bigquery.TableMetadata,
	stream *model.QRecordStream,
	syncedAtCol string,
	softDeleteCol string,
) (int, error) {
	startTime := time.Now()
	flowLog := slog.Group("sync_metadata",
		slog.String(string(shared.FlowNameKey), flowJobName),
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", dstTableName),
	)
	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(dstTableName, dstTableMetadata, syncedAtCol, softDeleteCol)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	s.connector.logger.Info("Obtained Avro schema for destination table", flowLog)
	s.connector.logger.Info(fmt.Sprintf("Avro schema: %v\n", avroSchema), flowLog)
	// create a staging table name with partitionID replace hyphens with underscores
	dstDatasetTable, _ := s.connector.convertToDatasetTable(dstTableName)
	stagingDatasetTable := &datasetTable{
		project: s.connector.projectID,
		dataset: dstDatasetTable.dataset,
		table: fmt.Sprintf("%s_%s_staging", dstDatasetTable.table,
			strings.ReplaceAll(partition.PartitionId, "-", "_")),
	}
	numRecords, err := s.writeToStage(partition.PartitionId, flowJobName, avroSchema,
		stagingDatasetTable, stream, flowJobName)
	if err != nil {
		return -1, fmt.Errorf("failed to push to avro stage: %w", err)
	}
	activity.RecordHeartbeat(s.connector.ctx, fmt.Sprintf(
		"Flow job %s: running insert-into-select transaction for"+
			" destination table %s and partition ID %s",
		flowJobName, dstTableName, partition.PartitionId),
	)
	bqClient := s.connector.client

	transformedColumns := getTransformedColumns(&dstTableMetadata.Schema, syncedAtCol, softDeleteCol)
	selector := strings.Join(transformedColumns, ", ")

	if softDeleteCol != "" { // PeerDB column
		selector += ", FALSE"
	}
	if syncedAtCol != "" { // PeerDB column
		selector += ", CURRENT_TIMESTAMP"
	}
	// Insert the records from the staging table into the destination table
	insertStmt := fmt.Sprintf("INSERT INTO `%s` SELECT %s FROM `%s`;",
		dstTableName, selector, stagingDatasetTable.string())

	s.connector.logger.Info("Performing transaction inside QRep sync function", flowLog)

	query := bqClient.Query(insertStmt)
	query.DefaultDatasetID = s.connector.datasetID
	query.DefaultProjectID = s.connector.projectID
	_, err = query.Read(s.connector.ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to execute statements in a transaction: %w", err)
	}

	err = s.connector.pgMetadata.FinishQrepPartition(partition, flowJobName, startTime)
	if err != nil {
		return -1, err
	}

	// drop the staging table
	if err := bqClient.DatasetInProject(s.connector.projectID, stagingDatasetTable.dataset).
		Table(stagingDatasetTable.table).Delete(s.connector.ctx); err != nil {
		// just log the error this isn't fatal.
		slog.Error("failed to delete staging table "+stagingDatasetTable.string(),
			slog.Any("error", err),
			flowLog)
	}

	s.connector.logger.Info(fmt.Sprintf("loaded stage into %s", dstTableName), flowLog)
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
	dstTableMetadata *bigquery.TableMetadata,
	syncedAtCol string,
	softDeleteCol string,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroFields := []AvroField{}
	nullableFields := make(map[string]struct{})

	for _, bqField := range dstTableMetadata.Schema {
		if bqField.Name == syncedAtCol || bqField.Name == softDeleteCol {
			continue
		}
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
		return nil, fmt.Errorf("failed to marshal Avro schema to JSON: %w", err)
	}

	return &model.QRecordAvroSchemaDefinition{
		Schema:         string(avroSchemaJSON),
		NullableFields: nullableFields,
	}, nil
}

func GetAvroType(bqField *bigquery.FieldSchema) (interface{}, error) {
	considerRepeated := func(typ string, repeated bool) interface{} {
		if repeated {
			return qvalue.AvroSchemaArray{
				Type:  "array",
				Items: typ,
			}
		} else {
			return typ
		}
	}

	switch bqField.Type {
	case bigquery.StringFieldType, bigquery.GeographyFieldType, bigquery.JSONFieldType:
		return considerRepeated("string", bqField.Repeated), nil
	case bigquery.BytesFieldType:
		return "bytes", nil
	case bigquery.IntegerFieldType:
		return considerRepeated("long", bqField.Repeated), nil
	case bigquery.FloatFieldType:
		return considerRepeated("double", bqField.Repeated), nil
	case bigquery.BooleanFieldType:
		return considerRepeated("boolean", bqField.Repeated), nil
	case bigquery.TimestampFieldType:
		timestampSchema := qvalue.AvroSchemaField{
			Type:        "long",
			LogicalType: "timestamp-micros",
		}
		if bqField.Repeated {
			return qvalue.AvroSchemaComplexArray{
				Type:  "array",
				Items: timestampSchema,
			}, nil
		}
		return timestampSchema, nil
	case bigquery.DateFieldType:
		dateSchema := qvalue.AvroSchemaField{
			Type:        "int",
			LogicalType: "date",
		}
		if bqField.Repeated {
			return qvalue.AvroSchemaComplexArray{
				Type:  "array",
				Items: dateSchema,
			}, nil
		}
		return dateSchema, nil

	case bigquery.TimeFieldType:
		return qvalue.AvroSchemaField{
			Type:        "long",
			LogicalType: "time-micros",
		}, nil
	case bigquery.DateTimeFieldType:
		return qvalue.AvroSchemaRecord{
			Type: "record",
			Name: "datetime",
			Fields: []qvalue.AvroSchemaField{
				{
					Name:        "date",
					Type:        "int",
					LogicalType: "date",
				},
				{
					Name:        "time",
					Type:        "long",
					LogicalType: "time-micros",
				},
			},
		}, nil
	case bigquery.NumericFieldType:
		return qvalue.AvroSchemaNumeric{
			Type:        "bytes",
			LogicalType: "decimal",
			Precision:   38,
			Scale:       9,
		}, nil
	case bigquery.RecordFieldType:
		avroFields := []qvalue.AvroSchemaField{}
		for _, bqSubField := range bqField.Schema {
			avroType, err := GetAvroType(bqSubField)
			if err != nil {
				return nil, err
			}
			avroFields = append(avroFields, qvalue.AvroSchemaField{
				Name: bqSubField.Name,
				Type: avroType,
			})
		}
		return qvalue.AvroSchemaRecord{
			Type:   "record",
			Name:   bqField.Name,
			Fields: avroFields,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported BigQuery field type: %s", bqField.Type)
	}
}

func (s *QRepAvroSyncMethod) writeToStage(
	syncID string,
	objectFolder string,
	avroSchema *model.QRecordAvroSchemaDefinition,
	stagingTable *datasetTable,
	stream *model.QRecordStream,
	flowName string,
) (int, error) {
	shutdown := utils.HeartbeatRoutine(s.connector.ctx, func() string {
		return fmt.Sprintf("writing to avro stage for objectFolder %s and staging table %s",
			objectFolder, stagingTable)
	})
	defer shutdown()

	var avroFile *avro.AvroFile
	ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema,
		avro.CompressNone, qvalue.QDWHTypeBigQuery)
	idLog := slog.Group("write-metadata",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.String("batchOrPartitionID", syncID),
	)
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
		s.connector.logger.Info("writing records to local file", idLog)
		avroFile, err = ocfWriter.WriteRecordsToAvroFile(avroFilePath)
		if err != nil {
			return 0, fmt.Errorf("failed to write records to local Avro file: %w", err)
		}
	}
	defer avroFile.Cleanup()

	if avroFile.NumRecords == 0 {
		return 0, nil
	}
	s.connector.logger.Info(fmt.Sprintf("wrote %d records", avroFile.NumRecords), idLog)

	bqClient := s.connector.client
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

	loader := bqClient.DatasetInProject(s.connector.projectID, stagingTable.dataset).Table(stagingTable.table).LoaderFrom(avroRef)
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

	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("failed to load Avro file into BigQuery table: %w", err)
	}
	s.connector.logger.Info(fmt.Sprintf("Pushed from %s to BigQuery", avroFile.FilePath), idLog)

	err = s.connector.waitForTableReady(stagingTable)
	if err != nil {
		return 0, fmt.Errorf("failed to wait for table to be ready: %w", err)
	}

	return avroFile.NumRecords, nil
}
