package connbigquery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	rawTableName string,
	dstTableMetadata *bigquery.TableMetadata,
	syncBatchID int64,
	stream *model.QRecordStream,
	tableNameRowsMapping map[string]*model.RecordTypeCounts,
) (*model.SyncResponse, error) {
	s.connector.logger.Info(
		fmt.Sprintf("Obtaining Avro schema for destination table %s and sync batch ID %d",
			rawTableName, syncBatchID))

	// You will need to define your Avro schema as a string
	avroSchema, err := DefineAvroSchema(rawTableName, dstTableMetadata, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	stagingTable := fmt.Sprintf("%s_%s_staging", rawTableName, strconv.FormatInt(syncBatchID, 10))
	numRecords, err := s.writeToStage(ctx, req.Env, strconv.FormatInt(syncBatchID, 10), rawTableName, avroSchema,
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

	query := bqClient.Query(insertStmt)
	query.DefaultDatasetID = s.connector.datasetID
	query.DefaultProjectID = s.connector.projectID
	_, err = query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statements in a transaction: %w", err)
	}

	lastCP := req.Records.GetLastCheckpoint()
	if err := s.connector.FinishBatch(ctx, req.FlowJobName, syncBatchID, lastCP); err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}

	// drop the staging table
	if err := bqClient.DatasetInProject(s.connector.projectID, datasetID).Table(stagingTable).Delete(ctx); err != nil {
		// just log the error this isn't fatal.
		s.connector.logger.Warn("failed to delete staging table "+stagingTable,
			slog.Any("error", err),
			slog.Int64("syncBatchID", syncBatchID),
			slog.String("destinationTable", rawTableName))
	}

	s.connector.logger.Info(fmt.Sprintf("loaded stage into %s.%s", datasetID, rawTableName),
		slog.String(string(shared.FlowNameKey), req.FlowJobName),
		slog.String("dstTableName", rawTableName))

	if err := s.connector.ReplayTableSchemaDeltas(
		ctx, req.Env, req.FlowJobName, req.TableMappings, req.Records.SchemaDeltas, nil,
	); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: lastCP,
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   syncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}

func getTransformedColumns(dstSchema *bigquery.Schema, syncedAtCol string, softDeleteCol string) []string {
	transformedColumns := make([]string, 0, len(*dstSchema))
	for _, col := range *dstSchema {
		if col.Name == syncedAtCol { // PeerDB column
			transformedColumns = append(transformedColumns, "CURRENT_TIMESTAMP AS `"+col.Name+"`")
			continue
		}
		if col.Name == softDeleteCol { // PeerDB column
			transformedColumns = append(transformedColumns, "FALSE AS `"+col.Name+"`")
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
	ctx context.Context,
	env map[string]string,
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableMetadata *bigquery.TableMetadata,
	stream *model.QRecordStream,
	syncedAtCol string,
	softDeleteCol string,
) (int64, error) {
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
	s.connector.logger.Info("Obtained Avro schema for destination table", flowLog, slog.Any("avroSchema", avroSchema))
	// create a staging table name with partitionID replace hyphens with underscores
	dstDatasetTable, _ := s.connector.convertToDatasetTable(dstTableName)
	stagingDatasetTable := &datasetTable{
		project: s.connector.projectID,
		dataset: dstDatasetTable.dataset,
		table: fmt.Sprintf("%s_%s_staging", dstDatasetTable.table,
			strings.ReplaceAll(partition.PartitionId, "-", "_")),
	}
	numRecords, err := s.writeToStage(ctx, env, partition.PartitionId, flowJobName, avroSchema,
		stagingDatasetTable, stream, flowJobName)
	if err != nil {
		return -1, fmt.Errorf("failed to push to avro stage: %w", err)
	}
	bqClient := s.connector.client

	insertColumns := make([]string, 0, len(dstTableMetadata.Schema))
	for _, col := range dstTableMetadata.Schema {
		insertColumns = append(insertColumns, fmt.Sprintf("`%s`", col.Name))
	}

	insertColumnSQL := strings.Join(insertColumns, ", ")
	transformedColumns := getTransformedColumns(&dstTableMetadata.Schema, syncedAtCol, softDeleteCol)
	selector := strings.Join(transformedColumns, ", ")

	// The staging table may not exist if there are no rows (not created by the bq loader)
	if numRecords > 0 {
		// Insert the records from the staging table into the destination table
		insertStmt := fmt.Sprintf("INSERT INTO `%s`(%s) SELECT %s FROM `%s`;",
			dstTableName, insertColumnSQL, selector, stagingDatasetTable.string())

		s.connector.logger.Info("Performing transaction inside QRep sync function", flowLog)

		query := bqClient.Query(insertStmt)
		query.DefaultDatasetID = s.connector.datasetID
		query.DefaultProjectID = s.connector.projectID
		_, err = query.Read(ctx)
		if err != nil {
			return -1, fmt.Errorf("SyncQRepRecords: failed to execute statements in a transaction: %w", err)
		}
	} else {
		s.connector.logger.Info("SyncQRepRecords: no rows to sync, hence skipping transaction", flowLog)
	}

	if err := s.connector.FinishQRepPartition(ctx, partition, flowJobName, startTime); err != nil {
		return -1, err
	}

	// drop the staging table
	if err := bqClient.DatasetInProject(s.connector.projectID, stagingDatasetTable.dataset).
		Table(stagingDatasetTable.table).Delete(ctx); err != nil {
		// just log the error this isn't fatal.
		s.connector.logger.Warn("failed to delete staging table "+stagingDatasetTable.string(),
			slog.Any("error", err),
			flowLog)
	}

	s.connector.logger.Info("loaded stage into "+dstTableName, flowLog)
	return numRecords, nil
}

func DefineAvroSchema(dstTableName string,
	dstTableMetadata *bigquery.TableMetadata,
	syncedAtCol string,
	softDeleteCol string,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroFields := make([]*avro.Field, 0, len(dstTableMetadata.Schema))
	qFields := make([]types.QField, 0, len(avroFields))
	for _, bqField := range dstTableMetadata.Schema {
		if bqField.Name == syncedAtCol || bqField.Name == softDeleteCol {
			continue
		}
		avroField, err := GetAvroField(bqField)
		if err != nil {
			return nil, err
		}
		avroFields = append(avroFields, avroField)
		qFields = append(qFields, BigQueryFieldToQField(bqField))
	}

	avroSchema, err := avro.NewRecordSchema(dstTableName, "", avroFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro schema: %w", err)
	}

	return &model.QRecordAvroSchemaDefinition{
		Schema: avroSchema,
		Fields: qFields,
	}, nil
}

func GetAvroType(bqField *bigquery.FieldSchema) (avro.Schema, error) {
	avroNumericPrecision, avroNumericScale := qvalue.DetermineNumericSettingForDWH(
		int16(bqField.Precision), int16(bqField.Scale), protos.DBType_BIGQUERY)

	considerRepeated := func(typ avro.Type, repeated bool) avro.Schema {
		if repeated {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(typ, nil))
		} else {
			return avro.NewPrimitiveSchema(typ, nil)
		}
	}

	switch bqField.Type {
	case bigquery.StringFieldType, bigquery.GeographyFieldType, bigquery.JSONFieldType:
		return considerRepeated(avro.String, bqField.Repeated), nil
	case bigquery.BytesFieldType:
		return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
	case bigquery.IntegerFieldType:
		return considerRepeated("long", bqField.Repeated), nil
	case bigquery.FloatFieldType:
		return considerRepeated("double", bqField.Repeated), nil
	case bigquery.BooleanFieldType:
		return considerRepeated("boolean", bqField.Repeated), nil
	case bigquery.TimestampFieldType:
		timestampSchema := avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros))
		if bqField.Repeated {
			return avro.NewArraySchema(timestampSchema), nil
		}
		return timestampSchema, nil
	case bigquery.DateFieldType:
		dateSchema := avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date))
		if bqField.Repeated {
			return avro.NewArraySchema(dateSchema), nil
		}
		return dateSchema, nil
	case bigquery.TimeFieldType:
		timeSchema := avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros))
		if bqField.Repeated {
			return avro.NewArraySchema(timeSchema), nil
		}
		return timeSchema, nil
	case bigquery.DateTimeFieldType:
		dateField, err := avro.NewField("date",
			avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date)))
		if err != nil {
			return nil, err
		}
		timeField, err := avro.NewField("time",
			avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros)))
		if err != nil {
			return nil, err
		}
		return avro.NewRecordSchema("datetime", "", []*avro.Field{dateField, timeField})
	case bigquery.BigNumericFieldType:
		bigNumericSchema := avro.NewPrimitiveSchema(avro.Bytes, avro.NewDecimalLogicalSchema(int(avroNumericPrecision), int(avroNumericScale)))
		if bqField.Repeated {
			return avro.NewArraySchema(bigNumericSchema), nil
		}
		return bigNumericSchema, nil
	case bigquery.RecordFieldType:
		avroFields := []*avro.Field{}
		for _, bqSubField := range bqField.Schema {
			avroType, err := GetAvroType(bqSubField)
			if err != nil {
				return nil, err
			}
			avroField, err := avro.NewField(
				bqSubField.Name,
				avroType,
			)
			if err != nil {
				return nil, err
			}
			avroFields = append(avroFields, avroField)
		}
		return avro.NewRecordSchema(bqField.Name, "", avroFields)

	default:
		return nil, fmt.Errorf("unsupported BigQuery field type: %s", bqField.Type)
	}
}

func GetAvroField(bqField *bigquery.FieldSchema) (*avro.Field, error) {
	avroType, err := GetAvroType(bqField)
	if err != nil {
		return nil, err
	}

	if !bqField.Required {
		avroType, err = qvalue.NullableAvroSchema(avroType)
		if err != nil {
			return nil, err
		}
	}

	return avro.NewField(bqField.Name, avroType)
}

func (s *QRepAvroSyncMethod) writeToStage(
	ctx context.Context,
	env map[string]string,
	syncID string,
	objectFolder string,
	avroSchema *model.QRecordAvroSchemaDefinition,
	stagingTable *datasetTable,
	stream *model.QRecordStream,
	flowName string,
) (int64, error) {
	var avroFile utils.AvroFile
	ocfWriter := utils.NewPeerDBOCFWriter(stream, avroSchema, ocf.Snappy, protos.DBType_BIGQUERY, nil)
	idLog := slog.Group("write-metadata",
		slog.String(string(shared.FlowNameKey), flowName),
		slog.String("batchOrPartitionID", syncID),
	)
	if s.gcsBucket != "" {
		bucket := s.connector.storageClient.Bucket(s.gcsBucket)
		avroFilePath := fmt.Sprintf("%s/%s.avro", objectFolder, syncID)
		obj := bucket.Object(avroFilePath)
		w := obj.NewWriter(ctx)

		numRecords, err := ocfWriter.WriteOCF(ctx, env, w, nil, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to write records to Avro file on GCS: %w", err)
		}
		if err := w.Close(); err != nil {
			return 0, fmt.Errorf("failed to close Avro file on GCS after writing: %w", err)
		}

		avroFile = utils.AvroFile{
			NumRecords:      numRecords,
			StorageLocation: utils.AvroGCSStorage,
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
		avroFile, err = ocfWriter.WriteRecordsToAvroFile(ctx, env, avroFilePath)
		if err != nil {
			return 0, fmt.Errorf("failed to write records to local Avro file: %w", err)
		}
	}
	defer avroFile.Cleanup(ctx)

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
	loader.DecimalTargetTypes = []bigquery.DecimalTargetType{bigquery.BigNumericTargetType}
	loader.WriteDisposition = bigquery.WriteTruncate
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
	s.connector.logger.Info(fmt.Sprintf("Pushed from %s to BigQuery", avroFile.FilePath), idLog)

	if err := s.connector.waitForTableReady(ctx, stagingTable); err != nil {
		return 0, fmt.Errorf("failed to wait for table to be ready: %w", err)
	}

	return avroFile.NumRecords, nil
}
