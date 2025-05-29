package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	avro "github.com/PeerDB-io/peerdb/flow/connectors/utils/avro"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type ClickHouseAvroSyncMethod struct {
	*ClickHouseConnector
	config *protos.QRepConfig
}

func NewClickHouseAvroSyncMethod(
	config *protos.QRepConfig,
	connector *ClickHouseConnector,
) *ClickHouseAvroSyncMethod {
	return &ClickHouseAvroSyncMethod{
		ClickHouseConnector: connector,
		config:              config,
	}
}

func (s *ClickHouseAvroSyncMethod) CopyStageToDestination(ctx context.Context, avroFile *avro.AvroFile) error {
	stagingPath := s.credsProvider.BucketPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}

	endpoint := s.credsProvider.Provider.GetEndpointURL()
	region := s.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
	creds, err := s.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return err
	}

	sessionTokenPart := ""
	if creds.AWS.SessionToken != "" {
		sessionTokenPart = fmt.Sprintf(", '%s'", creds.AWS.SessionToken)
	}

	query := fmt.Sprintf("INSERT INTO `%s` SELECT * FROM s3('%s','%s','%s'%s, 'Avro')",
		s.config.DestinationTableIdentifier, avroFileUrl,
		creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart)
	return s.exec(ctx, query)
}

func (s *ClickHouseAvroSyncMethod) SyncRecords(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	flowJobName string,
	syncBatchID int64,
) (int64, error) {
	dstTableName := s.config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return 0, err
	}
	s.logger.Info("sync function called and schema acquired",
		slog.String("dstTable", dstTableName))

	avroSchema, err := s.getAvroSchema(ctx, env, dstTableName, schema, nil)
	if err != nil {
		return 0, err
	}

	batchIdentifierForFile := fmt.Sprintf("%s_%d", shared.RandomString(16), syncBatchID)
	avroFile, err := s.writeToAvroFile(ctx, env, stream, nil, avroSchema, batchIdentifierForFile, flowJobName, nil)
	if err != nil {
		return 0, err
	}

	s.logger.Info("[SyncRecords] written records to Avro file",
		slog.String("dstTable", dstTableName),
		slog.String("avroFile", avroFile.FilePath),
		slog.Int64("numRecords", avroFile.NumRecords),
		slog.Int64("syncBatchID", syncBatchID))

	if err := SetAvroStage(ctx, flowJobName, syncBatchID, avroFile); err != nil {
		return 0, fmt.Errorf("failed to set avro stage: %w", err)
	}

	return avroFile.NumRecords, nil
}

func (s *ClickHouseAvroSyncMethod) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, error) {
	dstTableName := config.DestinationTableIdentifier
	startTime := time.Now()
	schema, err := stream.Schema()
	if err != nil {
		return 0, err
	}

	destTypeConversions := findTypeConversions(schema, config.Columns)
	if len(destTypeConversions) > 0 {
		schema = applyTypeConversions(schema, destTypeConversions)
	}

	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	avroFile, err := s.pushDataToS3(ctx, config, dstTableName, schema,
		columnNameAvroFieldMap, partition, stream, destTypeConversions)
	if err != nil {
		s.logger.Error("failed to push data to S3",
			slog.String("dstTable", dstTableName),
			slog.Any("error", err))
		return 0, err
	}

	if err := s.pushS3DataToClickHouse(
		ctx, avroFile.FilePath, schema, columnNameAvroFieldMap, config); err != nil {
		s.logger.Error("failed to push data to ClickHouse",
			slog.String("dstTable", dstTableName),
			slog.Any("error", err))
		return 0, err
	}

	if err := s.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		s.logger.Error("Failed to finish QRep partition", slog.Any("error", err))
		return 0, err
	}

	return avroFile.NumRecords, nil
}

func (s *ClickHouseAvroSyncMethod) pushDataToS3(
	ctx context.Context,
	config *protos.QRepConfig,
	dstTableName string,
	schema qvalue.QRecordSchema,
	columnNameAvroFieldMap map[string]string,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
	destTypeConversions map[string]qvalue.TypeConversion,
) (*avro.AvroFile, error) {
	avroSchema, err := s.getAvroSchema(ctx, config.Env, dstTableName, schema, columnNameAvroFieldMap)
	if err != nil {
		return nil, err
	}

	avroChunking, err := internal.PeerDBS3BytesPerAvroFile(ctx, config.Env)
	if err != nil {
		return nil, err
	}

	var avroFile *avro.AvroFile
	if avroChunking != 0 {
		avroFile = &avro.AvroFile{
			FilePath:   "",
			NumRecords: 0,
		}

		chunkNum := 0
		var done atomic.Bool
		for !done.Load() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			substream := model.NewQRecordStream(0)
			substream.SetSchema(schema)
			var avroSize atomic.Int64
			go func() {
				recordsDone := true
				for record := range stream.Records {
					substream.Records <- record
					if avroSize.Load() >= avroChunking {
						recordsDone = false
						break
					}
				}
				if recordsDone {
					done.Store(true)
				}
				substream.Close(stream.Err())
			}()

			subFile, err := s.writeToAvroFile(ctx, config.Env, substream, &avroSize, avroSchema,
				fmt.Sprintf("%s.%06d", partition.PartitionId, chunkNum),
				config.FlowJobName, destTypeConversions)
			if err != nil {
				return nil, err
			}
			if chunkNum == 0 {
				avroFile.FilePath = strings.TrimSuffix(subFile.FilePath, "000000.avro") + "*.avro"
			}
			chunkNum += 1
			avroFile.NumRecords += subFile.NumRecords
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	if avroFile == nil || avroFile.FilePath == "" {
		var err error
		avroFile, err = s.writeToAvroFile(
			ctx, config.Env, stream, nil, avroSchema, partition.PartitionId, config.FlowJobName, destTypeConversions,
		)
		if err != nil {
			return nil, err
		}
	}

	return avroFile, nil
}

func (s *ClickHouseAvroSyncMethod) pushS3DataToClickHouse(
	ctx context.Context,
	avroFilePath string,
	schema qvalue.QRecordSchema,
	columnNameAvroFieldMap map[string]string,
	config *protos.QRepConfig,
) error {
	stagingPath := s.credsProvider.BucketPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}

	creds, err := s.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return err
	}

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.Env)
	if err != nil {
		return err
	}

	endpoint := s.credsProvider.Provider.GetEndpointURL()
	region := s.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFilePath)
	selectedColumnNames := make([]string, 0, len(schema.Fields))
	insertedColumnNames := make([]string, 0, len(schema.Fields))
	for _, colName := range schema.GetColumnNames() {
		for _, excludedColumn := range config.Exclude {
			if colName == excludedColumn {
				continue
			}
		}
		avroColName, ok := columnNameAvroFieldMap[colName]
		if !ok {
			s.logger.Error("destination column not found in avro schema",
				slog.String("columnName", colName),
				slog.String("avroFieldName", avroColName))
			return fmt.Errorf("destination column %s not found in avro schema", colName)
		}
		selectedColumnNames = append(selectedColumnNames, "`"+avroColName+"`")
		insertedColumnNames = append(insertedColumnNames, "`"+colName+"`")
	}
	if sourceSchemaAsDestinationColumn {
		schemaTable, err := utils.ParseSchemaTable(config.WatermarkTable)
		if err != nil {
			return err
		}

		selectedColumnNames = append(selectedColumnNames, fmt.Sprintf("'%s'", peerdb_clickhouse.EscapeStr(schemaTable.Schema)))
		insertedColumnNames = append(insertedColumnNames, sourceSchemaColName)
	}

	selectorStr := strings.Join(selectedColumnNames, ",")
	insertedStr := strings.Join(insertedColumnNames, ",")
	sessionTokenPart := ""
	if creds.AWS.SessionToken != "" {
		sessionTokenPart = fmt.Sprintf(", '%s'", creds.AWS.SessionToken)
	}

	hashColName := columnNameAvroFieldMap[schema.Fields[0].Name]
	numParts, err := internal.PeerDBClickHouseInitialLoadPartsPerPartition(ctx, s.config.Env)
	if err != nil {
		s.logger.Warn("failed to get chunking parts, proceeding without chunking", slog.Any("error", err))
		numParts = 1
	}
	numParts = max(numParts, 1)

	for i := range numParts {
		var whereClause string
		if numParts > 1 {
			whereClause = fmt.Sprintf(" WHERE cityHash64(`%s`) %% %d = %d", hashColName, numParts, i)
		}
		query := fmt.Sprintf(
			"INSERT INTO `%s`(%s) SELECT %s FROM s3('%s','%s','%s'%s,'Avro')%s SETTINGS throw_on_max_partitions_per_insert_block = 0",
			config.DestinationTableIdentifier, insertedStr, selectorStr, avroFileUrl,
			creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart, whereClause)
		s.logger.Info("inserting part",
			slog.String("query", query),
			slog.Uint64("part", i),
			slog.Uint64("numParts", numParts))
		if err := s.exec(ctx, query); err != nil {
			s.logger.Error("failed to insert part",
				slog.String("query", query),
				slog.Uint64("part", i),
				slog.Uint64("numParts", numParts),
				slog.Any("error", err))
			return exceptions.NewQRepSyncError(err, config.DestinationTableIdentifier, s.ClickHouseConnector.config.Database)
		}
	}

	return nil
}

func (s *ClickHouseAvroSyncMethod) getAvroSchema(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	schema qvalue.QRecordSchema,
	avroNameMap map[string]string,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_CLICKHOUSE, avroNameMap)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	return avroSchema, nil
}

func (s *ClickHouseAvroSyncMethod) writeToAvroFile(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	avroSize *atomic.Int64,
	avroSchema *model.QRecordAvroSchemaDefinition,
	identifierForFile string,
	flowJobName string,
	typeConversions map[string]qvalue.TypeConversion,
) (*avro.AvroFile, error) {
	stagingPath := s.credsProvider.BucketPath
	ocfWriter := avro.NewPeerDBOCFWriter(stream, avroSchema, ocf.ZStandard, protos.DBType_CLICKHOUSE)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, flowJobName, identifierForFile)
	s3AvroFileKey = strings.TrimLeft(s3AvroFileKey, "/")
	avroFile, err := ocfWriter.WriteRecordsToS3(ctx, env, s3o.Bucket, s3AvroFileKey, s.credsProvider.Provider, avroSize, typeConversions)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to S3: %w", err)
	}

	return avroFile, nil
}

// add more supported type conversions as needed
var supportedDestinationTypes = map[string][]qvalue.TypeConversion{
	"String": {qvalue.NewTypeConversion(
		qvalue.NumericToStringSchemaConversion,
		qvalue.NumericToStringValueConversion,
	)},
}

func findTypeConversions(schema qvalue.QRecordSchema, columns []*protos.ColumnSetting) map[string]qvalue.TypeConversion {
	typeConversions := make(map[string]qvalue.TypeConversion)

	colNameToType := make(map[string]qvalue.QValueKind, len(schema.Fields))
	for _, field := range schema.Fields {
		colNameToType[field.Name] = field.Type
	}

	for _, col := range columns {
		colType, exist := colNameToType[col.SourceName]
		if !exist {
			continue
		}
		conversions, exist := supportedDestinationTypes[col.DestinationType]
		if !exist {
			continue
		}
		for _, conversion := range conversions {
			if conversion.FromKind() == colType {
				typeConversions[col.SourceName] = conversion
			}
		}
	}

	return typeConversions
}

func applyTypeConversions(schema qvalue.QRecordSchema, typeConversions map[string]qvalue.TypeConversion) qvalue.QRecordSchema {
	for i, field := range schema.Fields {
		if conversion, exist := typeConversions[field.Name]; exist {
			schema.Fields[i] = conversion.SchemaConversion(field)
		}
	}
	return schema
}
