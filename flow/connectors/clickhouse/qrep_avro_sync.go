package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
) (int, error) {
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
	avroFile, err := s.writeToAvroFile(ctx, env, stream, avroSchema, batchIdentifierForFile, flowJobName)
	if err != nil {
		return 0, err
	}

	s.logger.Info("[SyncRecords] written records to Avro file",
		slog.String("dstTable", dstTableName),
		slog.String("avroFile", avroFile.FilePath),
		slog.Int("numRecords", avroFile.NumRecords),
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
) (int, error) {
	dstTableName := config.DestinationTableIdentifier
	stagingPath := s.credsProvider.BucketPath
	startTime := time.Now()

	schema, err := stream.Schema()
	if err != nil {
		return 0, err
	}

	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	avroSchema, err := s.getAvroSchema(ctx, config.Env, dstTableName, schema, columnNameAvroFieldMap)
	if err != nil {
		return 0, err
	}

	avroFile, err := s.writeToAvroFile(ctx, config.Env, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return 0, err
	}

	creds, err := s.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return 0, err
	}

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, config.Env)
	if err != nil {
		return 0, err
	}

	endpoint := s.credsProvider.Provider.GetEndpointURL()
	region := s.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
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
			return 0, fmt.Errorf("destination column %s not found in avro schema", colName)
		}
		selectedColumnNames = append(selectedColumnNames, "`"+avroColName+"`")
		insertedColumnNames = append(insertedColumnNames, "`"+colName+"`")
	}
	if sourceSchemaAsDestinationColumn {
		schemaTable, err := utils.ParseSchemaTable(config.WatermarkTable)
		if err != nil {
			return 0, err
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
			"INSERT INTO `%s`(%s) SELECT %s FROM s3('%s','%s','%s'%s, 'Avro')%s SETTINGS throw_on_max_partitions_per_insert_block = 0",
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
			return 0, exceptions.NewQRepSyncError(err, config.DestinationTableIdentifier, s.ClickHouseConnector.config.Database)
		}
	}

	if err := s.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		s.logger.Error("Failed to finish QRep partition", slog.Any("error", err))
		return 0, err
	}

	return avroFile.NumRecords, nil
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
	avroSchema *model.QRecordAvroSchemaDefinition,
	identifierForFile string,
	flowJobName string,
) (*avro.AvroFile, error) {
	stagingPath := s.credsProvider.BucketPath
	ocfWriter := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressZstd, protos.DBType_CLICKHOUSE)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, flowJobName, identifierForFile)
	s3AvroFileKey = strings.Trim(s3AvroFileKey, "/")
	avroFile, err := ocfWriter.WriteRecordsToS3(ctx, env, s3o.Bucket, s3AvroFileKey, s.credsProvider.Provider)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to S3: %w", err)
	}

	return avroFile, nil
}
