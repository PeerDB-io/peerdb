package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseAvroSyncMethod struct {
	config    *protos.QRepConfig
	connector *ClickhouseConnector
}

func NewClickhouseAvroSyncMethod(
	config *protos.QRepConfig,
	connector *ClickhouseConnector,
) *ClickhouseAvroSyncMethod {
	return &ClickhouseAvroSyncMethod{
		config:    config,
		connector: connector,
	}
}

func (s *ClickhouseAvroSyncMethod) CopyStageToDestination(ctx context.Context, avroFile *avro.AvroFile) error {
	stagingPath := s.connector.credsProvider.BucketPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}

	endpoint := s.connector.credsProvider.Provider.GetEndpointURL()
	region := s.connector.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
	creds, err := s.connector.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return err
	}

	sessionTokenPart := ""
	if creds.AWS.SessionToken != "" {
		sessionTokenPart = fmt.Sprintf(", '%s'", creds.AWS.SessionToken)
	}
	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM s3('%s','%s','%s'%s, 'Avro')",
		s.config.DestinationTableIdentifier, avroFileUrl,
		creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart)

	return s.connector.database.Exec(ctx, query)
}

func (s *ClickhouseAvroSyncMethod) SyncRecords(
	ctx context.Context,
	stream *model.QRecordStream,
	flowJobName string,
	syncBatchID int64,
) (int, error) {
	dstTableName := s.config.DestinationTableIdentifier

	schema := stream.Schema()
	s.connector.logger.Info("sync function called and schema acquired",
		slog.String("dstTable", dstTableName))

	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	batchIdentifierForFile := fmt.Sprintf("%s_%d", shared.RandomString(16), syncBatchID)
	avroFile, err := s.writeToAvroFile(ctx, stream, avroSchema, batchIdentifierForFile, flowJobName)
	if err != nil {
		return 0, err
	}

	s.connector.logger.Info("[SyncRecords] written records to Avro file",
		slog.String("dstTable", dstTableName),
		slog.String("avroFile", avroFile.FilePath),
		slog.Int("numRecords", avroFile.NumRecords),
		slog.Int64("syncBatchID", syncBatchID))

	err = s.connector.s3Stage.SetAvroStage(ctx, flowJobName, syncBatchID, avroFile)
	if err != nil {
		return 0, fmt.Errorf("failed to set avro stage: %w", err)
	}

	return avroFile.NumRecords, nil
}

func (s *ClickhouseAvroSyncMethod) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []driver.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier
	stagingPath := s.connector.credsProvider.BucketPath

	avroSchema, err := s.getAvroSchema(dstTableName, stream.Schema())
	if err != nil {
		return 0, err
	}

	avroFile, err := s.writeToAvroFile(ctx, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return 0, err
	}

	creds, err := s.connector.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return 0, err
	}

	endpoint := s.connector.credsProvider.Provider.GetEndpointURL()
	region := s.connector.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
	selector := make([]string, 0, len(dstTableSchema))
	for _, col := range dstTableSchema {
		colName := col.Name()
		if strings.EqualFold(colName, config.SoftDeleteColName) ||
			strings.EqualFold(colName, signColName) ||
			strings.EqualFold(colName, config.SyncedAtColName) ||
			strings.EqualFold(colName, versionColName) {
			continue
		}

		selector = append(selector, "`"+colName+"`")
	}
	selectorStr := strings.Join(selector, ",")

	sessionTokenPart := ""
	if creds.AWS.SessionToken != "" {
		sessionTokenPart = fmt.Sprintf(", '%s'", creds.AWS.SessionToken)
	}
	query := fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM s3('%s','%s','%s'%s, 'Avro')",
		config.DestinationTableIdentifier, selectorStr, selectorStr, avroFileUrl,
		creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart)

	err = s.connector.database.Exec(ctx, query)
	if err != nil {
		s.connector.logger.Error("Failed to insert into select for Clickhouse: ", err)
		return 0, err
	}

	err = s.insertMetadata(ctx, partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	return avroFile.NumRecords, nil
}

func (s *ClickhouseAvroSyncMethod) getAvroSchema(
	dstTableName string,
	schema qvalue.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema, protos.DBType_CLICKHOUSE)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	return avroSchema, nil
}

func (s *ClickhouseAvroSyncMethod) writeToAvroFile(
	ctx context.Context,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	identifierForFile string,
	flowJobName string,
) (*avro.AvroFile, error) {
	stagingPath := s.connector.credsProvider.BucketPath
	ocfWriter := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressZstd, protos.DBType_CLICKHOUSE)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, flowJobName, identifierForFile)
	s3AvroFileKey = strings.Trim(s3AvroFileKey, "/")
	avroFile, err := ocfWriter.WriteRecordsToS3(ctx, s3o.Bucket, s3AvroFileKey, s.connector.credsProvider.Provider)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to S3: %w", err)
	}

	return avroFile, nil
}

func (s *ClickhouseAvroSyncMethod) insertMetadata(
	ctx context.Context,
	partition *protos.QRepPartition,
	flowJobName string,
	startTime time.Time,
) error {
	partitionLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		s.connector.logger.Error("failed to create metadata insert statement",
			slog.Any("error", err), partitionLog)
		return fmt.Errorf("failed to create metadata insert statement: %w", err)
	}

	if err := s.connector.database.Exec(ctx, insertMetadataStmt); err != nil {
		return fmt.Errorf("failed to execute metadata insert statement: %w", err)
	}

	return nil
}
