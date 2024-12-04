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

	return s.database.Exec(ctx, query)
}

func (s *ClickHouseAvroSyncMethod) SyncRecords(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	flowJobName string,
	syncBatchID int64,
) (int, error) {
	dstTableName := s.config.DestinationTableIdentifier

	schema := stream.Schema()
	s.logger.Info("sync function called and schema acquired",
		slog.String("dstTable", dstTableName))

	avroSchema, err := s.getAvroSchema(ctx, env, dstTableName, schema)
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
	dstTableSchema []driver.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	dstTableName := config.DestinationTableIdentifier
	stagingPath := s.credsProvider.BucketPath
	startTime := time.Now()

	avroSchema, err := s.getAvroSchema(ctx, config.Env, dstTableName, stream.Schema())
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

	endpoint := s.credsProvider.Provider.GetEndpointURL()
	region := s.credsProvider.Provider.GetRegion()
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
	query := fmt.Sprintf("INSERT INTO `%s`(%s) SELECT %s FROM s3('%s','%s','%s'%s, 'Avro')",
		config.DestinationTableIdentifier, selectorStr, selectorStr, avroFileUrl,
		creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart)

	if err := s.database.Exec(ctx, query); err != nil {
		s.logger.Error("Failed to insert into select for ClickHouse", slog.Any("error", err))
		return 0, err
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
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_CLICKHOUSE)
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
