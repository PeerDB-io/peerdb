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
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseAvroSyncMethod struct {
	config    *protos.QRepConfig
	connector *ClickhouseConnector
	env       map[string]string
}

func NewClickhouseAvroSyncMethod(
	config *protos.QRepConfig,
	connector *ClickhouseConnector,
	env map[string]string,
) *ClickhouseAvroSyncMethod {
	return &ClickhouseAvroSyncMethod{
		config:    config,
		connector: connector,
		env:       env,
	}
}

func (s *ClickhouseAvroSyncMethod) CopyStageToDestination(ctx context.Context, avroFiles []*avro.AvroFile) error {
	stagingPath := s.connector.credsProvider.BucketPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}

	endpoint := s.connector.credsProvider.Provider.GetEndpointURL()
	region := s.connector.credsProvider.Provider.GetRegion()
	creds, err := s.connector.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return err
	}

	for _, avroFile := range avroFiles {
		avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
		sessionTokenPart := ""
		if creds.AWS.SessionToken != "" {
			sessionTokenPart = fmt.Sprintf(", '%s'", creds.AWS.SessionToken)
		}

		query := fmt.Sprintf("INSERT INTO %s SELECT * FROM s3('%s','%s','%s'%s, 'Avro')",
			s.config.DestinationTableIdentifier, avroFileUrl,
			creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart)

		s.connector.logger.Info("copying avro file to destination", slog.String("query", query))
		if err := s.connector.database.Exec(ctx, query); err != nil {
			return err
		}
		s.connector.logger.Info("avro file copied to destination")
	}

	return nil
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

	partitionID := shared.RandomString(16)
	avroFiles, err := s.writeToAvroFiles(ctx, stream, avroSchema, partitionID, flowJobName)
	if err != nil {
		return 0, err
	}
	defer func() {
		for _, avroFile := range avroFiles {
			avroFile.Cleanup()
		}
	}()
	totalRecords := 0
	for _, avroFile := range avroFiles {
		err = s.connector.s3Stage.SetAvroStage(ctx, flowJobName, syncBatchID, avroFile)
		if err != nil {
			return 0, fmt.Errorf("failed to set avro stage: %w", err)
		}
		totalRecords += avroFile.NumRecords
	}

	s.connector.logger.Info(fmt.Sprintf("written %d records to Avro files", totalRecords),
		slog.String("dstTable", dstTableName))
	err = s.CopyStageToDestination(ctx, avroFiles)
	if err != nil {
		return 0, err
	}

	s.connector.logger.Info("[SyncRecords] written records to Avro files",
		slog.String("dstTable", dstTableName),
		slog.Int("numRecords", totalRecords),
		slog.Int64("syncBatchID", syncBatchID))

	return totalRecords, nil
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

	avroFiles, err := s.writeToAvroFiles(ctx, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}
	totalRecords := 0
	for _, avroFile := range avroFiles {
		totalRecords += avroFile.NumRecords
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
	hashColName := dstTableSchema[0].Name()
	numParts := 29
	for _, avroFile := range avroFiles {
		avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFile.FilePath)
		for i := 0; i < numParts; i++ {
			whereClause := fmt.Sprintf("cityHash64(%s) %% %d = %d", hashColName, numParts, i)
			query := fmt.Sprintf("INSERT INTO %s(%s) SELECT %s FROM s3('%s','%s','%s'%s, 'Avro') WHERE %s",
				config.DestinationTableIdentifier, selectorStr, selectorStr, avroFileUrl,
				creds.AWS.AccessKeyID, creds.AWS.SecretAccessKey, sessionTokenPart, whereClause)
			s.connector.logger.Info("running query", slog.String("query", query), slog.Int("part", i), slog.Int("numParts", numParts))
			err := s.connector.database.Exec(ctx, query)
			if err != nil {
				s.connector.logger.Error("failed to execute query", slog.String("query", query), slog.Int("part", i), slog.Int("numParts", numParts), slog.Any("error", err))
				return 0, err
			}
		}
	}

	err = s.insertMetadata(ctx, partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	return totalRecords, nil
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

func (s *ClickhouseAvroSyncMethod) writeToAvroFiles(
	ctx context.Context,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	identifierForFile string,
	flowJobName string,
) ([]*avro.AvroFile, error) {
	stagingPath := s.connector.credsProvider.BucketPath
	ocfWriter := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressZstd, protos.DBType_CLICKHOUSE)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, flowJobName, identifierForFile)
	s3AvroFileKey = strings.Trim(s3AvroFileKey, "/")
	maxRecordsPerFile, err := peerdbenv.PeerDBClickhouseNumRowsPerAvroFile(ctx, s.env)
	if err != nil {
		return nil, fmt.Errorf("failed to get max records per file: %w", err)
	}
	avroFiles, err := ocfWriter.WriteRecordsToS3Parts(
		ctx, s3o.Bucket, s3AvroFileKey, s.connector.credsProvider.Provider, int64(maxRecordsPerFile))
	if err != nil {
		return nil, fmt.Errorf("failed to write records to S3: %w", err)
	}

	s.connector.logger.Info("avro files written to S3", slog.Int("numFiles", len(avroFiles)))
	for _, avroFile := range avroFiles {
		s.connector.logger.Info("avro file written to S3", slog.String("filePath", avroFile.FilePath))
	}

	return avroFiles, nil
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
