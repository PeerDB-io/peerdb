package connclickhouse

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

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

func (s *ClickhouseAvroSyncMethod) CopyStageToDestination(avroFile *avro.AvroFile) error {
	stagingPath := s.config.StagingPath
	if stagingPath == "" {
		stagingPath = s.config.DestinationPeer.GetClickhouseConfig().S3Path // "s3://avro-clickhouse"
	}
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}
	awsCreds, err := utils.GetAWSSecrets(s.connector.creds)
	avroFileUrl := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", s3o.Bucket, awsCreds.Region, avroFile.FilePath)

	if err != nil {
		return err
	}
	//nolint:gosec
	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM s3('%s','%s','%s', 'Avro')",
		s.config.DestinationTableIdentifier, avroFileUrl, awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

	_, err = s.connector.database.Exec(query)

	return err
}

func (s *ClickhouseAvroSyncMethod) SyncRecords(
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
	flowJobName string,
) (int, error) {
	tableLog := slog.String("destinationTable", s.config.DestinationTableIdentifier)
	dstTableName := s.config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	s.connector.logger.Info("sync function called and schema acquired", tableLog)

	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	partitionID := shared.RandomString(16)
	avroFile, err := s.writeToAvroFile(stream, avroSchema, partitionID, flowJobName)
	if err != nil {
		return 0, err
	}
	defer avroFile.Cleanup()
	s.connector.logger.Info(fmt.Sprintf("written %d records to Avro file", avroFile.NumRecords), tableLog)
	err = s.CopyStageToDestination(avroFile)
	if err != nil {
		return 0, err
	}

	return avroFile.NumRecords, nil
}

func (s *ClickhouseAvroSyncMethod) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	stagingPath := s.config.DestinationPeer.GetClickhouseConfig().S3Path

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}
	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	avroFile, err := s.writeToAvroFile(stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return 0, err
	}
	awsCreds, err := utils.GetAWSSecrets(s.connector.creds)
	avroFileUrl := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", s3o.Bucket, awsCreds.Region, avroFile.FilePath)

	if err != nil {
		return 0, err
	}
	selector := strings.Join(schema.GetColumnNames(), ",")
	//nolint:gosec
	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT * FROM s3('%s','%s','%s', 'Avro')",
		config.DestinationTableIdentifier, selector, avroFileUrl, awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

	_, err = s.connector.database.Exec(query)

	if err != nil {
		return 0, err
	}

	err = s.insertMetadata(partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	activity.RecordHeartbeat(s.connector.ctx, "finished syncing records")

	return avroFile.NumRecords, nil
}

func (s *ClickhouseAvroSyncMethod) getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema, qvalue.QDWHTypeClickhouse)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	return avroSchema, nil
}

func (s *ClickhouseAvroSyncMethod) writeToAvroFile(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
	flowJobName string,
) (*avro.AvroFile, error) {
	stagingPath := s.config.StagingPath // "s3://avro-clickhouse"
	if stagingPath == "" {
		stagingPath = s.config.DestinationPeer.GetClickhouseConfig().S3Path // "s3://avro-clickhouse"
	}
	ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema, avro.CompressZstd,
		qvalue.QDWHTypeClickhouse)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, flowJobName, partitionID) // s.config.FlowJobName
	s3AvroFileKey = strings.Trim(s3AvroFileKey, "/")

	avroFile, err := ocfWriter.WriteRecordsToS3(s3o.Bucket, s3AvroFileKey, s.connector.creds)
	if err != nil {
		return nil, fmt.Errorf("failed to write records to S3: %w", err)
	}
	return avroFile, nil
}

func (s *ClickhouseAvroSyncMethod) insertMetadata(
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

	if _, err := s.connector.database.Exec(insertMetadataStmt); err != nil {
		return fmt.Errorf("failed to execute metadata insert statement: %w", err)
	}

	return nil
}

type ClickhouseAvroWriteHandler struct {
	connector    *ClickhouseConnector
	dstTableName string
	stage        string
	copyOpts     []string
}

// NewClickhouseAvroWriteHandler creates a new ClickhouseAvroWriteHandler
func NewClickhouseAvroWriteHandler(
	connector *ClickhouseConnector,
	dstTableName string,
	stage string,
	copyOpts []string,
) *ClickhouseAvroWriteHandler {
	return &ClickhouseAvroWriteHandler{
		connector:    connector,
		dstTableName: dstTableName,
		stage:        stage,
		copyOpts:     copyOpts,
	}
}
