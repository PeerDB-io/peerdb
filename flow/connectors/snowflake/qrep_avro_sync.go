package connsnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/hamba/avro/v2/ocf"
	_ "github.com/snowflakedb/gosnowflake"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type SnowflakeAvroSyncHandler struct {
	*SnowflakeConnector
	config *protos.QRepConfig
}

func NewSnowflakeAvroSyncHandler(
	config *protos.QRepConfig,
	connector *SnowflakeConnector,
) *SnowflakeAvroSyncHandler {
	return &SnowflakeAvroSyncHandler{
		SnowflakeConnector: connector,
		config:             config,
	}
}

func (s *SnowflakeAvroSyncHandler) SyncRecords(
	ctx context.Context,
	env map[string]string,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
	flowJobName string,
) (int64, error) {
	tableLog := slog.String("destinationTable", s.config.DestinationTableIdentifier)
	dstTableName := s.config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return 0, stream.Err()
	}

	s.logger.Info("sync function called and schema acquired", tableLog)

	avroSchema, err := s.getAvroSchema(ctx, env, dstTableName, schema)
	if err != nil {
		return 0, err
	}

	partitionID := shared.RandomString(16)
	avroFile, err := s.writeToAvroFile(ctx, env, stream, avroSchema, partitionID, flowJobName)
	if err != nil {
		return 0, err
	}
	defer avroFile.Cleanup()
	s.logger.Info(fmt.Sprintf("written %d records to Avro file", avroFile.NumRecords), tableLog)

	stage := s.getStageNameForJob(s.config.FlowJobName)
	if err := s.createStage(ctx, stage, s.config); err != nil {
		return 0, err
	}
	s.logger.Info("Created stage " + stage)

	if err := s.putFileToStage(ctx, avroFile, stage); err != nil {
		return 0, err
	}
	s.logger.Info("pushed avro file to stage", tableLog)

	writeHandler := NewSnowflakeAvroConsolidateHandler(s.SnowflakeConnector, s.config, s.config.DestinationTableIdentifier, stage)
	if err := writeHandler.CopyStageToDestination(ctx); err != nil {
		return 0, err
	}
	s.logger.Info(fmt.Sprintf("copying records into %s from stage %s",
		s.config.DestinationTableIdentifier, stage))

	return avroFile.NumRecords, nil
}

func (s *SnowflakeAvroSyncHandler) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
) (int64, error) {
	partitionLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return 0, err
	}
	s.logger.Info("sync function called and schema acquired", partitionLog)

	avroSchema, err := s.getAvroSchema(ctx, config.Env, dstTableName, schema)
	if err != nil {
		return 0, err
	}

	avroFile, err := s.writeToAvroFile(ctx, config.Env, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}
	defer avroFile.Cleanup()

	stage := s.getStageNameForJob(config.FlowJobName)

	if err := s.putFileToStage(ctx, avroFile, stage); err != nil {
		return 0, err
	}
	s.logger.Info("Put file to stage in Avro sync for snowflake", partitionLog)

	if err := s.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		return 0, err
	}

	return avroFile.NumRecords, nil
}

func (s *SnowflakeAvroSyncHandler) getAvroSchema(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	schema types.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	// TODO: Support avroNameMap for avro-incompatible column name support
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_SNOWFLAKE, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	s.logger.Info(fmt.Sprintf("Avro schema: %v\n", avroSchema))
	return avroSchema, nil
}

func (s *SnowflakeAvroSyncHandler) writeToAvroFile(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
	flowJobName string,
) (utils.AvroFile, error) {
	if s.config.StagingPath == "" {
		ocfWriter := utils.NewPeerDBOCFWriter(stream, avroSchema, ocf.ZStandard, protos.DBType_SNOWFLAKE)
		tmpDir := fmt.Sprintf("%s/peerdb-avro-%s", os.TempDir(), flowJobName)
		err := os.MkdirAll(tmpDir, os.ModePerm)
		if err != nil {
			return utils.AvroFile{}, fmt.Errorf("failed to create temp dir: %w", err)
		}

		localFilePath := fmt.Sprintf("%s/%s.avro", tmpDir, partitionID)
		s.logger.Info("writing records to local file " + localFilePath)
		avroFile, err := ocfWriter.WriteRecordsToAvroFile(ctx, env, localFilePath)
		if err != nil {
			return utils.AvroFile{}, fmt.Errorf("failed to write records to Avro file: %w", err)
		}

		return avroFile, nil
	} else if strings.HasPrefix(s.config.StagingPath, "s3://") {
		ocfWriter := utils.NewPeerDBOCFWriter(stream, avroSchema, ocf.ZStandard, protos.DBType_SNOWFLAKE)
		s3o, err := utils.NewS3BucketAndPrefix(s.config.StagingPath)
		if err != nil {
			return utils.AvroFile{}, fmt.Errorf("failed to parse staging path: %w", err)
		}

		s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, s.config.FlowJobName, partitionID)
		s.logger.Info("OCF: Writing records to S3",
			slog.String(string(shared.PartitionIDKey), partitionID))

		provider, err := utils.GetAWSCredentialsProvider(ctx, "snowflake", utils.PeerAWSCredentials{})
		if err != nil {
			return utils.AvroFile{}, err
		}
		avroFile, err := ocfWriter.WriteRecordsToS3(ctx, env, s3o.Bucket, s3AvroFileKey, provider, nil, nil)
		if err != nil {
			return utils.AvroFile{}, fmt.Errorf("failed to write records to S3: %w", err)
		}

		return avroFile, nil
	}

	return utils.AvroFile{}, fmt.Errorf("unsupported staging path: %s", s.config.StagingPath)
}

func (s *SnowflakeAvroSyncHandler) putFileToStage(ctx context.Context, avroFile utils.AvroFile, stage string) error {
	if avroFile.StorageLocation != utils.AvroLocalStorage {
		s.logger.Info("no file to put to stage")
		return nil
	}

	putCmd := fmt.Sprintf("PUT file://%s @%s", avroFile.FilePath, stage)

	if _, err := s.ExecContext(ctx, putCmd); err != nil {
		return fmt.Errorf("failed to put file to stage: %w", err)
	}

	s.logger.Info(fmt.Sprintf("put file %s to stage %s", avroFile.FilePath, stage))
	return nil
}
