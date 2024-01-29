package connsnowflake

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SnowflakeAvroSyncHandler struct {
	config    *protos.QRepConfig
	connector *SnowflakeConnector
}

func NewSnowflakeAvroSyncHandler(
	config *protos.QRepConfig,
	connector *SnowflakeConnector,
) *SnowflakeAvroSyncHandler {
	return &SnowflakeAvroSyncHandler{
		config:    config,
		connector: connector,
	}
}

func (s *SnowflakeAvroSyncHandler) SyncRecords(
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

	stage := s.connector.getStageNameForJob(s.config.FlowJobName)
	err = s.connector.createStage(stage, s.config)
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info(fmt.Sprintf("Created stage %s", stage))

	err = s.putFileToStage(avroFile, stage)
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info("pushed avro file to stage", tableLog)

	writeHandler := NewSnowflakeAvroConsolidateHandler(s.connector, s.config, s.config.DestinationTableIdentifier, stage)
	err = writeHandler.CopyStageToDestination()
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info(fmt.Sprintf("copying records into %s from stage %s",
		s.config.DestinationTableIdentifier, stage))

	return avroFile.NumRecords, nil
}

func (s *SnowflakeAvroSyncHandler) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	partitionLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}
	s.connector.logger.Info("sync function called and schema acquired", partitionLog)

	err = s.addMissingColumns(
		schema,
		dstTableSchema,
		dstTableName,
		partition,
	)
	if err != nil {
		return 0, err
	}

	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	avroFile, err := s.writeToAvroFile(stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}
	defer avroFile.Cleanup()

	stage := s.connector.getStageNameForJob(config.FlowJobName)

	err = s.putFileToStage(avroFile, stage)
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info("Put file to stage in Avro sync for snowflake", partitionLog)

	err = s.insertMetadata(partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	activity.RecordHeartbeat(s.connector.ctx, "finished syncing records")

	return avroFile.NumRecords, nil
}

func (s *SnowflakeAvroSyncHandler) addMissingColumns(
	schema *model.QRecordSchema,
	dstTableSchema []*sql.ColumnType,
	dstTableName string,
	partition *protos.QRepPartition,
) error {
	partitionLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	// check if avro schema has additional columns compared to destination table
	// if so, we need to add those columns to the destination table
	colsToTypes := map[string]qvalue.QValueKind{}
	for _, col := range schema.Fields {
		hasColumn := false
		// check ignoring case
		for _, dstCol := range dstTableSchema {
			if strings.EqualFold(col.Name, dstCol.Name()) {
				hasColumn = true
				break
			}
		}

		if !hasColumn {
			s.connector.logger.Info(fmt.Sprintf("adding column %s to destination table %s",
				col.Name, dstTableName), partitionLog)
			colsToTypes[col.Name] = col.Type
		}
	}

	if len(colsToTypes) > 0 {
		tx, err := s.connector.database.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		for colName, colType := range colsToTypes {
			sfColType, err := colType.ToDWHColumnType(qvalue.QDWHTypeSnowflake)
			if err != nil {
				return fmt.Errorf("failed to convert QValueKind to Snowflake column type: %w", err)
			}
			upperCasedColName := strings.ToUpper(colName)
			alterTableCmd := fmt.Sprintf("ALTER TABLE %s ", dstTableName)
			alterTableCmd += fmt.Sprintf("ADD COLUMN IF NOT EXISTS \"%s\" %s;", upperCasedColName, sfColType)

			s.connector.logger.Info(fmt.Sprintf("altering destination table %s with command `%s`",
				dstTableName, alterTableCmd), partitionLog)

			if _, err := tx.ExecContext(s.connector.ctx, alterTableCmd); err != nil {
				return fmt.Errorf("failed to alter destination table: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		s.connector.logger.Info("successfully added missing columns to destination table "+
			dstTableName, partitionLog)
	} else {
		s.connector.logger.Info("no missing columns found in destination table "+dstTableName, partitionLog)
	}

	return nil
}

func (s *SnowflakeAvroSyncHandler) getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema, qvalue.QDWHTypeSnowflake)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	s.connector.logger.Info(fmt.Sprintf("Avro schema: %v\n", avroSchema))
	return avroSchema, nil
}

func (s *SnowflakeAvroSyncHandler) writeToAvroFile(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
	flowJobName string,
) (*avro.AvroFile, error) {
	if s.config.StagingPath == "" {
		ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema, avro.CompressZstd,
			qvalue.QDWHTypeSnowflake)
		tmpDir := fmt.Sprintf("%s/peerdb-avro-%s", os.TempDir(), flowJobName)
		err := os.MkdirAll(tmpDir, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}

		localFilePath := fmt.Sprintf("%s/%s.avro.zst", tmpDir, partitionID)
		s.connector.logger.Info("writing records to local file " + localFilePath)
		avroFile, err := ocfWriter.WriteRecordsToAvroFile(localFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to write records to Avro file: %w", err)
		}

		return avroFile, nil
	} else if strings.HasPrefix(s.config.StagingPath, "s3://") {
		ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema, avro.CompressZstd,
			qvalue.QDWHTypeSnowflake)
		s3o, err := utils.NewS3BucketAndPrefix(s.config.StagingPath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse staging path: %w", err)
		}

		s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, s.config.FlowJobName, partitionID)
		s.connector.logger.Info("OCF: Writing records to S3",
			slog.String(string(shared.PartitionIDKey), partitionID))
		avroFile, err := ocfWriter.WriteRecordsToS3(s3o.Bucket, s3AvroFileKey, utils.S3PeerCredentials{})
		if err != nil {
			return nil, fmt.Errorf("failed to write records to S3: %w", err)
		}

		return avroFile, nil
	}

	return nil, fmt.Errorf("unsupported staging path: %s", s.config.StagingPath)
}

func (s *SnowflakeAvroSyncHandler) putFileToStage(avroFile *avro.AvroFile, stage string) error {
	if avroFile.StorageLocation != avro.AvroLocalStorage {
		s.connector.logger.Info("no file to put to stage")
		return nil
	}

	activity.RecordHeartbeat(s.connector.ctx, "putting file to stage")
	putCmd := fmt.Sprintf("PUT file://%s @%s", avroFile.FilePath, stage)

	shutdown := utils.HeartbeatRoutine(s.connector.ctx, func() string {
		return fmt.Sprintf("putting file to stage %s", stage)
	})
	defer shutdown()

	if _, err := s.connector.database.ExecContext(s.connector.ctx, putCmd); err != nil {
		return fmt.Errorf("failed to put file to stage: %w", err)
	}

	s.connector.logger.Info(fmt.Sprintf("put file %s to stage %s", avroFile.FilePath, stage))
	return nil
}

func (s *SnowflakeAvroSyncHandler) insertMetadata(
	partition *protos.QRepPartition,
	flowJobName string,
	startTime time.Time,
) error {
	partitionLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)
	err := s.connector.pgMetadata.FinishQrepPartition(partition, flowJobName, startTime)
	if err != nil {
		return fmt.Errorf("failed to execute metadata insert statement: %w", err)
	}

	s.connector.logger.Info("inserted metadata for partition", partitionLog)
	return nil
}
