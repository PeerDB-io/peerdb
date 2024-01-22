package connclickhouse

import (
	"database/sql"
	"fmt"
	"log/slog"
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

// func (s *ClickhouseAvroSyncMethod) putFileToStage(avroFile *avro.AvroFile, stage string) error {
// 	if avroFile.StorageLocation != avro.AvroLocalStorage {
// 		s.connector.logger.Info("no file to put to stage")
// 		return nil
// 	}

// 	activity.RecordHeartbeat(s.connector.ctx, "putting file to stage")
// 	putCmd := fmt.Sprintf("PUT file://%s @%s", avroFile.FilePath, stage)

// 	shutdown := utils.HeartbeatRoutine(s.connector.ctx, 10*time.Second, func() string {
// 		return fmt.Sprintf("putting file to stage %s", stage)
// 	})
// 	defer shutdown()

// 	if _, err := s.connector.database.ExecContext(s.connector.ctx, putCmd); err != nil {
// 		return fmt.Errorf("failed to put file to stage: %w", err)
// 	}

// 	s.connector.logger.Info(fmt.Sprintf("put file %s to stage %s", avroFile.FilePath, stage))
// 	return nil
// }

func (s *ClickhouseAvroSyncMethod) CopyStageToDestination(avroFile *avro.AvroFile) error {
	fmt.Printf("\n************************* in CopyStageToDesti stagingPath: %+v", s.config.StagingPath)
	stagingPath := "s3://avro-clickhouse" //s.config.StagingPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return err
	}
	awsCreds, err := utils.GetAWSSecrets(utils.S3PeerCredentials{})
	avroFileUrl := fmt.Sprintf("https://%s.s3.%s.amazonaws.com%s", s3o.Bucket, awsCreds.Region, avroFile.FilePath)

	if err != nil {
		return err
	}
	//nolint:gosec
	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM s3('%s','%s','%s', 'Avro')",
		s.config.DestinationTableIdentifier, avroFileUrl, awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

	fmt.Printf("\n************************ CopyStagingToDestination query: %s\n", query)

	_, err = s.connector.database.Exec(query)

	return err
}

func (s *ClickhouseAvroSyncMethod) SyncRecords(
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
	flowJobName string,
) (int, error) {
	fmt.Printf("\n************************* in qrep_avro_sync: SyncRecords1  dstTableSchema	 %+v", dstTableSchema)
	fmt.Printf("\n************************ in qrep_avro_sync: SyncRecords2 config %+v", s.config)
	//s.config.StagingPath = "s3://avro-clickhouse"
	tableLog := slog.String("destinationTable", s.config.DestinationTableIdentifier)
	dstTableName := s.config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	fmt.Printf("\n******************************* in qrep_avro_sync: SyncRecords3  stream schema %+v", schema)

	s.connector.logger.Info("sync function called and schema acquired", tableLog)

	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	fmt.Printf("\n******************************* in qrep_avro_sync: SyncRecords5  avro schema %+v", avroSchema)

	partitionID := shared.RandomString(16)
	fmt.Printf("\n******************* calling writeToAvroFile partitionId: %+v", partitionID)
	avroFile, err := s.writeToAvroFile(stream, avroSchema, partitionID, flowJobName)
	fmt.Printf("\n******************* records written to avrofile %+v", avroFile)
	if err != nil {
		return 0, err
	}
	defer avroFile.Cleanup()
	s.connector.logger.Info(fmt.Sprintf("written %d records to Avro file", avroFile.NumRecords), tableLog)

	// stage := s.connector.getStageNameForJob(s.config.FlowJobName)
	// err = s.connector.createStage(stage, s.config)
	// if err != nil {
	// 	return 0, err
	// }
	// s.connector.logger.Info(fmt.Sprintf("Created stage %s", stage))

	// colNames, _, err := s.connector.getColsFromTable(s.config.DestinationTableIdentifier)
	// if err != nil {
	// 	return 0, err
	// }

	// err = s.putFileToStage(avroFile, "stage")
	// if err != nil {
	// 	return 0, err
	// }
	// s.connector.logger.Info("pushed avro file to stage", tableLog)

	// err = CopyStageToDestination(s.connector, s.config, s.config.DestinationTableIdentifier, stage, colNames)
	// if err != nil {
	// 	return 0, err
	// }
	// s.connector.logger.Info(fmt.Sprintf("copying records into %s from stage %s",
	// 	s.config.DestinationTableIdentifier, stage))

	//Copy stage/avro to destination
	err = s.CopyStageToDestination(avroFile)
	fmt.Printf("\n ***************** in qrep_avro_sync: SyncRecords after CopyStageToDestination err: %+v", err)
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
	fmt.Printf("\n************************* in SyncQRepRecords 1")
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier
	s.config.StagingPath = "s3://avro-clickhouse"

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

	s3o, err := utils.NewS3BucketAndPrefix(s.config.StagingPath)
	if err != nil {
		return 0, err
	}
	awsCreds, err := utils.GetAWSSecrets(utils.S3PeerCredentials{})
	avroFileUrl := fmt.Sprintf("https://%s.s3.%s.amazonaws.com%s", s3o.Bucket, awsCreds.Region, avroFile.FilePath)

	fmt.Printf("\n*********************** in qrep_avro_sync SyncQRepRecords 4 avroFileUrl: %+v", avroFileUrl)

	if err != nil {
		return 0, err
	}
	//nolint:gosec
	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM s3('%s','%s','%s', 'Avro')",
		config.DestinationTableIdentifier, avroFileUrl, awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

	fmt.Printf("\n************************************ in qrep_avro_sync SyncQRepRecords 5 query: %s\n", query)

	_, err = s.connector.database.Exec(query)

	fmt.Printf("\n************************************ in qrep_avro_sync SyncQRepRecords 6 err: %+v\n", err)

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
	stagingPath := "s3://avro-clickhouse" //s.config.StagingPath //
	fmt.Printf("\n****************************************** StagingPath: %+v*****\n", s.config.StagingPath)
	ocfWriter := avro.NewPeerDBOCFWriter(s.connector.ctx, stream, avroSchema, avro.CompressZstd,
		qvalue.QDWHTypeClickhouse)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro.zst", s3o.Prefix, flowJobName, partitionID)           // s.config.FlowJobName
	avroFile, err := ocfWriter.WriteRecordsToS3(s3o.Bucket, s3AvroFileKey, utils.S3PeerCredentials{}) ///utils.S3PeerCredentials{})
	fmt.Printf("\n************************* writeToAvroFile 2 avroFile %+v, err: %+v", avroFile, err)
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
		return fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	if _, err := s.connector.database.Exec(insertMetadataStmt); err != nil {
		return fmt.Errorf("failed to execute metadata insert statement: %v", err)
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
