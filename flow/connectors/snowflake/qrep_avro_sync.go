package connsnowflake

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/connectors/utils/metrics"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
	log "github.com/sirupsen/logrus"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeAvroSyncMethod struct {
	config    *protos.QRepConfig
	connector *SnowflakeConnector
}

func NewSnowflakeAvroSyncMethod(
	config *protos.QRepConfig,
	connector *SnowflakeConnector) *SnowflakeAvroSyncMethod {
	return &SnowflakeAvroSyncMethod{
		config:    config,
		connector: connector,
	}
}

func (s *SnowflakeAvroSyncMethod) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	avroSchema, err := s.getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	numRecords, localFilePath, err := s.writeToAvroFile(stream, avroSchema, partition.PartitionId)
	if err != nil {
		return 0, err
	}

	stage := s.connector.getStageNameForJob(config.FlowJobName)

	putFileStartTime := time.Now()
	err = s.putFileToStage(localFilePath, stage)
	if err != nil {
		return 0, err
	}
	metrics.LogQRepSyncMetrics(s.connector.ctx, config.FlowJobName, int64(numRecords),
		time.Since(putFileStartTime))

	err = s.insertMetadata(partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	return numRecords, nil
}

func (s *SnowflakeAvroSyncMethod) getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	log.Infof("Avro schema: %v\n", avroSchema)
	return avroSchema, nil
}

func (s *SnowflakeAvroSyncMethod) writeToAvroFile(
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
) (int, string, error) {
	var numRecords int
	if s.config.StagingPath == "" {
		tmpDir, err := os.MkdirTemp("", "peerdb-avro")
		if err != nil {
			return 0, "", fmt.Errorf("failed to create temp dir: %w", err)
		}

		localFilePath := fmt.Sprintf("%s/%s.avro", tmpDir, partitionID)
		numRecords, err = avro.WriteRecordsToAvroFile(stream, avroSchema, localFilePath)
		if err != nil {
			return 0, "", fmt.Errorf("failed to write records to Avro file: %w", err)
		}

		return numRecords, localFilePath, nil
	} else if strings.HasPrefix(s.config.StagingPath, "s3://") {
		s3o, err := utils.NewS3BucketAndPrefix(s.config.StagingPath)
		if err != nil {
			return 0, "", fmt.Errorf("failed to parse staging path: %w", err)
		}

		s3Key := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, s.config.FlowJobName, partitionID)
		numRecords, err = avro.WriteRecordsToS3(stream, avroSchema, s3o.Bucket, s3Key)
		if err != nil {
			return 0, "", fmt.Errorf("failed to write records to S3: %w", err)
		}

		return numRecords, "", nil
	}

	return 0, "", fmt.Errorf("unsupported staging path: %s", s.config.StagingPath)
}

func (s *SnowflakeAvroSyncMethod) putFileToStage(localFilePath string, stage string) error {
	if localFilePath == "" {
		log.Infof("no file to put to stage")
		return nil
	}

	putCmd := fmt.Sprintf("PUT file://%s @%s", localFilePath, stage)
	if _, err := s.connector.database.Exec(putCmd); err != nil {
		return fmt.Errorf("failed to put file to stage: %w", err)
	}

	log.Infof("put file %s to stage %s", localFilePath, stage)
	return nil
}

func CopyStageToDestination(
	connector *SnowflakeConnector,
	config *protos.QRepConfig,
	dstTableName string,
	stage string,
	allCols []string,
) error {
	copyOpts := []string{
		"FILE_FORMAT = (TYPE = AVRO)",
		"MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
		"PURGE = TRUE",
	}

	writeHandler := NewSnowflakeAvroWriteHandler(connector, dstTableName, stage, copyOpts)

	appendMode := true
	if config.WriteMode != nil {
		writeType := config.WriteMode.WriteType
		if writeType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
			appendMode = false
		}
	}

	switch appendMode {
	case true:
		err := writeHandler.HandleAppendMode(config.FlowJobName)
		if err != nil {
			return fmt.Errorf("failed to handle append mode: %w", err)
		}

	case false:
		upsertKeyCols := config.WriteMode.UpsertKeyColumns
		err := writeHandler.HandleUpsertMode(allCols, upsertKeyCols, config.WatermarkColumn,
			config.FlowJobName)
		if err != nil {
			return fmt.Errorf("failed to handle upsert mode: %w", err)
		}
	}

	return nil
}

func (s *SnowflakeAvroSyncMethod) insertMetadata(
	partition *protos.QRepPartition,
	flowJobName string,
	startTime time.Time,
) error {
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		return fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	if _, err := s.connector.database.Exec(insertMetadataStmt); err != nil {
		return fmt.Errorf("failed to execute metadata insert statement: %v", err)
	}

	log.Infof("inserted metadata for partition %s", partition)
	return nil
}

type SnowflakeAvroWriteHandler struct {
	connector    *SnowflakeConnector
	dstTableName string
	stage        string
	copyOpts     []string
}

// NewSnowflakeAvroWriteHandler creates a new SnowflakeAvroWriteHandler
func NewSnowflakeAvroWriteHandler(
	connector *SnowflakeConnector,
	dstTableName string,
	stage string,
	copyOpts []string,
) *SnowflakeAvroWriteHandler {
	return &SnowflakeAvroWriteHandler{
		connector:    connector,
		dstTableName: dstTableName,
		stage:        stage,
		copyOpts:     copyOpts,
	}
}

func (s *SnowflakeAvroWriteHandler) HandleAppendMode(flowJobName string) error {
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s", s.dstTableName, s.stage, strings.Join(s.copyOpts, ","))
	log.Infof("running copy command: %s", copyCmd)
	_, err := s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}

	log.Infof("copied file from stage %s to table %s", s.stage, s.dstTableName)
	return nil
}

func GenerateMergeCommand(
	allCols []string,
	upsertKeyCols []string,
	watermarkCol string,
	tempTableName string,
	dstTable string,
) (string, error) {
	// all cols are acquired from snowflake schema, so let us try to make upsert key cols match the case
	// and also the watermark col, then the quoting should be fine
	caseMatchedCols := map[string]string{}
	for _, col := range allCols {
		caseMatchedCols[strings.ToLower(col)] = col
	}

	for i, col := range upsertKeyCols {
		upsertKeyCols[i] = caseMatchedCols[strings.ToLower(col)]
	}

	watermarkCol, ok := caseMatchedCols[strings.ToLower(watermarkCol)]
	if !ok {
		return "", fmt.Errorf("watermark column '%s' not found in destination table", watermarkCol)
	}

	upsertKeys := []string{}
	partitionKeyCols := []string{}
	for _, key := range upsertKeyCols {
		quotedKey := utils.QuoteIdentifier(key)
		upsertKeys = append(upsertKeys, fmt.Sprintf("dst.%s = src.%s", quotedKey, quotedKey))
		partitionKeyCols = append(partitionKeyCols, quotedKey)
	}
	upsertKeyClause := strings.Join(upsertKeys, " AND ")

	updateSetClauses := []string{}
	insertColumnsClauses := []string{}
	insertValuesClauses := []string{}
	for _, column := range allCols {
		quotedColumn := utils.QuoteIdentifier(column)
		updateSetClauses = append(updateSetClauses, fmt.Sprintf("%s = src.%s", quotedColumn, quotedColumn))
		insertColumnsClauses = append(insertColumnsClauses, quotedColumn)
		insertValuesClauses = append(insertValuesClauses, fmt.Sprintf("src.%s", quotedColumn))
	}
	updateSetClause := strings.Join(updateSetClauses, ", ")
	insertColumnsClause := strings.Join(insertColumnsClauses, ", ")
	insertValuesClause := strings.Join(insertValuesClauses, ", ")

	quotedWMC := utils.QuoteIdentifier(watermarkCol)

	selectCmd := fmt.Sprintf(`
		SELECT *
		FROM %s
		QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) = 1
	`, tempTableName, strings.Join(partitionKeyCols, ","), quotedWMC)

	mergeCmd := fmt.Sprintf(`
		MERGE INTO %s dst
		USING (%s) src
		ON %s
		WHEN MATCHED AND src.%s > dst.%s THEN UPDATE SET %s
		WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
	`, dstTable, selectCmd, upsertKeyClause, quotedWMC, quotedWMC,
		updateSetClause, insertColumnsClause, insertValuesClause)

	return mergeCmd, nil
}

// HandleUpsertMode handles the upsert mode
func (s *SnowflakeAvroWriteHandler) HandleUpsertMode(
	allCols []string,
	upsertKeyCols []string,
	watermarkCol string,
	flowJobName string,
) error {
	runID, err := util.RandomUInt64()
	if err != nil {
		return fmt.Errorf("failed to generate run ID: %w", err)
	}

	tempTableName := fmt.Sprintf("%s_temp_%d", s.dstTableName, runID)

	//nolint:gosec
	createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
		tempTableName, s.dstTableName)
	if _, err := s.connector.database.Exec(createTempTableCmd); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	log.Infof("created temp table %s", tempTableName)

	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s",
		tempTableName, s.stage, strings.Join(s.copyOpts, ","))
	_, err = s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	log.Infof("copied file from stage %s to temp table %s", s.stage, tempTableName)

	mergeCmd, err := GenerateMergeCommand(allCols, upsertKeyCols, watermarkCol, tempTableName, s.dstTableName)
	if err != nil {
		return fmt.Errorf("failed to generate merge command: %w", err)
	}

	startTime := time.Now()
	rows, err := s.connector.database.Exec(mergeCmd)
	if err != nil {
		return fmt.Errorf("failed to merge data into destination table '%s': %w", mergeCmd, err)
	}
	rowCount, err := rows.RowsAffected()
	if err == nil {
		totalRowsAtTarget, err := s.connector.getTableCounts([]string{s.dstTableName})
		if err != nil {
			return err
		}
		metrics.LogQRepNormalizeMetrics(s.connector.ctx, flowJobName, rowCount, time.Since(startTime),
			totalRowsAtTarget)
	} else {
		log.Errorf("failed to get rows affected: %v", err)
	}

	log.Infof("merged data from temp table %s into destination table %s",
		tempTableName, s.dstTableName)
	return nil
}
