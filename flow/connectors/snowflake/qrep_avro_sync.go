package connsnowflake

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	util "github.com/PeerDB-io/peer-flow/utils"
	_ "github.com/snowflakedb/gosnowflake"
	"go.temporal.io/sdk/activity"
)

type CopyInfo struct {
	transformationSQL string
	columnsSQL        string
}

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

func (s *SnowflakeAvroSyncMethod) SyncRecords(
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

	partitionID := util.RandomString(16)
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

	colInfo, err := s.connector.getColsFromTable(s.config.DestinationTableIdentifier)
	if err != nil {
		return 0, err
	}

	allCols := colInfo.Columns
	err = s.putFileToStage(avroFile, stage)
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info("pushed avro file to stage", tableLog)

	err = CopyStageToDestination(s.connector, s.config, s.config.DestinationTableIdentifier, stage, allCols)
	if err != nil {
		return 0, err
	}
	s.connector.logger.Info(fmt.Sprintf("copying records into %s from stage %s", s.config.DestinationTableIdentifier, stage))

	return avroFile.NumRecords, nil
}

func (s *SnowflakeAvroSyncMethod) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	stream *model.QRecordStream,
) (int, error) {
	partitionLog := slog.String(string(shared.PartitionIdKey), partition.PartitionId)
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return -1, fmt.Errorf("failed to get schema from stream: %w", err)
	}
	s.connector.logger.Info("sync function called and schema acquired", partitionLog)

	err = s.addMissingColumns(
		config.FlowJobName,
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

func (s *SnowflakeAvroSyncMethod) addMissingColumns(
	flowJobName string,
	schema *model.QRecordSchema,
	dstTableSchema []*sql.ColumnType,
	dstTableName string,
	partition *protos.QRepPartition,
) error {
	partitionLog := slog.String(string(shared.PartitionIdKey), partition.PartitionId)
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

			if _, err := tx.Exec(alterTableCmd); err != nil {
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

func (s *SnowflakeAvroSyncMethod) getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	s.connector.logger.Info(fmt.Sprintf("Avro schema: %v\n", avroSchema))
	return avroSchema, nil
}

func (s *SnowflakeAvroSyncMethod) writeToAvroFile(
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
			slog.String(string(shared.PartitionIdKey), partitionID))
		avroFile, err := ocfWriter.WriteRecordsToS3(s3o.Bucket, s3AvroFileKey, utils.S3PeerCredentials{})
		if err != nil {
			return nil, fmt.Errorf("failed to write records to S3: %w", err)
		}

		return avroFile, nil
	}

	return nil, fmt.Errorf("unsupported staging path: %s", s.config.StagingPath)
}

func (s *SnowflakeAvroSyncMethod) putFileToStage(avroFile *avro.AvroFile, stage string) error {
	if avroFile.StorageLocation != avro.AvroLocalStorage {
		s.connector.logger.Info("no file to put to stage")
		return nil
	}

	activity.RecordHeartbeat(s.connector.ctx, "putting file to stage")
	putCmd := fmt.Sprintf("PUT file://%s @%s", avroFile.FilePath, stage)

	shutdown := utils.HeartbeatRoutine(s.connector.ctx, 10*time.Second, func() string {
		return fmt.Sprintf("putting file to stage %s", stage)
	})

	defer func() {
		shutdown <- struct{}{}
	}()

	if _, err := s.connector.database.Exec(putCmd); err != nil {
		return fmt.Errorf("failed to put file to stage: %w", err)
	}

	s.connector.logger.Info(fmt.Sprintf("put file %s to stage %s", avroFile.FilePath, stage))
	return nil
}

func (c *SnowflakeConnector) GetCopyTransformation(
	dstTableName string,
) (*CopyInfo, error) {
	colInfo, colsErr := c.getColsFromTable(dstTableName)
	if colsErr != nil {
		return nil, fmt.Errorf("failed to get columns from  destination table: %w", colsErr)
	}

	var transformations []string
	var columnOrder []string
	for colName, colType := range colInfo.ColumnMap {
		columnOrder = append(columnOrder, fmt.Sprintf("\"%s\"", colName))
		switch colType {
		case "GEOGRAPHY":
			transformations = append(transformations,
				fmt.Sprintf("TO_GEOGRAPHY($1:\"%s\"::string, true) AS \"%s\"", strings.ToLower(colName), colName))
		case "GEOMETRY":
			transformations = append(transformations,
				fmt.Sprintf("TO_GEOMETRY($1:\"%s\"::string, true) AS \"%s\"", strings.ToLower(colName), colName))
		case "NUMBER":
			transformations = append(transformations,
				fmt.Sprintf("$1:\"%s\" AS \"%s\"", strings.ToLower(colName), colName))
		default:
			transformations = append(transformations,
				fmt.Sprintf("($1:\"%s\")::%s AS \"%s\"", strings.ToLower(colName), colType, colName))
		}
	}
	transformationSQL := strings.Join(transformations, ",")
	columnsSQL := strings.Join(columnOrder, ",")
	return &CopyInfo{transformationSQL, columnsSQL}, nil
}

func CopyStageToDestination(
	connector *SnowflakeConnector,
	config *protos.QRepConfig,
	dstTableName string,
	stage string,
	allCols []string,
) error {
	connector.logger.Info("Copying stage to destination " + dstTableName)
	copyOpts := []string{
		"FILE_FORMAT = (TYPE = AVRO)",
		"PURGE = TRUE",
		"ON_ERROR = 'CONTINUE'",
	}

	writeHandler := NewSnowflakeAvroWriteHandler(connector, dstTableName, stage, copyOpts)

	appendMode := true
	if config.WriteMode != nil {
		writeType := config.WriteMode.WriteType
		if writeType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
			appendMode = false
		}
	}

	copyTransformation, err := connector.GetCopyTransformation(dstTableName)
	if err != nil {
		return fmt.Errorf("failed to get copy transformation: %w", err)
	}
	switch appendMode {
	case true:
		err := writeHandler.HandleAppendMode(copyTransformation)
		if err != nil {
			return fmt.Errorf("failed to handle append mode: %w", err)
		}

	case false:
		upsertKeyCols := config.WriteMode.UpsertKeyColumns
		err := writeHandler.HandleUpsertMode(allCols, upsertKeyCols, config.WatermarkColumn,
			config.FlowJobName, copyTransformation)
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
	partitionLog := slog.String(string(shared.PartitionIdKey), partition.PartitionId)
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		s.connector.logger.Error("failed to create metadata insert statement",
			slog.Any("error", err), partitionLog)
		return fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	if _, err := s.connector.database.Exec(insertMetadataStmt); err != nil {
		s.connector.logger.Error("failed to execute metadata insert statement "+insertMetadataStmt,
			slog.Any("error", err), partitionLog)
		return fmt.Errorf("failed to execute metadata insert statement: %v", err)
	}

	s.connector.logger.Info("inserted metadata for partition", partitionLog)
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

func (s *SnowflakeAvroWriteHandler) HandleAppendMode(
	copyInfo *CopyInfo) error {
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) %s",
		s.dstTableName, copyInfo.columnsSQL, copyInfo.transformationSQL, s.stage, strings.Join(s.copyOpts, ","))
	s.connector.logger.Info("running copy command: " + copyCmd)
	_, err := s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}

	s.connector.logger.Info("copied file from stage " + s.stage + " to table " + s.dstTableName)
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
	selectCmd := fmt.Sprintf(`
		SELECT *
		FROM %s
		QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) = 1
	`, tempTableName, strings.Join(partitionKeyCols, ","), partitionKeyCols[0])

	mergeCmd := fmt.Sprintf(`
			MERGE INTO %s dst
			USING (%s) src
			ON %s
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
		`, dstTable, selectCmd, upsertKeyClause,
		updateSetClause, insertColumnsClause, insertValuesClause)

	return mergeCmd, nil
}

// HandleUpsertMode handles the upsert mode
func (s *SnowflakeAvroWriteHandler) HandleUpsertMode(
	allCols []string,
	upsertKeyCols []string,
	watermarkCol string,
	flowJobName string,
	copyInfo *CopyInfo,
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
	s.connector.logger.Info("created temp table " + tempTableName)

	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) %s",
		tempTableName, copyInfo.columnsSQL, copyInfo.transformationSQL, s.stage, strings.Join(s.copyOpts, ","))
	_, err = s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	s.connector.logger.Info("copied file from stage " + s.stage + " to temp table " + tempTableName)

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
		s.connector.logger.Info(fmt.Sprintf("merged %d rows into destination table %s, total rows at target: %d",
			rowCount, s.dstTableName, totalRowsAtTarget))
	} else {
		s.connector.logger.Error("failed to get rows affected", slog.Any("error", err))
	}

	s.connector.logger.Info(fmt.Sprintf("merged data from temp table %s into destination table %s, time taken %v",
		tempTableName, s.dstTableName, time.Since(startTime)))
	return nil
}
