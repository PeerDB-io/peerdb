package connsnowflake

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	util "github.com/PeerDB-io/peer-flow/utils"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeAvroSyncMethod struct {
	connector *SnowflakeConnector
	localDir  string
}

func NewSnowflakeAvroSyncMethod(connector *SnowflakeConnector, localDir string) *SnowflakeAvroSyncMethod {
	return &SnowflakeAvroSyncMethod{
		connector: connector,
		localDir:  localDir,
	}
}

func (s *SnowflakeAvroSyncMethod) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	records *model.QRecordBatch,
) (int, error) {
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier
	avroSchema, err := s.getAvroSchema(dstTableName, records.Schema)
	if err != nil {
		return 0, err
	}

	localFilePath, err := s.writeToAvroFile(records, avroSchema, partition.PartitionId)
	if err != nil {
		return 0, err
	}

	stage := s.connector.getStageNameForJob(config.FlowJobName)

	err = s.putFileToStage(localFilePath, stage)
	if err != nil {
		return 0, err
	}

	err = s.insertMetadata(partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, err
	}

	return len(records.Records), nil
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
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
) (string, error) {
	localFilePath := fmt.Sprintf("%s/%s.avro", s.localDir, partitionID)
	err := WriteRecordsToAvroFile(records, avroSchema, localFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to write records to Avro file: %w", err)
	}

	return localFilePath, nil
}

func (s *SnowflakeAvroSyncMethod) putFileToStage(localFilePath string, stage string) error {
	putCmd := fmt.Sprintf("PUT file://%s @%s", localFilePath, stage)
	if _, err := s.connector.database.Exec(putCmd); err != nil {
		return fmt.Errorf("failed to put file to stage: %w", err)
	}

	log.Infof("put file %s to stage %s", localFilePath, stage)
	return nil
}

func CopyStageToDestination(
	database *sql.DB,
	config *protos.QRepConfig,
	dstTableName string,
	stage string,
	allCols []string,
) error {
	copyOpts := []string{
		"FILE_FORMAT = (TYPE = AVRO)",
		"MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
	}

	writeHandler := NewSnowflakeAvroWriteHandler(database, dstTableName, stage, copyOpts)

	appendMode := true
	if config.WriteMode != nil {
		wirteType := config.WriteMode.WriteType
		if wirteType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
			appendMode = false
		}
	}

	switch appendMode {
	case true:
		err := writeHandler.HandleAppendMode()
		if err != nil {
			return fmt.Errorf("failed to handle append mode: %w", err)
		}

	case false:
		upsertKeyCols := config.WriteMode.UpsertKeyColumns
		err := writeHandler.HandleUpsertMode(allCols, upsertKeyCols, config.WatermarkColumn)
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

func WriteRecordsToAvroFile(
	records *model.QRecordBatch,
	avroSchema *model.QRecordAvroSchemaDefinition,
	filePath string,
) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create OCF Writer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      file,
		Schema: avroSchema.Schema,
	})
	if err != nil {
		return fmt.Errorf("failed to create OCF writer: %w", err)
	}

	colNames := records.Schema.GetColumnNames()

	// Write each QRecord to the OCF file
	for _, qRecord := range records.Records {
		avroConverter := model.NewQRecordAvroConverter(
			qRecord,
			qvalue.QDWHTypeSnowflake,
			&avroSchema.NullableFields,
			colNames,
		)
		avroMap, err := avroConverter.Convert()
		if err != nil {
			log.Errorf("failed to convert QRecord to Avro compatible map: %v", err)
			return fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			log.Errorf("failed to write record to OCF file: %v", err)
			return fmt.Errorf("failed to write record to OCF file: %w", err)
		}
	}

	return nil
}

type SnowflakeAvroWriteHandler struct {
	db           *sql.DB
	dstTableName string
	stage        string
	copyOpts     []string
}

// NewSnowflakeAvroWriteHandler creates a new SnowflakeAvroWriteHandler
func NewSnowflakeAvroWriteHandler(
	db *sql.DB,
	dstTableName string,
	stage string,
	copyOpts []string,
) *SnowflakeAvroWriteHandler {
	return &SnowflakeAvroWriteHandler{
		db:           db,
		dstTableName: dstTableName,
		stage:        stage,
		copyOpts:     copyOpts,
	}
}

func (s *SnowflakeAvroWriteHandler) HandleAppendMode() error {
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s", s.dstTableName, s.stage, strings.Join(s.copyOpts, ","))
	if _, err := s.db.Exec(copyCmd); err != nil {
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
) error {
	runID, err := util.RandomUInt64()
	if err != nil {
		return fmt.Errorf("failed to generate run ID: %w", err)
	}

	tempTableName := fmt.Sprintf("%s_temp_%d", s.dstTableName, runID)

	//nolint:gosec
	createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
		tempTableName, s.dstTableName)
	if _, err := s.db.Exec(createTempTableCmd); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	log.Infof("created temp table %s", tempTableName)

	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s",
		tempTableName, s.stage, strings.Join(s.copyOpts, ","))
	if _, err := s.db.Exec(copyCmd); err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	log.Infof("copied file from stage %s to temp table %s", s.stage, tempTableName)

	mergeCmd, err := GenerateMergeCommand(allCols, upsertKeyCols, watermarkCol, tempTableName, s.dstTableName)
	if err != nil {
		return fmt.Errorf("failed to generate merge command: %w", err)
	}

	if _, err := s.db.Exec(mergeCmd); err != nil {
		return fmt.Errorf("failed to merge data into destination table '%s': %w", mergeCmd, err)
	}

	log.Infof("merged data from temp table %s into destination table %s",
		tempTableName, s.dstTableName)
	return nil
}
