package connsnowflake

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

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
	records *model.QRecordBatch) (int, error) {
	startTime := time.Now()
	dstTableName := config.DestinationTableIdentifier

	// You will need to define your Avro schema as a string
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, records.Schema)
	if err != nil {
		return 0, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	fmt.Printf("Avro schema: %v\n", avroSchema)

	// Create a local file path with flowJobName and partitionID
	localFilePath := fmt.Sprintf("%s/%s.avro", s.localDir, partition.PartitionId)
	file, err := os.Create(localFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create OCF Writer
	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      file,
		Schema: avroSchema.Schema,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create OCF writer: %w", err)
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
			return 0, fmt.Errorf("failed to convert QRecord to Avro compatible map: %w", err)
		}

		err = ocfWriter.Append([]interface{}{avroMap})
		if err != nil {
			log.Errorf("failed to write record to OCF file: %v", err)
			return 0, fmt.Errorf("failed to write record to OCF file: %w", err)
		}
	}

	// this runID is just used for the staging table name
	runID, err := util.RandomUInt64()
	if err != nil {
		return 0, fmt.Errorf("failed to generate run ID: %w", err)
	}

	// create temp stag
	stage := fmt.Sprintf("%s_%d", dstTableName, runID)
	createStageCmd := fmt.Sprintf("CREATE TEMPORARY STAGE %s FILE_FORMAT = (TYPE = AVRO)", stage)
	if _, err = s.connector.database.Exec(createStageCmd); err != nil {
		return 0, fmt.Errorf("failed to create temp stage: %w", err)
	}
	log.Infof("created temp stage %s", stage)

	// Put the local Avro file to the Snowflake stage
	putCmd := fmt.Sprintf("PUT file://%s @%s", localFilePath, stage)
	if _, err = s.connector.database.Exec(putCmd); err != nil {
		return 0, fmt.Errorf("failed to put file to stage: %w", err)
	}
	log.Infof("put file %s to stage %s", localFilePath, stage)

	// write this file to snowflake using COPY INTO statement
	copyOpts := []string{
		"FILE_FORMAT = (TYPE = AVRO)",
		"MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
	}

	// based on the write mode we will upsert or append, write mode.
	writeType := config.WriteMode.WriteType
	switch writeType {
	case protos.QRepWriteType_QREP_WRITE_MODE_APPEND:
		//nolint:gosec
		copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s", dstTableName, stage, strings.Join(copyOpts, ","))
		if _, err = s.connector.database.Exec(copyCmd); err != nil {
			return 0, fmt.Errorf("failed to run COPY INTO command: %w", err)
		}
		log.Infof("copied file from stage %s to table %s", stage, dstTableName)

	case protos.QRepWriteType_QREP_WRITE_MODE_UPSERT:
		tempTableName := fmt.Sprintf("%s_temp_%d", dstTableName, runID)

		// Create a temporary table
		//nolint:gosec
		createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
			tempTableName, dstTableName)
		if _, err = s.connector.database.Exec(createTempTableCmd); err != nil {
			return 0, fmt.Errorf("failed to create temp table: %w", err)
		}
		log.Infof("created temp table %s", tempTableName)

		// Copy the data into the temporary table
		//nolint:gosec
		copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s", tempTableName, stage, strings.Join(copyOpts, ","))
		if _, err = s.connector.database.Exec(copyCmd); err != nil {
			return 0, fmt.Errorf("failed to run COPY INTO command: %w", err)
		}
		log.Infof("copied file from stage %s to temp table %s", stage, tempTableName)

		// Define upsert keys
		upsertKey := strings.Join(config.WriteMode.UpsertKeyColumns, ", ")

		updateSetClauses := []string{}
		insertColumnsClauses := []string{}
		insertValuesClauses := []string{}
		for _, column := range colNames {
			updateSetClauses = append(updateSetClauses, fmt.Sprintf("%s = src.%s", column, column))
			insertColumnsClauses = append(insertColumnsClauses, column)
			insertValuesClauses = append(insertValuesClauses, fmt.Sprintf("src.%s", column))
		}
		updateSetClause := strings.Join(updateSetClauses, ", ")
		insertColumnsClause := strings.Join(insertColumnsClauses, ", ")
		insertValuesClause := strings.Join(insertValuesClauses, ", ")

		// Merge (upsert) the data into the destination table
		//nolint:gosec
		mergeCmd := fmt.Sprintf(`
			MERGE INTO %s dst
			USING %s src
			ON %s
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
		`, dstTableName, tempTableName, upsertKey, updateSetClause,
			insertColumnsClause, insertValuesClause)
		if _, err = s.connector.database.Exec(mergeCmd); err != nil {
			return 0, fmt.Errorf("failed to merge data into destination table: %w", err)
		}
		log.Infof("merged data from temp table %s into destination table %s", tempTableName, dstTableName)
	default:
		return 0, fmt.Errorf("unknown write type: %v", writeType)
	}

	// Insert metadata statement
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, config.FlowJobName, startTime)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	// Execute the metadata insert statement
	if _, err = s.connector.database.Exec(insertMetadataStmt); err != nil {
		return -1, fmt.Errorf("failed to execute metadata insert statement: %v", err)
	}
	log.Infof("inserted metadata for partition %s", partition)

	return len(records.Records), nil
}
