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
	flowJobName string,
	dstTableName string,
	partition *protos.QRepPartition,
	dstTableSchema []*sql.ColumnType,
	records *model.QRecordBatch) (int, error) {
	startTime := time.Now()

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
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s FROM @%s %s", dstTableName, stage, strings.Join(copyOpts, ","))
	if _, err = s.connector.database.Exec(copyCmd); err != nil {
		return 0, fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	log.Infof("copied file from stage %s to table %s", stage, dstTableName)

	// Insert metadata statement
	insertMetadataStmt, err := s.connector.createMetadataInsertStatement(partition, flowJobName, startTime)
	if err != nil {
		return -1, fmt.Errorf("failed to create metadata insert statement: %v", err)
	}

	// Execute the metadata insert statement
	if _, err = s.connector.database.Exec(insertMetadataStmt); err != nil {
		return -1, fmt.Errorf("failed to execute metadata insert statement: %v", err)
	}

	log.Infof("pushed %d records to local file %s and loaded into Snowflake table %s",
		len(records.Records), localFilePath, dstTableName)
	return len(records.Records), nil
}
