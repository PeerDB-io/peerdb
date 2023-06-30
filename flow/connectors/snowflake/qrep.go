package connsnowflake

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *SnowflakeConnector) GetQRepPartitions(config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) PullQRepRecords(config *protos.QRepConfig,
	partition *protos.QRepPartition,
) (*model.QRecordBatch, error) {
	panic("not implemented")
}

func (c *SnowflakeConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier

	tblSchema, err := c.getTableSchema(destTable)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema of table %s: %w", destTable, err)
	}

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition %s is synced: %w", partition.PartitionId, err)
	}

	if done {
		log.Infof("Partition %s has already been synced", partition.PartitionId)
		return 0, nil
	}

	syncMode := config.SyncMode
	switch syncMode {
	case protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT:
		return 0, fmt.Errorf("multi-insert sync mode not supported for snowflake")
	case protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO:
		avroSync := NewSnowflakeAvroSyncMethod(config, c)
		return avroSync.SyncQRepRecords(config, partition, tblSchema, records)
	default:
		return 0, fmt.Errorf("unsupported sync mode: %s", syncMode)
	}
}

func (c *SnowflakeConnector) createMetadataInsertStatement(
	partition *protos.QRepPartition,
	jobName string,
	startTime time.Time,
) (string, error) {
	// marshal the partition to json using protojson
	pbytes, err := protojson.Marshal(partition)
	if err != nil {
		return "", fmt.Errorf("failed to marshal partition to json: %v", err)
	}

	// convert the bytes to string
	partitionJSON := string(pbytes)

	insertMetadataStmt := fmt.Sprintf(
		`INSERT INTO %s.%s
			(flowJobName, partitionID, syncPartition, syncStartTime, syncFinishTime)
			VALUES ('%s', '%s', '%s', '%s'::timestamp, CURRENT_TIMESTAMP);`,
		"public", qRepMetadataTableName, jobName, partition.PartitionId,
		partitionJSON, startTime.Format(time.RFC3339))

	return insertMetadataStmt, nil
}

func (c *SnowflakeConnector) getTableSchema(tableName string) ([]*sql.ColumnType, error) {
	//nolint:gosec
	queryString := fmt.Sprintf(`
	SELECT *
	FROM %s
	LIMIT 0
	`, tableName)

	rows, err := c.database.Query(queryString)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	return columnTypes, nil
}

func (c *SnowflakeConnector) isPartitionSynced(partitionID string) (bool, error) {
	//nolint:gosec
	queryString := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM _peerdb_query_replication_metadata
		WHERE partitionID = '%s'
	`, partitionID)

	row := c.database.QueryRow(queryString)

	var count int
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return count > 0, nil
}

func (c *SnowflakeConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	// Define the schema
	schemaStatement := `
		CREATE TABLE IF NOT EXISTS %s.%s (
			flowJobName STRING,
			partitionID STRING,
			syncPartition STRING,
			syncStartTime TIMESTAMP_LTZ,
			syncFinishTime TIMESTAMP_LTZ
		);
	`
	queryString := fmt.Sprintf(schemaStatement, "public", qRepMetadataTableName)

	// Execute the query
	_, err := c.database.Exec(queryString)
	if err != nil {
		return fmt.Errorf("failed to create table %s.%s: %w", "public", qRepMetadataTableName, err)
	}

	log.Infof("Created table %s", qRepMetadataTableName)

	stageName := c.getStageNameForJob(config.FlowJobName)

	var createStageStmt string
	// if config staging path starts with S3 we need to create an external stage.
	if strings.HasPrefix(config.StagingPath, "s3://") {
		awsCreds, err := utils.GetAWSSecrets()
		if err != nil {
			log.Errorf("failed to get AWS secrets: %v", err)
			return fmt.Errorf("failed to get AWS secrets: %w", err)
		}

		credsStr := fmt.Sprintf("CREDENTIALS=(AWS_KEY_ID='%s' AWS_SECRET_KEY='%s')",
			awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

		stageStatement := `
			CREATE OR REPLACE STAGE %s
			URL = '%s/%s'
			%s
			FILE_FORMAT = (TYPE = AVRO);
			`
		createStageStmt = fmt.Sprintf(stageStatement, stageName, config.StagingPath, config.FlowJobName, credsStr)
	} else {
		stageStatement := `
			CREATE OR REPLACE STAGE %s
			FILE_FORMAT = (TYPE = AVRO);
			`
		createStageStmt = fmt.Sprintf(stageStatement, stageName)
	}

	// Execute the query
	_, err = c.database.Exec(createStageStmt)
	if err != nil {
		log.Errorf("failed to create stage %s: %v", stageName, err)
		return fmt.Errorf("failed to create stage %s: %w", stageName, err)
	}

	log.Infof("Created stage %s", stageName)
	return nil
}

func (c *SnowflakeConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	log.Infof("Consolidating partitions for flow job %s", config.FlowJobName)

	destTable := config.DestinationTableIdentifier
	stageName := c.getStageNameForJob(config.FlowJobName)

	syncMode := config.SyncMode
	switch syncMode {
	case protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT:
		return fmt.Errorf("multi-insert sync mode not supported for snowflake")
	case protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO:
		allCols, err := c.getColsFromTable(destTable)
		if err != nil {
			log.Errorf("failed to get columns from table %s: %v", destTable, err)
			return fmt.Errorf("failed to get columns from table %s: %w", destTable, err)
		}

		err = CopyStageToDestination(c.database, config, destTable, stageName, allCols)
		if err != nil {
			log.Errorf("failed to copy stage to destination: %v", err)
			return fmt.Errorf("failed to copy stage to destination: %w", err)
		}

		return c.dropStage(config.FlowJobName)
	default:
		return fmt.Errorf("unsupported sync mode: %s", syncMode)
	}
}

func (c *SnowflakeConnector) getColsFromTable(tableName string) ([]string, error) {
	// parse the table name to get the schema and table name
	components, err := parseTableName(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}

	// convert tableIdentifier and schemaIdentifier to upper case
	components.tableIdentifier = strings.ToUpper(components.tableIdentifier)
	components.schemaIdentifier = strings.ToUpper(components.schemaIdentifier)

	//nolint:gosec
	queryString := fmt.Sprintf(`
	SELECT column_name
	FROM information_schema.columns
	WHERE UPPER(table_name) = '%s' AND UPPER(table_schema) = '%s'
	`, components.tableIdentifier, components.schemaIdentifier)

	rows, err := c.database.Query(queryString)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		cols = append(cols, col)
	}

	return cols, nil
}

// dropStage drops the stage for the given job.
func (c *SnowflakeConnector) dropStage(job string) error {
	stageName := c.getStageNameForJob(job)
	stmt := fmt.Sprintf("DROP STAGE IF EXISTS %s", stageName)

	_, err := c.database.Exec(stmt)
	if err != nil {
		return fmt.Errorf("failed to drop stage %s: %w", stageName, err)
	}

	log.Infof("Dropped stage %s", stageName)
	return nil
}

func (c *SnowflakeConnector) getStageNameForJob(job string) string {
	// TODO move this from public to peerdb internal schema.
	return fmt.Sprintf("public.peerdb_stage_%s", job)
}
