package connsnowflake

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	err := c.createQRepMetadataTable()
	if err != nil {
		return err
	}

	stageName := c.getStageNameForJob(config.FlowJobName)

	err = c.createStage(stageName, config)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnowflakeConnector) createQRepMetadataTable() error {
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

	_, err := c.database.Exec(queryString)
	if err != nil {
		log.Errorf("failed to create table %s.%s: %v", "public", qRepMetadataTableName, err)
		return fmt.Errorf("failed to create table %s.%s: %w", "public", qRepMetadataTableName, err)
	}

	log.Infof("Created table %s", qRepMetadataTableName)
	return nil
}

func (c *SnowflakeConnector) createStage(stageName string, config *protos.QRepConfig) error {
	var createStageStmt string
	if strings.HasPrefix(config.StagingPath, "s3://") {
		stmt, err := c.createExternalStage(stageName, config)
		if err != nil {
			return err
		}
		createStageStmt = stmt
	} else {
		stageStatement := `
			CREATE OR REPLACE STAGE %s
			FILE_FORMAT = (TYPE = AVRO);
			`
		createStageStmt = fmt.Sprintf(stageStatement, stageName)
	}

	// Execute the query
	_, err := c.database.Exec(createStageStmt)
	if err != nil {
		log.Errorf("failed to create stage %s: %v", stageName, err)
		return fmt.Errorf("failed to create stage %s: %w", stageName, err)
	}

	log.Infof("Created stage %s", stageName)
	return nil
}

func (c *SnowflakeConnector) createExternalStage(stageName string, config *protos.QRepConfig) (string, error) {
	awsCreds, err := utils.GetAWSSecrets()
	if err != nil {
		log.Errorf("failed to get AWS secrets: %v", err)
		return "", fmt.Errorf("failed to get AWS secrets: %w", err)
	}

	s3o, err := utils.NewS3BucketAndPrefix(config.StagingPath)
	if err != nil {
		log.Errorf("failed to extract S3 bucket and prefix: %v", err)
		return "", fmt.Errorf("failed to extract S3 bucket and prefix: %w", err)
	}

	cleanURL := fmt.Sprintf("s3://%s/%s/%s", s3o.Bucket, s3o.Prefix, config.FlowJobName)

	s3Int := config.DestinationPeer.GetSnowflakeConfig().S3Integration
	if s3Int == "" {
		credsStr := fmt.Sprintf("CREDENTIALS=(AWS_KEY_ID='%s' AWS_SECRET_KEY='%s')",
			awsCreds.AccessKeyID, awsCreds.SecretAccessKey)

		stageStatement := `
		CREATE OR REPLACE STAGE %s
		URL = '%s'
		%s
		FILE_FORMAT = (TYPE = AVRO);`
		return fmt.Sprintf(stageStatement, stageName, cleanURL, credsStr), nil
	} else {
		stageStatement := `
		CREATE OR REPLACE STAGE %s
		URL = '%s'
		STORAGE_INTEGRATION = %s
		FILE_FORMAT = (TYPE = AVRO);`
		return fmt.Sprintf(stageStatement, stageName, cleanURL, s3Int), nil
	}
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

		err = CopyStageToDestination(c, config, destTable, stageName, allCols)
		if err != nil {
			log.Errorf("failed to copy stage to destination: %v", err)
			return fmt.Errorf("failed to copy stage to destination: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("unsupported sync mode: %s", syncMode)
	}
}

// CleanupQRepFlow function for snowflake connector
func (c *SnowflakeConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	log.Infof("Cleaning up flow job %s", config.FlowJobName)
	return c.dropStage(config.StagingPath, config.FlowJobName)
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
func (c *SnowflakeConnector) dropStage(stagingPath string, job string) error {
	stageName := c.getStageNameForJob(job)
	stmt := fmt.Sprintf("DROP STAGE IF EXISTS %s", stageName)

	_, err := c.database.Exec(stmt)
	if err != nil {
		return fmt.Errorf("failed to drop stage %s: %w", stageName, err)
	}

	// if s3 we need to delete the contents of the bucket
	if strings.HasPrefix(stagingPath, "s3://") {
		s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
		if err != nil {
			log.Errorf("failed to create S3 bucket and prefix: %v", err)
			return fmt.Errorf("failed to create S3 bucket and prefix: %w", err)
		}

		log.Infof("Deleting contents of bucket %s with prefix %s/%s", s3o.Bucket, s3o.Prefix, job)

		// deleting the contents of the bucket with prefix
		s3svc, err := utils.CreateS3Client()
		if err != nil {
			log.Errorf("failed to create S3 client: %v", err)
			return fmt.Errorf("failed to create S3 client: %w", err)
		}

		// Create a list of all objects with the defined prefix in the bucket
		iter := s3manager.NewDeleteListIterator(s3svc, &s3.ListObjectsInput{
			Bucket: aws.String(s3o.Bucket),
			Prefix: aws.String(fmt.Sprintf("%s/%s", s3o.Prefix, job)),
		})

		// Iterate through the objects in the bucket with the prefix and delete them
		s3Client := s3manager.NewBatchDeleteWithClient(s3svc)
		if err := s3Client.Delete(aws.BackgroundContext(), iter); err != nil {
			log.Errorf("failed to delete objects from bucket: %v", err)
			return fmt.Errorf("failed to delete objects from bucket: %w", err)
		}

		log.Infof("Deleted contents of bucket %s with prefix %s/%s", s3o.Bucket, s3o.Prefix, job)
	}

	log.Infof("Dropped stage %s", stageName)
	return nil
}

func (c *SnowflakeConnector) getStageNameForJob(job string) string {
	// TODO move this from public to peerdb internal schema.
	return fmt.Sprintf("public.peerdb_stage_%s", job)
}
