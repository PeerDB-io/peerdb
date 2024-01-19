package connsnowflake

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/encoding/protojson"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *SnowflakeConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	flowLog := slog.Group("sync_metadata",
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", destTable),
	)
	tblSchema, err := c.getTableSchema(destTable)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema of table %s: %w", destTable, err)
	}
	c.logger.Info("Called QRep sync function and obtained table schema", flowLog)

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition %s is synced: %w", partition.PartitionId, err)
	}

	if done {
		c.logger.Info("Partition has already been synced", flowLog)
		return 0, nil
	}

	avroSync := NewSnowflakeAvroSyncMethod(config, c)
	return avroSync.SyncQRepRecords(config, partition, tblSchema, stream)
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
		c.metadataSchema, qRepMetadataTableName, jobName, partition.PartitionId,
		partitionJSON, startTime.Format(time.RFC3339))

	return insertMetadataStmt, nil
}

func (c *SnowflakeConnector) getTableSchema(tableName string) ([]*sql.ColumnType, error) {
	schematable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table '%s'", tableName)
	}

	//nolint:gosec
	queryString := fmt.Sprintf(`
	SELECT *
	FROM %s
	LIMIT 0
	`, snowflakeSchemaTableNormalize(schematable))

	rows, err := c.database.QueryContext(c.ctx, queryString)
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
		FROM %s.%s
		WHERE partitionID = '%s'
	`, c.metadataSchema, qRepMetadataTableName, partitionID)

	row := c.database.QueryRow(queryString)

	var count pgtype.Int8
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	return count.Int64 > 0, nil
}

func (c *SnowflakeConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	// NOTE that Snowflake does not support transactional DDL
	createMetadataTablesTx, err := c.database.BeginTx(c.ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for creating metadata tables: %w", err)
	}
	// in case we return after error, ensure transaction is rolled back
	defer func() {
		deferErr := createMetadataTablesTx.Rollback()
		if deferErr != sql.ErrTxDone && deferErr != nil {
			c.logger.Error("error while rolling back transaction for creating metadata tables",
				slog.Any("error", deferErr))
		}
	}()
	err = c.createPeerDBInternalSchema(createMetadataTablesTx)
	if err != nil {
		return err
	}
	err = c.createQRepMetadataTable(createMetadataTablesTx)
	if err != nil {
		return err
	}

	stageName := c.getStageNameForJob(config.FlowJobName)

	err = c.createStage(stageName, config)
	if err != nil {
		return err
	}

	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = c.database.Exec(fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	err = createMetadataTablesTx.Commit()
	if err != nil {
		return fmt.Errorf("unable to commit transaction for creating metadata tables: %w", err)
	}

	return nil
}

func (c *SnowflakeConnector) createQRepMetadataTable(createMetadataTableTx *sql.Tx) error {
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
	queryString := fmt.Sprintf(schemaStatement, c.metadataSchema, qRepMetadataTableName)

	_, err := createMetadataTableTx.Exec(queryString)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create table %s.%s", c.metadataSchema, qRepMetadataTableName),
			slog.Any("error", err))
		return fmt.Errorf("failed to create table %s.%s: %w", c.metadataSchema, qRepMetadataTableName, err)
	}

	c.logger.Info(fmt.Sprintf("Created table %s", qRepMetadataTableName))
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
		c.logger.Error(fmt.Sprintf("failed to create stage %s", stageName), slog.Any("error", err))
		return fmt.Errorf("failed to create stage %s: %w", stageName, err)
	}

	c.logger.Info(fmt.Sprintf("Created stage %s", stageName))
	return nil
}

func (c *SnowflakeConnector) createExternalStage(stageName string, config *protos.QRepConfig) (string, error) {
	awsCreds, err := utils.GetAWSSecrets(utils.S3PeerCredentials{})
	if err != nil {
		c.logger.Error("failed to get AWS secrets", slog.Any("error", err))
		return "", fmt.Errorf("failed to get AWS secrets: %w", err)
	}

	s3o, err := utils.NewS3BucketAndPrefix(config.StagingPath)
	if err != nil {
		c.logger.Error("failed to extract S3 bucket and prefix", slog.Any("error", err))
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
	c.logger.Info("Consolidating partitions")

	destTable := config.DestinationTableIdentifier
	stageName := c.getStageNameForJob(config.FlowJobName)

	colNames, _, err := c.getColsFromTable(destTable)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to get columns from table %s", destTable), slog.Any("error", err))
		return fmt.Errorf("failed to get columns from table %s: %w", destTable, err)
	}

	err = CopyStageToDestination(c, config, destTable, stageName, colNames)
	if err != nil {
		c.logger.Error("failed to copy stage to destination", slog.Any("error", err))
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}

	return nil
}

// CleanupQRepFlow function for snowflake connector
func (c *SnowflakeConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	c.logger.Info("Cleaning up flow job")
	return c.dropStage(config.StagingPath, config.FlowJobName)
}

func (c *SnowflakeConnector) getColsFromTable(tableName string) ([]string, []string, error) {
	// parse the table name to get the schema and table name
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse table name: %w", err)
	}

	rows, err := c.database.QueryContext(
		c.ctx,
		getTableSchemaSQL,
		strings.ToUpper(schemaTable.Schema),
		strings.ToUpper(schemaTable.Table),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var colName, colType pgtype.Text
	colNames := make([]string, 0, 8)
	colTypes := make([]string, 0, 8)
	for rows.Next() {
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}
		colNames = append(colNames, colName.String)
		colTypes = append(colTypes, colType.String)
	}

	if len(colNames) == 0 {
		return nil, nil, fmt.Errorf("cannot load schema: table %s.%s does not exist", schemaTable.Schema, schemaTable.Table)
	}

	return colNames, colTypes, nil
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
			c.logger.Error("failed to create S3 bucket and prefix", slog.Any("error", err))
			return fmt.Errorf("failed to create S3 bucket and prefix: %w", err)
		}

		c.logger.Info(fmt.Sprintf("Deleting contents of bucket %s with prefix %s/%s", s3o.Bucket, s3o.Prefix, job))

		// deleting the contents of the bucket with prefix
		s3svc, err := utils.CreateS3Client(utils.S3PeerCredentials{})
		if err != nil {
			c.logger.Error("failed to create S3 client", slog.Any("error", err))
			return fmt.Errorf("failed to create S3 client: %w", err)
		}

		// Create a list of all objects with the defined prefix in the bucket
		pages := s3.NewListObjectsV2Paginator(s3svc, &s3.ListObjectsV2Input{
			Bucket: aws.String(s3o.Bucket),
			Prefix: aws.String(fmt.Sprintf("%s/%s", s3o.Prefix, job)),
		})
		for pages.HasMorePages() {
			page, err := pages.NextPage(c.ctx)
			if err != nil {
				c.logger.Error("failed to list objects from bucket", slog.Any("error", err))
				return fmt.Errorf("failed to list objects from bucket: %w", err)
			}

			for _, object := range page.Contents {
				_, err = s3svc.DeleteObject(c.ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(s3o.Bucket),
					Key:    object.Key,
				})
				if err != nil {
					c.logger.Error("failed to delete objects from bucket", slog.Any("error", err))
					return fmt.Errorf("failed to delete objects from bucket: %w", err)
				}
			}
		}

		c.logger.Info(fmt.Sprintf("Deleted contents of bucket %s with prefix %s/%s", s3o.Bucket, s3o.Prefix, job))
	}

	c.logger.Info(fmt.Sprintf("Dropped stage %s", stageName))
	return nil
}

func (c *SnowflakeConnector) getStageNameForJob(job string) string {
	return fmt.Sprintf("%s.peerdb_stage_%s", c.metadataSchema, job)
}
