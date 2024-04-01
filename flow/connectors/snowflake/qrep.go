package connsnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *SnowflakeConnector) SyncQRepRecords(
	ctx context.Context,
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
	tblSchema, err := c.getTableSchema(ctx, destTable)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema of table %s: %w", destTable, err)
	}
	c.logger.Info("Called QRep sync function and obtained table schema", flowLog)

	avroSync := NewSnowflakeAvroSyncHandler(config, c)
	return avroSync.SyncQRepRecords(ctx, config, partition, tblSchema, stream)
}

func (c *SnowflakeConnector) getTableSchema(ctx context.Context, tableName string) ([]*sql.ColumnType, error) {
	schematable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table '%s'", tableName)
	}

	//nolint:gosec
	queryString := fmt.Sprintf("SELECT * FROM %s LIMIT 0", snowflakeSchemaTableNormalize(schematable))

	//nolint:rowserrcheck
	rows, err := c.database.QueryContext(ctx, queryString)
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

func (c *SnowflakeConnector) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	var schemaExists sql.NullBool
	err := c.database.QueryRowContext(ctx, checkIfSchemaExistsSQL, c.rawSchema).Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("error while checking if schema %s for raw table exists: %w", c.rawSchema, err)
	}

	if !schemaExists.Valid || !schemaExists.Bool {
		_, err := c.database.ExecContext(ctx, fmt.Sprintf(createSchemaSQL, c.rawSchema))
		if err != nil {
			return err
		}
	}

	stageName := c.getStageNameForJob(config.FlowJobName)
	err = c.createStage(ctx, stageName, config)
	if err != nil {
		return err
	}

	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = c.database.ExecContext(ctx, "TRUNCATE TABLE "+config.DestinationTableIdentifier)
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
}

func (c *SnowflakeConnector) createStage(ctx context.Context, stageName string, config *protos.QRepConfig) error {
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
	_, err := c.database.ExecContext(ctx, createStageStmt)
	if err != nil {
		c.logger.Error("failed to create stage "+stageName, slog.Any("error", err))
		return fmt.Errorf("failed to create stage %s: %w", stageName, err)
	}

	c.logger.Info("Created stage " + stageName)
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

	var s3Int string
	if config.DestinationPeer != nil {
		s3Int = config.DestinationPeer.GetSnowflakeConfig().S3Integration
	}
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

func (c *SnowflakeConnector) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig) error {
	c.logger.Info("Consolidating partitions")

	destTable := config.DestinationTableIdentifier
	stageName := c.getStageNameForJob(config.FlowJobName)

	writeHandler := NewSnowflakeAvroConsolidateHandler(c, config, destTable, stageName)
	err := writeHandler.CopyStageToDestination(ctx)
	if err != nil {
		c.logger.Error("failed to copy stage to destination", slog.Any("error", err))
		return fmt.Errorf("failed to copy stage to destination: %w", err)
	}

	return nil
}

// CleanupQRepFlow function for snowflake connector
func (c *SnowflakeConnector) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	c.logger.Info("Cleaning up flow job")
	return c.dropStage(ctx, config.StagingPath, config.FlowJobName)
}

func (c *SnowflakeConnector) getColsFromTable(ctx context.Context, tableName string) ([]string, []string, error) {
	// parse the table name to get the schema and table name
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse table name: %w", err)
	}

	rows, err := c.database.QueryContext(
		ctx,
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

	err = rows.Err()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read rows: %w", err)
	}

	if len(colNames) == 0 {
		return nil, nil, fmt.Errorf("cannot load schema: table %s.%s does not exist", schemaTable.Schema, schemaTable.Table)
	}

	return colNames, colTypes, nil
}

// dropStage drops the stage for the given job.
func (c *SnowflakeConnector) dropStage(ctx context.Context, stagingPath string, job string) error {
	stageName := c.getStageNameForJob(job)
	stmt := "DROP STAGE IF EXISTS " + stageName

	_, err := c.database.ExecContext(ctx, stmt)
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
			page, err := pages.NextPage(ctx)
			if err != nil {
				c.logger.Error("failed to list objects from bucket", slog.Any("error", err))
				return fmt.Errorf("failed to list objects from bucket: %w", err)
			}

			for _, object := range page.Contents {
				_, err = s3svc.DeleteObject(ctx, &s3.DeleteObjectInput{
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

	c.logger.Info("Dropped stage " + stageName)
	return nil
}

func (c *SnowflakeConnector) getStageNameForJob(job string) string {
	return fmt.Sprintf("%s.peerdb_stage_%s", c.rawSchema, job)
}
