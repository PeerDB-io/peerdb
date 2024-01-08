package connclickhouse

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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"
)

const qRepMetadataTableName = "_peerdb_query_replication_metadata"

func (c *ClickhouseConnector) SyncQRepRecords(
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

	avroSync := NewClickhouseAvroSyncMethod(config, c)

	return avroSync.SyncQRepRecords(config, partition, tblSchema, stream)
}

func (c *ClickhouseConnector) createMetadataInsertStatement(
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
		`INSERT INTO %s
			(flowJobName, partitionID, syncPartition, syncStartTime, syncFinishTime)
			VALUES ('%s', '%s', '%s', '%s', NOW());`,
		qRepMetadataTableName, jobName, partition.PartitionId,
		partitionJSON, startTime.Format("2006-01-02 15:04:05.000000")) //c.metadataSchema

	return insertMetadataStmt, nil
}

func (c *ClickhouseConnector) getTableSchema(tableName string) ([]*sql.ColumnType, error) {
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

func (c *ClickhouseConnector) isPartitionSynced(partitionID string) (bool, error) {
	//nolint:gosec
	queryString := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		WHERE partitionID = '%s'
	`, qRepMetadataTableName, partitionID) //c.metadataSchema

	row := c.database.QueryRow(queryString)

	var count int
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	//return count.Int64 > 0, nil
	return count > 0, nil
}

func (c *ClickhouseConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	//createMetadataTablesTx, err := c.database.BeginTx(c.ctx, nil)
	// if err != nil {
	// 	return fmt.Errorf("unable to begin transaction for creating metadata tables: %w", err)
	// }
	// in case we return after error, ensure transaction is rolled back
	// defer func() {
	// 	deferErr := createMetadataTablesTx.Rollback()
	// 	if deferErr != sql.ErrTxDone && deferErr != nil {
	// 		c.logger.Error("error while rolling back transaction for creating metadata tables",
	// 			slog.Any("error", deferErr))
	// 	}
	// }()
	//err = c.createPeerDBInternalSchema(createMetadataTablesTx)
	// if err != nil {
	// 	return err
	// }
	err := c.createQRepMetadataTable() //(createMetadataTablesTx)
	if err != nil {
		return err
	}

	//not needed for clickhouse
	// err = c.createStage(stageName, config)
	// if err != nil {
	// 	return err
	// }

	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = c.database.Exec(fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	// err = createMetadataTablesTx.Commit()
	// if err != nil {
	// 	return fmt.Errorf("unable to commit transaction for creating metadata tables: %w", err)
	// }

	return nil
}

func (c *ClickhouseConnector) createQRepMetadataTable() error { //createMetadataTableTx *sql.Tx
	// Define the schema
	schemaStatement := `
	CREATE TABLE IF NOT EXISTS %s (
		flowJobName String,
		partitionID String,
		syncPartition String,
		syncStartTime DateTime64,
		syncFinishTime DateTime64
		) ENGINE = MergeTree()
		ORDER BY partitionID;
	`
	queryString := fmt.Sprintf(schemaStatement, qRepMetadataTableName) //c.metadataSchema,
	//_, err := createMetadataTableTx.Exec(queryString)
	_, err := c.database.Exec(queryString)
	//_, err := c.database.Exec("select * from tasks;")

	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create table %s", qRepMetadataTableName),
			slog.Any("error", err))

		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	c.logger.Info(fmt.Sprintf("Created table %s", qRepMetadataTableName))
	return nil
}

func (c *ClickhouseConnector) createStage(stageName string, config *protos.QRepConfig) error {
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

func (c *ClickhouseConnector) createExternalStage(stageName string, config *protos.QRepConfig) (string, error) {
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

	s3Int := config.DestinationPeer.GetClickhouseConfig().S3Integration
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

func (c *ClickhouseConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	c.logger.Info("Consolidating partitions noop")

	// destTable := config.DestinationTableIdentifier
	// stageName := c.getStageNameForJob(config.FlowJobName)

	// colInfo, err := c.getColsFromTable(destTable)
	// if err != nil {
	// 	c.logger.Error(fmt.Sprintf("failed to get columns from table %s", destTable), slog.Any("error", err))
	// 	return fmt.Errorf("failed to get columns from table %s: %w", destTable, err)
	// }

	// allCols := colInfo.Columns
	// err = CopyStageToDestination(c, config, destTable, stageName, allCols)
	// if err != nil {
	// 	c.logger.Error("failed to copy stage to destination", slog.Any("error", err))
	// 	return fmt.Errorf("failed to copy stage to destination: %w", err)
	// }

	return nil
}

// TODO: this is not called right now from the main flow for clickhouse, check core.go#GetQRepConsolidateConnector
// CleanupQRepFlow function for clickhouse connector
func (c *ClickhouseConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	c.logger.Info("Cleaning up flow job")
	return c.dropStage(config.StagingPath, config.FlowJobName)
}

func (c *ClickhouseConnector) getColsFromTable(tableName string) (*model.ColumnInformation, error) {
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
	SELECT column_name, data_type
	FROM information_schema.columns
	WHERE UPPER(table_name) = '%s' AND UPPER(table_schema) = '%s'
	`, components.tableIdentifier, components.schemaIdentifier)

	rows, err := c.database.Query(queryString)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnMap := map[string]string{}
	for rows.Next() {
		var colName pgtype.Text
		var colType pgtype.Text
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		columnMap[colName.String] = colType.String
	}

	return &model.ColumnInformation{
		ColumnMap: columnMap,
		Columns:   maps.Keys(columnMap),
	}, nil
}

// dropStage drops the stage for the given job.
func (c *ClickhouseConnector) dropStage(stagingPath string, job string) error {
	fmt.Printf("\n********************* qrep drop stage 1*********************\n stagingPath:%+v, job: %+v", stagingPath, job)
	//stageName := c.getStageNameForJob(job)

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
		iter := s3manager.NewDeleteListIterator(s3svc, &s3.ListObjectsInput{
			Bucket: aws.String(s3o.Bucket),
			Prefix: aws.String(fmt.Sprintf("%s/%s", s3o.Prefix, job)),
		})

		// Iterate through the objects in the bucket with the prefix and delete them
		s3Client := s3manager.NewBatchDeleteWithClient(s3svc)
		if err := s3Client.Delete(aws.BackgroundContext(), iter); err != nil {
			c.logger.Error("failed to delete objects from bucket", slog.Any("error", err))
			return fmt.Errorf("failed to delete objects from bucket: %w", err)
		}

		c.logger.Info(fmt.Sprintf("Deleted contents of bucket %s with prefix %s/%s", s3o.Bucket, s3o.Prefix, job))
	}

	c.logger.Info(fmt.Sprintf("Dropped stage %s", stageName))
	return nil
}

// func (c *ClickhouseConnector) getStageNameForJob(job string) string {
// 	return fmt.Sprintf("%s.peerdb_stage_%s", c.metadataSchema, job)
// }
