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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	done, err := c.isPartitionSynced(partition.PartitionId)
	if err != nil {
		return 0, fmt.Errorf("failed to check if partition %s is synced: %w", partition.PartitionId, err)
	}

	if done {
		c.logger.Info("Partition has already been synced", flowLog)
		return 0, nil
	}

	tblSchema, err := c.getTableSchema(destTable)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema of table %s: %w", destTable, err)
	}
	c.logger.Info("Called QRep sync function and obtained table schema", flowLog)

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
		partitionJSON, startTime.Format("2006-01-02 15:04:05.000000"))

	return insertMetadataStmt, nil
}

func (c *ClickhouseConnector) getTableSchema(tableName string) ([]*sql.ColumnType, error) {
	//nolint:gosec
	queryString := fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, tableName)
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
	queryString := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE partitionID = '%s'`, qRepMetadataTableName, partitionID)

	row := c.database.QueryRow(queryString)

	var count int
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}
	return count > 0, nil
}

func (c *ClickhouseConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	err := c.createQRepMetadataTable()
	if err != nil {
		return err
	}

	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_OVERWRITE {
		_, err = c.database.Exec(fmt.Sprintf("TRUNCATE TABLE %s", config.DestinationTableIdentifier))
		if err != nil {
			return fmt.Errorf("failed to TRUNCATE table before query replication: %w", err)
		}
	}

	return nil
}

func (c *ClickhouseConnector) createQRepMetadataTable() error {
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
	queryString := fmt.Sprintf(schemaStatement, qRepMetadataTableName)
	_, err := c.database.Exec(queryString)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to create table %s", qRepMetadataTableName),
			slog.Any("error", err))

		return fmt.Errorf("failed to create table %s: %w", qRepMetadataTableName, err)
	}
	c.logger.Info(fmt.Sprintf("Created table %s", qRepMetadataTableName))
	return nil
}

func (c *ClickhouseConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	c.logger.Info("Consolidating partitions noop")
	return nil
}

// CleanupQRepFlow function for clickhouse connector
func (c *ClickhouseConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	c.logger.Info("Cleaning up flow job")
	return c.dropStage(config.StagingPath, config.FlowJobName)
}

// dropStage drops the stage for the given job.
func (c *ClickhouseConnector) dropStage(stagingPath string, job string) error {
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

	c.logger.Info(fmt.Sprintf("Dropped stage %s", stagingPath))
	return nil
}
