package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (*ClickHouseConnector) SetupQRepMetadataTables(_ context.Context, _ *protos.QRepConfig) error {
	return nil
}

func (c *ClickHouseConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, shared.QRepWarnings, error) {
	// Ensure the destination table is available.
	destTable := config.DestinationTableIdentifier
	flowLog := slog.Group("sync_metadata",
		slog.String(string(shared.PartitionIDKey), partition.PartitionId),
		slog.String("destinationTable", destTable),
	)

	c.logger.Info("Called QRep sync function", flowLog)

	avroSync := NewClickHouseAvroSyncMethod(config, c)

	return avroSync.SyncQRepRecords(ctx, config, partition, stream)
}

func (c *ClickHouseConnector) ConsolidateQRepPartitions(_ context.Context, config *protos.QRepConfig) error {
	c.logger.Info("Consolidating partitions noop")
	return nil
}

// CleanupQRepFlow function for clickhouse connector
func (c *ClickHouseConnector) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	c.logger.Info("Cleaning up flow job")
	return c.dropStage(ctx, config.StagingPath, config.FlowJobName)
}

func (c *ClickHouseConnector) AvroImport(ctx context.Context, config *protos.CreateImportS3Request, urls map[string][]string) error {
	for _, mapping := range config.TableMappings {
		var engineOrderBy string
		if mapping.Engine == protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE {
			engineOrderBy = "ReplacingMergeTree"
		} else {
			engineOrderBy = "MergeTree"
		}

		if mapping.PartitionKey != "" {
			engineOrderBy += fmt.Sprintf(" ORDER BY (`%s`)", mapping.PartitionKey)
		}
		for _, uri := range urls[mapping.SourceTableIdentifier] {
			if err := c.SyncFromAvroUrl(ctx, uri, mapping.DestinationTableIdentifier, engineOrderBy); err != nil {
				return err
			}
			engineOrderBy = ""
		}
	}
	return nil
}

func (c *ClickHouseConnector) SyncFromAvroUrl(
	ctx context.Context,
	avroFileUrl string,
	destinationTableIdentifier string,
	engine string,
) error {
	var query string
	if engine == "" {
		query = fmt.Sprintf("INSERT INTO TABLE %s (*) SELECT * FROM url(?,'Avro')", destinationTableIdentifier)
	} else {
		// https://github.com/ClickHouse/ClickHouse/issues/35408
		query = fmt.Sprintf("CREATE TABLE %s ENGINE = %s SETTINGS allow_nullable_key = true EMPTY AS SELECT * FROM url(?,'Avro')",
			destinationTableIdentifier, engine)
	}

	err := c.database.Exec(ctx, query, avroFileUrl)
	if err != nil {
		c.logger.Error("Failed to insert into select for Clickhouse", slog.Any("error", err))
		return err
	}

	if engine != "" {
		// Need to actually populate data now
		return c.SyncFromAvroUrl(ctx, avroFileUrl, destinationTableIdentifier, "")
	}
	return nil
}

// dropStage drops the stage for the given job.
func (c *ClickHouseConnector) dropStage(ctx context.Context, stagingPath string, job string) error {
	// if s3 we need to delete the contents of the bucket
	if strings.HasPrefix(stagingPath, "s3://") {
		s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
		if err != nil {
			c.logger.Error("failed to create S3 bucket and prefix", slog.Any("error", err))
			return fmt.Errorf("failed to create S3 bucket and prefix: %w", err)
		}

		prefix := fmt.Sprintf("%s/%s", s3o.Prefix, job)
		c.logger.Info("Deleting contents of bucket", slog.String("bucket", s3o.Bucket), slog.String("prefix", prefix))

		// deleting the contents of the bucket with prefix
		s3svc, err := utils.CreateS3Client(ctx, c.credsProvider.Provider)
		if err != nil {
			c.logger.Error("failed to create S3 client", slog.Any("error", err))
			return fmt.Errorf("failed to create S3 client: %w", err)
		}

		// Create a list of all objects with the defined prefix in the bucket
		pages := s3.NewListObjectsV2Paginator(s3svc, &s3.ListObjectsV2Input{
			Bucket: aws.String(s3o.Bucket),
			Prefix: aws.String(prefix),
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

		c.logger.Info("Deleted contents of bucket", slog.String("bucket", s3o.Bucket), slog.String("prefix", prefix))
	}

	c.logger.Info("Dropped stage", slog.String("path", stagingPath))
	return nil
}
