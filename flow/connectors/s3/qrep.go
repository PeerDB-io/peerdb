package conns3

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (c *S3Connector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	schema, err := stream.Schema()
	if err != nil {
		c.logger.Error("failed to get schema from stream",
			slog.Any("error", err),
			slog.String(string(shared.PartitionIDKey), partition.PartitionId))
		return 0, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	dstTableName := config.DestinationTableIdentifier
	avroSchema, err := getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	numRecords, err := c.writeToAvroFile(ctx, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	return numRecords, nil
}

func getAvroSchema(
	dstTableName string,
	schema *model.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema, qvalue.QDWHTypeS3)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	return avroSchema, nil
}

func (c *S3Connector) writeToAvroFile(
	ctx context.Context,
	stream *model.QRecordStream,
	avroSchema *model.QRecordAvroSchemaDefinition,
	partitionID string,
	jobName string,
) (int, error) {
	s3o, err := utils.NewS3BucketAndPrefix(c.url)
	if err != nil {
		return 0, fmt.Errorf("failed to parse bucket path: %w", err)
	}

	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, jobName, partitionID)
	writer := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressNone, qvalue.QDWHTypeSnowflake)
	avroFile, err := writer.WriteRecordsToS3(ctx, s3o.Bucket, s3AvroFileKey, c.creds)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to S3: %w", err)
	}
	defer avroFile.Cleanup()

	return avroFile.NumRecords, nil
}

// S3 just sets up destination, not metadata tables
func (c *S3Connector) SetupQRepMetadataTables(_ context.Context, config *protos.QRepConfig) error {
	c.logger.Info("QRep metadata setup not needed for S3.")
	return nil
}
