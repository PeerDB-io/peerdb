package conns3

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	avro "github.com/PeerDB-io/peer-flow/connectors/utils/avro"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func (c *S3Connector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	schema := stream.Schema()

	dstTableName := config.DestinationTableIdentifier
	avroSchema, err := getAvroSchema(ctx, config.Env, dstTableName, schema)
	if err != nil {
		return 0, err
	}

	numRecords, err := c.writeToAvroFile(ctx, config.Env, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, err
	}

	return numRecords, nil
}

func getAvroSchema(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	schema qvalue.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_S3)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	return avroSchema, nil
}

func (c *S3Connector) writeToAvroFile(
	ctx context.Context,
	env map[string]string,
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

	writer := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressNone, protos.DBType_SNOWFLAKE)
	avroFile, err := writer.WriteRecordsToS3(ctx, env, s3o.Bucket, s3AvroFileKey, c.credentialsProvider)
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

// S3 doesn't check if partition is already synced, but file with same name is overwritten
func (c *S3Connector) IsQRepPartitionSynced(_ context.Context,
	config *protos.IsQRepPartitionSyncedInput,
) (bool, error) {
	return false, nil
}
