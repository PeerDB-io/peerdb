package conns3

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *S3Connector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, shared.QRepWarnings, error) {
	schema, err := stream.Schema()
	if err != nil {
		return 0, nil, err
	}

	dstTableName := config.DestinationTableIdentifier
	avroSchema, err := getAvroSchema(ctx, config.Env, dstTableName, schema)
	if err != nil {
		return 0, nil, err
	}

	numRecords, err := c.writeToAvroFile(ctx, config.Env, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	if err != nil {
		return 0, nil, err
	}

	return numRecords, nil, nil
}

func getAvroSchema(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	schema types.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	// TODO: Support avro-incompatible column names
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_S3, nil)
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
) (int64, error) {
	s3o, err := utils.NewS3BucketAndPrefix(c.url)
	if err != nil {
		return 0, fmt.Errorf("failed to parse bucket path: %w", err)
	}

	s3UuidPrefix, err := internal.PeerDBS3UuidPrefix(ctx, env)
	if err != nil {
		return 0, err
	}
	var s3AvroFileKey string
	if s3UuidPrefix {
		s3AvroFileKey = fmt.Sprintf("%s/%s/%s/%s.avro", s3o.Prefix, uuid.NewString(), jobName, partitionID)
	} else {
		s3AvroFileKey = fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, jobName, partitionID)
	}

	var codec ocf.CodecName
	switch c.codec {
	case protos.AvroCodec_Null:
		codec = ocf.Null
	case protos.AvroCodec_Deflate:
		codec = ocf.Deflate
	case protos.AvroCodec_Snappy:
		codec = ocf.Snappy
	case protos.AvroCodec_ZStandard:
		codec = ocf.ZStandard
	default:
		return 0, fmt.Errorf("unsupported codec %s", c.codec)
	}

	writer := utils.NewPeerDBOCFWriter(stream, avroSchema, codec, protos.DBType_S3, nil)
	avroFile, err := writer.WriteRecordsToS3(ctx, env, s3o.Bucket, s3AvroFileKey, c.credentialsProvider, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to write records to S3: %w", err)
	}
	defer avroFile.Cleanup(ctx)

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
