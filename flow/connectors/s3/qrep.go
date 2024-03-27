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
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/writer"
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

	var numRecords int
	switch stream.RecordSerializationFormat {
	case model.FORMAT_AVRO:
		avroSchema, err := getAvroSchema(dstTableName, schema)
		if err != nil {
			return 0, err
		}
		numRecords, err = c.writeToAvroFile(ctx, stream, avroSchema, partition.PartitionId, config.FlowJobName)
		if err != nil {
			return 0, err
		}
	case model.FORMAT_PARQUET_ARROW:
		numRecords, err = c.writeToParquetArrowFile(ctx, stream, partition.PartitionId, config.FlowJobName)
		if err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unsupported serialization format: %v", stream.RecordSerializationFormat)
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

func (c *S3Connector) writeToParquetArrowFile(
	ctx context.Context,
	stream *model.QRecordStream,
	partitionID string,
	jobName string,
) (int, error) {
	s3o, err := utils.NewS3BucketAndPrefix(c.url)
	if err != nil {
		return 0, fmt.Errorf("failed to parse bucket path: %w", err)
	}
	numRecords := 0

	s3ParquetFileKey := fmt.Sprintf("%s/%s/%s.parquet", s3o.Prefix, jobName, partitionID)
	s3fw, err := s3v2.NewS3FileWriterWithClient(ctx, &c.client, s3o.Bucket, s3ParquetFileKey, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create local parquet file: %w", err)
	}
	defer s3fw.Close()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	// hard-coded schema for now
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "_peerdb_uid", Type: arrow.BinaryTypes.String},
			{Name: "_peerdb_timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "_peerdb_destination_table_name", Type: arrow.BinaryTypes.String},
			{Name: "_peerdb_data", Type: arrow.BinaryTypes.String},
			{Name: "_peerdb_record_type", Type: arrow.PrimitiveTypes.Int64},
			{Name: "_peerdb_match_data", Type: arrow.BinaryTypes.String},
			{Name: "_peerdb_batch_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "_peerdb_unchanged_toast_columns", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	for qRecordOrErr := range stream.Records {
		if qRecordOrErr.Err != nil {
			return 0, fmt.Errorf("[parquet] failed to get record from stream: %w", qRecordOrErr.Err)
		}

		for _, idx := range []int{0, 2, 3, 5, 7} {
			b.Field(idx).(*array.StringBuilder).Append(qRecordOrErr.Record[idx].Value().(string))
		}
		for _, idx := range []int{1, 4, 6} {
			b.Field(idx).(*array.Int64Builder).Append(qRecordOrErr.Record[idx].Value().(int64))
		}

		numRecords++
	}

	rec := b.NewRecord()
	defer rec.Release()

	w, err := writer.NewArrowWriter(schema, s3fw, 1)
	if err != nil {
		return 0, fmt.Errorf("[parquet] can't create parquet writer: %w", err)
	}
	if err = w.WriteArrow(rec); err != nil {
		return 0, fmt.Errorf("[parquet] WriteArrow error: %w", err)
	}
	if err = w.WriteStop(); err != nil {
		return 0, fmt.Errorf("[parquet] WriteStop error: %w", err)
	}

	return numRecords, nil
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
