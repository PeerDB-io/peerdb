package iceberg

import (
	"context"
	"fmt"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/linkedin/goavro/v2"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func (c *IcebergConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	schema := stream.Schema()

	dstTableName := config.DestinationTableIdentifier

	avroSchema, err := getAvroSchema(dstTableName, schema)
	if err != nil {
		return 0, err
	}

	avroConverter := model.NewQRecordAvroConverter(
		avroSchema,
		protos.DBType_ICEBERG,
		schema.GetColumnNames(),
		logger.LoggerFromCtx(ctx),
	)
	codec, err := goavro.NewCodec(avroSchema.Schema)
	if err != nil {
		return 0, fmt.Errorf("failed to create Avro codec: %w", err)
	}
	binaryRecords := make([]*protos.InsertRecord, 0)
	for record := range stream.Records {
		converted, err := avroConverter.Convert(record)
		if err != nil {
			return 0, fmt.Errorf("failed to convert QRecord to Avro-compatible map: %w", err)
		}
		binaryData := make([]byte, 0)
		native, err := codec.BinaryFromNative(binaryData, converted)
		if err != nil {
			return 0, fmt.Errorf("failed to convert Avro map to binary: %w", err)
		}
		binaryRecords = append(binaryRecords, &protos.InsertRecord{
			Record: native,
		})

	}

	appendRecordsResponse, err := c.proxyClient.AppendRecords(ctx,
		&protos.AppendRecordsRequest{
			TableInfo: &protos.TableInfo{
				//Namespace:       nil,
				TableName:      dstTableName,
				IcebergCatalog: c.config.CatalogConfig,
				//PrimaryKey:      nil,
			},
			Schema:  avroSchema.Schema,
			Records: binaryRecords,
		},
	)

	if err != nil {
		return 0, err
	}

	logger.LoggerFromCtx(ctx).Info("AppendRecordsResponse", slog.Any("response", appendRecordsResponse))

	//numRecords, err := c.writeToAvroFile(ctx, stream, avroSchema, partition.PartitionId, config.FlowJobName)
	//if err != nil {
	//	return 0, err
	//}

	return len(binaryRecords), nil
}

//func (c *IcebergConnector) writeToIceberg(
//	ctx context.Context,
//	stream *model.QRecordStream,
//	avroSchema *model.QRecordAvroSchemaDefinition,
//	destinationTableName string,
//) (int, error) {
//	c.proxyClient.InsertChanges(ctx, &protos.InsertChangesRequest{
//		TableInfo: &protos.TableInfo{
//			Namespace:      nil,
//			TableName:      destinationTableName,
//			IcebergCatalog: c.config.CatalogConfig,
//			PrimaryKey:,
//		},
//		Schema:  avroSchema.Schema,
//		Changes: nil,
//	})
//}

func getAvroSchema(
	dstTableName string,
	schema qvalue.QRecordSchema,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(dstTableName, schema, protos.DBType_ICEBERG)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}

	return avroSchema, nil
}

//func (c *IcebergConnector) writeToAvroFile(
//	ctx context.Context,
//	stream *model.QRecordStream,
//	avroSchema *model.QRecordAvroSchemaDefinition,
//	partitionID string,
//	jobName string,
//) (int, error) {
//	s3o, err := utils.NewS3BucketAndPrefix(c.url)
//	if err != nil {
//		return 0, fmt.Errorf("failed to parse bucket path: %w", err)
//	}
//
//	s3AvroFileKey := fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, jobName, partitionID)
//
//	writer := avro.NewPeerDBOCFWriter(stream, avroSchema, avro.CompressNone, protos.DBType_SNOWFLAKE)
//	avroFile, err := writer.WriteRecordsToS3(ctx, s3o.Bucket, s3AvroFileKey, c.credentialsProvider)
//	if err != nil {
//		return 0, fmt.Errorf("failed to write records to S3: %w", err)
//	}
//	defer avroFile.Cleanup()
//
//	return avroFile.NumRecords, nil
//}

// S3 just sets up destination, not metadata tables
func (c *IcebergConnector) SetupQRepMetadataTables(_ context.Context, config *protos.QRepConfig) error {
	c.logger.Info("QRep metadata setup not needed for S3.")
	return nil
}

// S3 doesn't check if partition is already synced, but file with same name is overwritten
func (c *IcebergConnector) IsQRepPartitionSynced(_ context.Context,
	config *protos.IsQRepPartitionSyncedInput,
) (bool, error) {
	return false, nil
}
