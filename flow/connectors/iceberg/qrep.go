package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/linkedin/goavro/v2"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func (c *IcebergConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	return c.streamRecords(ctx, config, partition, stream)
	//return c.sendRecordsJoined(ctx, config, partition, stream)
}

//nolint:unused
func (c *IcebergConnector) sendRecordsJoined(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	schema := stream.Schema()

	schema.Fields = addPeerMetaColumns(schema.Fields, config.SoftDeleteColName, config.SyncedAtColName)
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
		record = append(record,
			// Add soft delete
			qvalue.QValueBoolean{
				Val: false,
			}, // add synced at colname
			qvalue.QValueTimestampTZ{
				Val: time.Now(),
			})

		converted, err := avroConverter.Convert(record)
		if err != nil {
			return 0, err
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

	requestIdempotencyKey := fmt.Sprintf("_peerdb_qrep-%s-%s", config.FlowJobName, partition.PartitionId)

	appendRecordsResponse, err := c.proxyClient.AppendRecords(ctx,
		&protos.AppendRecordsRequest{
			TableHeader: &protos.AppendRecordTableHeader{
				TableInfo: &protos.TableInfo{
					// Namespace:       nil,
					TableName:      dstTableName,
					IcebergCatalog: c.config.CatalogConfig,
					// PrimaryKey:      nil,
				},
				Schema:         avroSchema.Schema,
				IdempotencyKey: &requestIdempotencyKey,
			},
			Records: binaryRecords,
		},
	)
	if err != nil {
		return 0, err
	}

	logger.LoggerFromCtx(ctx).Info("AppendRecordsResponse", slog.Any("response", appendRecordsResponse.Success))

	err = c.PostgresMetadata.FinishQRepPartition(ctx, partition, config.FlowJobName, time.Now())
	if err != nil {
		return 0, err
	}
	return len(binaryRecords), nil
}

func (c *IcebergConnector) streamRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	c.logger.Info("[iceberg qrep.go]:streaming records")
	schema := stream.Schema()

	schema.Fields = addPeerMetaColumns(schema.Fields, config.SoftDeleteColName, config.SyncedAtColName)
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
	c.logger.Info("obtained goavro codec")
	requestIdempotencyKey := fmt.Sprintf("_peerdb_qrep-%s-%s", config.FlowJobName, partition.PartitionId)

	tableHeader := &protos.AppendRecordTableHeader{
		TableInfo: &protos.TableInfo{
			// Namespace:       nil,
			TableName:      dstTableName,
			IcebergCatalog: c.config.CatalogConfig,
			// PrimaryKey:      nil,
		},
		Schema:         avroSchema.Schema,
		IdempotencyKey: &requestIdempotencyKey,
	}
	recordStream, err := c.proxyClient.StreamingAppendRecords(ctx)
	if err != nil {
		return 0, err
	}

	err = recordStream.Send(&protos.AppendRecordsStreamRequest{
		Command: &protos.AppendRecordsStreamRequest_TableHeader{
			TableHeader: tableHeader,
		},
	})
	// TODO what to do with recordStream?
	if err != nil {
		return 0, err
	}

	recordCount := 0
	once := true
	for record := range stream.Records {
		if once {
			c.logger.Info("enter record loop", "record", record)
			once = false
		}
		record = append(record,
			// Add soft delete
			qvalue.QValueBoolean{
				Val: false,
			}, // add synced at colname
			qvalue.QValueTimestampTZ{
				Val: time.Now(),
			})

		converted, err := avroConverter.Convert(record)
		if err != nil {
			return 0, err
		}
		binaryData := make([]byte, 0)
		native, err := codec.BinaryFromNative(binaryData, converted)
		if err != nil {
			return 0, fmt.Errorf("failed to convert Avro map to binary: %w", err)
		}
		c.logger.Info("sending record", "record", native)
		insertRecord := &protos.InsertRecord{
			Record: native,
		}
		err = recordStream.Send(&protos.AppendRecordsStreamRequest{
			Command: &protos.AppendRecordsStreamRequest_Record{
				Record: insertRecord,
			},
		})
		if err != nil {
			return 0, err
		}
		c.logger.Info("sent record", "record", insertRecord)
		recordCount++
	}

	c.logger.Info("closing record stream")
	appendRecordsStreamResponse, err := recordStream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	logger.LoggerFromCtx(ctx).Info("AppendRecordsResponse", slog.Any("response", appendRecordsStreamResponse.Success))

	err = c.PostgresMetadata.FinishQRepPartition(ctx, partition, config.FlowJobName, time.Now())
	if err != nil {
		return 0, err
	}
	return recordCount, nil
}

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

// Iceberg just sets up destination, not metadata tables
func (c *IcebergConnector) SetupQRepMetadataTables(_ context.Context, config *protos.QRepConfig) error {
	c.logger.Info("QRep metadata setup not needed for Iceberg.")
	return nil
}

// Iceberg doesn't check if partition is already synced, but file with same name is overwritten
func (c *IcebergConnector) IsQRepPartitionSynced(ctx context.Context,
	config *protos.IsQRepPartitionSyncedInput,
) (bool, error) {
	// TODO look at this
	return c.PostgresMetadata.IsQRepPartitionSynced(ctx, config)
}
