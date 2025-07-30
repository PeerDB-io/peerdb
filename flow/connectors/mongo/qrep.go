package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	shared_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	// default to FullTablePartition=true until parallel initial load is implemented
	return []*protos.QRepPartition{
		{
			PartitionId:        shared_mongo.MongoFullTablePartitionId,
			Range:              nil,
			FullTablePartition: true,
		},
	}, nil
}

func (c *MongoConnector) PullQRepRecords(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	var totalRecords int64

	parseWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Schema).Collection(parseWatermarkTable.Table)

	stream.SetSchema(GetDefaultSchema())

	c.bytesRead.Store(0)
	shutDown := shared.Interval(ctx, time.Minute, func() {
		if read := c.bytesRead.Swap(0); read != 0 {
			otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		}
	})
	defer shutDown()

	filter := bson.D{}
	// FullTablePartition is always true until parallel initial load is implemented, see `GetQRepPartitions`
	if !partition.FullTablePartition {
		filter, err = toRangeFilter(partition.Range)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert partition range to filter: %w", err)
		}
	}

	batchSize := config.NumRowsPerPartition
	if config.NumRowsPerPartition == 0 || config.NumRowsPerPartition > math.MaxInt32 {
		batchSize = math.MaxInt32
	}

	// MongoDb will use the lesser of batchSize and 16MiB
	// https://www.mongodb.com/docs/manual/reference/method/cursor.batchsize/
	cursor, err := collection.Find(ctx, filter, options.Find().SetBatchSize(int32(batchSize)))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query for records: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return 0, 0, fmt.Errorf("failed to decode record: %w", err)
		}

		record, err := QValuesFromDocument(doc)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}
		stream.Records <- record
		totalRecords += 1
	}
	close(stream.Records)
	if err := cursor.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			c.logger.Warn("context canceled while reading documents",
				slog.Any("partition", partition.PartitionId),
				slog.String("watermark_table", config.WatermarkTable))
		} else {
			c.logger.Error("error while reading documents",
				slog.Any("partition", partition.PartitionId),
				slog.String("watermark_table", config.WatermarkTable),
				slog.String("error", err.Error()))
		}
		return 0, 0, fmt.Errorf("cursor error: %w", err)
	}

	return totalRecords, c.bytesRead.Swap(0), nil
}

func GetDefaultSchema() types.QRecordSchema {
	schema := make([]types.QField, 0, 2)
	schema = append(schema,
		types.QField{
			Name:     DefaultDocumentKeyColumnName,
			Type:     types.QValueKindString,
			Nullable: false,
		},
		types.QField{
			Name:     DefaultFullDocumentColumnName,
			Type:     types.QValueKindJSON,
			Nullable: false,
		})
	return types.QRecordSchema{Fields: schema}
}

func toRangeFilter(partitionRange *protos.PartitionRange) (bson.D, error) {
	switch r := partitionRange.Range.(type) {
	case *protos.PartitionRange_ObjectIdRange:
		return bson.D{
			bson.E{Key: DefaultDocumentKeyColumnName, Value: bson.D{
				bson.E{Key: "$gte", Value: r.ObjectIdRange.Start},
				bson.E{Key: "$lte", Value: r.ObjectIdRange.End},
			}},
		}, nil
	default:
		return nil, errors.New("unsupported partition range type")
	}
}

func QValuesFromDocument(doc bson.D) ([]types.QValue, error) {
	var qValues []types.QValue

	var qvalueId types.QValueString
	var err error
	for _, v := range doc {
		if v.Key == DefaultDocumentKeyColumnName {
			qvalueId, err = qValueStringFromKey(v.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert key %s: %w", DefaultDocumentKeyColumnName, err)
			}
			break
		}
	}
	if qvalueId.Val == "" {
		return nil, fmt.Errorf("key %s not found", DefaultDocumentKeyColumnName)
	}
	qValues = append(qValues, qvalueId)

	qvalueDoc, err := qValueJSONFromDocument(doc)
	if err != nil {
		return nil, err
	}
	qValues = append(qValues, qvalueDoc)

	return qValues, nil
}
