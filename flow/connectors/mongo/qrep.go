package connmongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MongoConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if last != nil {
		return nil, errors.New("not implemented")
	}
	parseWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("unable to parse watermark table: %w", err)
	}

	collection := c.client.Database(parseWatermarkTable.Schema).Collection(parseWatermarkTable.Table)

	// ignore partitioning for now
	numBuckets := 1
	pipeline := mongo.Pipeline{
		bson.D{
			bson.E{Key: "$bucketAuto", Value: bson.D{
				bson.E{Key: "groupBy", Value: "$_id"},
				bson.E{Key: "buckets", Value: numBuckets},
			}},
		},
	}
	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	defer cursor.Close(ctx)

	var results []struct {
		ID struct {
			Min bson.ObjectID `bson:"min"`
			Max bson.ObjectID `bson:"max"`
		} `bson:"_id"`
		Count int `bson:"count"`
	}
	if err = cursor.All(ctx, &results); err != nil {
		log.Fatal("Failed to start snapshot for collection:", err)
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	for _, result := range results {
		if err := partitionHelepr.AddPartition(result.ID.Min, result.ID.Max); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	return partitionHelepr.GetPartitions(), nil
}

func (c *MongoConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	var totalRecords int64
	var totalBytes int64

	parseWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse watermark table: %w", err)
	}
	collection := c.client.Database(parseWatermarkTable.Schema).Collection(parseWatermarkTable.Table)
	findOpts := options.Find().SetBatchSize(1000)

	var filter bson.D
	if partition.FullTablePartition {
		filter = toRangeFilter(partition.Range)
	}
	cursor, err := collection.Find(ctx, filter, findOpts)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query for records: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return 0, 0, fmt.Errorf("failed to decode record: %w", err)
		}

		record, bytes, err := QValueFromMongoDocumentValue(doc)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert record: %w", err)
		}
		stream.Records <- record
		totalRecords += 1
		totalBytes += bytes
	}

	close(stream.Records)
	return totalRecords, totalBytes, nil
}

func toRangeFilter(partitionRange *protos.PartitionRange) bson.D {
	return bson.D{
		bson.E{Key: "_id", Value: bson.D{
			bson.E{Key: "$gte", Value: partitionRange.GetObjectIdRange().Start},
			bson.E{Key: "$lte", Value: partitionRange.GetObjectIdRange().End},
		}},
	}
}

func QValueFromMongoDocumentValue(doc bson.D) ([]types.QValue, int64, error) {
	var size int64
	var qValues []types.QValue

	var qvalueId types.QValueString
	for _, v := range doc {
		if v.Key == "_id" {
			qvalueId = types.QValueString{Val: v.Value.(string)}
			break
		}
	}
	size += int64(len(qvalueId.Val))

	jsonb, err := json.Marshal(doc)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert doc to json: %w", err)
	}
	qvalueDoc := types.QValueJSON{Val: string(jsonb), IsArray: false}
	size += int64(len(qvalueDoc.Val))

	qValues = append(qValues, qvalueId, qvalueDoc)
	return qValues, size, nil
}
