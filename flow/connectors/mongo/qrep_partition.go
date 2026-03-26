package connmongo

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// minObjectIDForTimestamp creates an ObjectID with the given timestamp and
// all trailing bytes set to 0x00, producing the smallest possible ObjectID
// for that second.
func minObjectIDForTimestamp(t time.Time) bson.ObjectID {
	var oid [12]byte
	binary.BigEndian.PutUint32(oid[0:4], uint32(t.Unix()))
	return oid
}

// maxObjectIDForTimestamp creates an ObjectID with the given timestamp and
// all trailing bytes set to 0xFF, producing the largest possible ObjectID
// for that second.
func maxObjectIDForTimestamp(t time.Time) bson.ObjectID {
	var oid [12]byte
	binary.BigEndian.PutUint32(oid[0:4], uint32(t.Unix()))
	for i := 4; i < 12; i++ {
		oid[i] = 0xFF
	}
	return oid
}

// minMaxPartitions creates partitions by querying only the min and max _id
// from the collection (leveraging the default _id index), then uniformly
// dividing the timestamp range encoded in the ObjectIDs. Note that partition
// may be skewed if document insertion rate varies significantly over time.
func (c *MongoConnector) minMaxPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	fullTablePartition := []*protos.QRepPartition{{
		PartitionId:        utils.FullTablePartitionID,
		Range:              nil,
		FullTablePartition: true,
	}}

	minRaw, err := findBoundaryID(ctx, collection, 1)
	if err != nil {
		c.logger.Info("[mongo] could not find min _id, collection may be empty, falling back to full table partition",
			slog.String("error", err.Error()))
		return fullTablePartition, nil
	}
	maxRaw, err := findBoundaryID(ctx, collection, -1)
	if err != nil {
		c.logger.Info("[mongo] could not find max _id, falling back to full table partition",
			slog.String("error", err.Error()))
		return fullTablePartition, nil
	}

	// Given MongoDB's type-ordered _id, if both min and max are ObjectID
	// then every document in the collection must have an ObjectID _id.
	if minRaw.Type != bson.TypeObjectID || maxRaw.Type != bson.TypeObjectID {
		c.logger.Info("[mongo] _id contains non-ObjectID type, falling back to full table partition",
			slog.String("minType", minRaw.Type.String()),
			slog.String("maxType", maxRaw.Type.String()))
		return fullTablePartition, nil
	}

	minID := minRaw.ObjectID()
	maxID := maxRaw.ObjectID()
	minTs := minID.Timestamp()
	maxTs := maxID.Timestamp()
	tsRange := maxTs.Unix() - minTs.Unix() + 1
	if tsRange <= 0 {
		c.logger.Info("[mongo] min/max timestamps are equal, falling back to full table partition")
		return fullTablePartition, nil
	}
	// Cap partitions to the timestamp range in seconds so we don't create additional
	// empty partitions unnecessarily when docs span fewer seconds than partitions
	numPartitions = min(numPartitions, tsRange)

	c.logger.Info("[mongo] using min/max ObjectID timestamp partitioning",
		slog.Time("minTimestamp", minTs),
		slog.Time("maxTimestamp", maxTs),
		slog.Int64("numPartitions", numPartitions))

	secondsPerPartition := shared.DivCeil(tsRange, numPartitions)
	partitions := make([]*protos.QRepPartition, 0, numPartitions)
	for i := range numPartitions {
		start := minObjectIDForTimestamp(time.Unix(minTs.Unix()+secondsPerPartition*i, 0))
		end := maxObjectIDForTimestamp(time.Unix(minTs.Unix()+secondsPerPartition*(i+1)-1, 0))
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: start.Hex(),
						End:   end.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}

	return partitions, nil
}

// findBoundaryID returns the raw _id value of the min (direction=1) or max (direction=-1)
// document in the collection. Because _id is always indexed, this is an efficient O(log n) operation.
// The caller should inspect the returned RawValue.Type to determine the BSON type.
func findBoundaryID(
	ctx context.Context,
	collection *mongo.Collection,
	direction int,
) (bson.RawValue, error) {
	findCmd := bson.D{
		{Key: "find", Value: collection.Name()},
		{Key: "sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: direction}}},
		{Key: "limit", Value: 1},
		{Key: "projection", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, findCmd, options.RunCmd())
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("failed to run find command: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return bson.RawValue{}, fmt.Errorf("collection is empty")
	}
	rv := cursor.Current.Lookup(DefaultDocumentKeyColumnName)
	if rv.IsZero() {
		return bson.RawValue{}, fmt.Errorf("document _id is nil")
	}
	return rv, nil
}
