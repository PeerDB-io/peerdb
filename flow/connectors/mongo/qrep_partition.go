package connmongo

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// minMaxPartitions creates partitions by querying only the min and max _id
// from the collection (leveraging the default _id index), then uniformly
// dividing the ObjectID range using integer arithmetic on the full 12-byte
// value. Since the first 4-byte timestamp is the most significant component,
// this naturally partitions by time while also provides best-effort handling
// of edge cases where records are all inserted in the same second.
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

	minID, maxID, found, err := findMinMaxObjectIDs(ctx, collection, c.config.ReadPreference)
	if err != nil {
		return nil, err
	}
	if !found {
		c.logger.Info("[mongo] no valid min/max ObjectIDs found, falling back to full table partition")
		return fullTablePartition, nil
	}
	minInt := new(big.Int).SetBytes(minID[:])
	maxInt := new(big.Int).SetBytes(maxID[:])
	intRange := new(big.Int).Sub(maxInt, minInt)
	if intRange.Sign() <= 0 {
		c.logger.Info("[mongo] min/max ObjectID range is non-positive, falling back to full table partition")
		return fullTablePartition, nil
	}

	c.logger.Info("[mongo] using min/max ObjectID partitioning",
		slog.String("minID", minID.Hex()),
		slog.String("maxID", maxID.Hex()),
		slog.Int64("numPartitions", numPartitions))

	step := shared.BigIntDivCeil(intRange, big.NewInt(numPartitions))

	partitions := make([]*protos.QRepPartition, 0, numPartitions)
	for start := new(big.Int).Set(minInt); start.Cmp(maxInt) <= 0; start.Add(start, step) {
		end := new(big.Int).Add(start, step)
		end.Sub(end, big.NewInt(1))
		if end.Cmp(maxInt) > 0 {
			end.Set(maxInt)
		}
		startObjectID, err := bigIntToObjectID(start)
		if err != nil {
			return nil, err
		}
		endObjectID, err := bigIntToObjectID(end)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: startObjectID.Hex(),
						End:   endObjectID.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}

	return partitions, nil
}

// bigIntToObjectID converts a big.Int back to a 12-byte ObjectID.
func bigIntToObjectID(n *big.Int) (bson.ObjectID, error) {
	var oid bson.ObjectID
	if n.BitLen() > 96 {
		return oid, fmt.Errorf("big.Int value exceeds 96 bits (ObjectID size)")
	}
	n.FillBytes(oid[:])
	return oid, nil
}

// findMinMaxObjectIDs returns the min and max object IDs from the collection.
// The two boundary queries are not wrapped in a snapshot session: a concurrent
// insert between them may shift the true min/max, but this is safe because the
// change stream resume token is captured before either query runs. Any writes
// that occur during or after partitioning will be replayed by the change stream.
func findMinMaxObjectIDs(
	ctx context.Context,
	collection *mongo.Collection,
	readPreference protos.ReadPreference,
) (bson.ObjectID, bson.ObjectID, bool, error) {
	minRaw, err := findBoundaryID(ctx, collection, Lower, readPreference)
	if err != nil {
		return bson.ObjectID{}, bson.ObjectID{}, false, fmt.Errorf("failed to find min _id: %w", err)
	}
	if minRaw.Type != bson.TypeObjectID {
		return bson.ObjectID{}, bson.ObjectID{}, false, nil
	}

	maxRaw, err := findBoundaryID(ctx, collection, Upper, readPreference)
	if err != nil {
		return bson.ObjectID{}, bson.ObjectID{}, false, fmt.Errorf("failed to find max _id: %w", err)
	}
	if maxRaw.Type != bson.TypeObjectID {
		return bson.ObjectID{}, bson.ObjectID{}, false, nil
	}

	return minRaw.ObjectID(), maxRaw.ObjectID(), true, nil
}

type Boundary int

const (
	Lower Boundary = 1
	Upper Boundary = -1
)

// findBoundaryID returns the lower- or upper-bound _id of a collection
func findBoundaryID(
	ctx context.Context,
	collection *mongo.Collection,
	boundary Boundary,
	readPreference protos.ReadPreference,
) (bson.RawValue, error) {
	findCmd := bson.D{
		{Key: "find", Value: collection.Name()},
		{Key: "sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: boundary}}},
		{Key: "limit", Value: 1},
		{Key: "projection", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, findCmd, options.RunCmd().SetReadPreference(protoToReadPref[readPreference]))
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("failed to run find command: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return bson.RawValue{}, fmt.Errorf("cursor error: %w", err)
		}
		// no results
		return bson.RawValue{}, nil
	}
	return cursor.Current.Lookup(DefaultDocumentKeyColumnName), nil
}
