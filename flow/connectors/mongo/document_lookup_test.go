package connmongo

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func commandPipeline(t *testing.T, command bson.D) mongo.Pipeline {
	t.Helper()
	for _, element := range command {
		if element.Key == "pipeline" {
			return element.Value.(mongo.Pipeline)
		}
	}
	require.FailNow(t, "command has no pipeline")
	return nil
}

func requireSameBSON(t *testing.T, expected bson.D, actual bson.D) {
	t.Helper()
	expectedRaw, err := bson.Marshal(expected)
	require.NoError(t, err)
	actualRaw, err := bson.Marshal(actual)
	require.NoError(t, err)
	require.Equal(t, bson.Raw(expectedRaw).String(), bson.Raw(actualRaw).String())
}

func TestDocumentLookupCommandComparesEveryKeyFieldLiterally(t *testing.T) {
	documentKey, err := bson.Marshal(bson.D{
		{Key: "region", Value: "eu"},
		{Key: "_id", Value: bson.D{{Key: "$gt", Value: 1}}},
	})
	require.NoError(t, err)
	elements, err := bson.Raw(documentKey).Elements()
	require.NoError(t, err)
	requests := []*documentLookupRequest{{documentKey: elements, latestClusterTime: bson.Timestamp{T: 7, I: 1}}}

	command := buildBatchLookupCommand("orders", requests)
	requireSameBSON(t, bson.D{{Key: "$match", Value: bson.D{
		{Key: "region", Value: bson.D{{Key: "$eq", Value: "eu"}}},
		{Key: "_id", Value: bson.D{{Key: "$eq", Value: bson.D{{Key: "$gt", Value: 1}}}}},
	}}}, commandPipeline(t, command)[0])
}

func idValue(t *testing.T, id any) bson.RawValue {
	t.Helper()
	return idKey(t, id)[0].Value()
}

func idKey(t *testing.T, id any) []bson.RawElement {
	t.Helper()
	documentKey, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	require.NoError(t, err)
	elements, err := bson.Raw(documentKey).Elements()
	require.NoError(t, err)
	return elements
}

func TestDocumentLookupCommandBatchesAnyIDType(t *testing.T) {
	objectID := bson.NewObjectID()
	requests := []*documentLookupRequest{
		{documentKey: idKey(t, objectID), batchable: true, latestClusterTime: bson.Timestamp{T: 10, I: 2}},
		{documentKey: idKey(t, "order-7"), batchable: true, latestClusterTime: bson.Timestamp{T: 12, I: 1}},
		{documentKey: idKey(t, int64(42)), batchable: true, latestClusterTime: bson.Timestamp{T: 11, I: 5}},
	}

	requireSameBSON(t, bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{
				objectID, "order-7", int64(42),
			}}}}}}},
			bson.D{{Key: "$limit", Value: 4}},
		}},
		{Key: "cursor", Value: bson.D{{Key: "batchSize", Value: 4}}},
		{Key: "readConcern", Value: bson.D{
			{Key: "level", Value: "majority"},
			{Key: "afterClusterTime", Value: bson.Timestamp{T: 12, I: 1}},
		}},
		{Key: "comment", Value: "PeerDB parallel document lookup"},
	}, buildBatchLookupCommand("orders", requests))
}

func TestRawValueKeyDistinguishesTypes(t *testing.T) {
	first, second := idValue(t, "a"), idValue(t, "a")
	require.Equal(t, rawValueKey(first), rawValueKey(second), "the same value from two documents matches")
	require.NotEqual(t, rawValueKey(idValue(t, int32(1))), rawValueKey(idValue(t, int64(1))))
	require.NotEqual(t, rawValueKey(idValue(t, "1")), rawValueKey(idValue(t, int32(1))))
}

func TestGroupLookupQueries(t *testing.T) {
	var requests []*documentLookupRequest
	for range lookupQueryBatchSize + 1 {
		requests = append(requests, &documentLookupRequest{sourceTable: "app.orders", batchable: true})
	}
	requests = append(requests,
		&documentLookupRequest{sourceTable: "app.users", batchable: true},
		&documentLookupRequest{sourceTable: "app.orders"},
		&documentLookupRequest{sourceTable: "app.orders"},
	)

	queries := groupLookupRequests(requests)

	var sizes []int
	seen := 0
	for _, query := range queries {
		sizes = append(sizes, len(query))
		seen += len(query)
		for _, request := range query {
			require.Equal(t, query[0].sourceTable, request.sourceTable, "a query never spans collections")
			require.Equal(t, query[0].batchable, request.batchable, "a query never mixes batched and single-key requests")
		}
		if !query[0].batchable {
			require.Len(t, query, 1, "a single-key request gets its own query")
		}
	}
	slices.Sort(sizes)
	// 65 batchable orders split into 64 + 1, one batchable user, and two single-key orders.
	require.Equal(t, []int{1, 1, 1, 1, lookupQueryBatchSize}, sizes)
	require.Equal(t, lookupQueryBatchSize+4, seen)
}
