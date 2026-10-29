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
		{documentKey: idKey(t, objectID), keyFields: idOnlyKeyFields, latestClusterTime: bson.Timestamp{T: 10, I: 2}},
		{documentKey: idKey(t, "order-7"), keyFields: idOnlyKeyFields, latestClusterTime: bson.Timestamp{T: 12, I: 1}},
		{documentKey: idKey(t, int64(42)), keyFields: idOnlyKeyFields, latestClusterTime: bson.Timestamp{T: 11, I: 5}},
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
	compound := "region" + keyFieldSeparator + "_id"
	var requests []*documentLookupRequest
	for range lookupQueryBatchSize + 1 {
		requests = append(requests, &documentLookupRequest{sourceTable: "app.orders", keyFields: idOnlyKeyFields})
	}
	requests = append(requests,
		&documentLookupRequest{sourceTable: "app.users", keyFields: idOnlyKeyFields},
		&documentLookupRequest{sourceTable: "app.orders", keyFields: compound},
		&documentLookupRequest{sourceTable: "app.orders", keyFields: compound},
	)

	queries := groupLookupRequests(requests)

	var sizes []int
	seen := 0
	for _, query := range queries {
		sizes = append(sizes, len(query))
		seen += len(query)
		for _, request := range query {
			require.Equal(t, query[0].sourceTable, request.sourceTable, "a query never spans collections")
			require.Equal(t, query[0].keyFields, request.keyFields, "a query never mixes document key shapes")
		}
	}
	slices.Sort(sizes)
	// 65 _id-keyed orders split into 64 + 1, one _id-keyed user, and two compound-keyed orders together.
	require.Equal(t, []int{1, 1, 2, lookupQueryBatchSize}, sizes)
	require.Equal(t, lookupQueryBatchSize+4, seen)
}

func compoundKey(t *testing.T, region string, id any) []bson.RawElement {
	t.Helper()
	documentKey, err := bson.Marshal(bson.D{{Key: "region", Value: region}, {Key: "_id", Value: id}})
	require.NoError(t, err)
	elements, err := bson.Raw(documentKey).Elements()
	require.NoError(t, err)
	return elements
}

func TestDocumentLookupCommandBatchesCompoundKeysWithOr(t *testing.T) {
	keyFields := "region" + keyFieldSeparator + "_id"
	requests := []*documentLookupRequest{
		{documentKey: compoundKey(t, "eu", int32(1)), keyFields: keyFields, latestClusterTime: bson.Timestamp{T: 7, I: 1}},
		{documentKey: compoundKey(t, "us", int32(2)), keyFields: keyFields, latestClusterTime: bson.Timestamp{T: 8, I: 1}},
	}

	command := buildBatchLookupCommand("orders", requests)
	requireSameBSON(t, bson.D{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "region", Value: bson.D{{Key: "$eq", Value: "eu"}}}, {Key: "_id", Value: bson.D{{Key: "$eq", Value: int32(1)}}}},
		bson.D{{Key: "region", Value: bson.D{{Key: "$eq", Value: "us"}}}, {Key: "_id", Value: bson.D{{Key: "$eq", Value: int32(2)}}}},
	}}}}}, commandPipeline(t, command)[0])
	requireSameBSON(t, bson.D{{Key: "$limit", Value: 3}}, commandPipeline(t, command)[1])
}

func TestDocumentKeyOfMatchesRequestKey(t *testing.T) {
	request := &documentLookupRequest{documentKey: compoundKey(t, "eu", int32(1))}
	fields := []string{"region", "_id"}

	document, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: 2}, {Key: "region", Value: "eu"}})
	require.NoError(t, err)
	require.Equal(t, requestKey(request), documentKeyOf(document, fields), "field order in the document does not matter")

	other, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "region", Value: "us"}})
	require.NoError(t, err)
	require.NotEqual(t, requestKey(request), documentKeyOf(other, fields), "the same _id under another shard key value is another document")

	nestedKey, err := bson.Marshal(bson.D{{Key: "a.b", Value: "x"}, {Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	nestedElements, err := bson.Raw(nestedKey).Elements()
	require.NoError(t, err)
	nestedDocument, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: bson.D{{Key: "b", Value: "x"}}}})
	require.NoError(t, err)
	require.Equal(t, requestKey(&documentLookupRequest{documentKey: nestedElements}),
		documentKeyOf(nestedDocument, []string{"a.b", "_id"}), "a dotted shard key field reads the nested value")

	nullKey, err := bson.Marshal(bson.D{{Key: "region", Value: nil}, {Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	nullElements, err := bson.Raw(nullKey).Elements()
	require.NoError(t, err)
	missing, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	require.Equal(t, requestKey(&documentLookupRequest{documentKey: nullElements}), documentKeyOf(missing, fields),
		"a missing shard key field matches a null documentKey value")
}

func TestDecodeEventLeavesMissingPostImageNil(t *testing.T) {
	// With fullDocument: "whenAvailable", an update whose pre-image is missing carries fullDocument: null.
	// It must decode to nil, because the lookup only fetches updates without a document.
	raw, err := bson.Marshal(bson.D{
		{Key: "operationType", Value: "update"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: int32(1)}}},
		{Key: "fullDocument", Value: nil},
	})
	require.NoError(t, err)
	var changeEvent ChangeEvent
	require.NoError(t, decodeEvent(raw, &changeEvent))
	require.Nil(t, changeEvent.FullDocument)
}

func TestHydrateUpdateEventsSkipsUpdatesWithPostImage(t *testing.T) {
	document, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: 2}})
	require.NoError(t, err)
	postImage := bson.Raw(document)
	documentKey, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	require.NoError(t, err)
	events := []encodedMongoEvent{{
		operationType:     operationTypeUpdate,
		sourceTableName:   "db.coll",
		documentKey:       documentKey,
		maybeFullDocument: &postImage,
		clusterTime:       bson.Timestamp{T: 1, I: 1},
	}}

	// The connector has no client, so any lookup query would fail: every update already has its document.
	fetched, err := (&MongoConnector{clientUpdateLookup: true}).hydrateUpdateEvents(t.Context(), events)
	require.NoError(t, err)
	require.Zero(t, fetched)
	require.Equal(t, &postImage, events[0].maybeFullDocument)
}
