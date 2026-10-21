//go:build tilt

package connmongo

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

func TestBuildObjectIDPartitionsMongo(t *testing.T) {
	connector, database := setupPartitionTestMongo(t)

	t.Run("clustered IDs with extreme outliers", func(t *testing.T) {
		const numRows = 1000
		const numPartitions = 100
		collection := database.Collection("clustered_object_ids")
		maxID, err := bson.ObjectIDFromHex("ffffffffffffffffffffffff")
		require.NoError(t, err)
		baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		docs := make([]any, numRows)
		expected := make(map[bson.ObjectID]struct{}, numRows)
		for i := range numRows {
			id := bson.NewObjectIDFromTimestamp(baseTime.Add(time.Duration(i) * time.Second))
			switch i {
			case 0:
				id = bson.ObjectID{}
			case numRows - 1:
				id = maxID
			}
			docs[i] = bson.D{{Key: "_id", Value: id}}
			expected[id] = struct{}{}
		}
		_, err = collection.InsertMany(t.Context(), docs)
		require.NoError(t, err)

		partitions, err := connector.buildPartitions(t.Context(), collection, numPartitions)
		require.NoError(t, err)
		require.Len(t, partitions, numPartitions)

		seen := make(map[bson.ObjectID]int, numRows)
		minRows, maxRows, totalRows := numRows, 0, 0
		for i, partition := range partitions {
			require.False(t, partition.FullTablePartition)
			filter, err := toRangeFilter("_id", partition.Range)
			require.NoError(t, err)
			cursor, err := collection.Find(t.Context(), filter, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
			require.NoError(t, err)
			var rows []struct {
				ID bson.ObjectID `bson:"_id"`
			}
			require.NoError(t, cursor.All(t.Context(), &rows))
			for _, row := range rows {
				seen[row.ID]++
			}
			minRows = min(minRows, len(rows))
			maxRows = max(maxRows, len(rows))
			totalRows += len(rows)

			if i == len(partitions)/2 {
				explain, err := database.RunCommand(t.Context(), bson.D{
					{Key: "explain", Value: bson.D{
						{Key: "find", Value: collection.Name()},
						{Key: "filter", Value: filter},
					}},
					{Key: "verbosity", Value: "queryPlanner"},
				}).Raw()
				require.NoError(t, err)
				plan, err := bson.MarshalExtJSON(explain.Lookup("queryPlanner", "winningPlan").Document(), false, false)
				require.NoError(t, err)
				require.Contains(t, string(plan), `"indexName":"_id_"`)
				require.NotContains(t, string(plan), `"stage":"COLLSCAN"`)
				t.Logf("partition range winning plan: %s", plan)
			}
		}
		t.Logf("partitions=%d rows=%d min=%d max=%d", len(partitions), totalRows, minRows, maxRows)
		require.Equal(t, numRows, totalRows)
		require.Len(t, seen, numRows)
		for id := range expected {
			require.Equal(t, 1, seen[id], "ObjectID %s must be pulled exactly once", id.Hex())
		}
		// Sampling exceeds the fixture size, so MongoDB returns every ID. Allow
		// quantile rounding while rejecting a uniform split's 998-row partition.
		require.Positive(t, minRows)
		require.LessOrEqual(t, maxRows, 2*numRows/numPartitions)
	})

	t.Run("empty collection", func(t *testing.T) {
		partitions, err := connector.buildPartitions(t.Context(), database.Collection("empty"), 100)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		require.True(t, partitions[0].FullTablePartition)
	})

	t.Run("single ObjectID", func(t *testing.T) {
		collection := database.Collection("single_object_id")
		_, err := collection.InsertOne(t.Context(), bson.D{{Key: "_id", Value: bson.ObjectID{}}})
		require.NoError(t, err)
		partitions, err := connector.buildPartitions(t.Context(), collection, 100)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		require.True(t, partitions[0].FullTablePartition)
	})
}

func TestBuildStringPartitionsMongoCollation(t *testing.T) {
	connector, database := setupPartitionTestMongo(t)
	const collectionName = "numeric_string_collation"
	require.NoError(t, database.CreateCollection(t.Context(), collectionName,
		options.CreateCollection().SetCollation(&options.Collation{Locale: "en", NumericOrdering: true})))
	collection := database.Collection(collectionName)
	ids := []string{"a1", "a2", "a10", "a11", "a20", "z1"}
	docs := make([]any, len(ids))
	for i, id := range ids {
		docs[i] = bson.D{{Key: "_id", Value: id}}
	}
	_, err := collection.InsertMany(t.Context(), docs)
	require.NoError(t, err)
	partitions, err := connector.buildPartitions(t.Context(), collection, 3)
	require.NoError(t, err)
	require.Len(t, partitions, 3)
	seen := make(map[string]int, len(ids))
	for _, partition := range partitions {
		require.False(t, partition.FullTablePartition)
		filter, err := toRangeFilter("_id", partition.Range)
		require.NoError(t, err)
		cursor, err := collection.Find(t.Context(), filter)
		require.NoError(t, err)
		var rows []struct {
			ID string `bson:"_id"`
		}
		require.NoError(t, cursor.All(t.Context(), &rows))
		for _, row := range rows {
			seen[row.ID]++
		}
	}
	require.Len(t, seen, len(ids))
	for _, id := range ids {
		require.Equal(t, 1, seen[id], "string ID %s must be pulled exactly once", id)
	}
}

func setupPartitionTestMongo(t *testing.T) (*MongoConnector, *mongo.Database) {
	t.Helper()
	credentials := testutil.MongoAdminTestCredentials(t)
	client, err := mongo.Connect(options.Client().ApplyURI(credentials.URI).
		SetAuth(options.Credential{Username: credentials.Username, Password: credentials.Password}).
		SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	database := client.Database("peerdb_partition_test_" + uuid.NewString())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := database.Drop(ctx); err != nil {
			t.Errorf("drop partition test database: %v", err)
		}
		if err := client.Disconnect(ctx); err != nil {
			t.Errorf("disconnect partition test client: %v", err)
		}
	})
	require.NoError(t, client.Ping(t.Context(), readpref.Primary()))
	return &MongoConnector{
		logger: internal.LoggerFromCtx(t.Context()),
		config: &protos.MongoConfig{ReadPreference: protos.ReadPreference_PRIMARY},
	}, database
}
