package e2e_mongo

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_clickhouse "github.com/PeerDB-io/peerdb/flow/e2e/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

type MongoClickhouseSuite struct {
	e2e.GenericSuite
}

func TestMongoClickhouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupMongoClickhouseSuite)
}

func SetupMongoClickhouseSuite(t *testing.T) MongoClickhouseSuite {
	t.Helper()
	return MongoClickhouseSuite{e2e_clickhouse.SetupSuite(t, func(t *testing.T) (*MongoSource, string, error) {
		t.Helper()
		suffix := "mongoch_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMongo(t, suffix)
		return source, suffix, err
	})(t)}
}

func SetupMongo(t *testing.T, suffix string) (*MongoSource, error) {
	t.Helper()

	mongoUri := os.Getenv("CI_MONGO_URI")
	require.NotEmpty(t, mongoUri, "missing CI_MONGO_URI env var")

	mongoConfig := &protos.MongoConfig{Uri: mongoUri}

	mongoConn, err := connmongo.NewMongoConnector(t.Context(), mongoConfig)
	require.NoError(t, err, "failed to setup mongo connector")

	testDb := GetTestDatabase(suffix)
	db := mongoConn.Client().Database(testDb)
	_ = db.Drop(t.Context())

	return &MongoSource{conn: mongoConn, config: mongoConfig}, err
}

func (s MongoClickhouseSuite) Test_Simple_Flow() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_simple"
	dstTable := "test_simple_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	client := s.Source().Connector().(*connmongo.MongoConnector).Client()
	collection := client.Database(srcDatabase).Collection(srcTable)
	// insert 10 rows into the source table for initial load
	for i := range 10 {
		testKey := fmt.Sprintf("init_key_%d", i)
		testValue := fmt.Sprintf("init_value_%d", i)
		res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: testKey, Value: testValue}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table for cdc
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: testKey, Value: testValue}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	e2e.EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,_full_document")
	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Inconsistent_Schema() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_schema_change"
	dstTable := "test_schema_change_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	client := s.Source().Connector().(*connmongo.MongoConnector).Client()
	collection := client.Database(srcDatabase).Collection(srcTable)

	// adding/removing fields should work
	docs := []bson.D{
		{bson.E{Key: "field1", Value: 1}},
		{bson.E{Key: "field1", Value: 2}, bson.E{Key: "field2", Value: "v1"}},
		{bson.E{Key: "field2", Value: "v2"}},
	}
	for _, doc := range docs {
		res, err := collection.InsertOne(t.Context(), doc, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// inconsistent data type for a given field should work
	docs = []bson.D{
		{bson.E{Key: "field3", Value: 3}},
		{bson.E{Key: "field3", Value: "3"}},
	}
	for _, doc := range docs {
		res, err := collection.InsertOne(t.Context(), doc, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}
	e2e.EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,_full_document")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Update_Replace_Delete_Events() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_update_replace_delete"
	dstTable := "test_update_replace_delete_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	client := s.Source().Connector().(*connmongo.MongoConnector).Client()
	collection := client.Database(srcDatabase).Collection(srcTable)

	insertRes, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: 1}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, dstTable, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	updateRes, err := collection.UpdateOne(
		t.Context(),
		bson.D{bson.E{Key: "key", Value: 1}},
		bson.D{bson.E{Key: "$set", Value: bson.D{bson.E{Key: "key", Value: 2}}}},
		options.UpdateOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), updateRes.ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "update event", srcTable, dstTable, "_id,_full_document")

	replaceRes, err := collection.ReplaceOne(
		t.Context(),
		bson.D{bson.E{Key: "key", Value: 2}},
		bson.D{bson.E{Key: "key", Value: 3}},
		options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "replace event", srcTable, dstTable, "_id,_full_document")

	deleteRes, err := collection.DeleteOne(t.Context(), bson.D{bson.E{Key: "key", Value: 3}}, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "delete event", srcTable, dstTable, "_id,_full_document")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
