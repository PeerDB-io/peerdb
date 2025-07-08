package e2e_mongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

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
	return MongoClickhouseSuite{e2e_clickhouse.SetupSuite(t, false, func(t *testing.T) (*MongoSource, string, error) {
		t.Helper()
		suffix := "mongoch_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMongo(t, suffix)
		return source, suffix, err
	})(t)}
}

func SetupMongo(t *testing.T, suffix string) (*MongoSource, error) {
	t.Helper()

	mongoAdminUri := os.Getenv("CI_MONGO_ADMIN_URI")
	require.NotEmpty(t, mongoAdminUri, "missing CI_MONGO_ADMIN_URI env var")
	mongoAdminUsername := os.Getenv("CI_MONGO_ADMIN_USERNAME")
	require.NotEmpty(t, mongoAdminUsername, "missing CI_MONGO_ADMIN_USERNAME env var")
	mongoAdminPassword := os.Getenv("CI_MONGO_ADMIN_PASSWORD")
	require.NotEmpty(t, mongoAdminPassword, "missing CI_MONGO_ADMIN_PASSWORD env var")
	adminClient, err := mongo.Connect(options.Client().
		ApplyURI(mongoAdminUri).
		SetAppName("Mongo admin client").
		SetCompressors([]string{"zstd", "snappy"}).
		SetReadPreference(readpref.Primary()).
		SetAuth(options.Credential{
			Username: mongoAdminUsername,
			Password: mongoAdminPassword,
		}))
	require.NoError(t, err, "failed to setup mongo admin client")

	mongoUri := os.Getenv("CI_MONGO_URI")
	require.NotEmpty(t, mongoUri, "missing CI_MONGO_URI env var")
	mongoUsername := os.Getenv("CI_MONGO_USERNAME")
	require.NotEmpty(t, mongoUsername, "missing CI_MONGO_USERNAME env var")
	mongoPassword := os.Getenv("CI_MONGO_PASSWORD")
	require.NotEmpty(t, mongoPassword, "missing CI_MONGO_PASSWORD env var")

	mongoConfig := &protos.MongoConfig{
		Uri:        mongoUri,
		Username:   mongoUsername,
		Password:   mongoPassword,
		DisableTls: true,
	}

	mongoConn, err := connmongo.NewMongoConnector(t.Context(), mongoConfig)
	require.NoError(t, err, "failed to setup mongo connector")

	testDb := GetTestDatabase(suffix)
	db := adminClient.Database(testDb)
	_ = db.Drop(t.Context())

	return &MongoSource{conn: mongoConn, config: mongoConfig, adminClient: adminClient}, err
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

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)
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

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

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

func (s MongoClickhouseSuite) Test_CDC() {
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

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	insertRes, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: 1}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable, dstTable, "_id,_full_document")

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

func (s MongoClickhouseSuite) Test_Large_Document_At_Limit() {
	t := s.T()

	largeDoc := func(ch string) bson.D {
		// maximum byte size that can be inserted for this doc
		// one more byte we get 'object to insert too large' error
		sizeBytes := 16*1024*1024 - 41
		largeString := strings.Repeat(ch, sizeBytes)
		return bson.D{bson.E{Key: "large_string", Value: largeString}}
	}

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_large_event"
	dstTable := "test_large_event_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection("test_large_event")

	// insert large doc for initial load
	res, err := collection.InsertOne(t.Context(), largeDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, dstTable, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// insert large doc for cdc (to test change event with "fullDocument")
	res, err = collection.InsertOne(t.Context(), largeDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "insert events to match", srcTable, dstTable, "_id,_full_document")

	oid := bson.D{bson.E{Key: "_id", Value: res.InsertedID}}

	// update large doc for cdc
	updateRes, err := collection.UpdateOne(t.Context(), oid, bson.D{bson.E{Key: "$set", Value: largeDoc("Y")}}, options.UpdateOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), updateRes.ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "update events to match", srcTable, dstTable, "_id,_full_document")

	// replace large doc for cdc
	replaceRes, err := collection.ReplaceOne(t.Context(), oid, largeDoc("Z"), options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "replace events to match", srcTable, dstTable, "_id,_full_document")

	// delete large doc for cdc
	deleteRes, err := collection.DeleteOne(t.Context(), oid, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "delete events to match", srcTable, dstTable, "_id,_full_document")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Transactions_Across_Collections() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable1 := "test_transaction_t1"
	dstTable1 := "test_transaction_t1_dst"
	srcTable2 := "test_transaction_t2"
	dstTable2 := "test_transaction_t2_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, "test_transaction"),
		TableMappings: e2e.TableMappings(s, srcTable1, dstTable1, srcTable2, dstTable2),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	session, err := adminClient.StartSession()
	require.NoError(t, err)
	defer session.EndSession(t.Context())

	coll1 := adminClient.Database(srcDatabase).Collection(srcTable1)
	coll2 := adminClient.Database(srcDatabase).Collection(srcTable2)
	res, err := session.WithTransaction(t.Context(), func(ctx context.Context) (interface{}, error) {
		res1, err1 := coll1.InsertOne(t.Context(), bson.D{bson.E{Key: "foo", Value: 1}}, options.InsertOne())
		res2, err2 := coll2.InsertOne(t.Context(), bson.D{bson.E{Key: "bar", Value: 2}}, options.InsertOne())
		err := err1
		if err2 != nil {
			err = err2
		}
		return []*mongo.InsertOneResult{res1, res2}, err
	}, options.Transaction())
	require.NoError(t, err)
	require.True(t, res.([]*mongo.InsertOneResult)[0].Acknowledged)
	require.True(t, res.([]*mongo.InsertOneResult)[1].Acknowledged)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable1, dstTable1, "_id,_full_document")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable2, dstTable2, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	res, err = session.WithTransaction(t.Context(), func(ctx context.Context) (interface{}, error) {
		res1, err1 := coll1.UpdateOne(t.Context(),
			bson.D{bson.E{Key: "foo", Value: 1}},
			bson.D{bson.E{Key: "$set", Value: bson.D{bson.E{Key: "foo", Value: 11}}}},
			options.UpdateOne())
		res2, err2 := coll2.UpdateOne(t.Context(),
			bson.D{bson.E{Key: "bar", Value: 2}},
			bson.D{bson.E{Key: "$set", Value: bson.D{bson.E{Key: "bar", Value: 22}}}},
			options.UpdateOne())
		err := err1
		if err2 != nil {
			err = err2
		}
		return []*mongo.UpdateResult{res1, res2}, err
	}, options.Transaction())
	require.NoError(t, err)
	require.Equal(t, int64(1), res.([]*mongo.UpdateResult)[0].ModifiedCount)
	require.Equal(t, int64(1), res.([]*mongo.UpdateResult)[1].ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "t1 to match", srcTable1, dstTable1, "_id,_full_document")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "t2 to match", srcTable2, dstTable2, "_id,_full_document")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Enable_Json() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_full_document_json"
	dstTable := "test_full_document_json_dst"

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, srcTable),
		TableMappings: e2e.TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: "val"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, dstTable, "_id,_full_document")

	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	insertRes, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key2", Value: "val2"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable, dstTable, "_id,_full_document")
	oid := bson.D{bson.E{Key: "_id", Value: res.InsertedID}}

	replaceRes, err := collection.ReplaceOne(t.Context(), oid, bson.D{bson.E{Key: "key2", Value: "val2"}}, options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "replace event", srcTable, dstTable, "_id,_full_document")

	deleteRes, err := collection.DeleteOne(t.Context(), oid, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "delete event", srcTable, dstTable, "_id,_full_document")

	env.Cancel(t.Context())
	e2e.RequireEnvCanceled(t, env)
}
