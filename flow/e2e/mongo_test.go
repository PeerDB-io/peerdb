package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MongoClickhouseSuite struct {
	GenericSuite
}

func TestMongoClickhouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupMongoClickhouseSuite)
}

func SetupMongoClickhouseSuite(t *testing.T) MongoClickhouseSuite {
	t.Helper()
	return MongoClickhouseSuite{SetupClickHouseSuite(t, false, func(t *testing.T) (*MongoSource, string, error) {
		t.Helper()
		suffix := "mongoch_" + strings.ToLower(shared.RandomString(8))
		source, err := SetupMongo(t, suffix)
		return source, suffix, err
	})(t)}
}

func (s MongoClickhouseSuite) generateFlowConnectionConfigsDefaultEnv(
	connectionGen FlowConnectionGenerationConfig,
) *protos.FlowConnectionConfigs {
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Env = map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	return flowConnConfig
}

func (s MongoClickhouseSuite) Test_Simple_Flow() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_simple"
	dstTable := "test_simple_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
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

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table for cdc
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: testKey, Value: testValue}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,doc")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Simple_Flow_Partitioned() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_simple_partitioned"
	dstTable := "test_simple_dst_partitioned"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.TableMappings[0].PartitionKey = "_id"
	flowConnConfig.SnapshotNumRowsPerPartition = 10

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)
	// insert 1000 rows into the source table for initial load
	for i := range 1000 {
		testKey := fmt.Sprintf("init_key_%d", i)
		testValue := fmt.Sprintf("init_value_%d", i)
		res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: testKey, Value: testValue}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	// insert 10 rows into the source table for cdc
	for i := range 10 {
		testKey := fmt.Sprintf("test_key_%d", i)
		testValue := fmt.Sprintf("test_value_%d", i)
		res, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: testKey, Value: testValue}}, options.InsertOne())
		require.NoError(t, err)
		require.True(t, res.Acknowledged)
	}

	EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,doc")
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Inconsistent_Schema() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_schema_change"
	dstTable := "test_schema_change_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
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

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load to match", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

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
	EnvWaitForEqualTablesWithNames(env, s, "cdc events to match", srcTable, dstTable, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_CDC() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_update_replace_delete"
	dstTable := "test_update_replace_delete_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	err := adminClient.Database(srcDatabase).CreateCollection(t.Context(), srcTable)
	require.NoError(t, err)

	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	insertRes, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: 1}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable, dstTable, "_id,doc")

	updateRes, err := collection.UpdateOne(
		t.Context(),
		bson.D{bson.E{Key: "key", Value: 1}},
		bson.D{bson.E{Key: "$set", Value: bson.D{bson.E{Key: "key", Value: 2}}}},
		options.UpdateOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), updateRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "update event", srcTable, dstTable, "_id,doc")

	replaceRes, err := collection.ReplaceOne(
		t.Context(),
		bson.D{bson.E{Key: "key", Value: 2}},
		bson.D{bson.E{Key: "key", Value: 3}},
		options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "replace event", srcTable, dstTable, "_id,doc")

	deleteRes, err := collection.DeleteOne(t.Context(), bson.D{bson.E{Key: "key", Value: 3}}, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	EnvWaitForEqualTablesWithNames(env, s, "delete event", srcTable, dstTable, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Document_With_Dots_In_Keys() {
	t := s.T()

	envWaitForDocument := func(env WorkflowRun, dstTable string, expectedCount int, expectedDoc map[string]any, reason string) {
		EnvWaitFor(t, env, 3*time.Minute, reason, func() bool {
			clickhouseRows, err := s.GetRows(dstTable, "doc")
			if err != nil {
				t.Log(err)
				return false
			}
			if len(clickhouseRows.Records) < expectedCount {
				t.Logf("record count mismatch: expected %d, got %d", expectedCount, len(clickhouseRows.Records))
				return false
			}
			for i := range clickhouseRows.Records {
				clickhouseDocJsonStr := clickhouseRows.Records[i][0].Value().(string)
				var clickhouseDoc map[string]any
				if err := json.Unmarshal([]byte(clickhouseDocJsonStr), &clickhouseDoc); err != nil {
					t.Logf("failed to unmarshal clickhouse doc: %v", err)
					return false
				}
				// remove _id to keep comparison logic simple
				delete(clickhouseDoc, "_id")
				if !reflect.DeepEqual(clickhouseDoc, expectedDoc) {
					t.Logf("record %d doc mismatch: expected %v, got %v", i, expectedDoc, clickhouseDoc)
					return false
				}
			}
			return true
		})
	}

	doc := bson.D{
		bson.E{Key: "a", Value: bson.D{bson.E{Key: "b.c", Value: 1}}},
		bson.E{Key: "a.b", Value: bson.D{bson.E{Key: "c", Value: 1}}},
	}

	// Test current version should escape dots
	expectedDocWithEscapedDots := map[string]any{
		"a": map[string]any{
			"b%2Ec": float64(1),
		},
		"a%2Eb": map[string]any{
			"c": float64(1),
		},
	}

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_document_key_containing_dot"
	dstTable := "test_document_key_containing_dot_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	res, err := collection.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	envWaitForDocument(env, dstTable, 1, expectedDocWithEscapedDots, "initial load should match")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	res, err = collection.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)
	envWaitForDocument(env, dstTable, 2, expectedDocWithEscapedDots, "insert events should match")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)

	// Test older version should expand dots into nested structures
	expectedDocWithExpandedDots := map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": float64(1),
			},
		},
	}

	srcTableOld := "test_document_key_containing_dot_old"
	dstTableOld := "test_document_key_containing_dot_dst_old"

	connectionGenOld := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTableOld),
		TableMappings: TableMappings(s, srcTableOld, dstTableOld),
		Destination:   s.Peer().Name,
	}
	flowConnConfigOld := s.generateFlowConnectionConfigsDefaultEnv(connectionGenOld)
	flowConnConfigOld.DoInitialSnapshot = true

	flowConnConfigOld.Env["PEERDB_FORCE_INTERNAL_VERSION"] = strconv.FormatUint(
		uint64(shared.InternalVersion_MongoDBFullDocumentColumnToDoc), 10)
	flowConnConfigOld.Version = shared.InternalVersion_MongoDBFullDocumentColumnToDoc

	collectionOld := adminClient.Database(srcDatabase).Collection(srcTableOld)

	res, err = collectionOld.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	envOld := ExecutePeerflow(t, tc, flowConnConfigOld)
	envWaitForDocument(envOld, dstTableOld, 1, expectedDocWithExpandedDots, "initial load should expand dots")

	SetupCDCFlowStatusQuery(t, envOld, flowConnConfigOld)

	res, err = collectionOld.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	envWaitForDocument(envOld, dstTableOld, 2, expectedDocWithExpandedDots, "insert events should expand dots")

	envOld.Cancel(t.Context())
	RequireEnvCanceled(t, envOld)
}

func (s MongoClickhouseSuite) Test_Nested_Document_At_Limit() {
	t := s.T()

	nestedDoc := func(ch string) bson.D {
		var v any = ch
		for i := 100; i >= 1; i-- {
			v = bson.D{bson.E{Key: fmt.Sprintf("lvl_%d", i), Value: v}}
		}
		return v.(bson.D)
	}

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_nested_event"
	dstTable := "test_nested_event_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	// insert nested doc for initial load
	res, err := collection.InsertOne(t.Context(), nestedDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// insert nested doc for cdc
	res, err = collection.InsertOne(t.Context(), nestedDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "insert events to match", srcTable, dstTable, "_id,doc")

	oid := bson.D{bson.E{Key: "_id", Value: res.InsertedID}}

	// update nested doc for cdc
	updateRes, err := collection.UpdateOne(t.Context(), oid, bson.D{bson.E{Key: "$set", Value: nestedDoc("Y")}}, options.UpdateOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), updateRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "update events to match", srcTable, dstTable, "_id,doc")

	// replace nested doc for cdc
	replaceRes, err := collection.ReplaceOne(t.Context(), oid, nestedDoc("Z"), options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "replace events to match", srcTable, dstTable, "_id,doc")

	// delete nested doc for cdc
	deleteRes, err := collection.DeleteOne(t.Context(), oid, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	EnvWaitForEqualTablesWithNames(env, s, "delete events to match", srcTable, dstTable, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
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

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection("test_large_event")

	// insert large doc for initial load
	res, err := collection.InsertOne(t.Context(), largeDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// insert large doc for cdc (to test change event with "fullDocument")
	res, err = collection.InsertOne(t.Context(), largeDoc("X"), options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "insert events to match", srcTable, dstTable, "_id,doc")

	oid := bson.D{bson.E{Key: "_id", Value: res.InsertedID}}

	// update large doc for cdc
	updateRes, err := collection.UpdateOne(t.Context(), oid, bson.D{bson.E{Key: "$set", Value: largeDoc("Y")}}, options.UpdateOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), updateRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "update events to match", srcTable, dstTable, "_id,doc")

	// replace large doc for cdc
	replaceRes, err := collection.ReplaceOne(t.Context(), oid, largeDoc("Z"), options.Replace())
	require.NoError(t, err)
	require.Equal(t, int64(1), replaceRes.ModifiedCount)
	EnvWaitForEqualTablesWithNames(env, s, "replace events to match", srcTable, dstTable, "_id,doc")

	// delete large doc for cdc
	deleteRes, err := collection.DeleteOne(t.Context(), oid, options.DeleteOne())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteRes.DeletedCount)
	EnvWaitForEqualTablesWithNames(env, s, "delete events to match", srcTable, dstTable, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Long_Field_Name_Snapshot_And_CDC() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_long_field_name"
	dstTable := "test_long_field_name_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	// Regression coverage for GODRIVER-3809: long field names can trigger
	// "bufio: buffer full" when decoded through cursor.Decode.
	longFieldName := strings.Repeat("x", 5000)
	longKeyDoc := bson.D{{Key: longFieldName, Value: "test"}}

	res, err := collection.InsertOne(t.Context(), longKeyDoc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load with long field name", srcTable, dstTable, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	res, err = collection.InsertOne(t.Context(), longKeyDoc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, res.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "cdc with long field name", srcTable, dstTable, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Transactions_Across_Collections() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable1 := "test_transaction_t1"
	dstTable1 := "test_transaction_t1_dst"
	srcTable2 := "test_transaction_t2"
	dstTable2 := "test_transaction_t2_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "test_transaction"),
		TableMappings: TableMappings(s, srcTable1, dstTable1, srcTable2, dstTable2),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	session, err := adminClient.StartSession()
	require.NoError(t, err)
	defer session.EndSession(t.Context())

	coll1 := adminClient.Database(srcDatabase).Collection(srcTable1)
	coll2 := adminClient.Database(srcDatabase).Collection(srcTable2)
	res, err := session.WithTransaction(t.Context(), func(ctx context.Context) (any, error) {
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

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable1, dstTable1, "_id,doc")
	EnvWaitForEqualTablesWithNames(env, s, "initial load", srcTable2, dstTable2, "_id,doc")

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	res, err = session.WithTransaction(t.Context(), func(ctx context.Context) (any, error) {
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
	EnvWaitForEqualTablesWithNames(env, s, "t1 to match", srcTable1, dstTable1, "_id,doc")
	EnvWaitForEqualTablesWithNames(env, s, "t2 to match", srcTable2, dstTable2, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Json_Disabled() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_json_disabled"
	dstTable := "test_json_disabled_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env["PEERDB_CLICKHOUSE_ENABLE_JSON"] = "false"

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	insertRes, err := collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: "val"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForCount(env, s, "initial load", dstTable, "_id,doc", 1)

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	insertRes, err = collection.InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: "val"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	EnvWaitForCount(env, s, "cdc", dstTable, "_id,doc", 2)

	peer := s.Peer()
	ch, err := connclickhouse.Connect(t.Context(), nil, peer.GetClickhouseConfig())
	require.NoError(t, err)
	defer ch.Close()

	var columnType string
	row := ch.QueryRow(t.Context(),
		fmt.Sprintf("SELECT type FROM system.columns WHERE database = '%s' AND table = '%s' AND name = 'doc'",
			peer.GetClickhouseConfig().Database, dstTable))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&columnType))
	require.Equal(t, "String", columnType, "doc column should be of type String when JSON is disabled")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Mongo_Can_Resume_After_Delete_Table() {
	t := s.T()

	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable1 := "t1"
	dstTable1 := "t1_dst"
	srcTable2 := "t2"
	dstTable2 := "t2_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, "can_resume_after_delete_table"),
		TableMappings: TableMappings(s, srcTable1, dstTable1, srcTable2, dstTable2),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	db := s.Source().(*MongoSource).AdminClient().Database(srcDatabase)
	err := db.CreateCollection(t.Context(), srcTable1)
	require.NoError(t, err)
	err = db.CreateCollection(t.Context(), srcTable2)
	require.NoError(t, err)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// insert a document to t1 and t2
	// since t2 is written last, saved resume token references t2
	insertRes, err := db.Collection(srcTable1).InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: "val"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	insertRes, err = db.Collection(srcTable2).InsertOne(t.Context(), bson.D{bson.E{Key: "key", Value: "val"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable1, dstTable1, "_id,doc")
	EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable2, dstTable2, "_id,doc")

	// pause workflow
	SignalWorkflow(t.Context(), env, model.FlowSignal, model.PauseSignal)
	EnvWaitFor(t, env, 1*time.Minute, "paused workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})

	// resume workflow with t2 removed from table mapping
	SignalWorkflow(t.Context(), env, model.CDCDynamicPropertiesSignal, &protos.CDCFlowConfigUpdate{
		RemovedTables: []*protos.TableMapping{{
			SourceTableIdentifier:      srcDatabase + "." + srcTable2,
			DestinationTableIdentifier: srcDatabase + "." + dstTable2,
		}},
	})
	EnvWaitFor(t, env, 1*time.Minute, "resumed workflow", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	// insert a document to t1 should succeed
	insertRes, err = db.Collection(srcTable1).InsertOne(t.Context(), bson.D{bson.E{Key: "key2", Value: "val2"}}, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	EnvWaitForEqualTablesWithNames(env, s, "insert event", srcTable1, dstTable1, "_id,doc")

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s MongoClickhouseSuite) Test_Json_Types() {
	t := s.T()
	srcDatabase := GetTestDatabase(s.Suffix())
	srcTable := "test_json_types"
	dstTable := "test_json_types_dst"

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := s.generateFlowConnectionConfigsDefaultEnv(connectionGen)
	flowConnConfig.DoInitialSnapshot = true

	adminClient := s.Source().(*MongoSource).AdminClient()
	collection := adminClient.Database(srcDatabase).Collection(srcTable)

	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	decimal128, err := bson.ParseDecimal128("123.4567890987654321")
	require.NoError(t, err)

	doc := bson.D{
		// String types
		{Key: "string", Value: "hello"},
		{Key: "empty_string", Value: ""},
		{Key: "string_special", Value: "hello\nworld\t\"quoted\""},

		// Boolean types
		{Key: "bool_true", Value: true},
		{Key: "bool_false", Value: false},

		// Integer types
		{Key: "int", Value: 42},
		{Key: "int8", Value: int8(127)},
		{Key: "int16", Value: int16(32767)},
		{Key: "int32", Value: int32(2147483647)},
		{Key: "int64", Value: int64(9223372036854775807)},
		{Key: "uint", Value: uint(42)},
		{Key: "uint8", Value: uint8(255)},
		{Key: "uint16", Value: uint16(65535)},
		{Key: "uint32", Value: uint32(4294967295)},

		// Negative integers
		{Key: "neg_int", Value: -42},
		{Key: "neg_int8", Value: int8(-128)},
		{Key: "neg_int16", Value: int16(-32768)},
		{Key: "neg_int32", Value: int32(-2147483648)},
		{Key: "neg_int64", Value: int64(-9223372036854775807)},

		// Floating point types
		{Key: "float64", Value: float64(3.14159265359)},
		{Key: "neg_float64", Value: float64(-3.14159265359)},
		{Key: "float64_max_int64", Value: float64(math.MaxInt64)},
		{Key: "float64_min_int64", Value: float64(math.MinInt64)},
		{Key: "float64_greater_than_max_int64", Value: math.Pow(2, 65)},
		{Key: "float64_less_than_min_int64", Value: -math.Pow(2, 65)},
		{Key: "float64_scientific_notation", Value: 1e100},

		// Special float values
		{Key: "nan", Value: math.NaN()},
		{Key: "pos_inf", Value: math.Inf(1)},
		{Key: "neg_inf", Value: math.Inf(-1)},

		// Datetime
		{Key: "date_time", Value: bson.DateTime(1672531200000)},                   // 2023-01-01 00:00:00 UTC
		{Key: "date_time_outside_rfc3339", Value: bson.DateTime(253433923200000)}, // 10001-01-01 00:00:00 UTC
		{Key: "date_time_max", Value: bson.DateTime(math.MaxInt64)},
		{Key: "date_time_min", Value: bson.DateTime(math.MinInt64)},

		// Arrays with special values
		{Key: "array_mixed", Value: bson.A{1, "str", true}},
		{Key: "array_special_values", Value: bson.A{math.NaN(), math.Inf(1), math.Inf(-1), bson.DateTime(math.MaxInt64)}},

		// Complex nested documents
		{Key: "nested_doc", Value: bson.D{
			{Key: "inner1", Value: "str"},
			{Key: "inner2", Value: 1},
			{Key: "inner3", Value: true},
			{Key: "inner4", Value: bson.D{
				{Key: "a", Value: math.NaN()},
				{Key: "b", Value: bson.A{"hello", "world"}},
			}},
		}},

		// Complex nested array
		{Key: "nested_array", Value: bson.A{
			bson.D{{Key: "inner1", Value: bson.A{
				bson.D{{Key: "inner_inner", Value: bson.A{
					math.NaN(), math.Inf(1), math.Inf(-1), bson.DateTime(math.MaxInt64),
				}}},
			}}},
			bson.D{{Key: "inner2", Value: 1.23}},
		}},
		{Key: "nested_array_2", Value: bson.A{
			bson.D{{Key: "NaN", Value: math.NaN()}},
			bson.D{{Key: "binary", Value: bson.Binary{Subtype: 0x00, Data: []byte("test")}}},
			bson.D{{
				Key:   "nested_arr",
				Value: bson.A{bson.A{1}, bson.A{2}, bson.A{3}},
			}},
			bson.D{{
				Key:   "nested_doc",
				Value: bson.D{{Key: "str", Value: "hello world"}},
			}},
			bson.D{{Key: "timestamp", Value: bson.Timestamp{T: 1672531200, I: 1}}},
		}},

		// Other bson types
		{Key: "object_id", Value: oid},
		{Key: "symbol", Value: bson.Symbol("test_symbol")},
		{Key: "binary", Value: bson.Binary{Subtype: 0x02, Data: []byte("hello world")}},
		{Key: "binary_empty", Value: bson.Binary{Subtype: 0x00, Data: []byte{}}},
		{Key: "timestamp", Value: bson.Timestamp{T: 1672531200, I: 1}},
		{Key: "regex", Value: bson.Regex{Pattern: "^test.*", Options: "i"}},
		{Key: "decimal128", Value: decimal128},
		{Key: "javascript", Value: bson.JavaScript("function() { return 42; }")},
		{Key: "js_with_scope", Value: bson.CodeWithScope{Code: "function(x) { return x + y; }", Scope: bson.D{{Key: "y", Value: 10}}}},
		{Key: "db_pointer", Value: bson.DBPointer{DB: "test_db", Pointer: oid}},

		// Other bson types (not propagated)
		{Key: "undefined_field", Value: bson.Undefined{}},
		{Key: "null_field", Value: bson.Null{}},
		{Key: "max_key", Value: bson.MaxKey{}},
		{Key: "min_key", Value: bson.MinKey{}},
	}

	insertRes, err := collection.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	EnvWaitForCount(env, s, "initial load", dstTable, "_id,doc", 1)

	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	insertRes, err = collection.InsertOne(t.Context(), doc, options.InsertOne())
	require.NoError(t, err)
	require.True(t, insertRes.Acknowledged)
	EnvWaitForCount(env, s, "cdc", dstTable, "_id,doc", 2)

	rows, err := s.GetRows(dstTable, "_id,doc")
	require.NoError(t, err)
	require.Len(t, rows.Records, 2, "Expected 2 rows in destination table")

	row1 := rows.Records[0][1].Value().(string)
	row2 := rows.Records[1][1].Value().(string)
	for _, row := range []string{row1, row2} {
		require.Contains(t, row, `"string":"hello"`)
		require.Contains(t, row, `"empty_string":""`)
		require.Contains(t, row, `"string_special":"hello\nworld\t\"quoted\""`)
		require.Contains(t, row, `"bool_true":true`)
		require.Contains(t, row, `"bool_false":false`)
		require.Contains(t, row, `"int":42`)
		require.Contains(t, row, `"int8":127`)
		require.Contains(t, row, `"int16":32767`)
		require.Contains(t, row, `"int32":2147483647`)
		require.Contains(t, row, `"int64":9223372036854775807`)
		require.Contains(t, row, `"uint":42`)
		require.Contains(t, row, `"uint8":255`)
		require.Contains(t, row, `"uint16":65535`)
		require.Contains(t, row, `"uint32":4294967295`)
		require.Contains(t, row, `"neg_int":-42`)
		require.Contains(t, row, `"neg_int8":-128`)
		require.Contains(t, row, `"neg_int16":-32768`)
		require.Contains(t, row, `"neg_int32":-2147483648`)
		require.Contains(t, row, `"neg_int64":-9223372036854775807`)
		require.Contains(t, row, `"float64":3.14159265359`)
		require.Contains(t, row, `"neg_float64":-3.14159265359`)
		require.Contains(t, row, `"float64_max_int64":9223372036854776000`)
		require.Contains(t, row, `"float64_min_int64":-9223372036854776000`)
		require.Contains(t, row, `"float64_greater_than_max_int64":36893488147419103000`)
		require.Contains(t, row, `"float64_less_than_min_int64":-36893488147419103000`)
		require.Contains(t, row, `"float64_scientific_notation":1e+100`)
		require.Contains(t, row, `"nan":"NaN"`)
		require.Contains(t, row, `"pos_inf":"+Inf"`)
		require.Contains(t, row, `"neg_inf":"-Inf"`)
		require.Contains(t, row, `"date_time":"2023-01-01T00:00:00Z"`)
		require.Contains(t, row, `"date_time_outside_rfc3339":"10001-01-01T00:00:00Z"`)
		require.Contains(t, row, `"date_time_max":"292278994-08-17T07:12:55.807Z"`)
		require.Contains(t, row, `"date_time_min":"-292275055-05-16T16:47:04.192Z"`)
		// mixed array promoted common type
		require.Contains(t, row, `"array_mixed":[1,"str",true]`)
		require.Contains(t, row, `"array_special_values":["NaN","+Inf","-Inf","292278994-08-17T07:12:55.807Z"]`)
		require.Contains(t, row, `"nested_doc":{"inner1":"str","inner2":1,"inner3":true,"inner4":{"a":"NaN","b":["hello","world"]}}`)
		require.Contains(t, row,
			`"nested_array":[{"inner1":[{"inner_inner":["NaN","+Inf","-Inf","292278994-08-17T07:12:55.807Z"]}]},{"inner2":1.23}]`)
		require.Contains(t, row, `"nested_array_2":[{"NaN":"NaN"},{"binary":{"Data":"dGVzdA==","Subtype":0}},`+
			`{"nested_arr":[[1],[2],[3]]},{"nested_doc":{"str":"hello world"}},{"timestamp":{"I":1,"T":1672531200}}]`)

		require.Contains(t, row, `"object_id":"507f1f77bcf86cd799439011"`)
		require.Contains(t, row, `"symbol":"test_symbol"`)
		// binary data should be base64 encoded
		require.Contains(t, row, `"binary":{"Data":"aGVsbG8gd29ybGQ=","Subtype":2}`)
		require.Contains(t, row, `"binary_empty":{"Data":"","Subtype":0}`)
		require.Contains(t, row, `"timestamp":{"I":1,"T":1672531200}`)
		require.Contains(t, row, `"regex":{"Options":"i","Pattern":"^test.*"}`)
		// decimal12 should be converted to string
		require.Contains(t, row, `"decimal128":"123.4567890987654321"`)
		require.Contains(t, row, `"javascript":"function() { return 42; }"`)
		require.Contains(t, row, `"js_with_scope":{"Code":"function(x) { return x + y; }","Scope":{"y":10}}`)
		require.Contains(t, row, `"db_pointer":{"DB":"test_db","Pointer":"507f1f77bcf86cd799439011"}`)

		// check unsupported types should not be propagated
		require.NotContains(t, row, `"null_field"`)
		require.NotContains(t, row, `"undefined_field"`)
		require.NotContains(t, row, `"max_key"`)
		require.NotContains(t, row, `"min_key"`)
	}
	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}
