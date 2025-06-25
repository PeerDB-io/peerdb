package e2e_mongo

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2e/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows"
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

	mongoVersion := os.Getenv("CI_MONGO_VERSION")
	require.NotEmpty(t, mongoVersion, "missing CI_MONGO_VERSION env var")

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
