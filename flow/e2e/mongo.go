package e2e

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MongoSource struct {
	conn   *connmongo.MongoConnector
	config *protos.MongoConfig

	// more privileged admin client for writing to mongo for tests
	adminClient *mongo.Client
}

func (s *MongoSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "mongo",
		Type: protos.DBType_MONGO,
		Config: &protos.Peer_MongoConfig{
			MongoConfig: s.config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *MongoSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	db := s.adminClient.Database(GetTestDatabase(suffix))
	_ = db.Drop(t.Context())
}

func (s *MongoSource) Connector() connectors.Connector {
	return s.conn
}

func (s *MongoSource) AdminClient() *mongo.Client {
	return s.adminClient
}

func (s *MongoSource) Exec(ctx context.Context, sql string, args ...any) error {
	return errors.ErrUnsupported
}

func (s *MongoSource) GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error) {
	collection := s.adminClient.Database(GetTestDatabase(suffix)).Collection(table)
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	recordBatch := &model.QRecordBatch{
		Schema:  connmongo.GetDefaultSchema(shared.InternalVersion_MongoDBFullDocumentColumnToDoc),
		Records: nil,
	}

	converter := connmongo.NewDirectBsonConverter()

	for cursor.Next(ctx) {
		record, err := connmongo.QValuesFromBsonRaw(cursor.Current, shared.InternalVersion_Latest, converter, table)
		if err != nil {
			return nil, err
		}
		recordBatch.Records = append(recordBatch.Records, record)
	}

	// sort by _id string value to match ClickHouse's `ORDER BY 1`
	sort.Slice(recordBatch.Records, func(i, j int) bool {
		return recordBatch.Records[i][0].Value().(string) < recordBatch.Records[j][0].Value().(string)
	})

	return recordBatch, nil
}

func GetTestDatabase(suffix string) string {
	return "e2e_test_" + suffix
}

func SetupMongo(t *testing.T, suffix string) (*MongoSource, error) {
	t.Helper()

	admin := internal.MongoAdminTestCredentials(t)
	adminClient, err := mongo.Connect(options.Client().
		ApplyURI(admin.URI).
		SetAppName("Mongo admin client").
		SetCompressors([]string{"zstd", "snappy"}).
		SetReadPreference(readpref.Primary()).
		SetAuth(options.Credential{
			Username: admin.Username,
			Password: admin.Password,
		}))
	require.NoError(t, err, "failed to setup mongo admin client")

	user := internal.MongoUserTestCredentials(t)

	mongoConfig := &protos.MongoConfig{
		Uri:        user.URI,
		Username:   user.Username,
		Password:   user.Password,
		DisableTls: true,
	}

	mongoConn, err := connmongo.NewMongoConnector(t.Context(), mongoConfig)
	require.NoError(t, err, "failed to setup mongo connector")

	testDb := GetTestDatabase(suffix)
	db := adminClient.Database(testDb)
	_ = db.Drop(t.Context())

	return &MongoSource{conn: mongoConn, config: mongoConfig, adminClient: adminClient}, err
}
