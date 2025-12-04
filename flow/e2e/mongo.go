package e2e

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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

func (s *MongoSource) Exec(ctx context.Context, sql string) error {
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

	for cursor.Next(ctx) {
		var doc bson.D
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, err
		}
		record, err := connmongo.QValuesFromDocument(doc)
		if err != nil {
			return nil, err
		}
		recordBatch.Records = append(recordBatch.Records, record)
	}

	return recordBatch, nil
}

func GetTestDatabase(suffix string) string {
	return "e2e_test_" + suffix
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
