package e2e_mongo

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
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
	e2e.CreatePeer(t, peer)
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
		Schema:  connmongo.GetDefaultSchema(),
		Records: nil,
	}

	for cursor.Next(ctx) {
		var doc bson.D
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, err
		}
		record, _, err := connmongo.QValuesFromDocument(doc)
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
