package e2e_mongo

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

type MongoSource struct {
	conn   *connmongo.MongoConnector
	config *protos.MongoConfig
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
	db := s.conn.Client().Database(GetTestDatabase(suffix))
	_ = db.Drop(t.Context())
}

func (s *MongoSource) Connector() connectors.Connector {
	return s.conn
}

func (s *MongoSource) Exec(ctx context.Context, sql string) error {
	return errors.New("not supported")
}

func (s *MongoSource) GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error) {
	collection := s.conn.Client().Database(GetTestDatabase(suffix)).Collection(table)
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
