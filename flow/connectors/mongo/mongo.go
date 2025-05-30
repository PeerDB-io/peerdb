package connmongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

type MongoConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MongoConfig
	client *mongo.Client
	logger log.Logger
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	client, err := mongo.Connect(options.Client().ApplyURI(config.Uri))
	if err != nil {
		return nil, err
	}
	return &MongoConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		client:           client,
		logger:           logger,
	}, nil
}

func (c *MongoConnector) Close() error {
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return c.client.Disconnect(timeout)
	}
	return nil
}

func (c *MongoConnector) GetVersion(ctx context.Context) (string, error) {
	db := c.client.Database(c.config.Database)

	var buildInfoDoc bson.M
	if err := db.RunCommand(ctx, bson.D{bson.E{Key: "buildInfo", Value: 1}}).Decode(&buildInfoDoc); err != nil {
		return "", fmt.Errorf("failed to run buildInfo command: %w", err)
	}
	version, ok := buildInfoDoc["version"].(string)
	if !ok {
		return "", fmt.Errorf("buildInfo.version is not a string, but %T", buildInfoDoc["version"])
	}
	return version, nil
}
