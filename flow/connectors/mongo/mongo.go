package connmongo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
)

const (
	DefaultDocumentKeyColumnName  = "_id"
	DefaultFullDocumentColumnName = "_full_document"
)

type MongoConnector struct {
	*metadataStore.PostgresMetadata
	config    *protos.MongoConfig
	client    *mongo.Client
	logger    log.Logger
	bytesRead atomic.Int64
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	clientOptions, err := parseAsClientOptions(config)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(clientOptions)
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
	if c != nil && c.client != nil {
		// Use a timeout to ensure the disconnect operation does not hang indefinitely
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return c.client.Disconnect(timeout)
	}
	return nil
}

func (c *MongoConnector) ConnectionActive(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	return nil
}

func (c *MongoConnector) GetVersion(ctx context.Context) (string, error) {
	buildInfo, err := peerdb_mongo.GetBuildInfo(ctx, c.client)
	if err != nil {
		return "", err
	}
	return buildInfo.Version, nil
}

func parseAsClientOptions(config *protos.MongoConfig) (*options.ClientOptions, error) {
	connStr, err := connstring.Parse(config.Uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing uri: %w", err)
	}

	if connStr.Username != "" || connStr.Password != "" {
		return nil, errors.New("connection string should not contain username and password")
	}

	clientOptions := options.Client().
		ApplyURI(config.Uri).
		SetAppName("PeerDB Mongo Connector").
		SetAuth(options.Credential{
			Username: config.Username,
			Password: config.Password,
		}).
		// always use compression
		SetCompressors([]string{"zstd", "snappy"}).
		// always use majority read concern for correctness
		SetReadConcern(readconcern.Majority())

	switch config.ReadPreference {
	case protos.ReadPreference_PRIMARY:
		clientOptions.SetReadPreference(readpref.Primary())
	case protos.ReadPreference_PRIMARY_PREFERRED:
		clientOptions.SetReadPreference(readpref.PrimaryPreferred())
	case protos.ReadPreference_SECONDARY:
		clientOptions.SetReadPreference(readpref.Secondary())
	case protos.ReadPreference_SECONDARY_PREFERRED:
		clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	case protos.ReadPreference_NEAREST:
		clientOptions.SetReadPreference(readpref.Nearest())
	case protos.ReadPreference_PREFERENCE_UNKNOWN:
		// use `secondaryPreferred` as default
		clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	default:
		return nil, fmt.Errorf("invalid ReadPreference: %s", config.ReadPreference)
	}

	if !config.DisableTls {
		tlsConfig, err := shared.CreateTlsConfig(tls.VersionTLS12, config.RootCa, "", config.TlsHost, false)
		if err != nil {
			return nil, err
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	err = clientOptions.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating client options: %w", err)
	}
	return clientOptions, nil
}
