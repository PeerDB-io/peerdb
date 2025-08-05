package connmongo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
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
	logger log.Logger
	*metadataStore.PostgresMetadata
	config    *protos.MongoConfig
	client    *mongo.Client
	ssh       utils.SSHTunnel
	bytesRead atomic.Int64
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	mongoConnector := &MongoConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           logger,
	}

	sshTunnel, err := utils.NewSSHTunnel(ctx, config.SshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}
	mongoConnector.ssh = sshTunnel

	var meteredDialer utils.MeteredDialer
	if sshTunnel.Client != nil {
		meteredDialer = utils.NewMeteredDialer(&mongoConnector.bytesRead, sshTunnel.Client.DialContext, true)
	} else {
		meteredDialer = utils.NewMeteredDialer(&mongoConnector.bytesRead, (&net.Dialer{Timeout: time.Minute}).DialContext, false)
	}

	clientOptions, err := parseAsClientOptions(config, meteredDialer)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	mongoConnector.client = client

	return mongoConnector, nil
}

func (c *MongoConnector) Close() error {
	if c != nil && c.client != nil {
		// Use a timeout to ensure the disconnect operation does not hang indefinitely
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return c.client.Disconnect(timeout)
	}
	if c.ssh.Client != nil {
		if err := c.ssh.Close(); err != nil {
			return fmt.Errorf("failed to close SSH tunnel: %w", err)
		}
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

func parseAsClientOptions(config *protos.MongoConfig, meteredDialer utils.MeteredDialer) (*options.ClientOptions, error) {
	connStr, err := connstring.Parse(config.Uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing uri: %w", err)
	}

	if connStr.HasAuthParameters() {
		return nil, errors.New("connection string should not contain username and password")
	} else if len(connStr.Options) > 0 || len(connStr.UnknownOptions) > 0 {
		return nil, errors.New("connection string should not contain options")
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
		SetReadConcern(readconcern.Majority()).
		SetDialer(&meteredDialer)

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
