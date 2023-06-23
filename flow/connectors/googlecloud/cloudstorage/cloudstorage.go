package conncloudstorage

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/PeerDB-io/peer-flow/connectors/googlecloud"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"google.golang.org/appengine/log"
)

const (
	SyncRecordsBatchSize = 1024
)

// CloudStorageConnector is a connector for Google Cloud Storage.
type CloudStorageConnector struct {
	ctx                    context.Context
	csConfig               *protos.CloudStorageConfig
	storageClient          *storage.Client
	tableNameSchemaMapping map[string]*protos.TableSchema
	bucketID               string
}

func NewCloudStorageConnector(ctx context.Context, config *protos.CloudStorageConfig) (*CloudStorageConnector, error) {
	gcsa, err := googlecloud.NewGoogleCloudServiceAccount(config.AuthConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create GoogleCloudServiceAccount: %v", err)
	}

	storageClient, err := gcsa.CreateStorageClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	return &CloudStorageConnector{
		ctx:           ctx,
		csConfig:      config,
		bucketID:      config.GetBucketId(),
		storageClient: storageClient,
	}, nil
}

// Close closes the CloudStorageConnector.
func (c *CloudStorageConnector) Close() error {
	if c == nil || c.storageClient == nil {
		return nil
	}

	err := c.storageClient.Close()
	if err != nil {
		return fmt.Errorf("failed to close Storage client: %v", err)
	}
	return nil
}

func (c *CloudStorageConnector) ConnectionActive() bool {
	return c.storageClient != nil
}

// NeedsSetupMetadataTables is always false since we ain't dealing with tables here.
func (c *CloudStorageConnector) NeedsSetupMetadataTables() bool {
	return false
}

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *CloudStorageConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableNameSchemaMapping = req
	return nil
}

func (c *CloudStorageConnector) SetupMetadataTables() error {
	log.Errorf(c.ctx, "panicking at call to SetupMetadataTables for CloudStorageConnector")
	panic("SetupMetadataTables not implemented for CloudStorageConnector")
}
