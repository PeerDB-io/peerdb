package conns3

import (
	"context"
	"fmt"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

type S3Connector struct {
	ctx        context.Context
	url        string
	pgMetadata *metadataStore.PostgresMetadataStore
	client     s3.S3
	creds      utils.S3PeerCredentials
}

func NewS3Connector(ctx context.Context,
	config *protos.S3Config) (*S3Connector, error) {
	keyID := ""
	if config.AccessKeyId != nil {
		keyID = *config.AccessKeyId
	}
	secretKey := ""
	if config.SecretAccessKey != nil {
		secretKey = *config.SecretAccessKey
	}
	roleArn := ""
	if config.RoleArn != nil {
		roleArn = *config.RoleArn
	}
	region := ""
	if config.Region != nil {
		region = *config.Region
	}
	endpoint := ""
	if config.Endpoint != nil {
		endpoint = *config.Endpoint
	}
	s3PeerCreds := utils.S3PeerCredentials{
		AccessKeyID:     keyID,
		SecretAccessKey: secretKey,
		AwsRoleArn:      roleArn,
		Region:          region,
		Endpoint:        endpoint,
	}
	s3Client, err := utils.CreateS3Client(s3PeerCreds)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	metadataSchemaName := "peerdb_s3_metadata" // #nosec G101
	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx,
		config.GetMetadataDb(), metadataSchemaName)
	if err != nil {
		log.Errorf("failed to create postgres metadata store: %v", err)
		return nil, err
	}

	return &S3Connector{
		ctx:        ctx,
		url:        config.Url,
		pgMetadata: pgMetadata,
		client:     *s3Client,
		creds:      s3PeerCreds,
	}, nil
}

func (c *S3Connector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	log.Infof("CreateRawTable for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	log.Infof("InitializeTableSchema for S3 is a no-op")
	return nil
}

func (c *S3Connector) Close() error {
	log.Debugf("Closing s3 connector is a noop")
	return nil
}

func (c *S3Connector) ConnectionActive() bool {
	_, err := c.client.ListBuckets(nil)
	return err == nil
}

func (c *S3Connector) NeedsSetupMetadataTables() bool {
	return c.pgMetadata.NeedsSetupMetadata()
}

func (c *S3Connector) SetupMetadataTables() error {
	err := c.pgMetadata.SetupMetadata()
	if err != nil {
		log.Errorf("failed to setup metadata tables: %v", err)
		return err
	}

	return nil
}

func (c *S3Connector) GetLastSyncBatchID(jobName string) (int64, error) {
	syncBatchID, err := c.pgMetadata.GetLastBatchID(jobName)
	if err != nil {
		return 0, err
	}

	return syncBatchID, nil
}

func (c *S3Connector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	res, err := c.pgMetadata.FetchLastOffset(jobName)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// update offset for a job
func (c *S3Connector) updateLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		log.Errorf("failed to update last offset: %v", err)
		return err
	}

	return nil
}

func (c *S3Connector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID = syncBatchID + 1

	tableNameRowsMapping := make(map[string]uint32)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	streamRes, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}
	recordStream := streamRes.Stream
	qrepConfig := &protos.QRepConfig{
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: fmt.Sprintf("raw_table_%s", req.FlowJobName),
	}
	partition := &protos.QRepPartition{
		PartitionId: fmt.Sprint(syncBatchID),
	}
	numRecords, err := c.SyncQRepRecords(qrepConfig, partition, recordStream)
	if err != nil {
		return nil, err
	}
	log.Infof("Synced %d records", numRecords)

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = c.updateLastOffset(req.FlowJobName, lastCheckpoint)
	if err != nil {
		log.Errorf("failed to update last offset for s3 cdc: %v", err)
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		log.Errorf("%v", err)
		return nil, err
	}

	return &model.SyncResponse{
		FirstSyncedCheckPointID: req.Records.GetFirstCheckpoint(),
		LastSyncedCheckPointID:  lastCheckpoint,
		NumRecordsSynced:        int64(numRecords),
		TableNameRowsMapping:    tableNameRowsMapping,
	}, nil
}

func (c *S3Connector) SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput,
	error) {
	log.Infof("SetupNormalizedTables for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) SyncFlowCleanup(jobName string) error {
	err := c.pgMetadata.DropMetadata(jobName)
	if err != nil {
		return err
	}
	return nil
}
