package conns3

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	_peerDBCheck = "peerdb_check"
)

type S3Connector struct {
	ctx        context.Context
	url        string
	pgMetadata *metadataStore.PostgresMetadataStore
	client     s3.Client
	creds      utils.S3PeerCredentials
	logger     slog.Logger
}

func NewS3Connector(ctx context.Context,
	config *protos.S3Config,
) (*S3Connector, error) {
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
		slog.ErrorContext(ctx, "failed to create postgres metadata store", slog.Any("error", err))
		return nil, err
	}
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &S3Connector{
		ctx:        ctx,
		url:        config.Url,
		pgMetadata: pgMetadata,
		client:     *s3Client,
		creds:      s3PeerCreds,
		logger:     *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

func (c *S3Connector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	c.logger.Info("CreateRawTable for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) Close() error {
	c.logger.Debug("Closing metadata store connection")
	return c.pgMetadata.Close()
}

func ValidCheck(ctx context.Context, s3Client *s3.Client, bucketURL string, metadataDB *metadataStore.PostgresMetadataStore) error {
	_, listErr := s3Client.ListBuckets(ctx, nil)
	if listErr != nil {
		return fmt.Errorf("failed to list buckets: %w", listErr)
	}

	reader := strings.NewReader(time.Now().Format(time.RFC3339))

	bucketPrefix, parseErr := utils.NewS3BucketAndPrefix(bucketURL)
	if parseErr != nil {
		return fmt.Errorf("failed to parse bucket url: %w", parseErr)
	}

	// Write an empty file and then delete it
	// to check if we have write permissions
	bucketName := aws.String(bucketPrefix.Bucket)
	_, putErr := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: bucketName,
		Key:    aws.String(_peerDBCheck),
		Body:   reader,
	})
	if putErr != nil {
		return fmt.Errorf("failed to write to bucket: %w", putErr)
	}

	_, delErr := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: bucketName,
		Key:    aws.String(_peerDBCheck),
	})
	if delErr != nil {
		return fmt.Errorf("failed to delete from bucket: %w", delErr)
	}

	// check if we can ping external metadata
	err := metadataDB.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping external metadata: %w", err)
	}

	return nil
}

func (c *S3Connector) ConnectionActive() error {
	_, listErr := c.client.ListBuckets(c.ctx, nil)
	if listErr != nil {
		return listErr
	}

	validErr := ValidCheck(c.ctx, &c.client, c.url, c.pgMetadata)
	if validErr != nil {
		c.logger.Error("failed to validate s3 connector:", slog.Any("error", validErr))
		return validErr
	}

	return nil
}

func (c *S3Connector) NeedsSetupMetadataTables() bool {
	return c.pgMetadata.NeedsSetupMetadata()
}

func (c *S3Connector) SetupMetadataTables() error {
	err := c.pgMetadata.SetupMetadata()
	if err != nil {
		c.logger.Error("failed to setup metadata tables", slog.Any("error", err))
		return err
	}

	return nil
}

func (c *S3Connector) GetLastSyncBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(jobName)
}

func (c *S3Connector) GetLastOffset(jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(jobName)
}

// update offset for a job
func (c *S3Connector) SetLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		c.logger.Error("failed to update last offset: ", slog.Any("error", err))
		return err
	}

	return nil
}

func (c *S3Connector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous syncBatchID: %w", err)
	}
	syncBatchID += 1

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
		PartitionId: strconv.FormatInt(syncBatchID, 10),
	}
	numRecords, err := c.SyncQRepRecords(qrepConfig, partition, recordStream)
	if err != nil {
		return nil, err
	}
	c.logger.Info(fmt.Sprintf("Synced %d records", numRecords))

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = c.SetLastOffset(req.FlowJobName, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to update last offset for s3 cdc", slog.Any("error", err))
		return nil, err
	}
	err = c.pgMetadata.IncrementID(req.FlowJobName)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.WaitForSchemaDeltas(req.TableMappings),
	}, nil
}

func (c *S3Connector) ReplayTableSchemaDeltas(flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	c.logger.Info("ReplayTableSchemaDeltas for S3 is a no-op")
	return nil
}

func (c *S3Connector) SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput,
	error,
) {
	c.logger.Info("SetupNormalizedTables for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) SyncFlowCleanup(jobName string) error {
	return c.pgMetadata.DropMetadata(jobName)
}
