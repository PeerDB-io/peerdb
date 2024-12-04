package conns3

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

type S3Connector struct {
	*metadataStore.PostgresMetadata
	logger              log.Logger
	credentialsProvider utils.AWSCredentialsProvider
	client              s3.Client
	url                 string
}

func NewS3Connector(
	ctx context.Context,
	config *protos.S3Config,
) (*S3Connector, error) {
	logger := shared.LoggerFromCtx(ctx)

	provider, err := utils.GetAWSCredentialsProvider(ctx, "s3", utils.PeerAWSCredentials{
		Credentials: aws.Credentials{
			AccessKeyID:     config.GetAccessKeyId(),
			SecretAccessKey: config.GetSecretAccessKey(),
		},
		RoleArn:     config.RoleArn,
		EndpointUrl: config.Endpoint,
		Region:      config.GetRegion(),
	})
	if err != nil {
		return nil, err
	}

	s3Client, err := utils.CreateS3Client(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}
	return &S3Connector{
		url:                 config.Url,
		PostgresMetadata:    pgMetadata,
		client:              *s3Client,
		credentialsProvider: provider,
		logger:              logger,
	}, nil
}

func (c *S3Connector) CreateRawTable(_ context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	c.logger.Info("CreateRawTable for S3 is a no-op")
	return nil, nil
}

func (c *S3Connector) Close() error {
	return nil
}

func (c *S3Connector) ValidateCheck(ctx context.Context) error {
	bucketPrefix, parseErr := utils.NewS3BucketAndPrefix(c.url)
	if parseErr != nil {
		return fmt.Errorf("failed to parse bucket url: %w", parseErr)
	}

	return utils.PutAndRemoveS3(ctx, &c.client, bucketPrefix.Bucket, bucketPrefix.Prefix)
}

func (c *S3Connector) ConnectionActive(ctx context.Context) error {
	return nil
}

func (c *S3Connector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, req.SyncBatchID)
	recordStream, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}
	qrepConfig := &protos.QRepConfig{
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: "raw_table_" + req.FlowJobName,
	}
	partition := &protos.QRepPartition{
		PartitionId: strconv.FormatInt(req.SyncBatchID, 10),
	}
	numRecords, err := c.SyncQRepRecords(ctx, qrepConfig, partition, recordStream)
	if err != nil {
		return nil, err
	}
	c.logger.Info(fmt.Sprintf("Synced %d records", numRecords))

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err = c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to increment id", "error", err)
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *S3Connector) ReplayTableSchemaDeltas(_ context.Context, _ map[string]string,
	flowJobName string, schemaDeltas []*protos.TableSchemaDelta,
) error {
	c.logger.Info("ReplayTableSchemaDeltas for S3 is a no-op")
	return nil
}
