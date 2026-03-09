package conns3

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

type S3Connector struct {
	*metadataStore.PostgresMetadata
	logger              log.Logger
	credentialsProvider utils.AWSCredentialsProvider
	client              s3.Client
	url                 string
	codec               protos.AvroCodec
}

func NewS3Connector(
	ctx context.Context,
	config *protos.S3Config,
) (*S3Connector, error) {
	logger := internal.LoggerFromCtx(ctx)

	provider, err := utils.GetAWSCredentialsProvider(ctx, "s3", utils.NewPeerAWSCredentials(config))
	if err != nil {
		return nil, err
	}

	s3Client, err := utils.CreateS3Client(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", slog.Any("error", err))
		return nil, err
	}
	return &S3Connector{
		PostgresMetadata:    pgMetadata,
		client:              *s3Client,
		credentialsProvider: provider,
		logger:              logger,
		url:                 config.Url,
		codec:               config.Codec,
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
	streamReq := model.NewRecordsToStreamRequest(
		req.Records.GetRecords(), tableNameRowsMapping, req.SyncBatchID, false, protos.DBType_S3,
	)
	recordStream, err := utils.RecordsToRawTableStream(streamReq, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}
	qrepConfig := &protos.QRepConfig{
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: "raw_table_" + req.FlowJobName,
		Env:                        req.Env,
		Version:                    req.Version,
	}
	partition := &protos.QRepPartition{
		PartitionId: strconv.FormatInt(req.SyncBatchID, 10),
	}
	numRecords, _, err := c.SyncQRepRecords(ctx, qrepConfig, partition, recordStream)
	if err != nil {
		return nil, err
	}
	c.logger.Info(fmt.Sprintf("Synced %d records", numRecords))

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: lastCheckpoint,
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   req.SyncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}

func (c *S3Connector) ReplayTableSchemaDeltas(_ context.Context, _ map[string]string,
	flowJobName string, _ []*protos.TableMapping, schemaDeltas []*protos.TableSchemaDelta, _ []string,
) error {
	return nil
}
