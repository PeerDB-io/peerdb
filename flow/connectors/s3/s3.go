package conns3

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

type S3Connector struct {
	ctx    context.Context
	url    string
	client s3.S3
}

func NewS3Connector(ctx context.Context,
	s3ProtoConfig *protos.S3Config) (*S3Connector, error) {
	s3Client, err := utils.CreateS3Client()
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	return &S3Connector{
		ctx:    ctx,
		url:    s3ProtoConfig.Url,
		client: *s3Client,
	}, nil
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
	log.Errorf("NeedsSetupMetadataTables not supported for S3")
	return false
}

func (c *S3Connector) SetupMetadataTables() error {
	log.Errorf("SetupMetadataTables not supported for S3")
	return fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	log.Errorf("GetLastOffset not supported for S3")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) GetLastSyncBatchID(jobName string) (int64, error) {
	log.Errorf("GetLastSyncBatchID not supported for S3")
	return 0, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) GetLastNormalizeBatchID() (int64, error) {
	log.Errorf("GetLastNormalizeBatchID not supported for S3")
	return 0, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) GetTableSchema(
	req *protos.GetTableSchemaBatchInput) (*protos.GetTableSchemaBatchOutput, error) {
	log.Errorf("GetTableSchema not supported for S3 flow connector")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
	*protos.SetupNormalizedTableBatchOutput, error) {
	log.Errorf("SetupNormalizedTable not supported for S3")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	log.Errorf("InitializeTableSchema not supported for S3")
	return fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) ReplayTableSchemaDelta(flowJobName string, schemaDelta *model.TableSchemaDelta) error {
	log.Errorf("panicking at call to ReplayTableSchemaDelta for S3 flow connector")
	panic("ReplayTableSchemaDelta is not implemented for the S3 flow connector")
}

func (c *S3Connector) PullRecords(req *model.PullRecordsRequest) (*model.RecordsWithTableSchemaDelta, error) {
	log.Errorf("panicking at call to PullRecords for S3 flow connector")
	panic("PullRecords is not implemented for the S3 flow connector")
}

func (c *S3Connector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	log.Errorf("SyncRecords not supported for S3")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	log.Errorf("NormalizeRecords not supported for S3")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	log.Errorf("CreateRawTable not supported for S3")
	return nil, fmt.Errorf("cdc based replication is not currently supported for S3 target")
}

func (c *S3Connector) EnsurePullability(req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	log.Errorf("panicking at call to EnsurePullability for S3 flow connector")
	panic("EnsurePullability is not implemented for the S3 flow connector")
}

func (c *S3Connector) PullFlowCleanup(jobName string) error {
	log.Errorf("panicking at call to PullFlowCleanup for S3 flow connector")
	panic("PullFlowCleanup is not implemented for the S3 flow connector")
}

func (c *S3Connector) SyncFlowCleanup(jobName string) error {
	log.Errorf("SyncFlowCleanup not supported for S3")
	return fmt.Errorf("cdc based replication is not currently supported for S3 target")
}
