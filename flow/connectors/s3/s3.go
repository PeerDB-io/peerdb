package conns3

import (
	"context"
	"database/sql"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

type S3Connector struct {
	url string
}

func NewS3Connector(ctx context.Context,
	S3ProtoConfig *protos.S3Config) (*S3Connector, error) {
	return &S3Connector{
		url: S3ProtoConfig.Url,
	}, nil
}

func (c *S3Connector) Close() error {
	panic("Not implemented")
}

func (c *S3Connector) ConnectionActive() bool {
	panic("Not implemented")
}

func (c *S3Connector) NeedsSetupMetadataTables() bool {
	panic("Not implemented")
}

func (c *S3Connector) SetupMetadataTables() error {
	panic("Not implemented")
}

func (c *S3Connector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	panic("Not implemented")
}

func (c *S3Connector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) ([]string, error) {
	panic("Not implemented")
}

func (c *S3Connector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) (map[string][]string, error) {
	panic("Not implemented")
}

func (c *S3Connector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	log.Errorf("panicking at call to GetTableSchema for S3 flow connector")
	panic("GetTableSchema is not implemented for the S3 flow connector")
}

func (c *S3Connector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error) {
	panic("Not implemented")
}

func (c *S3Connector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	panic("Not implemented")
}

func (c *S3Connector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	log.Errorf("panicking at call to PullRecords for S3 flow connector")
	panic("PullRecords is not implemented for the S3 flow connector")
}

func (c *S3Connector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	panic("Not implemented")
}

// NormalizeRecords normalizes raw table to destination table.
func (c *S3Connector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	panic("Not implemented")
}

func (c *S3Connector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	panic("Not implemented")
}

// EnsurePullability ensures that the table is pullable, implementing the Connector interface.
func (c *S3Connector) EnsurePullability(req *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	log.Errorf("panicking at call to EnsurePullability for S3 flow connector")
	panic("EnsurePullability is not implemented for the S3 flow connector")
}

// SetupReplication sets up replication for the source connector.
func (c *S3Connector) SetupReplication(req *protos.SetupReplicationInput) error {
	log.Errorf("panicking at call to SetupReplication for S3 flow connector")
	panic("SetupReplication is not implemented for the S3 flow connector")
}

func (c *S3Connector) PullFlowCleanup(jobName string) error {
	log.Errorf("panicking at call to PullFlowCleanup for S3 flow connector")
	panic("PullFlowCleanup is not implemented for the S3 flow connector")
}

func (c *S3Connector) SyncFlowCleanup(jobName string) error {
	panic("Not implemented")
}

func (c *S3Connector) checkIfTableExists(schemaIdentifier string, tableIdentifier string) (bool, error) {
	panic("Not implemented")
}

func generateCreateTableSQLForNormalizedTable(sourceTableIdentifier string, sourceTableSchema *protos.TableSchema) string {
	panic("Not implemented")
}

func generateMultiValueInsertSQL(tableIdentifier string, chunkSize int) string {
	panic("Not implemented")
}

func getRawTableIdentifier(jobName string) string {
	panic("Not implemented")
}

func (c *S3Connector) insertRecordsInRawTable() error {
	panic("Not implemented")
}

func (c *S3Connector) generateAndExecuteMergeStatement(destinationTableIdentifier string,
	unchangedToastColumns []string,
	rawTableIdentifier string, syncBatchID int64, normalizeBatchID int64,
	normalizeRecordsTx *sql.Tx) error {
	panic("Not implemented")
}

func (c *S3Connector) jobMetadataExists(jobName string) (bool, error) {
	panic("Not implemented")
}

func (c *S3Connector) updateSyncMetadata(flowJobName string, lastCP int64, syncBatchID int64, syncRecordsTx *sql.Tx) error {
	panic("Not implemented")
}

func (c *S3Connector) updateNormalizeMetadata(flowJobName string, normalizeBatchID int64, normalizeRecordsTx *sql.Tx) error {
	panic("Not implemented")
}

func (c *S3Connector) createPeerDBInternalSchema(createsSchemaTx *sql.Tx) error {
	panic("Not implemented")
}

func (c *S3Connector) generateUpdateStatement(allCols []string, unchangedToastCols []string) []string {
	panic("Not implemented")
}
