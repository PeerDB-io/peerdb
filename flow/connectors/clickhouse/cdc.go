package connclickhouse

import (
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

const (
	checkIfTableExistsSQL     = `SELECT exists(SELECT 1 FROM system.tables WHERE database = ? AND name = ?) AS table_exists;`
	mirrorJobsTableIdentifier = "PEERDB_MIRROR_JOBS"
)

// getRawTableName returns the raw table name for the given table identifier.
func (c *ClickhouseConnector) getRawTableName(flowJobName string) string {
	// replace all non-alphanumeric characters with _
	flowJobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(flowJobName, "_")
	return fmt.Sprintf("_peerdb_raw_%s", flowJobName)
}

func (c *ClickhouseConnector) checkIfTableExists(databaseName string, tableIdentifier string) (bool, error) {
	var result sql.NullInt32
	err := c.database.QueryRowContext(c.ctx, checkIfTableExistsSQL, databaseName, tableIdentifier).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while reading result row: %w", err)
	}

	if !result.Valid {
		return false, fmt.Errorf("[clickhouse] checkIfTableExists: result is not valid")
	}

	return result.Int32 == 1, nil
}

type MirrorJobRow struct {
	MirrorJobName    string
	Offset           int
	SyncBatchID      int
	NormalizeBatchID int
}

func (c *ClickhouseConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	createRawTableSQL := `CREATE TABLE IF NOT EXISTS %s (
		_peerdb_uid String NOT NULL,
		_peerdb_timestamp Int64 NOT NULL,
		_peerdb_destination_table_name String NOT NULL,
		_peerdb_data String NOT NULL,
		_peerdb_record_type Int NOT NULL,
		_peerdb_match_data String,
		_peerdb_batch_id Int,
		_peerdb_unchanged_toast_columns String
	) ENGINE = ReplacingMergeTree ORDER BY _peerdb_uid;`

	_, err := c.database.ExecContext(c.ctx,
		fmt.Sprintf(createRawTableSQL, rawTableName))
	if err != nil {
		return nil, fmt.Errorf("unable to create raw table: %w", err)
	}
	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *ClickhouseConnector) syncRecordsViaAvro(
	req *model.SyncRecordsRequest,
	rawTableIdentifier string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := make(map[string]uint32)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	streamRes, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	qrepConfig := &protos.QRepConfig{
		StagingPath:                c.config.S3Integration,
		FlowJobName:                req.FlowJobName,
		DestinationTableIdentifier: strings.ToLower(rawTableIdentifier),
	}
	avroSyncer := NewClickhouseAvroSyncMethod(qrepConfig, c)
	destinationTableSchema, err := c.getTableSchema(qrepConfig.DestinationTableIdentifier)
	if err != nil {
		return nil, err
	}

	numRecords, err := avroSyncer.SyncRecords(destinationTableSchema, streamRes.Stream, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	err = c.ReplayTableSchemaDeltas(req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *ClickhouseConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Clickhouse table %s", rawTableName))

	res, err := c.syncRecordsViaAvro(req, rawTableName, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	lastCheckpoint, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %w", err)
	}

	err = c.pgMetadata.FinishBatch(req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return res, nil
}

func (c *ClickhouseConnector) SyncFlowCleanup(jobName string) error {
	err := c.pgMetadata.DropMetadata(jobName)
	if err != nil {
		return err
	}
	return nil
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *ClickhouseConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	return nil
}

func (c *ClickhouseConnector) NeedsSetupMetadataTables() bool {
	return false
}

func (c *ClickhouseConnector) SetupMetadataTables() error {
	return nil
}

func (c *ClickhouseConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(jobName)
}

func (c *ClickhouseConnector) GetLastOffset(jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(jobName)
}

// update offset for a job
func (c *ClickhouseConnector) SetLastOffset(jobName string, offset int64) error {
	err := c.pgMetadata.UpdateLastOffset(jobName, offset)
	if err != nil {
		c.logger.Error("failed to update last offset: ", slog.Any("error", err))
		return err
	}

	return nil
}
