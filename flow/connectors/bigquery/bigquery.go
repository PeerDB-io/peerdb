package connbigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	cc "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.temporal.io/sdk/activity"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	/*
		Different batch Ids in code/BigQuery
		1. batchID - identifier in raw table on target to depict which batch a row was inserted.
		3. syncBatchID - batch id that was last synced or will be synced
		4. normalizeBatchID - batch id that was last normalized or will be normalized.
	*/
	// MirrorJobsTable has the following schema:
	// CREATE TABLE peerdb_mirror_jobs (
	//   mirror_job_id STRING NOT NULL,
	//   offset INTEGER NOT NULL,
	//   sync_batch_id INTEGER NOT NULL
	//   normalize_batch_id INTEGER
	// )
	MirrorJobsTable      = "peerdb_mirror_jobs"
	SyncRecordsBatchSize = 1024
)

type BigQueryServiceAccount struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
}

// BigQueryConnector is a Connector implementation for BigQuery.
type BigQueryConnector struct {
	ctx           context.Context
	bqConfig      *protos.BigqueryConfig
	client        *bigquery.Client
	storageClient *storage.Client
	datasetID     string
	catalogPool   *pgxpool.Pool
	logger        slog.Logger
}

// Create BigQueryServiceAccount from BigqueryConfig
func NewBigQueryServiceAccount(bqConfig *protos.BigqueryConfig) (*BigQueryServiceAccount, error) {
	var serviceAccount BigQueryServiceAccount
	serviceAccount.Type = bqConfig.AuthType
	serviceAccount.ProjectID = bqConfig.ProjectId
	serviceAccount.PrivateKeyID = bqConfig.PrivateKeyId
	serviceAccount.PrivateKey = bqConfig.PrivateKey
	serviceAccount.ClientEmail = bqConfig.ClientEmail
	serviceAccount.ClientID = bqConfig.ClientId
	serviceAccount.AuthURI = bqConfig.AuthUri
	serviceAccount.TokenURI = bqConfig.TokenUri
	serviceAccount.AuthProviderX509CertURL = bqConfig.AuthProviderX509CertUrl
	serviceAccount.ClientX509CertURL = bqConfig.ClientX509CertUrl

	if err := serviceAccount.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate BigQueryServiceAccount: %w", err)
	}

	return &serviceAccount, nil
}

// Validate validates a BigQueryServiceAccount, that none of the fields are empty.
func (bqsa *BigQueryServiceAccount) Validate() error {
	v := reflect.ValueOf(*bqsa)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).String() == "" {
			return fmt.Errorf("field %s is empty", v.Type().Field(i).Name)
		}
	}
	return nil
}

// Return BigQueryServiceAccount as JSON byte array
func (bqsa *BigQueryServiceAccount) ToJSON() ([]byte, error) {
	return json.Marshal(bqsa)
}

// CreateBigQueryClient creates a new BigQuery client from a BigQueryServiceAccount.
func (bqsa *BigQueryServiceAccount) CreateBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	bqsaJSON, err := bqsa.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := bigquery.NewClient(
		ctx,
		bqsa.ProjectID,
		option.WithCredentialsJSON(bqsaJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}

// CreateStorageClient creates a new Storage client from a BigQueryServiceAccount.
func (bqsa *BigQueryServiceAccount) CreateStorageClient(ctx context.Context) (*storage.Client, error) {
	bqsaJSON, err := bqsa.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON(bqsaJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	return client, nil
}

// NewBigQueryConnector creates a new BigQueryConnector from a PeerConnectionConfig.
func NewBigQueryConnector(ctx context.Context, config *protos.BigqueryConfig) (*BigQueryConnector, error) {
	bqsa, err := NewBigQueryServiceAccount(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %v", err)
	}

	client, err := bqsa.CreateBigQueryClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	datasetID := config.GetDatasetId()
	_, checkErr := client.Dataset(datasetID).Metadata(ctx)
	if checkErr != nil {
		slog.ErrorContext(ctx, "failed to get dataset metadata", slog.Any("error", checkErr))
		return nil, fmt.Errorf("failed to get dataset metadata: %v", checkErr)
	}

	storageClient, err := bqsa.CreateStorageClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	catalogPool, err := cc.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog connection pool: %v", err)
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)

	return &BigQueryConnector{
		ctx:           ctx,
		bqConfig:      config,
		client:        client,
		datasetID:     datasetID,
		storageClient: storageClient,
		catalogPool:   catalogPool,
		logger:        *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}, nil
}

// Close closes the BigQuery driver.
func (c *BigQueryConnector) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// ConnectionActive returns true if the connection is active.
func (c *BigQueryConnector) ConnectionActive() error {
	_, err := c.client.Dataset(c.datasetID).Metadata(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get dataset metadata: %v", err)
	}

	if c.client == nil {
		return fmt.Errorf("BigQuery client is nil")
	}
	return nil
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *BigQueryConnector) NeedsSetupMetadataTables() bool {
	_, err := c.client.Dataset(c.datasetID).Table(MirrorJobsTable).Metadata(c.ctx)
	return err != nil
}

func (c *BigQueryConnector) waitForTableReady(datasetTable *datasetTable) error {
	table := c.client.Dataset(datasetTable.dataset).Table(datasetTable.table)
	maxDuration := 5 * time.Minute
	deadline := time.Now().Add(maxDuration)
	sleepInterval := 5 * time.Second
	attempt := 0

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout reached while waiting for table %s to be ready", datasetTable)
		}

		_, err := table.Metadata(c.ctx)
		if err == nil {
			return nil
		}

		slog.Info("waiting for table to be ready",
			slog.String("table", datasetTable.table), slog.Int("attempt", attempt))
		attempt++
		time.Sleep(sleepInterval)
	}
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *BigQueryConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			dstDatasetTable, _ := c.convertToDatasetTable(schemaDelta.DstTableName)
			_, err := c.client.Query(fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS `%s` %s", dstDatasetTable.dataset,
				dstDatasetTable.table, addedColumn.ColumnName,
				qValueKindToBigQueryType(addedColumn.ColumnType))).Read(c.ctx)
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.ColumnName,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s to table %s",
				addedColumn.ColumnName, addedColumn.ColumnType, schemaDelta.DstTableName))
		}
	}

	return nil
}

// SetupMetadataTables sets up the metadata tables.
func (c *BigQueryConnector) SetupMetadataTables() error {
	// check if the dataset exists
	dataset := c.client.Dataset(c.datasetID)
	if _, err := dataset.Metadata(c.ctx); err != nil {
		// create the dataset as it doesn't exist
		if err := dataset.Create(c.ctx, nil); err != nil {
			return fmt.Errorf("failed to create dataset %s: %w", c.datasetID, err)
		}
	}

	// Create the mirror jobs table, NeedsSetupMetadataTables ensures it doesn't exist.
	mirrorJobsTable := dataset.Table(MirrorJobsTable)
	mirrorJobsTableMetadata := &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "mirror_job_name", Type: bigquery.StringFieldType},
			{Name: "offset", Type: bigquery.IntegerFieldType},
			{Name: "sync_batch_id", Type: bigquery.IntegerFieldType},
			{Name: "normalize_batch_id", Type: bigquery.IntegerFieldType},
		},
	}
	if err := mirrorJobsTable.Create(c.ctx, mirrorJobsTableMetadata); err != nil {
		// if the table already exists, ignore the error
		if !strings.Contains(err.Error(), "Already Exists") {
			return fmt.Errorf("failed to create table %s: %w", MirrorJobsTable, err)
		} else {
			c.logger.Info(fmt.Sprintf("table %s already exists", MirrorJobsTable))
		}
	}

	return nil
}

func (c *BigQueryConnector) GetLastOffset(jobName string) (int64, error) {
	query := fmt.Sprintf("SELECT offset FROM %s.%s WHERE mirror_job_name = '%s'", c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return 0, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		c.logger.Info("no row found, returning nil")
		return 0, nil
	}

	if row[0] == nil {
		c.logger.Info("no offset found, returning nil")
		return 0, nil
	} else {
		return row[0].(int64), nil
	}
}

func (c *BigQueryConnector) SetLastOffset(jobName string, lastOffset int64) error {
	query := fmt.Sprintf(
		"UPDATE %s.%s SET offset = GREATEST(offset, %d) WHERE mirror_job_name = '%s'",
		c.datasetID,
		MirrorJobsTable,
		lastOffset,
		jobName,
	)
	q := c.client.Query(query)
	_, err := q.Read(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
	}

	return nil
}

func (c *BigQueryConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	query := fmt.Sprintf("SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name = '%s'",
		c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return -1, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		c.logger.Info("no row found")
		return 0, nil
	}

	if row[0] == nil {
		c.logger.Info("no sync_batch_id found, returning 0")
		return 0, nil
	} else {
		return row[0].(int64), nil
	}
}

func (c *BigQueryConnector) GetLastSyncAndNormalizeBatchID(jobName string) (model.SyncAndNormalizeBatchID, error) {
	query := fmt.Sprintf("SELECT sync_batch_id, normalize_batch_id FROM %s.%s WHERE mirror_job_name = '%s'",
		c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return model.SyncAndNormalizeBatchID{}, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		c.logger.Info("no row found for job")
		return model.SyncAndNormalizeBatchID{}, nil
	}

	syncBatchID := int64(0)
	normBatchID := int64(0)
	if row[0] != nil {
		syncBatchID = row[0].(int64)
	}
	if row[1] != nil {
		normBatchID = row[1].(int64)
	}
	return model.SyncAndNormalizeBatchID{
		SyncBatchID:      syncBatchID,
		NormalizeBatchID: normBatchID,
	}, nil
}

func (c *BigQueryConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	query := fmt.Sprintf(`SELECT DISTINCT _peerdb_destination_table_name FROM %s.%s
	 WHERE _peerdb_batch_id > %d and _peerdb_batch_id <= %d`,
		c.datasetID, rawTableName, normalizeBatchID, syncBatchID)
	// Run the query
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return nil, err
	}

	// Store the distinct values in an array
	var distinctTableNames []string
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) > 0 {
			value := row[0].(string)
			distinctTableNames = append(distinctTableNames, value)
		}
	}

	return distinctTableNames, nil
}

func (c *BigQueryConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) (map[string][]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	// we want to only select the unchanged cols from UpdateRecords, as we have a workaround
	// where a placeholder value for unchanged cols can be set in DeleteRecord if there is no backfill
	// we don't want these particular DeleteRecords to be used in the update statement
	query := fmt.Sprintf(`SELECT _peerdb_destination_table_name,
	array_agg(DISTINCT _peerdb_unchanged_toast_columns) as unchanged_toast_columns FROM %s.%s
	 WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d AND _peerdb_record_type != 2
	 GROUP BY _peerdb_destination_table_name`,
		c.datasetID, rawTableName, normalizeBatchID, syncBatchID)
	// Run the query
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return nil, err
	}
	// Create a map to store the results.
	resultMap := make(map[string][]string)

	// Process the query results using an iterator.
	var row struct {
		Tablename             string   `bigquery:"_peerdb_destination_table_name"`
		UnchangedToastColumns []string `bigquery:"unchanged_toast_columns"`
	}
	for {
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		resultMap[row.Tablename] = row.UnchangedToastColumns
	}
	return resultMap, nil
}

// SyncRecords pushes records to the destination.
// Currently only supports inserts, updates, and deletes.
// More record types will be added in the future.
func (c *BigQueryConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	c.logger.Info(fmt.Sprintf("pushing records to %s.%s...", c.datasetID, rawTableName))

	// generate a sequential number for last synced batch this sequence will be
	// used to keep track of records that are normalized in NormalizeFlowWorkflow
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}
	syncBatchID += 1

	res, err := c.syncRecordsViaAvro(req, rawTableName, syncBatchID)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *BigQueryConnector) syncRecordsViaAvro(
	req *model.SyncRecordsRequest,
	rawTableName string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := make(map[string]uint32)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	streamRes, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	avroSync := NewQRepAvroSyncMethod(c, req.StagingPath, req.FlowJobName)
	rawTableMetadata, err := c.client.Dataset(c.datasetID).Table(rawTableName).Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of destination table: %v", err)
	}

	numRecords, err := avroSync.SyncRecords(rawTableName, req.FlowJobName,
		req.Records, rawTableMetadata, syncBatchID, streamRes.Stream)
	if err != nil {
		return nil, fmt.Errorf("failed to sync records via avro : %v", err)
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s.%s", numRecords, c.datasetID, rawTableName))

	lastCP, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %v", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckPointID: lastCP,
		NumRecordsSynced:       int64(numRecords),
		CurrentSyncBatchID:     syncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
	}, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *BigQueryConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	batchIDs, err := c.GetLastSyncAndNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	hasJob, err := c.metadataHasJob(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if job exists: %w", err)
	}
	// if job is not yet found in the peerdb_mirror_jobs_table
	// OR sync is lagging end normalize
	if !hasJob || batchIDs.NormalizeBatchID >= batchIDs.SyncBatchID {
		c.logger.Info("waiting for sync to catch up, so finishing")
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: batchIDs.NormalizeBatchID,
			EndBatchID:   batchIDs.SyncBatchID,
		}, nil
	}
	distinctTableNames, err := c.getDistinctTableNamesInBatch(
		req.FlowJobName,
		batchIDs.SyncBatchID,
		batchIDs.NormalizeBatchID,
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get distinct table names to normalize: %w", err)
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(
		req.FlowJobName,
		batchIDs.SyncBatchID,
		batchIDs.NormalizeBatchID,
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tablename to unchanged cols mapping: %w", err)
	}

	// append all the statements to one list
	c.logger.Info(fmt.Sprintf("merge raw records to corresponding tables: %s %s %v",
		c.datasetID, rawTableName, distinctTableNames))

	for _, tableName := range distinctTableNames {
		unchangedToastColumns := tableNametoUnchangedToastCols[tableName]
		dstDatasetTable, _ := c.convertToDatasetTable(tableName)
		mergeGen := &mergeStmtGenerator{
			rawDatasetTable: &datasetTable{
				dataset: c.datasetID,
				table:   rawTableName,
			},
			dstTableName:          tableName,
			dstDatasetTable:       dstDatasetTable,
			normalizedTableSchema: req.TableNameSchemaMapping[tableName],
			syncBatchID:           batchIDs.SyncBatchID,
			normalizeBatchID:      batchIDs.NormalizeBatchID,
			peerdbCols: &protos.PeerDBColumns{
				SoftDeleteColName: req.SoftDeleteColName,
				SyncedAtColName:   req.SyncedAtColName,
				SoftDelete:        req.SoftDelete,
			},
			shortColumn: map[string]string{},
		}
		// normalize anything between last normalized batch id to last sync batchid
		mergeStmts := mergeGen.generateMergeStmts(unchangedToastColumns)
		for i, mergeStmt := range mergeStmts {
			c.logger.Info(fmt.Sprintf("running merge statement [%d/%d] for table %s..",
				i+1, len(mergeStmts), tableName))
			q := c.client.Query(mergeStmt)
			_, err = q.Read(c.ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to execute merge statement %s: %v", mergeStmt, err)
			}
		}
	}
	// update metadata to make the last normalized batch id to the recent last sync batch id.
	updateMetadataStmt := fmt.Sprintf(
		"UPDATE %s.%s SET normalize_batch_id=%d WHERE mirror_job_name='%s';",
		c.datasetID, MirrorJobsTable, batchIDs.SyncBatchID, req.FlowJobName)

	_, err = c.client.Query(updateMetadataStmt).Read(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update metadata statements %s: %v", updateMetadataStmt, err)
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: batchIDs.NormalizeBatchID + 1,
		EndBatchID:   batchIDs.SyncBatchID,
	}, nil
}

// CreateRawTable creates a raw table, implementing the Connector interface.
// create a table with the following schema
// _peerdb_uid STRING
// _peerdb_timestamp TIMESTAMP
// _peerdb_data STRING
// _peerdb_record_type INT - 0 for insert, 1 for update, 2 for delete
// _peerdb_match_data STRING - json of the match data (only for update and delete)
func (c *BigQueryConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	schema := bigquery.Schema{
		{Name: "_peerdb_uid", Type: bigquery.StringFieldType},
		{Name: "_peerdb_timestamp", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_destination_table_name", Type: bigquery.StringFieldType},
		{Name: "_peerdb_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_record_type", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_match_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_batch_id", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_unchanged_toast_columns", Type: bigquery.StringFieldType},
	}

	// create the table
	table := c.client.Dataset(c.datasetID).Table(rawTableName)

	// check if the table exists
	tableRef, err := table.Metadata(c.ctx)
	if err == nil {
		// table exists, check if the schema matches
		if !reflect.DeepEqual(tableRef.Schema, schema) {
			return nil, fmt.Errorf("table %s.%s already exists with different schema", c.datasetID, rawTableName)
		} else {
			return &protos.CreateRawTableOutput{
				TableIdentifier: rawTableName,
			}, nil
		}
	}

	partitioning := &bigquery.RangePartitioning{
		Field: "_peerdb_batch_id",
		Range: &bigquery.RangePartitioningRange{
			Start:    0,
			End:      1_000_000,
			Interval: 100,
		},
	}

	clustering := &bigquery.Clustering{
		Fields: []string{
			"_peerdb_batch_id",
			"_peerdb_destination_table_name",
			"_peerdb_timestamp",
		},
	}

	metadata := &bigquery.TableMetadata{
		Schema:            schema,
		RangePartitioning: partitioning,
		Clustering:        clustering,
		Name:              rawTableName,
	}

	// table does not exist, create it
	err = table.Create(c.ctx, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, rawTableName, err)
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

// getUpdateMetadataStmt updates the metadata tables for a given job.
func (c *BigQueryConnector) getUpdateMetadataStmt(jobName string, lastSyncedCheckpointID int64,
	batchID int64,
) (string, error) {
	hasJob, err := c.metadataHasJob(jobName)
	if err != nil {
		return "", fmt.Errorf("failed to check if job exists: %w", err)
	}

	// create the job in the metadata table
	jobStatement := fmt.Sprintf(
		"INSERT INTO %s.%s (mirror_job_name,offset,sync_batch_id) VALUES ('%s',%d,%d);",
		c.datasetID, MirrorJobsTable, jobName, lastSyncedCheckpointID, batchID)
	if hasJob {
		jobStatement = fmt.Sprintf(
			"UPDATE %s.%s SET offset=GREATEST(offset,%d),sync_batch_id=%d WHERE mirror_job_name = '%s';",
			c.datasetID, MirrorJobsTable, lastSyncedCheckpointID, batchID, jobName)
	}

	return jobStatement, nil
}

// metadataHasJob checks if the metadata table has the given job.
func (c *BigQueryConnector) metadataHasJob(jobName string) (bool, error) {
	checkStmt := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s WHERE mirror_job_name = '%s'",
		c.datasetID, MirrorJobsTable, jobName)

	q := c.client.Query(checkStmt)
	it, err := q.Read(c.ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check if job exists: %w", err)
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return false, fmt.Errorf("failed read row: %w", err)
	}

	count, ok := row[0].(int64)
	if !ok {
		return false, fmt.Errorf("failed to convert count to int64")
	}

	return count > 0, nil
}

// SetupNormalizedTables sets up normalized tables, implementing the Connector interface.
// This runs CREATE TABLE IF NOT EXISTS on bigquery, using the schema and table name provided.
func (c *BigQueryConnector) SetupNormalizedTables(
	req *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	tableExistsMapping := make(map[string]bool)
	datasetTablesSet := make(map[datasetTable]struct{})
	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		// only place where we check for parsing errors
		datasetTable, err := c.convertToDatasetTable(tableIdentifier)
		if err != nil {
			return nil, err
		}
		_, ok := datasetTablesSet[*datasetTable]
		if ok {
			return nil, fmt.Errorf("invalid mirror: two tables mirror to the same BigQuery table %s",
				datasetTable.string())
		}
		dataset := c.client.Dataset(datasetTable.dataset)
		_, err = dataset.Metadata(c.ctx)
		// just assume this means dataset don't exist, and create it
		if err != nil {
			// if err message does not contain `notFound`, then other error happened.
			if !strings.Contains(err.Error(), "notFound") {
				return nil, fmt.Errorf("error while checking metadata for BigQuery dataset %s: %w",
					datasetTable.dataset, err)
			}
			c.logger.InfoContext(c.ctx, fmt.Sprintf("creating dataset %s...", dataset.DatasetID))
			err = dataset.Create(c.ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create BigQuery dataset %s: %w", dataset.DatasetID, err)
			}
		}
		table := dataset.Table(datasetTable.table)

		// check if the table exists
		_, err = table.Metadata(c.ctx)
		if err == nil {
			// table exists, go to next table
			tableExistsMapping[tableIdentifier] = true
			datasetTablesSet[*datasetTable] = struct{}{}
			continue
		}

		// convert the column names and types to bigquery types
		columns := make([]*bigquery.FieldSchema, 0, len(tableSchema.Columns)+2)
		utils.IterColumns(tableSchema, func(colName, genericColType string) {
			columns = append(columns, &bigquery.FieldSchema{
				Name:     colName,
				Type:     qValueKindToBigQueryType(genericColType),
				Repeated: qvalue.QValueKind(genericColType).IsArray(),
			})
		})

		if req.SoftDeleteColName != "" {
			columns = append(columns, &bigquery.FieldSchema{
				Name:     req.SoftDeleteColName,
				Type:     bigquery.BooleanFieldType,
				Repeated: false,
			})
		}

		if req.SyncedAtColName != "" {
			columns = append(columns, &bigquery.FieldSchema{
				Name:     req.SyncedAtColName,
				Type:     bigquery.TimestampFieldType,
				Repeated: false,
			})
		}

		// create the table using the columns
		schema := bigquery.Schema(columns)

		// cluster by the primary key if < 4 columns.
		var clustering *bigquery.Clustering
		numPkeyCols := len(tableSchema.PrimaryKeyColumns)
		if numPkeyCols > 0 && numPkeyCols < 4 {
			clustering = &bigquery.Clustering{
				Fields: tableSchema.PrimaryKeyColumns,
			}
		}

		metadata := &bigquery.TableMetadata{
			Schema:     schema,
			Name:       datasetTable.table,
			Clustering: clustering,
		}

		err = table.Create(c.ctx, metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to create table %s: %w", tableIdentifier, err)
		}

		tableExistsMapping[tableIdentifier] = false
		datasetTablesSet[*datasetTable] = struct{}{}
		// log that table was created
		c.logger.Info(fmt.Sprintf("created table %s", tableIdentifier))
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (c *BigQueryConnector) SyncFlowCleanup(jobName string) error {
	dataset := c.client.Dataset(c.datasetID)
	// deleting PeerDB specific tables
	err := dataset.Table(c.getRawTableName(jobName)).Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete raw table: %w", err)
	}

	// deleting job from metadata table
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE mirror_job_name = '%s'", c.datasetID, MirrorJobsTable, jobName)
	_, err = c.client.Query(query).Read(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete job from metadata table: %w", err)
	}
	return nil
}

// getRawTableName returns the raw table name for the given table identifier.
func (c *BigQueryConnector) getRawTableName(flowJobName string) string {
	// replace all non-alphanumeric characters with _
	flowJobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(flowJobName, "_")
	return fmt.Sprintf("_peerdb_raw_%s", flowJobName)
}

func (c *BigQueryConnector) RenameTables(req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	// BigQuery doesn't really do transactions properly anyway so why bother?
	for _, renameRequest := range req.RenameTableOptions {
		srcDatasetTable, _ := c.convertToDatasetTable(renameRequest.CurrentName)
		dstDatasetTable, _ := c.convertToDatasetTable(renameRequest.NewName)
		c.logger.InfoContext(c.ctx, fmt.Sprintf("renaming table '%s' to '%s'...", srcDatasetTable.string(),
			dstDatasetTable.string()))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("renaming table '%s' to '%s'...", srcDatasetTable.string(),
			dstDatasetTable.string()))

		if req.SoftDeleteColName != nil {
			allCols := strings.Join(utils.TableSchemaColumnNames(renameRequest.TableSchema), ",")
			pkeyCols := strings.Join(renameRequest.TableSchema.PrimaryKeyColumns, ",")

			c.logger.InfoContext(c.ctx, fmt.Sprintf("handling soft-deletes for table '%s'...", dstDatasetTable.string()))

			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("handling soft-deletes for table '%s'...", dstDatasetTable.string()))

			c.logger.InfoContext(c.ctx, fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
				srcDatasetTable.string(), fmt.Sprintf("%s,%s", allCols, *req.SoftDeleteColName),
				allCols, *req.SoftDeleteColName, dstDatasetTable.string(),
				pkeyCols, pkeyCols, srcDatasetTable.string()))
			_, err := c.client.Query(
				fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
					srcDatasetTable.string(), fmt.Sprintf("%s,%s", allCols, *req.SoftDeleteColName),
					allCols, *req.SoftDeleteColName, dstDatasetTable.string(),
					pkeyCols, pkeyCols, srcDatasetTable.string())).Read(c.ctx)
			if err != nil {
				return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dstDatasetTable.string(), err)
			}
		}

		if req.SyncedAtColName != nil {
			c.logger.Info(fmt.Sprintf("setting synced at column for table '%s'...", srcDatasetTable.string()))

			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("setting synced at column for table '%s'...",
				srcDatasetTable.string()))

			c.logger.InfoContext(c.ctx,
				fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL", srcDatasetTable.string(),
					*req.SyncedAtColName, *req.SyncedAtColName))
			_, err := c.client.Query(
				fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL", srcDatasetTable.string(),
					*req.SyncedAtColName, *req.SyncedAtColName)).Read(c.ctx)
			if err != nil {
				return nil, fmt.Errorf("unable to set synced at column for table %s: %w", srcDatasetTable.string(), err)
			}
		}

		c.logger.InfoContext(c.ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
			dstDatasetTable.string()))
		// drop the dst table if exists
		_, err := c.client.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s",
			dstDatasetTable.string())).Read(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dstDatasetTable.string(), err)
		}

		c.logger.InfoContext(c.ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table))
		// rename the src table to dst
		_, err = c.client.Query(fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table)).Read(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", srcDatasetTable.string(),
				dstDatasetTable.string(), err)
		}

		c.logger.InfoContext(c.ctx, fmt.Sprintf("successfully renamed table '%s' to '%s'", srcDatasetTable.string(),
			dstDatasetTable.string()))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *BigQueryConnector) CreateTablesFromExisting(req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	for newTable, existingTable := range req.NewToExistingTableMapping {
		newDatasetTable, _ := c.convertToDatasetTable(newTable)
		existingDatasetTable, _ := c.convertToDatasetTable(existingTable)
		c.logger.Info(fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		// rename the src table to dst
		_, err := c.client.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE `%s`",
			newDatasetTable.string(), existingDatasetTable.string())).Read(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to create table %s: %w", newTable, err)
		}

		c.logger.Info(fmt.Sprintf("successfully created table '%s'", newTable))
	}

	return &protos.CreateTablesFromExistingOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

type datasetTable struct {
	dataset string
	table   string
}

func (d *datasetTable) string() string {
	return fmt.Sprintf("%s.%s", d.dataset, d.table)
}

func (c *BigQueryConnector) convertToDatasetTable(tableName string) (*datasetTable, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		return &datasetTable{
			dataset: c.datasetID,
			table:   parts[0],
		}, nil
	} else if len(parts) == 2 {
		return &datasetTable{
			dataset: parts[0],
			table:   parts[1],
		}, nil
	} else {
		return nil, fmt.Errorf("invalid BigQuery table name: %s", tableName)
	}
}
