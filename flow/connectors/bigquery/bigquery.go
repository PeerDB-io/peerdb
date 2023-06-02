package connbigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	/*
		Different batch Ids in code/BigQuery
		1. batchID - identifier in raw/staging tables on target to depict which batch a row was inserted.
		2. stagingBatchID - the random batch id we generate before ingesting into staging table.
		   helps filter rows in the current batch before inserting into raw table.
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
	ctx                    context.Context
	bqConfig               *protos.BigqueryConfig
	client                 *bigquery.Client
	tableNameSchemaMapping map[string]*protos.TableSchema
	datasetID              string
}

type StagingBQRecord struct {
	uid                  string    `bigquery:"_peerdb_uid"`
	timestamp            time.Time `bigquery:"_peerdb_timestamp"`
	timestampNanos       int64     `bigquery:"_peerdb_timestamp_nanos"`
	destinationTableName string    `bigquery:"_peerdb_destination_table_name"`
	data                 string    `bigquery:"_peerdb_data"`
	recordType           int       `bigquery:"_peerdb_record_type"`
	matchData            string    `bigquery:"_peerdb_match_data"`
	batchID              int64     `bigquery:"_peerdb_batch_id"`
	stagingBatchID       int64     `bigquery:"_peerdb_staging_batch_id"`
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

// CreateClient creates a new BigQuery client from a BigQueryServiceAccount.
func (bqsa *BigQueryServiceAccount) CreateClient(ctx context.Context) (*bigquery.Client, error) {
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

// NewBigQueryConnector creates a new BigQueryConnector from a PeerConnectionConfig.
func NewBigQueryConnector(ctx context.Context, config *protos.BigqueryConfig) (*BigQueryConnector, error) {
	bqsa, err := NewBigQueryServiceAccount(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %v", err)
	}

	client, err := bqsa.CreateClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return &BigQueryConnector{
		ctx:       ctx,
		bqConfig:  config,
		client:    client,
		datasetID: config.GetDatasetId(),
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
func (c *BigQueryConnector) ConnectionActive() bool {
	return c.client != nil
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *BigQueryConnector) NeedsSetupMetadataTables() bool {
	_, err := c.client.Dataset(c.datasetID).Table(MirrorJobsTable).Metadata(c.ctx)
	return err != nil
}

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *BigQueryConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableNameSchemaMapping = req
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
		return fmt.Errorf("failed to create table %s: %w", MirrorJobsTable, err)
	}

	return nil
}

// GetLastOffset returns the last synced ID.
func (c *BigQueryConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	query := fmt.Sprintf("SELECT offset FROM %s.%s WHERE mirror_job_name = '%s'", c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return nil, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		log.Printf("no row found for job %s, returning nil", jobName)
		return nil, nil
	}

	if row[0] == nil {
		log.Printf("no offset found for job %s, returning nil", jobName)
		return nil, nil
	} else {
		return &protos.LastSyncState{
			Checkpoint: row[0].(int64),
		}, nil
	}
}

func (c *BigQueryConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	query := fmt.Sprintf("SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name = '%s'", c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return -1, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		log.Printf("no row found for job %s", jobName)
		return 0, nil
	}

	if row[0] == nil {
		log.Printf("no sync_batch_id found for job %s, returning 0", jobName)
		return 0, nil
	} else {
		return row[0].(int64), nil
	}
}

func (c *BigQueryConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	query := fmt.Sprintf("SELECT normalize_batch_id FROM %s.%s WHERE mirror_job_name = '%s'", c.datasetID, MirrorJobsTable, jobName)
	q := c.client.Query(query)
	it, err := q.Read(c.ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return -1, err
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		log.Printf("no row found for job %s", jobName)
		return 0, nil
	}

	if row[0] == nil {
		log.Printf("no normalize_batch_id found for job %s, returning 0", jobName)
		return 0, nil
	} else {
		return row[0].(int64), nil
	}
}

func (c *BigQueryConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) ([]string, error) {
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

// PullRecords pulls records from the source.
func (c *BigQueryConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	panic("not implemented")
}

// ValueSaver interface for bqRecord
func (r StagingBQRecord) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"_peerdb_uid":                    r.uid,
		"_peerdb_timestamp":              r.timestamp,
		"_peerdb_timestamp_nanos":        r.timestampNanos,
		"_peerdb_destination_table_name": r.destinationTableName,
		"_peerdb_data":                   r.data,
		"_peerdb_record_type":            r.recordType,
		"_peerdb_match_data":             r.matchData,
		"_peerdb_batch_id":               r.batchID,
		"_peerdb_staging_batch_id":       r.stagingBatchID,
	}, bigquery.NoDedupeID, nil
}

// SyncRecords pushes records to the destination.
// currently only supports inserts,updates and deletes
// more record types will be added in the future.
func (c *BigQueryConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	log.Printf("pushing %d records to %s.%s", len(req.Records.Records), c.datasetID, rawTableName)

	stagingTableName := c.getStagingTableName(req.FlowJobName)
	stagingTable := c.client.Dataset(c.datasetID).Table(stagingTableName)
	err := c.truncateTable(stagingTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate staging table: %v", err)
	}
	// separate staging batchID which is random/unique
	// to handle the case where ingestion into staging passes but raw fails
	// helps avoid duplicates in the raw table
	stagingBatchID := rand.Int63()

	// generate a sequential number for the last synced batch
	// this sequence will be used to keep track of records that are normalized
	// in the NormalizeFlowWorkflow
	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}
	syncBatchID = syncBatchID + 1

	records := make([]StagingBQRecord, 0)

	first := true
	var firstCP int64 = 0
	lastCP := req.Records.LastCheckPointID

	// loop over req.Records
	for _, record := range req.Records.Records {
		switch r := record.(type) {
		case *model.InsertRecord:
			// create the 3 required fields
			//   1. _peerdb_uid - uuid
			//   2. _peerdb_timestamp - current timestamp
			//   2. _peerdb_timestamp_nanos - current timestamp in nano seconds
			//   3. _peerdb_data - json of `r.Items`
			json, err := json.Marshal(r.Items)
			if err != nil {
				return nil, fmt.Errorf("failed to create items to json: %v", err)
			}

			// append the row to the records
			records = append(records, StagingBQRecord{
				uid:                  uuid.New().String(),
				timestamp:            time.Now(),
				timestampNanos:       time.Now().UnixNano(),
				destinationTableName: r.DestinationTableName,
				data:                 string(json),
				recordType:           0,
				matchData:            "",
				batchID:              syncBatchID,
				stagingBatchID:       stagingBatchID,
			})
		case *model.UpdateRecord:
			// create the 5 required fields
			//   1. _peerdb_uid - uuid
			//   2. _peerdb_timestamp - current timestamp
			//   3. _peerdb_data - json of `r.NewItems`
			//   4. _peerdb_record_type - 1
			//   5. _peerdb_match_data - json of `r.OldItems`

			newItemsJSON, err := json.Marshal(r.NewItems)
			if err != nil {
				return nil, fmt.Errorf("failed to create new items to json: %v", err)
			}

			oldItemsJSON, err := json.Marshal(r.OldItems)
			if err != nil {
				return nil, fmt.Errorf("failed to create old items to json: %v", err)
			}

			// append the row to the records
			records = append(records, StagingBQRecord{
				uid:                  uuid.New().String(),
				timestamp:            time.Now(),
				timestampNanos:       time.Now().UnixNano(),
				destinationTableName: r.DestinationTableName,
				data:                 string(newItemsJSON),
				recordType:           1,
				matchData:            string(oldItemsJSON),
				batchID:              syncBatchID,
				stagingBatchID:       stagingBatchID,
			})
		case *model.DeleteRecord:
			// create the 4 required fields
			//   1. _peerdb_uid - uuid
			//   2. _peerdb_timestamp - current timestamp
			//   3. _peerdb_record_type - 2
			//   4. _peerdb_match_data - json of `r.Items`

			itemsJSON, err := json.Marshal(r.Items)
			if err != nil {
				return nil, fmt.Errorf("failed to create items to json: %v", err)
			}

			// append the row to the records
			records = append(records, StagingBQRecord{
				uid:                  uuid.New().String(),
				timestamp:            time.Now(),
				timestampNanos:       time.Now().UnixNano(),
				destinationTableName: r.DestinationTableName,
				data:                 string(itemsJSON),
				recordType:           2,
				matchData:            string(itemsJSON),
				batchID:              syncBatchID,
				stagingBatchID:       stagingBatchID,
			})
		default:
			return nil, fmt.Errorf("record type %T not supported", r)
		}

		if first {
			firstCP = record.GetCheckPointID()
			first = false
		}
	}

	numRecords := len(records)
	if numRecords == 0 {
		return &model.SyncResponse{
			FirstSyncedCheckPointID: 0,
			LastSyncedCheckPointID:  0,
			NumRecordsSynced:        0,
		}, nil
	}

	// insert the records into the staging table
	stagingInserter := stagingTable.Inserter()
	stagingInserter.IgnoreUnknownValues = true

	// insert the records into the staging table in batches of size syncRecordsBatchSize
	for i := 0; i < numRecords; i += SyncRecordsBatchSize {
		end := i + SyncRecordsBatchSize

		if end > numRecords {
			end = numRecords
		}

		chunk := records[i:end]
		err = stagingInserter.Put(c.ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("failed to insert chunked rows into staging table: %v", err)
		}
	}

	// we have to do the following things in a transaction
	// 1. append the records in the staging table to the raw table.
	// 2. execute the update metadata query to store the last committed watermark.
	// 2.(contd) keep track of the last batchID that is synced.
	appendStmt := c.getAppendStagingToRawStmt(rawTableName, stagingTableName, stagingBatchID)

	updateMetadataStmt, err := c.getUpdateMetadataStmt(req.FlowJobName, lastCP, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to get update metadata statement: %v", err)
	}

	// append all the statements to one list
	stmts := []string{}
	stmts = append(stmts, "BEGIN TRANSACTION;")
	stmts = append(stmts, appendStmt)
	stmts = append(stmts, updateMetadataStmt)
	stmts = append(stmts, "COMMIT TRANSACTION;")

	// print the statements as one string
	// log.Printf("statements to execute in a transaction: %s", strings.Join(stmts, "\n"))

	// execute the statements in a transaction
	_, err = c.client.Query(strings.Join(stmts, "\n")).Read(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statements in a transaction: %v", err)
	}

	log.Printf("pushed %d records to %s.%s", numRecords, c.datasetID, rawTableName)

	return &model.SyncResponse{
		FirstSyncedCheckPointID: firstCP,
		LastSyncedCheckPointID:  lastCP,
		NumRecordsSynced:        int64(numRecords),
	}, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *BigQueryConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	syncBatchID, err := c.GetLastSyncBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// get last batchid that has been normalize
	normalizeBatchID, err := c.GetLastNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	hasJob, err := c.metadataHasJob(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if job exists: %w", err)
	}
	// if job is not yet found in the peerdb_mirror_jobs_table
	// OR sync is lagging end normalize
	if !hasJob || normalizeBatchID == syncBatchID {
		log.Printf("waiting for sync to catch up for job %s, so finishing", req.FlowJobName)
		return &model.NormalizeResponse{
			Done: true,
		}, nil
	}
	distinctTableNames, err := c.getDistinctTableNamesInBatch(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't get distinct table names to normalize: %w", err)
	}

	stmts := []string{}
	// append all the statements to one list
	log.Printf("merge raw records to corresponding tables: %s %s %v", c.datasetID, rawTableName, distinctTableNames)
	stmts = append(stmts, "BEGIN TRANSACTION;")

	for _, tableName := range distinctTableNames {
		mergeGen := &MergeStmtGenerator{
			Dataset:               c.datasetID,
			NormalizedTable:       tableName,
			RawTable:              rawTableName,
			NormalizedTableSchema: c.tableNameSchemaMapping[tableName],
			SyncBatchID:           syncBatchID,
			NormalizeBatchID:      normalizeBatchID,
		}
		// normalize anything between last normalized batch id to last sync batchid
		mergeStmts := mergeGen.GenerateMergeStmts()
		stmts = append(stmts, mergeStmts...)
	}
	//update metadata to make the last normalized batch id to the recent last sync batch id.
	updateMetadataStmt := fmt.Sprintf(
		"UPDATE %s.%s SET normalize_batch_id=%d WHERE mirror_job_name = '%s';",
		c.datasetID, MirrorJobsTable, syncBatchID, req.FlowJobName)
	stmts = append(stmts, updateMetadataStmt)
	stmts = append(stmts, "COMMIT TRANSACTION;")

	// put this within a transaction
	// TODO - not truncating rows in staging table as of now.
	// err = c.truncateTable(staging...)

	_, err = c.client.Query(strings.Join(stmts, "\n")).Read(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statements %s in a transaction: %v", strings.Join(stmts, "\n"), err)
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normalizeBatchID + 1,
		EndBatchID:   syncBatchID,
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
		{Name: "_peerdb_timestamp", Type: bigquery.TimestampFieldType},
		{Name: "_peerdb_timestamp_nanos", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_destination_table_name", Type: bigquery.StringFieldType},
		{Name: "_peerdb_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_record_type", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_match_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_batch_id", Type: bigquery.IntegerFieldType},
	}

	staging_schema := bigquery.Schema{
		{Name: "_peerdb_uid", Type: bigquery.StringFieldType},
		{Name: "_peerdb_timestamp", Type: bigquery.TimestampFieldType},
		{Name: "_peerdb_timestamp_nanos", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_destination_table_name", Type: bigquery.StringFieldType},
		{Name: "_peerdb_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_record_type", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_match_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_batch_id", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_staging_batch_id", Type: bigquery.IntegerFieldType},
	}

	// create the table
	table := c.client.Dataset(c.datasetID).Table(rawTableName)

	// check if the table exists
	meta, err := table.Metadata(c.ctx)
	if err == nil {
		// table exists, check if the schema matches
		if !reflect.DeepEqual(meta.Schema, schema) {
			return nil, fmt.Errorf("table %s.%s already exists with different schema", c.datasetID, rawTableName)
		} else {
			return &protos.CreateRawTableOutput{
				TableIdentifier: rawTableName,
			}, nil
		}
	}

	// table does not exist, create it
	err = table.Create(c.ctx, &bigquery.TableMetadata{
		Schema: schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, rawTableName, err)
	}

	// also create a staging table for this raw table
	stagingTableName := c.getStagingTableName(req.FlowJobName)
	stagingTable := c.client.Dataset(c.datasetID).Table(stagingTableName)
	err = stagingTable.Create(c.ctx, &bigquery.TableMetadata{
		Schema: staging_schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, stagingTableName, err)
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

// getUpdateMetadataStmt updates the metadata tables for a given job.
func (c *BigQueryConnector) getUpdateMetadataStmt(jobName string, lastSyncedCheckpointID int64, batchID int64) (string, error) {
	hasJob, err := c.metadataHasJob(jobName)
	if err != nil {
		return "", fmt.Errorf("failed to check if job exists: %w", err)
	}

	// create the job in the metadata table
	jobStatement := fmt.Sprintf(
		"INSERT INTO %s.%s (mirror_job_name, offset,sync_batch_id) VALUES ('%s',%d,%d);",
		c.datasetID, MirrorJobsTable, jobName, lastSyncedCheckpointID, batchID)
	if hasJob {
		jobStatement = fmt.Sprintf(
			"UPDATE %s.%s SET offset = %d,sync_batch_id=%d WHERE mirror_job_name = '%s';",
			c.datasetID, MirrorJobsTable, lastSyncedCheckpointID, batchID, jobName)
	}

	return jobStatement, nil
}

// getAppendStagingToRawStmt returns the statement to append the staging table to the raw table.
func (c *BigQueryConnector) getAppendStagingToRawStmt(
	rawTableName string, stagingTableName string, stagingBatchID int64,
) string {
	return fmt.Sprintf(
		`INSERT INTO %s.%s SELECT _peerdb_uid,_peerdb_timestamp,_peerdb_timestamp_nanos,
		_peerdb_destination_table_name,_peerdb_data,_peerdb_record_type,_peerdb_match_data,
		_peerdb_batch_id FROM %s.%s WHERE _peerdb_staging_batch_id = %d;`,
		c.datasetID, rawTableName, c.datasetID, stagingTableName, stagingBatchID)
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

// GetTableSchema returns the schema for a table, implementing the Connector interface.
func (c *BigQueryConnector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	panic("not implemented")
}

// SetupNormalizedTable sets up a normalized table, implementing the Connector interface.
// This runs CREATE TABLE IF NOT EXISTS on bigquery, using the schema and table name provided.
func (c *BigQueryConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput,
) (*protos.SetupNormalizedTableOutput, error) {
	// convert the column names and types to bigquery types
	sourceSchema := req.SourceTableSchema

	columns := make([]*bigquery.FieldSchema, len(sourceSchema.Columns))
	idx := 0
	for colName, genericColType := range sourceSchema.Columns {
		columns[idx] = &bigquery.FieldSchema{
			Name: colName,
			Type: getBigQueryColumnTypeForGenericColType(genericColType),
		}
		idx++
	}

	// create the table using the columns
	schema := bigquery.Schema(columns)
	table := c.client.Dataset(c.datasetID).Table(req.TableIdentifier)

	// check if the table exists
	_, err := table.Metadata(c.ctx)
	if err == nil {
		// table exists, return
		return &protos.SetupNormalizedTableOutput{
			TableIdentifier: req.TableIdentifier,
			AlreadyExists:   true,
		}, nil
	}

	err = table.Create(c.ctx, &bigquery.TableMetadata{Schema: schema})
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s: %w", req.TableIdentifier, err)
	}

	// log that table was created
	log.Printf("created table %s", req.TableIdentifier)

	return &protos.SetupNormalizedTableOutput{
		TableIdentifier: req.TableIdentifier,
		AlreadyExists:   false,
	}, nil
}

// EnsurePullability ensures that the given table is pullable, implementing the Connector interface.
func (c *BigQueryConnector) EnsurePullability(*protos.EnsurePullabilityInput) (*protos.EnsurePullabilityOutput, error) {
	panic("not implemented")
}

// SetupReplication sets up replication for the source connector.
func (c *BigQueryConnector) SetupReplication(req *protos.SetupReplicationInput) error {
	log.Errorf("panicking at call to SetupReplication for Snowflake flow connector")
	panic("SetupReplication is not implemented for the Snowflake flow connector")
}

func (c *BigQueryConnector) PullFlowCleanup(jobName string) error {
	panic("not implemented")
}

func (c *BigQueryConnector) SyncFlowCleanup(jobName string) error {
	dataset := c.client.Dataset(c.datasetID)
	// deleting PeerDB specific tables
	err := dataset.Table(c.getRawTableName(jobName)).Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete raw table: %w", err)
	}
	err = dataset.Table(c.getStagingTableName(jobName)).Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete staging table: %w", err)
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

// getStagingTableName returns the staging table name for the given table identifier.
func (c *BigQueryConnector) getStagingTableName(flowJobName string) string {
	// replace all non-alphanumeric characters with _
	flowJobName = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(flowJobName, "_")
	return fmt.Sprintf("_peerdb_staging_%s", flowJobName)
}

// truncateTable truncates a table.
func (c *BigQueryConnector) truncateTable(tableIdentifier string) error {
	// execute DELETE FROM table where the timestamp is older than 90 mins from now.
	// The timestamp is used to ensure that the streaming rows are not effected by the delete.
	// column of interest is the _peerdb_timestamp column.
	deleteStmt := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE _peerdb_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE)",
		c.datasetID, tableIdentifier)
	q := c.client.Query(deleteStmt)
	_, err := q.Read(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete rows from table %s: %w", tableIdentifier, err)
	}

	return nil
}

func getBigQueryColumnTypeForGenericColType(colType string) bigquery.FieldType {
	switch colType {
	// boolean
	case model.ColumnTypeBoolean:
		return bigquery.BooleanFieldType
	// integer types
	case model.ColumnTypeInt16:
		return bigquery.IntegerFieldType
	case model.ColumnTypeInt32:
		return bigquery.IntegerFieldType
	case model.ColumnTypeInt64:
		return bigquery.IntegerFieldType
	// decimal types
	case model.ColumnTypeFloat16:
		return bigquery.FloatFieldType
	case model.ColumnTypeFloat32:
		return bigquery.FloatFieldType
	case model.ColumnTypeFloat64:
		return bigquery.FloatFieldType
	case model.ColumnTypeNumeric:
		return bigquery.NumericFieldType
	// string related
	case model.ColumnTypeString:
		return bigquery.StringFieldType
	// json also is stored as string for now
	case model.ColumnTypeJSON:
		return bigquery.StringFieldType
	// time related
	case model.ColumnTypeTimestamp:
		return bigquery.TimestampFieldType
	// rest will be strings - uuid, json
	default:
		return bigquery.StringFieldType
	}
}

type MergeStmtGenerator struct {
	// dataset of all the tables
	Dataset string
	// the table to merge into
	NormalizedTable string
	// the table where the data is currently staged.
	RawTable string
	// last synced batchID.
	SyncBatchID int64
	// last normalized batchID.
	NormalizeBatchID int64
	// the schema of the table to merge into
	NormalizedTableSchema *protos.TableSchema
}

// GenerateMergeStmt generates a merge statements.
func (m *MergeStmtGenerator) GenerateMergeStmts() []string {
	// return an empty array for now
	flattenedCTE := m.generateFlattenedCTE()
	deDupedCTE := m.generateDeDupedCTE()

	// create temp table stmt
	createTempTableStmt := fmt.Sprintf(
		"CREATE TEMP TABLE _peerdb_de_duplicated_data AS (%s, %s);",
		flattenedCTE, deDupedCTE)

	mergeStmt := m.generateMergeStmt()

	dropTempTableStmt := "DROP TABLE _peerdb_de_duplicated_data;"

	return []string{createTempTableStmt, mergeStmt, dropTempTableStmt}
}

// generateFlattenedCTE generates a flattened CTE.
func (m *MergeStmtGenerator) generateFlattenedCTE() string {
	// for each column in the normalized table, generate CAST + JSON_EXTRACT_SCALAR
	// statement.
	flattenedProjs := make([]string, 0)
	for colName, colType := range m.NormalizedTableSchema.Columns {
		bq_type := getBigQueryColumnTypeForGenericColType(colType)
		// CAST doesn't work for FLOAT, so rewrite it to FLOAT64.
		if bq_type == bigquery.FloatFieldType {
			bq_type = "FLOAT64"
		}
		var castStmt string
		castStmt = fmt.Sprintf("CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s') AS %s) AS %s",
			colName, bq_type, colName)
		/*
			if the type is JSON, then JSON_EXTRACT_SCALAR doesn't work.
			We are still CASTING JSON to string because for flexibility reasons
		*/
		if colType == model.ColumnTypeJSON {
			castStmt = fmt.Sprintf("CAST(JSON_EXTRACT(_peerdb_data, '$.%s') AS %s) AS %s",
				colName, bq_type, colName)
		}
		flattenedProjs = append(flattenedProjs, castStmt)
	}
	flattenedProjs = append(flattenedProjs, "_peerdb_timestamp")
	flattenedProjs = append(flattenedProjs, "_peerdb_timestamp_nanos")
	flattenedProjs = append(flattenedProjs, "_peerdb_record_type")

	// normalize anything between last normalized batch id to last sync batchid
	return fmt.Sprintf(`WITH _peerdb_flattened AS
	 (SELECT %s FROM %s.%s WHERE _peerdb_batch_id > %d and _peerdb_batch_id <= %d and
	 _peerdb_destination_table_name='%s')`,
		strings.Join(flattenedProjs, ", "), m.Dataset, m.RawTable, m.NormalizeBatchID,
		m.SyncBatchID, m.NormalizedTable)
}

// generateDeDupedCTE generates a de-duped CTE.
func (m *MergeStmtGenerator) generateDeDupedCTE() string {
	return `_peerdb_de_duplicated_data_res AS (
		SELECT _peerdb_ranked.*
			FROM (
				SELECT RANK() OVER (
					PARTITION BY id ORDER BY _peerdb_timestamp_nanos DESC
				) as rank, * FROM _peerdb_flattened
			) _peerdb_ranked
			WHERE rank = 1
	) SELECT * FROM _peerdb_de_duplicated_data_res`
}

// generateMergeStmt generates a merge statement.
func (m *MergeStmtGenerator) generateMergeStmt() string {
	pkey := m.NormalizedTableSchema.PrimaryKeyColumn

	// comma separated list of column names
	colNames := make([]string, 0)
	setValues := make([]string, 0)
	for colName := range m.NormalizedTableSchema.Columns {
		colNames = append(colNames, colName)
		setValues = append(setValues, fmt.Sprintf("%s = _peerdb_deduped.%s", colName, colName))
	}
	csep := strings.Join(colNames, ", ")
	ssep := strings.Join(setValues, ", ")

	return fmt.Sprintf(`
	MERGE %s.%s _peerdb_target USING _peerdb_de_duplicated_data _peerdb_deduped
	ON _peerdb_target.%s = _peerdb_deduped.%s
		WHEN NOT MATCHED and (_peerdb_deduped._peerdb_record_type != 2) THEN
			INSERT (%s) VALUES (%s)
		WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type != 2) THEN
			UPDATE SET %s
		WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type = 2) THEN
	DELETE;
	`, m.Dataset, m.NormalizedTable, pkey, pkey, csep, csep, ssep)
}
