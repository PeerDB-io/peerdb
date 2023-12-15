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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.temporal.io/sdk/activity"
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
	storageClient          *storage.Client
	tableNameSchemaMapping map[string]*protos.TableSchema
	datasetID              string
	catalogPool            *pgxpool.Pool
	logger                 slog.Logger
}

type StagingBQRecord struct {
	uid                   string    `bigquery:"_peerdb_uid"`
	timestamp             time.Time `bigquery:"_peerdb_timestamp"`
	timestampNanos        int64     `bigquery:"_peerdb_timestamp_nanos"`
	destinationTableName  string    `bigquery:"_peerdb_destination_table_name"`
	data                  string    `bigquery:"_peerdb_data"`
	recordType            int       `bigquery:"_peerdb_record_type"`
	matchData             string    `bigquery:"_peerdb_match_data"`
	batchID               int64     `bigquery:"_peerdb_batch_id"`
	stagingBatchID        int64     `bigquery:"_peerdb_staging_batch_id"`
	unchangedToastColumns string    `bigquery:"_peerdb_unchanged_toast_columns"`
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

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *BigQueryConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	c.tableNameSchemaMapping = req
	return nil
}

func (c *BigQueryConnector) WaitForTableReady(tblName string) error {
	table := c.client.Dataset(c.datasetID).Table(tblName)
	maxDuration := 5 * time.Minute
	deadline := time.Now().Add(maxDuration)
	sleepInterval := 15 * time.Second
	attempt := 0

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout reached while waiting for table %s to be ready", tblName)
		}

		_, err := table.Metadata(c.ctx)
		if err == nil {
			return nil
		}

		slog.Info("waiting for table to be ready", slog.String("table", tblName), slog.Int("attempt", attempt))
		attempt++
		time.Sleep(sleepInterval)
	}
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *BigQueryConnector) ReplayTableSchemaDeltas(flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta) error {
	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			_, err := c.client.Query(fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS `%s` %s", c.datasetID,
				schemaDelta.DstTableName, addedColumn.ColumnName,
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

func (c *BigQueryConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	query := fmt.Sprintf("SELECT normalize_batch_id FROM %s.%s WHERE mirror_job_name = '%s'",
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
		c.logger.Info("no row found for job")
		return 0, nil
	}

	if row[0] == nil {
		c.logger.Info("no normalize_batch_id found returning 0")
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

func (c *BigQueryConnector) getTableNametoUnchangedCols(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) (map[string][]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	query := fmt.Sprintf(`SELECT _peerdb_destination_table_name,
	array_agg(DISTINCT _peerdb_unchanged_toast_columns) as unchanged_toast_columns FROM %s.%s
	 WHERE _peerdb_batch_id > %d and _peerdb_batch_id <= %d GROUP BY _peerdb_destination_table_name`,
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
			fmt.Printf("Error while iterating through results: %v\n", err)
			return nil, err
		}
		resultMap[row.Tablename] = row.UnchangedToastColumns
	}
	return resultMap, nil
}

// ValueSaver interface for bqRecord
func (r StagingBQRecord) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"_peerdb_uid":                     r.uid,
		"_peerdb_timestamp":               r.timestamp,
		"_peerdb_timestamp_nanos":         r.timestampNanos,
		"_peerdb_destination_table_name":  r.destinationTableName,
		"_peerdb_data":                    r.data,
		"_peerdb_record_type":             r.recordType,
		"_peerdb_match_data":              r.matchData,
		"_peerdb_batch_id":                r.batchID,
		"_peerdb_staging_batch_id":        r.stagingBatchID,
		"_peerdb_unchanged_toast_columns": r.unchangedToastColumns,
	}, bigquery.NoDedupeID, nil
}

// SyncRecords pushes records to the destination.
// currently only supports inserts,updates and deletes
// more record types will be added in the future.
func (c *BigQueryConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	c.logger.Info(fmt.Sprintf("pushing records to %s.%s...", c.datasetID, rawTableName))

	// generate a sequential number for the last synced batch
	// this sequence will be used to keep track of records that are normalized
	// in the NormalizeFlowWorkflow
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
	recordStream := model.NewQRecordStream(1 << 20)
	err := recordStream.SetSchema(&model.QRecordSchema{
		Fields: []*model.QField{
			{
				Name:     "_peerdb_uid",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_timestamp",
				Type:     qvalue.QValueKindTimestamp,
				Nullable: false,
			},
			{
				Name:     "_peerdb_timestamp_nanos",
				Type:     qvalue.QValueKindInt64,
				Nullable: false,
			},
			{
				Name:     "_peerdb_destination_table_name",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_data",
				Type:     qvalue.QValueKindString,
				Nullable: false,
			},
			{
				Name:     "_peerdb_record_type",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_match_data",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
			{
				Name:     "_peerdb_staging_batch_id",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_batch_id",
				Type:     qvalue.QValueKindInt64,
				Nullable: true,
			},
			{
				Name:     "_peerdb_unchanged_toast_columns",
				Type:     qvalue.QValueKindString,
				Nullable: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	// loop over req.Records
	for record := range req.Records.GetRecords() {
		var entries [10]qvalue.QValue
		switch r := record.(type) {
		case *model.InsertRecord:

			itemsJSON, err := r.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to create items to json: %v", err)
			}

			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: r.DestinationTableName,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 0,
			}
			entries[6] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: "",
			}
			entries[9] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: "",
			}

			tableNameRowsMapping[r.DestinationTableName] += 1
		case *model.UpdateRecord:
			newItemsJSON, err := r.NewItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to create new items to json: %v", err)
			}

			oldItemsJSON, err := r.OldItems.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to create old items to json: %v", err)
			}

			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: r.DestinationTableName,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: newItemsJSON,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 1,
			}
			entries[6] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: oldItemsJSON,
			}
			entries[9] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: utils.KeysToString(r.UnchangedToastColumns),
			}

			tableNameRowsMapping[r.DestinationTableName] += 1
		case *model.DeleteRecord:
			itemsJSON, err := r.Items.ToJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to create items to json: %v", err)
			}

			entries[3] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: r.DestinationTableName,
			}
			entries[4] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[5] = qvalue.QValue{
				Kind:  qvalue.QValueKindInt64,
				Value: 2,
			}
			entries[6] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: itemsJSON,
			}
			entries[9] = qvalue.QValue{
				Kind:  qvalue.QValueKindString,
				Value: "",
			}

			tableNameRowsMapping[r.DestinationTableName] += 1
		default:
			return nil, fmt.Errorf("record type %T not supported", r)
		}

		entries[0] = qvalue.QValue{
			Kind:  qvalue.QValueKindString,
			Value: uuid.New().String(),
		}
		entries[1] = qvalue.QValue{
			Kind:  qvalue.QValueKindTimestamp,
			Value: time.Now(),
		}
		entries[2] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: time.Now().UnixNano(),
		}
		entries[7] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: syncBatchID,
		}
		entries[8] = qvalue.QValue{
			Kind:  qvalue.QValueKindInt64,
			Value: syncBatchID,
		}
		recordStream.Records <- &model.QRecordOrError{
			Record: &model.QRecord{
				NumEntries: 10,
				Entries:    entries[:],
			},
		}
	}

	close(recordStream.Records)
	avroSync := NewQRepAvroSyncMethod(c, req.StagingPath, req.FlowJobName)
	rawTableMetadata, err := c.client.Dataset(c.datasetID).Table(rawTableName).Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of destination table: %v", err)
	}

	lastCP, err := req.Records.GetLastCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get last checkpoint: %v", err)
	}

	numRecords, err := avroSync.SyncRecords(rawTableName, req.FlowJobName,
		lastCP, rawTableMetadata, syncBatchID, recordStream)
	if err != nil {
		return nil, fmt.Errorf("failed to sync records via avro : %v", err)
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s.%s", numRecords, c.datasetID, rawTableName))

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
		c.logger.Info("waiting for sync to catch up, so finishing")
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normalizeBatchID,
			EndBatchID:   syncBatchID,
		}, nil
	}
	distinctTableNames, err := c.getDistinctTableNamesInBatch(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't get distinct table names to normalize: %w", err)
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(req.FlowJobName, syncBatchID, normalizeBatchID)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tablename to unchanged cols mapping: %w", err)
	}

	stmts := []string{}
	// append all the statements to one list
	c.logger.Info(fmt.Sprintf("merge raw records to corresponding tables: %s %s %v",
		c.datasetID, rawTableName, distinctTableNames))

	release, err := c.grabJobsUpdateLock()
	if err != nil {
		return nil, fmt.Errorf("failed to grab lock: %v", err)
	}

	defer func() {
		err := release()
		if err != nil {
			c.logger.Error("failed to release lock", slog.Any("error", err))
		}
	}()

	stmts = append(stmts, "BEGIN TRANSACTION;")

	for _, tableName := range distinctTableNames {
		mergeGen := &mergeStmtGenerator{
			Dataset:               c.datasetID,
			NormalizedTable:       tableName,
			RawTable:              rawTableName,
			NormalizedTableSchema: c.tableNameSchemaMapping[tableName],
			SyncBatchID:           syncBatchID,
			NormalizeBatchID:      normalizeBatchID,
			UnchangedToastColumns: tableNametoUnchangedToastCols[tableName],
		}
		// normalize anything between last normalized batch id to last sync batchid
		mergeStmts := mergeGen.generateMergeStmts()
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
		{Name: "_peerdb_unchanged_toast_columns", Type: bigquery.StringFieldType},
	}

	stagingSchema := bigquery.Schema{
		{Name: "_peerdb_uid", Type: bigquery.StringFieldType},
		{Name: "_peerdb_timestamp", Type: bigquery.TimestampFieldType},
		{Name: "_peerdb_timestamp_nanos", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_destination_table_name", Type: bigquery.StringFieldType},
		{Name: "_peerdb_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_record_type", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_match_data", Type: bigquery.StringFieldType},
		{Name: "_peerdb_batch_id", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_staging_batch_id", Type: bigquery.IntegerFieldType},
		{Name: "_peerdb_unchanged_toast_columns", Type: bigquery.StringFieldType},
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
		Schema: stagingSchema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, stagingTableName, err)
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

// getUpdateMetadataStmt updates the metadata tables for a given job.
func (c *BigQueryConnector) getUpdateMetadataStmt(jobName string, lastSyncedCheckpointID int64,
	batchID int64) (string, error) {
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
			"UPDATE %s.%s SET offset = %d,sync_batch_id=%d WHERE mirror_job_name = '%s';",
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
	for tableIdentifier, tableSchema := range req.TableNameSchemaMapping {
		table := c.client.Dataset(c.datasetID).Table(tableIdentifier)

		// check if the table exists
		_, err := table.Metadata(c.ctx)
		if err == nil {
			// table exists, go to next table
			tableExistsMapping[tableIdentifier] = true
			continue
		}

		// convert the column names and types to bigquery types
		columns := make([]*bigquery.FieldSchema, len(tableSchema.Columns))
		idx := 0
		for colName, genericColType := range tableSchema.Columns {
			columns[idx] = &bigquery.FieldSchema{
				Name:     colName,
				Type:     qValueKindToBigQueryType(genericColType),
				Repeated: strings.Contains(genericColType, "array"),
			}
			idx++
		}

		// create the table using the columns
		schema := bigquery.Schema(columns)
		err = table.Create(c.ctx, &bigquery.TableMetadata{Schema: schema})
		if err != nil {
			return nil, fmt.Errorf("failed to create table %s: %w", tableIdentifier, err)
		}

		tableExistsMapping[tableIdentifier] = false
		// log that table was created
		c.logger.Info(fmt.Sprintf("created table %s", tableIdentifier))
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (c *BigQueryConnector) SyncFlowCleanup(jobName string) error {
	release, err := c.grabJobsUpdateLock()
	if err != nil {
		return fmt.Errorf("failed to grab lock: %w", err)
	}

	defer func() {
		err := release()
		if err != nil {
			c.logger.Error("failed to release lock", slog.Any("error", err))
		}
	}()

	dataset := c.client.Dataset(c.datasetID)
	// deleting PeerDB specific tables
	err = dataset.Table(c.getRawTableName(jobName)).Delete(c.ctx)
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

// Bigquery doesn't allow concurrent updates to the same table.
// we grab a lock on catalog to ensure that only one job is updating
// bigquery tables at a time.
// returns a function to release the lock.
func (c *BigQueryConnector) grabJobsUpdateLock() (func() error, error) {
	tx, err := c.catalogPool.Begin(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// grab an advisory lock based on the mirror jobs table hash
	mjTbl := fmt.Sprintf("%s.%s", c.datasetID, MirrorJobsTable)
	_, err = tx.Exec(c.ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", mjTbl)
	if err != nil {
		err = tx.Rollback(c.ctx)
		return nil, fmt.Errorf("failed to grab lock on %s: %w", mjTbl, err)
	}

	return func() error {
		err = tx.Commit(c.ctx)
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}, nil
}

func (c *BigQueryConnector) RenameTables(req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	for _, renameRequest := range req.RenameTableOptions {
		src := renameRequest.CurrentName
		dst := renameRequest.NewName
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		// drop the dst table if exists
		_, err := c.client.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", c.datasetID, dst)).Run(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		_, err = c.client.Query(fmt.Sprintf("ALTER TABLE %s.%s RENAME TO %s",
			c.datasetID, src, dst)).Run(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", src, dst, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", src, dst))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *BigQueryConnector) CreateTablesFromExisting(req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error) {
	for newTable, existingTable := range req.NewToExistingTableMapping {
		c.logger.Info(fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		activity.RecordHeartbeat(c.ctx, fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		// rename the src table to dst
		_, err := c.client.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s",
			c.datasetID, newTable, c.datasetID, existingTable)).Run(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to create table %s: %w", newTable, err)
		}

		c.logger.Info(fmt.Sprintf("successfully created table '%s'", newTable))
	}

	return &protos.CreateTablesFromExistingOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}
