package connbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	cc "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
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
	pgMetadata    *metadataStore.PostgresMetadataStore
	datasetID     string
	projectID     string
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

// TableCheck:
// 1. Creates a table
// 2. Inserts one row into the table
// 3. Deletes the table
func TableCheck(ctx context.Context, client *bigquery.Client, dataset string, project string) error {
	dummyTable := "peerdb_validate_dummy_" + shared.RandomString(4)

	newTable := client.DatasetInProject(project, dataset).Table(dummyTable)

	createErr := newTable.Create(ctx, &bigquery.TableMetadata{
		Schema: []*bigquery.FieldSchema{
			{
				Name:     "dummy",
				Type:     bigquery.BooleanFieldType,
				Repeated: false,
			},
		},
	})
	if createErr != nil {
		return fmt.Errorf("unable to validate table creation within dataset: %w. "+
			"Please check if bigquery.tables.create permission has been granted", createErr)
	}

	var errs []error
	insertQuery := client.Query(fmt.Sprintf("INSERT INTO %s VALUES(true)", dummyTable))
	insertQuery.DefaultDatasetID = dataset
	insertQuery.DefaultProjectID = project
	_, insertErr := insertQuery.Run(ctx)
	if insertErr != nil {
		errs = append(errs, fmt.Errorf("unable to validate insertion into table: %w. ", insertErr))
	}

	// Drop the table
	deleteErr := newTable.Delete(ctx)
	if deleteErr != nil {
		errs = append(errs, fmt.Errorf("unable to delete table :%w. ", deleteErr))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// NewBigQueryConnector creates a new BigQueryConnector from a PeerConnectionConfig.
func NewBigQueryConnector(ctx context.Context, config *protos.BigqueryConfig) (*BigQueryConnector, error) {
	bqsa, err := NewBigQueryServiceAccount(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %v", err)
	}

	datasetID := config.GetDatasetId()
	projectID := config.GetProjectId()
	projectPart, datasetPart, found := strings.Cut(datasetID, ".")
	if found && strings.Contains(datasetPart, ".") {
		return nil,
			fmt.Errorf("invalid dataset ID: %s. Ensure that it is just a single string or string1.string2", datasetID)
	}
	if projectPart != "" && datasetPart != "" {
		datasetID = datasetPart
		projectID = projectPart
	}

	client, err := bqsa.CreateBigQueryClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	_, datasetErr := client.DatasetInProject(projectID, datasetID).Metadata(ctx)
	if datasetErr != nil {
		slog.ErrorContext(ctx, "failed to get dataset metadata", slog.Any("error", datasetErr))
		return nil, fmt.Errorf("failed to get dataset metadata: %v", datasetErr)
	}

	permissionErr := TableCheck(ctx, client, datasetID, projectID)
	if permissionErr != nil {
		slog.ErrorContext(ctx, "failed to get run mock table check", slog.Any("error", permissionErr))
		return nil, permissionErr
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
		projectID:     projectID,
		pgMetadata:    metadataStore.NewPostgresMetadataStoreFromCatalog(ctx, catalogPool),
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
	_, err := c.client.DatasetInProject(c.projectID, c.datasetID).Metadata(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get dataset metadata: %v", err)
	}

	if c.client == nil {
		return fmt.Errorf("BigQuery client is nil")
	}
	return nil
}

func (c *BigQueryConnector) NeedsSetupMetadataTables() bool {
	return false
}

func (c *BigQueryConnector) waitForTableReady(datasetTable *datasetTable) error {
	table := c.client.DatasetInProject(c.projectID, datasetTable.dataset).Table(datasetTable.table)
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
			query := c.client.Query(fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN IF NOT EXISTS `%s` %s",
				dstDatasetTable.table, addedColumn.ColumnName,
				qValueKindToBigQueryType(addedColumn.ColumnType)))
			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = dstDatasetTable.dataset
			_, err := query.Read(c.ctx)
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

func (c *BigQueryConnector) SetupMetadataTables() error {
	return nil
}

func (c *BigQueryConnector) GetLastOffset(jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(jobName)
}

func (c *BigQueryConnector) SetLastOffset(jobName string, offset int64) error {
	return c.pgMetadata.UpdateLastOffset(jobName, offset)
}

func (c *BigQueryConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(jobName)
}

func (c *BigQueryConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	return c.pgMetadata.GetLastNormalizeBatchID(jobName)
}

func (c *BigQueryConnector) getDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64,
) ([]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	query := fmt.Sprintf(`SELECT DISTINCT _peerdb_destination_table_name FROM %s
	 WHERE _peerdb_batch_id > %d and _peerdb_batch_id <= %d`,
		rawTableName, normalizeBatchID, syncBatchID)
	// Run the query
	q := c.client.Query(query)
	q.DefaultProjectID = c.projectID
	q.DefaultDatasetID = c.datasetID
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
	array_agg(DISTINCT _peerdb_unchanged_toast_columns) as unchanged_toast_columns FROM %s
	 WHERE _peerdb_batch_id > %d AND _peerdb_batch_id <= %d AND _peerdb_record_type != 2
	 GROUP BY _peerdb_destination_table_name`,
		rawTableName, normalizeBatchID, syncBatchID)
	// Run the query
	q := c.client.Query(query)
	q.DefaultDatasetID = c.datasetID
	q.DefaultProjectID = c.projectID
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

	res, err := c.syncRecordsViaAvro(req, rawTableName, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s.%s", res.NumRecordsSynced, c.datasetID, rawTableName))
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
	rawTableMetadata, err := c.client.DatasetInProject(c.projectID, c.datasetID).Table(rawTableName).Metadata(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of destination table: %w", err)
	}

	res, err := avroSync.SyncRecords(req, rawTableName,
		rawTableMetadata, syncBatchID, streamRes.Stream, streamReq.TableMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to sync records via avro: %w", err)
	}

	return res, nil
}

// NormalizeRecords normalizes raw table to destination table.
func (c *BigQueryConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	normBatchID, err := c.GetLastNormalizeBatchID(req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	distinctTableNames, err := c.getDistinctTableNamesInBatch(
		req.FlowJobName,
		req.SyncBatchID,
		normBatchID,
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get distinct table names to normalize: %w", err)
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(
		req.FlowJobName,
		req.SyncBatchID,
		normBatchID,
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
				project: c.projectID,
				dataset: c.datasetID,
				table:   rawTableName,
			},
			dstTableName:          tableName,
			dstDatasetTable:       dstDatasetTable,
			normalizedTableSchema: req.TableNameSchemaMapping[tableName],
			syncBatchID:           req.SyncBatchID,
			normalizeBatchID:      normBatchID,
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
			q.DefaultProjectID = c.projectID
			q.DefaultDatasetID = dstDatasetTable.dataset
			_, err = q.Read(c.ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to execute merge statement %s: %v", mergeStmt, err)
			}
		}
	}

	err = c.pgMetadata.UpdateNormalizeBatchID(req.FlowJobName, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
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
	table := c.client.DatasetInProject(c.projectID, c.datasetID).Table(rawTableName)

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
		dataset := c.client.DatasetInProject(c.projectID, datasetTable.dataset)
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
		columns := make([]*bigquery.FieldSchema, 0, len(tableSchema.ColumnNames)+2)
		for i, colName := range tableSchema.ColumnNames {
			genericColType := tableSchema.ColumnTypes[i]
			columns = append(columns, &bigquery.FieldSchema{
				Name:     colName,
				Type:     qValueKindToBigQueryType(genericColType),
				Repeated: qvalue.QValueKind(genericColType).IsArray(),
			})
		}

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
		utils.RecordHeartbeat(c.ctx, fmt.Sprintf("created table %s", tableIdentifier))
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (c *BigQueryConnector) SyncFlowCleanup(jobName string) error {
	err := c.pgMetadata.DropMetadata(jobName)
	if err != nil {
		return fmt.Errorf("unable to clear metadata for sync flow cleanup: %w", err)
	}

	dataset := c.client.DatasetInProject(c.projectID, c.datasetID)
	// deleting PeerDB specific tables
	err = dataset.Table(c.getRawTableName(jobName)).Delete(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to delete raw table: %w", err)
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
			allCols := strings.Join(renameRequest.TableSchema.ColumnNames, ",")
			pkeyCols := strings.Join(renameRequest.TableSchema.PrimaryKeyColumns, ",")

			c.logger.InfoContext(c.ctx, fmt.Sprintf("handling soft-deletes for table '%s'...", dstDatasetTable.string()))

			activity.RecordHeartbeat(c.ctx, fmt.Sprintf("handling soft-deletes for table '%s'...", dstDatasetTable.string()))

			c.logger.InfoContext(c.ctx, fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
				srcDatasetTable.string(), fmt.Sprintf("%s,%s", allCols, *req.SoftDeleteColName),
				allCols, *req.SoftDeleteColName, dstDatasetTable.string(),
				pkeyCols, pkeyCols, srcDatasetTable.string()))
			query := c.client.Query(
				fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
					srcDatasetTable.string(), fmt.Sprintf("%s,%s", allCols, *req.SoftDeleteColName),
					allCols, *req.SoftDeleteColName, dstDatasetTable.string(),
					pkeyCols, pkeyCols, srcDatasetTable.string()))

			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = c.datasetID
			_, err := query.Read(c.ctx)
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
			query := c.client.Query(
				fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL", srcDatasetTable.string(),
					*req.SyncedAtColName, *req.SyncedAtColName))

			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = c.datasetID
			_, err := query.Read(c.ctx)
			if err != nil {
				return nil, fmt.Errorf("unable to set synced at column for table %s: %w", srcDatasetTable.string(), err)
			}
		}

		c.logger.InfoContext(c.ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
			dstDatasetTable.string()))
		// drop the dst table if exists
		dropQuery := c.client.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s",
			dstDatasetTable.string()))
		dropQuery.DefaultProjectID = c.projectID
		dropQuery.DefaultDatasetID = c.datasetID
		_, err := dropQuery.Read(c.ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dstDatasetTable.string(), err)
		}

		c.logger.InfoContext(c.ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table))
		// rename the src table to dst
		query := c.client.Query(fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table))
		query.DefaultProjectID = c.projectID
		query.DefaultDatasetID = c.datasetID
		_, err = query.Read(c.ctx)
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
		query := c.client.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE `%s`",
			newDatasetTable.string(), existingDatasetTable.string()))
		query.DefaultProjectID = c.projectID
		query.DefaultDatasetID = c.datasetID
		_, err := query.Read(c.ctx)
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
	project string
	dataset string
	table   string
}

func (d *datasetTable) string() string {
	if d.project == "" {
		return fmt.Sprintf("%s.%s", d.dataset, d.table)
	}
	return fmt.Sprintf("%s.%s.%s", d.project, d.dataset, d.table)
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
	} else if len(parts) == 3 {
		return &datasetTable{
			project: parts[0],
			dataset: parts[1],
			table:   parts[2],
		}, nil
	} else {
		return nil, fmt.Errorf("invalid BigQuery table name: %s", tableName)
	}
}
