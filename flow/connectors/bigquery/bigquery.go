package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"
	"google.golang.org/api/iterator"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	SyncRecordsBatchSize = 1024
)

type BigQueryConnector struct {
	*metadataStore.PostgresMetadata
	logger        log.Logger
	bqConfig      *protos.BigqueryConfig
	client        *bigquery.Client
	storageClient *storage.Client
	catalogPool   *pgxpool.Pool
	datasetID     string
	projectID     string
}

func NewBigQueryServiceAccount(bqConfig *protos.BigqueryConfig) (*utils.GcpServiceAccount, error) {
	var serviceAccount utils.GcpServiceAccount
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

// ValidateCheck:
// 1. Creates a table
// 2. Inserts one row into the table
// 3. Deletes the table
func (c *BigQueryConnector) ValidateCheck(ctx context.Context) error {
	dummyTable := "peerdb_validate_dummy_" + shared.RandomString(4)

	newTable := c.client.DatasetInProject(c.projectID, c.datasetID).Table(dummyTable)

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
	insertQuery := c.client.Query(fmt.Sprintf("INSERT INTO %s VALUES(true)", dummyTable))
	insertQuery.DefaultDatasetID = c.datasetID
	insertQuery.DefaultProjectID = c.projectID
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

func NewBigQueryConnector(ctx context.Context, config *protos.BigqueryConfig) (*BigQueryConnector, error) {
	logger := logger.LoggerFromCtx(ctx)

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
		logger.Error("failed to get dataset metadata", "error", datasetErr)
		return nil, fmt.Errorf("failed to get dataset metadata: %v", datasetErr)
	}

	storageClient, err := bqsa.CreateStorageClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	catalogPool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog connection pool: %v", err)
	}

	return &BigQueryConnector{
		bqConfig:         config,
		client:           client,
		datasetID:        datasetID,
		projectID:        projectID,
		PostgresMetadata: metadataStore.NewPostgresMetadataFromCatalog(logger, catalogPool),
		storageClient:    storageClient,
		catalogPool:      catalogPool,
		logger:           logger,
	}, nil
}

// Close closes the BigQuery driver.
func (c *BigQueryConnector) Close() error {
	if c != nil {
		return c.client.Close()
	}
	return nil
}

// ConnectionActive returns nil if the connection is active.
func (c *BigQueryConnector) ConnectionActive(ctx context.Context) error {
	_, err := c.client.DatasetInProject(c.projectID, c.datasetID).Metadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to get dataset metadata: %v", err)
	}

	return nil
}

func (c *BigQueryConnector) waitForTableReady(ctx context.Context, datasetTable *datasetTable) error {
	table := c.client.DatasetInProject(c.projectID, datasetTable.dataset).Table(datasetTable.table)
	maxDuration := 5 * time.Minute
	deadline := time.Now().Add(maxDuration)
	sleepInterval := 5 * time.Second
	attempt := 0

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout reached while waiting for table %s to be ready", datasetTable)
		}

		_, err := table.Metadata(ctx)
		if err == nil {
			return nil
		}

		c.logger.Info("waiting for table to be ready",
			slog.String("table", datasetTable.table), slog.Int("attempt", attempt))
		attempt++
		time.Sleep(sleepInterval)
	}
}

// ReplayTableSchemaDeltas changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *BigQueryConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

	AddedColumnsLoop:
		for _, addedColumn := range schemaDelta.AddedColumns {
			dstDatasetTable, _ := c.convertToDatasetTable(schemaDelta.DstTableName)
			table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)
			dstMetadata, metadataErr := table.Metadata(ctx)
			if metadataErr != nil {
				return fmt.Errorf("failed to get metadata for table %s: %w", schemaDelta.DstTableName, metadataErr)
			}

			// check if the column already exists
			for _, field := range dstMetadata.Schema {
				if field.Name == addedColumn.Name {
					c.logger.Info(fmt.Sprintf("[schema delta replay] column %s already exists in table %s",
						addedColumn.Name, schemaDelta.DstTableName))
					continue AddedColumnsLoop
				}
			}

			addedColumnBigQueryType := qValueKindToBigQueryTypeString(addedColumn, schemaDelta.NullableEnabled, false)
			query := c.queryWithLogging(fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN IF NOT EXISTS `%s` %s",
				dstDatasetTable.table, addedColumn.Name, addedColumnBigQueryType))
			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = dstDatasetTable.dataset
			_, err := query.Read(ctx)
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s to table %s",
				addedColumn.Name, addedColumnBigQueryType, schemaDelta.DstTableName))
		}
	}

	return nil
}

func (c *BigQueryConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	batchId int64,
) ([]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	query := fmt.Sprintf(`SELECT DISTINCT _peerdb_destination_table_name FROM %s
	 WHERE _peerdb_batch_id = %d`,
		rawTableName, batchId)
	// Run the query
	q := c.client.Query(query)
	q.DefaultProjectID = c.projectID
	q.DefaultDatasetID = c.datasetID
	it, err := q.Read(ctx)
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

func (c *BigQueryConnector) getTableNametoUnchangedCols(
	ctx context.Context,
	flowJobName string,
	batchId int64,
) (map[string][]string, error) {
	rawTableName := c.getRawTableName(flowJobName)

	// Prepare the query to retrieve distinct tables in that batch
	// we want to only select the unchanged cols from UpdateRecords, as we have a workaround
	// where a placeholder value for unchanged cols can be set in DeleteRecord if there is no backfill
	// we don't want these particular DeleteRecords to be used in the update statement
	query := fmt.Sprintf(`SELECT _peerdb_destination_table_name,
	array_agg(DISTINCT _peerdb_unchanged_toast_columns) as unchanged_toast_columns FROM %s
	 WHERE _peerdb_batch_id = %d AND _peerdb_record_type != 2
	 GROUP BY _peerdb_destination_table_name`,
		rawTableName, batchId)
	// Run the query
	q := c.client.Query(query)
	q.DefaultDatasetID = c.datasetID
	q.DefaultProjectID = c.projectID
	it, err := q.Read(ctx)
	if err != nil {
		err = fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
		return nil, err
	}
	// Create a map to store the results.
	resultMap := make(map[string][]string)

	// Process the query results using an iterator.
	for {
		var row struct {
			Tablename             string   `bigquery:"_peerdb_destination_table_name"`
			UnchangedToastColumns []string `bigquery:"unchanged_toast_columns"`
		}
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
func (c *BigQueryConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	c.logger.Info(fmt.Sprintf("pushing records to %s.%s...", c.datasetID, rawTableName))

	res, err := c.syncRecordsViaAvro(ctx, req, rawTableName, req.SyncBatchID)
	if err != nil {
		return nil, err
	}

	c.logger.Info(fmt.Sprintf("pushed %d records to %s.%s", res.NumRecordsSynced, c.datasetID, rawTableName))
	return res, nil
}

func (c *BigQueryConnector) syncRecordsViaAvro(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	rawTableName string,
	syncBatchID int64,
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReq := model.NewRecordsToStreamRequest(req.Records.GetRecords(), tableNameRowsMapping, syncBatchID)
	stream, err := utils.RecordsToRawTableStream(streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to convert records to raw table stream: %w", err)
	}

	avroSync := NewQRepAvroSyncMethod(c, req.StagingPath, req.FlowJobName)
	rawTableMetadata, err := c.client.DatasetInProject(c.projectID, c.datasetID).Table(rawTableName).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata of destination table: %w", err)
	}

	res, err := avroSync.SyncRecords(ctx, req, rawTableName,
		rawTableMetadata, syncBatchID, stream, streamReq.TableMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to sync records via avro: %w", err)
	}

	return res, nil
}

// NormalizeRecords normalizes raw table to destination table,
// one batch at a time from the previous normalized batch to the currently synced batch.
func (c *BigQueryConnector) NormalizeRecords(ctx context.Context, req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	rawTableName := c.getRawTableName(req.FlowJobName)

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
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

	for batchId := normBatchID + 1; batchId <= req.SyncBatchID; batchId++ {
		mergeErr := c.mergeTablesInThisBatch(ctx, batchId,
			req.FlowJobName, rawTableName, req.TableNameSchemaMapping,
			&protos.PeerDBColumns{
				SoftDeleteColName: req.SoftDeleteColName,
				SyncedAtColName:   req.SyncedAtColName,
			})
		if mergeErr != nil {
			return nil, mergeErr
		}

		err = c.UpdateNormalizeBatchID(ctx, req.FlowJobName, batchId)
		if err != nil {
			return nil, err
		}
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

func (c *BigQueryConnector) runMergeStatement(ctx context.Context, datasetID string, mergeStmt string) error {
	q := c.client.Query(mergeStmt)
	q.DefaultProjectID = c.projectID
	q.DefaultDatasetID = datasetID
	if _, err := q.Read(ctx); err != nil {
		return fmt.Errorf("failed to execute merge statement %s: %v", mergeStmt, err)
	}
	return nil
}

func (c *BigQueryConnector) mergeTablesInThisBatch(
	ctx context.Context,
	batchId int64,
	flowName string,
	rawTableName string,
	tableToSchema map[string]*protos.TableSchema,
	peerdbColumns *protos.PeerDBColumns,
) error {
	tableNames, err := c.getDistinctTableNamesInBatch(
		ctx,
		flowName,
		batchId,
	)
	if err != nil {
		return fmt.Errorf("couldn't get distinct table names to normalize: %w", err)
	}

	tableNametoUnchangedToastCols, err := c.getTableNametoUnchangedCols(
		ctx,
		flowName,
		batchId,
	)
	if err != nil {
		return fmt.Errorf("couldn't get tablename to unchanged cols mapping: %w", err)
	}

	mergeGen := &mergeStmtGenerator{
		rawDatasetTable: datasetTable{
			project: c.projectID,
			dataset: c.datasetID,
			table:   rawTableName,
		},
		tableSchemaMapping: tableToSchema,
		mergeBatchId:       batchId,
		peerdbCols:         peerdbColumns,
		shortColumn:        map[string]string{},
	}

	for _, tableName := range tableNames {
		unchangedToastColumns := tableNametoUnchangedToastCols[tableName]
		dstDatasetTable, _ := c.convertToDatasetTable(tableName)

		// normalize anything between last normalized batch id to last sync batchid
		// TODO (kaushik): This is so that the statement size for individual merge statements
		// doesn't exceed the limit. We should make this configurable.
		const batchSize = 8
		chunkNumber := 0
		if len(unchangedToastColumns) == 0 {
			c.logger.Info("running single merge statement", slog.String("table", tableName))
			mergeStmt := mergeGen.generateMergeStmt(tableName, dstDatasetTable, nil)
			if err := c.runMergeStatement(ctx, dstDatasetTable.dataset, mergeStmt); err != nil {
				return err
			}
		} else {
			for chunk := range slices.Chunk(unchangedToastColumns, batchSize) {
				chunkNumber += 1
				c.logger.Info("running merge statement", slog.Int("chunk", chunkNumber), slog.String("table", tableName))
				mergeStmt := mergeGen.generateMergeStmt(tableName, dstDatasetTable, chunk)
				if err := c.runMergeStatement(ctx, dstDatasetTable.dataset, mergeStmt); err != nil {
					return err
				}
			}
		}
	}

	// append all the statements to one list
	c.logger.Info(fmt.Sprintf("merged raw records to corresponding tables: %s %s %v",
		c.datasetID, rawTableName, tableNames))
	return nil
}

// CreateRawTable creates a raw table, implementing the Connector interface.
// create a table with the following schema
// _peerdb_uid STRING
// _peerdb_timestamp TIMESTAMP
// _peerdb_data STRING
// _peerdb_record_type INT - 0 for insert, 1 for update, 2 for delete
// _peerdb_match_data STRING - json of the match data (only for update and delete)
func (c *BigQueryConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
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
	tableRef, err := table.Metadata(ctx)
	if err == nil {
		// table exists, check if the schema matches
		if !reflect.DeepEqual(tableRef.Schema, schema) {
			c.logger.Error("raw table already exists with different schema",
				slog.String("table", rawTableName),
				slog.Any("existingSchema", tableRef.Schema),
				slog.Any("schema", schema))
			return nil, fmt.Errorf("raw table %s.%s already exists with different schema", c.datasetID, rawTableName)
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
	c.logger.Info("creating raw table",
		slog.String("table", rawTableName),
		slog.Any("metadata", metadata))
	err = table.Create(ctx, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, rawTableName, err)
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *BigQueryConnector) StartSetupNormalizedTables(_ context.Context) (interface{}, error) {
	// needed since CreateNormalizedTable duplicate check isn't accurate enough
	return make(map[datasetTable]struct{}), nil
}

func (c *BigQueryConnector) FinishSetupNormalizedTables(_ context.Context, _ interface{}) error {
	return nil
}

func (c *BigQueryConnector) CleanupSetupNormalizedTables(_ context.Context, _ interface{}) {
}

// This runs CREATE TABLE IF NOT EXISTS on bigquery, using the schema and table name provided.
func (c *BigQueryConnector) SetupNormalizedTable(
	ctx context.Context,
	tx interface{},
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
) (bool, error) {
	tableSchema := config.TableNameSchemaMapping[tableIdentifier]
	datasetTablesSet := tx.(map[datasetTable]struct{})

	// only place where we check for parsing errors
	datasetTable, err := c.convertToDatasetTable(tableIdentifier)
	if err != nil {
		return false, err
	}
	_, ok := datasetTablesSet[datasetTable]
	if ok {
		return false, fmt.Errorf("invalid mirror: two tables mirror to the same BigQuery table %s",
			datasetTable.string())
	}
	datasetTablesSet[datasetTable] = struct{}{}
	dataset := c.client.DatasetInProject(c.projectID, datasetTable.dataset)
	_, err = dataset.Metadata(ctx)
	// just assume this means dataset don't exist, and create it
	if err != nil {
		// if err message does not contain `notFound`, then other error happened.
		if !strings.Contains(err.Error(), "notFound") {
			return false, fmt.Errorf("error while checking metadata for BigQuery dataset %s: %w",
				datasetTable.dataset, err)
		}
		c.logger.Info(fmt.Sprintf("creating dataset %s...", dataset.DatasetID))
		if err := dataset.Create(ctx, nil); err != nil {
			return false, fmt.Errorf("failed to create BigQuery dataset %s: %w", dataset.DatasetID, err)
		}
	}
	table := dataset.Table(datasetTable.table)

	// check if the table exists
	existingMetadata, err := table.Metadata(ctx)
	if err == nil {
		// table exists, go to next table
		c.logger.Info("[bigquery] table already exists, skipping",
			slog.String("table", tableIdentifier),
			slog.Any("existingMetadata", existingMetadata))
		return true, nil
	}

	// convert the column names and types to bigquery types
	columns := make([]*bigquery.FieldSchema, 0, len(tableSchema.Columns)+2)
	for _, column := range tableSchema.Columns {
		bqFieldSchema := qValueKindToBigQueryType(column, tableSchema.NullableEnabled)
		columns = append(columns, &bqFieldSchema)
	}

	if config.SoftDeleteColName != "" {
		columns = append(columns, &bigquery.FieldSchema{
			Name:                   config.SoftDeleteColName,
			Type:                   bigquery.BooleanFieldType,
			Repeated:               false,
			DefaultValueExpression: "false",
		})
	}

	if config.SyncedAtColName != "" {
		columns = append(columns, &bigquery.FieldSchema{
			Name:     config.SyncedAtColName,
			Type:     bigquery.TimestampFieldType,
			Repeated: false,
		})
	}

	// create the table using the columns
	schema := bigquery.Schema(columns)

	supportedPkeyCols := obtainClusteringColumns(tableSchema)
	// cluster by the supported primary keys if < 4 columns.
	numSupportedPkeyCols := len(supportedPkeyCols)
	var clustering *bigquery.Clustering
	if numSupportedPkeyCols > 0 && numSupportedPkeyCols < 4 {
		clustering = &bigquery.Clustering{
			Fields: supportedPkeyCols,
		}
	}

	timePartitionEnabled, err := peerdbenv.PeerDBBigQueryEnableSyncedAtPartitioning(ctx, config.Env)
	if err != nil {
		return false, fmt.Errorf("failed to get dynamic setting for BigQuery time partitioning: %w", err)
	}
	var timePartitioning *bigquery.TimePartitioning
	if timePartitionEnabled && config.SyncedAtColName != "" {
		timePartitioning = &bigquery.TimePartitioning{
			Type:  bigquery.DayPartitioningType,
			Field: config.SyncedAtColName,
		}
	}

	metadata := &bigquery.TableMetadata{
		Schema:           schema,
		Name:             datasetTable.table,
		Clustering:       clustering,
		TimePartitioning: timePartitioning,
	}

	if config.IsResync {
		_, existsErr := table.Metadata(ctx)
		if existsErr == nil {
			c.logger.Info("[bigquery] deleting existing resync table",
				slog.String("table", tableIdentifier))
			deleteErr := table.Delete(ctx)
			if deleteErr != nil {
				return false, fmt.Errorf("failed to delete table %s: %w", tableIdentifier, deleteErr)
			}
		} else if !strings.Contains(existsErr.Error(), "notFound") {
			return false, fmt.Errorf("error while checking metadata for BigQuery resynced table %s: %w",
				tableIdentifier, existsErr)
		}
	}

	c.logger.Info("[bigquery] creating table",
		slog.String("table", tableIdentifier),
		slog.Any("metadata", metadata))
	err = table.Create(ctx, metadata)
	if err != nil {
		return false, fmt.Errorf("failed to create table %s: %w", tableIdentifier, err)
	}

	datasetTablesSet[datasetTable] = struct{}{}
	return false, nil
}

func (c *BigQueryConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName)
	if err != nil {
		return fmt.Errorf("unable to clear metadata for sync flow cleanup: %w", err)
	}

	dataset := c.client.DatasetInProject(c.projectID, c.datasetID)
	rawTableHandle := dataset.Table(c.getRawTableName(jobName))
	// check if exists, then delete
	_, err = rawTableHandle.Metadata(ctx)
	if err == nil {
		c.logger.Info("deleting raw table", slog.String("table", rawTableHandle.FullyQualifiedName()))
		deleteErr := rawTableHandle.Delete(ctx)
		if deleteErr != nil {
			return fmt.Errorf("failed to delete raw table: %w", deleteErr)
		}
	}

	return nil
}

// getRawTableName returns the raw table name for the given table identifier.
func (c *BigQueryConnector) getRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *BigQueryConnector) RenameTables(ctx context.Context, req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	// BigQuery doesn't really do transactions properly anyway so why bother?
	for _, renameRequest := range req.RenameTableOptions {
		srcDatasetTable, _ := c.convertToDatasetTable(renameRequest.CurrentName)
		dstDatasetTable, _ := c.convertToDatasetTable(renameRequest.NewName)
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", srcDatasetTable.string(),
			dstDatasetTable.string()))

		// if _resync table does not exist, log and continue.
		dataset := c.client.DatasetInProject(c.projectID, srcDatasetTable.dataset)
		_, err := dataset.Table(srcDatasetTable.table).Metadata(ctx)
		if err != nil {
			if !strings.Contains(err.Error(), "notFound") {
				return nil, fmt.Errorf("[renameTable] failed to get metadata for _resync table %s: %w", srcDatasetTable.string(), err)
			}
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping...", srcDatasetTable.string()))
			continue
		}

		// if the original table does not exist, log and skip soft delete step
		originalTableExists := true
		_, err = dataset.Table(dstDatasetTable.table).Metadata(ctx)
		if err != nil {
			if !strings.Contains(err.Error(), "notFound") {
				return nil, fmt.Errorf("[renameTable] failed to get metadata for original table %s: %w", dstDatasetTable.string(), err)
			}
			originalTableExists = false
			c.logger.Info(fmt.Sprintf("original table '%s' does not exist, skipping soft delete step...", dstDatasetTable.string()))
		}

		if originalTableExists {
			// For a table with replica identity full and a JSON column
			// the equals to comparison we do down below will fail
			// so we need to use TO_JSON_STRING for those columns
			columnIsJSON := make(map[string]bool, len(renameRequest.TableSchema.Columns))
			columnNames := make([]string, 0, len(renameRequest.TableSchema.Columns))
			for _, col := range renameRequest.TableSchema.Columns {
				quotedCol := "`" + col.Name + "`"
				columnNames = append(columnNames, quotedCol)
				columnIsJSON[quotedCol] = (col.Type == "json" || col.Type == "jsonb")
			}

			if req.SoftDeleteColName != "" {
				allColsBuilder := strings.Builder{}
				for idx, col := range columnNames {
					allColsBuilder.WriteString("_pt.")
					allColsBuilder.WriteString(col)
					if idx < len(columnNames)-1 {
						allColsBuilder.WriteString(",")
					}
				}

				allColsWithoutAlias := strings.Join(columnNames, ",")
				allColsWithAlias := allColsBuilder.String()

				pkeyCols := make([]string, 0, len(renameRequest.TableSchema.PrimaryKeyColumns))
				for _, pkeyCol := range renameRequest.TableSchema.PrimaryKeyColumns {
					pkeyCols = append(pkeyCols, "`"+pkeyCol+"`")
				}

				c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dstDatasetTable.string()))

				pkeyOnClauseBuilder := strings.Builder{}
				ljWhereClauseBuilder := strings.Builder{}
				for idx, col := range pkeyCols {
					if columnIsJSON[col] {
						// We need to use TO_JSON_STRING for comparing JSON columns
						pkeyOnClauseBuilder.WriteString("TO_JSON_STRING(_pt.")
						pkeyOnClauseBuilder.WriteString(col)
						pkeyOnClauseBuilder.WriteString(")=TO_JSON_STRING(_resync.")
						pkeyOnClauseBuilder.WriteString(col)
						pkeyOnClauseBuilder.WriteString(")")
					} else {
						pkeyOnClauseBuilder.WriteString("_pt.")
						pkeyOnClauseBuilder.WriteString(col)
						pkeyOnClauseBuilder.WriteString("=_resync.")
						pkeyOnClauseBuilder.WriteString(col)
					}

					ljWhereClauseBuilder.WriteString("_resync.")
					ljWhereClauseBuilder.WriteString(col)
					ljWhereClauseBuilder.WriteString(" IS NULL")

					if idx < len(pkeyCols)-1 {
						pkeyOnClauseBuilder.WriteString(" AND ")
						ljWhereClauseBuilder.WriteString(" AND ")
					}
				}

				leftJoin := fmt.Sprintf("LEFT JOIN %s _resync ON %s WHERE %s", srcDatasetTable.string(),
					pkeyOnClauseBuilder.String(), ljWhereClauseBuilder.String())

				q := fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s _pt %s",
					srcDatasetTable.string(), fmt.Sprintf("%s,%s", allColsWithoutAlias, req.SoftDeleteColName),
					allColsWithAlias, req.SoftDeleteColName, dstDatasetTable.string(),
					leftJoin)

				query := c.queryWithLogging(q)

				query.DefaultProjectID = c.projectID
				query.DefaultDatasetID = c.datasetID
				_, err := query.Read(ctx)
				if err != nil {
					return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dstDatasetTable.string(), err)
				}
			}
		}

		if req.SyncedAtColName != "" {
			c.logger.Info(fmt.Sprintf("setting synced at column for table '%s'...", srcDatasetTable.string()))

			query := c.client.Query(
				fmt.Sprintf("UPDATE %s SET %s = CURRENT_TIMESTAMP WHERE %s IS NULL", srcDatasetTable.string(),
					req.SyncedAtColName, req.SyncedAtColName))

			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = c.datasetID
			_, err := query.Read(ctx)
			if err != nil {
				return nil, fmt.Errorf("unable to set synced at column for table %s: %w", srcDatasetTable.string(), err)
			}
		}

		// drop the dst table if exists
		dropQuery := c.queryWithLogging("DROP TABLE IF EXISTS " + dstDatasetTable.string())
		dropQuery.DefaultProjectID = c.projectID
		dropQuery.DefaultDatasetID = c.datasetID
		_, err = dropQuery.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dstDatasetTable.string(), err)
		}

		// rename the src table to dst
		query := c.queryWithLogging(fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table))
		query.DefaultProjectID = c.projectID
		query.DefaultDatasetID = c.datasetID
		_, err = query.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", srcDatasetTable.string(),
				dstDatasetTable.string(), err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", srcDatasetTable.string(),
			dstDatasetTable.string()))
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *BigQueryConnector) CreateTablesFromExisting(
	ctx context.Context,
	req *protos.CreateTablesFromExistingInput,
) (*protos.CreateTablesFromExistingOutput, error) {
	for newTable, existingTable := range req.NewToExistingTableMapping {
		newDatasetTable, _ := c.convertToDatasetTable(newTable)
		existingDatasetTable, _ := c.convertToDatasetTable(existingTable)
		c.logger.Info(fmt.Sprintf("creating table '%s' similar to '%s'", newTable, existingTable))

		// rename the src table to dst
		query := c.queryWithLogging(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` LIKE `%s`",
			newDatasetTable.string(), existingDatasetTable.string()))
		query.DefaultProjectID = c.projectID
		query.DefaultDatasetID = c.datasetID
		_, err := query.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to create table %s: %w", newTable, err)
		}

		c.logger.Info(fmt.Sprintf("successfully created table '%s'", newTable))
	}

	return &protos.CreateTablesFromExistingOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *BigQueryConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	removedTables *protos.RemoveTablesFromRawTableInput,
) error {
	rawTableIdentifier := c.getRawTableName(removedTables.FlowJobName)
	for _, tableName := range removedTables.DestinationTableNames {
		c.logger.Info(fmt.Sprintf("removing entries for table '%s' from raw table...", tableName))
		deleteCmd := c.queryWithLogging(fmt.Sprintf("DELETE FROM `%s` WHERE _peerdb_destination_table_name = '%s'",
			rawTableIdentifier, tableName))
		deleteCmd.DefaultProjectID = c.projectID
		deleteCmd.DefaultDatasetID = c.datasetID
		_, err := deleteCmd.Read(ctx)
		if err != nil {
			c.logger.Error("failed to remove entries from raw table", "error", err)
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
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

func (c *BigQueryConnector) convertToDatasetTable(tableName string) (datasetTable, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		return datasetTable{
			dataset: c.datasetID,
			table:   parts[0],
		}, nil
	} else if len(parts) == 2 {
		return datasetTable{
			dataset: parts[0],
			table:   parts[1],
		}, nil
	} else if len(parts) == 3 {
		return datasetTable{
			project: parts[0],
			dataset: parts[1],
			table:   parts[2],
		}, nil
	} else {
		return datasetTable{}, fmt.Errorf("invalid BigQuery table name: %s", tableName)
	}
}

func (c *BigQueryConnector) queryWithLogging(query string) *bigquery.Query {
	c.logger.Info("[biguery] executing DDL statement", slog.String("query", query))
	return c.client.Query(query)
}
