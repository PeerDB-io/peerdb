package connbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/credentials/downscope"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"github.com/google/uuid"
	"go.temporal.io/sdk/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	SyncRecordsBatchSize = 1024
)

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

type BigQueryConnector struct {
	*metadataStore.PostgresMetadata
	logger        log.Logger
	bqConfig      *protos.BigqueryConfig
	credentials   *auth.Credentials
	client        *bigquery.Client
	storageClient *storage.Client
	catalogPool   shared.CatalogPool
	datasetID     string
	projectID     string
}

func (c *BigQueryConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	// todo should we vaidate initial copy settings?

	if !cfg.InitialSnapshotOnly || !cfg.DoInitialSnapshot {
		return fmt.Errorf("BigQuery source connector only supports initial snapshot flows. CDC is not supported")
	}

	// todo is this a right place and way to do it? this currently won't work due to package dependency cycle
	//_, err := connectors.GetByNameAs[connectors.QRepSyncDownloadableObjectsConnector](ctx, cfg.Env, c.catalogPool, cfg.DestinationName)
	//if errors.Is(err, errors.ErrUnsupported) {
	//	return fmt.Errorf("destination connector %s is not supported for BigQuery source connector", cfg.DestinationName)
	//}

	for _, tableMapping := range cfg.TableMappings {
		dstDatasetTable, err := c.convertToDatasetTable(tableMapping.SourceTableIdentifier)
		if err != nil {
			return err
		}

		table := c.client.DatasetInProject(c.projectID, dstDatasetTable.dataset).Table(dstDatasetTable.table)

		if _, err := table.Metadata(ctx); err != nil {
			return fmt.Errorf("failed to get metadata for table %s: %w", tableMapping.DestinationTableIdentifier, err)
		}
	}

	if cfg.SnapshotStagingPath == "" {
		return fmt.Errorf("snapshot staging path is required for BigQuery source connector")
	}

	// todo refactor

	bucketUrl, err := url.Parse(cfg.SnapshotStagingPath)
	if err != nil {
		return fmt.Errorf("invalid snapshot staging path: %w", err)
	}

	if bucketUrl.Scheme != "gs" {
		return fmt.Errorf("invalid snapshot staging path: %s. Must start with gs://", cfg.SnapshotStagingPath)
	}

	bucketName := bucketUrl.Host
	prefix := strings.TrimPrefix(bucketUrl.Path, "/")
	bucket := c.storageClient.Bucket(bucketName)

	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	_, err = it.Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("failed to access snapshot staging path: %w", err)
	}

	return nil
}

func (c *BigQueryConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	var allTables []string

	datasetsIter := c.client.Datasets(ctx)
	datasetsIter.ProjectID = c.projectID

	for {
		dataset, err := datasetsIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list datasets: %w", err)
		}

		// Get all tables in this dataset
		tablesIter := dataset.Tables(ctx)
		for {
			table, err := tablesIter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list tables in dataset %s: %w", dataset.DatasetID, err)
			}

			// Format as dataset.table for BigQuery
			fullTableName := fmt.Sprintf("%s.%s", dataset.DatasetID, table.TableID)
			allTables = append(allTables, fullTableName)
		}
	}

	return &protos.AllTablesResponse{
		Tables: allTables,
	}, nil
}

func (c *BigQueryConnector) GetColumns(ctx context.Context, _ uint32, dataset string, table string) (*protos.TableColumnsResponse, error) {
	tableRef := c.client.DatasetInProject(c.projectID, dataset).Table(table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata for %s.%s: %w", dataset, table, err)
	}

	var columns []*protos.ColumnsItem
	for _, field := range metadata.Schema {
		colType := string(field.Type)
		qkind := string(BigQueryTypeToQValueKind(field))

		columns = append(columns, &protos.ColumnsItem{
			Name:  field.Name,
			Type:  colType,
			IsKey: false, // BigQuery doesn't have traditional primary keys
			Qkind: qkind,
		})
	}

	return &protos.TableColumnsResponse{
		Columns: columns,
	}, nil
}

func (c *BigQueryConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	var schemas []string

	// In BigQuery, datasets are equivalent to schemas
	datasetsIter := c.client.Datasets(ctx)
	datasetsIter.ProjectID = c.projectID

	for {
		dataset, err := datasetsIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list datasets: %w", err)
		}
		schemas = append(schemas, dataset.DatasetID)
	}

	return &protos.PeerSchemasResponse{
		Schemas: schemas,
	}, nil
}

func (c *BigQueryConnector) GetTablesInSchema(ctx context.Context, schema string, cdcEnabled bool) (*protos.SchemaTablesResponse, error) {
	dataset := c.client.DatasetInProject(c.projectID, schema)

	// Check if dataset exists
	if _, err := dataset.Metadata(ctx); err != nil {
		return nil, fmt.Errorf("failed to get dataset metadata for %s: %w", schema, err)
	}

	var tables []*protos.TableResponse
	tablesIter := dataset.Tables(ctx)
	for {
		table, err := tablesIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list tables in dataset %s: %w", schema, err)
		}

		// Get table metadata to calculate size
		metadata, err := table.Metadata(ctx)
		var tableSize string
		if err != nil {
			c.logger.Warn("failed to get table metadata for size calculation",
				slog.String("table", table.TableID),
				slog.Any("error", err))
			tableSize = "Unknown"
		} else {
			// Calculate human-readable table size from bytes
			tableSize = formatTableSize(metadata.NumBytes)
		}

		// For BigQuery source connector, all tables can be mirrored
		tableResponse := &protos.TableResponse{
			TableName: table.TableID,
			CanMirror: true,
			TableSize: tableSize,
		}
		tables = append(tables, tableResponse)
	}

	return &protos.SchemaTablesResponse{
		Tables: tables,
	}, nil
}

// formatTableSize converts bytes to human-readable format
// todo more somewhere
func formatTableSize(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp+1])
}

func NewBigQueryConnector(ctx context.Context, config *protos.BigqueryConfig) (*BigQueryConnector, error) {
	logger := internal.LoggerFromCtx(ctx)

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

	serviceAccount, err := NewBigQueryServiceAccount(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %w", err)
	}

	saJSON, err := json.Marshal(serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service account: %v", err)
	}

	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
		CredentialsJSON: saJSON,
		Scopes: []string{
			bigquery.Scope,
			storage.ScopeFullControl, // we should split it into two clients later
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials: %v", err)
	}

	client, err := bigquery.NewClient(
		ctx,
		bigquery.DetectProjectID,
		option.WithAuthCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	if _, err := client.DatasetInProject(projectID, datasetID).Metadata(ctx); err != nil {
		logger.Error("failed to get dataset metadata", "error", err)
		return nil, fmt.Errorf("failed to get dataset metadata: %v", err)
	}

	storageClient, err := storage.NewClient(ctx, option.WithAuthCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog connection pool: %v", err)
	}

	return &BigQueryConnector{
		credentials:      creds,
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

// ValidateCheck:
// 1. Creates a table
// 2. Inserts one row into the table
// 3. Deletes the table
func (c *BigQueryConnector) ValidateCheck(ctx context.Context) error {
	if _, err := c.client.DatasetInProject(c.projectID, c.datasetID).Metadata(ctx); err != nil {
		return fmt.Errorf("failed to get dataset metadata: %v", err)
	}

	return nil // todo: temporary. We need to distinguish between source and destination peers validation. For source peer we don't need to do the below checks

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

// Close closes the BigQuery driver.
func (c *BigQueryConnector) Close() error {
	if c != nil {
		return c.client.Close()
	}
	return nil
}

// ConnectionActive returns nil if the connection is active.
func (c *BigQueryConnector) ConnectionActive(ctx context.Context) error {
	if _, err := c.client.DatasetInProject(c.projectID, c.datasetID).Metadata(ctx); err != nil {
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
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout reached while waiting for table %s to be ready", datasetTable)
		}
		if _, err := table.Metadata(ctx); err == nil {
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
	env map[string]string,
	flowJobName string,
	_ []*protos.TableMapping,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

	AddedColumnsLoop:
		for _, addedColumn := range schemaDelta.AddedColumns {
			dstDatasetTable, err := c.convertToDatasetTable(schemaDelta.DstTableName)
			if err != nil {
				return err
			}

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
				"ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `%s` %s",
				dstDatasetTable.table, addedColumn.Name, addedColumnBigQueryType))
			query.DefaultProjectID = c.projectID
			query.DefaultDatasetID = dstDatasetTable.dataset
			if _, err := query.Read(ctx); err != nil {
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
	tableToSchema map[string]*protos.TableSchema,
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
		return nil, fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
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
			if _, ok := tableToSchema[value]; ok {
				distinctTableNames = append(distinctTableNames, value)
			} else {
				c.logger.Warn("table not found in table to schema mapping", "table", value)
			}
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
		return nil, fmt.Errorf("failed to run query %s on BigQuery:\n %w", query, err)
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
	streamReq := model.NewRecordsToStreamRequest(
		req.Records.GetRecords(), tableNameRowsMapping, syncBatchID, false, protos.DBType_BIGQUERY,
	)
	stream, err := utils.RecordsToRawTableStream(streamReq, nil)
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
func (c *BigQueryConnector) NormalizeRecords(ctx context.Context, req *model.NormalizeRecordsRequest) (model.NormalizeResponse, error) {
	unchangedToastMergeChunking, err := internal.PeerDBBigQueryToastMergeChunking(ctx, req.Env)
	if err != nil {
		c.logger.Warn("failed to load PEERDB_BIGQUERY_TOAST_MERGE_CHUNKING, continuing with 8", slog.Any("error", err))
		unchangedToastMergeChunking = 8
	} else if unchangedToastMergeChunking == 0 {
		unchangedToastMergeChunking = 8
	}

	rawTableName := c.getRawTableName(req.FlowJobName)

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return model.NormalizeResponse{}, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		return model.NormalizeResponse{
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	for batchId := normBatchID + 1; batchId <= req.SyncBatchID; batchId++ {
		if err := c.mergeTablesInThisBatch(ctx, batchId,
			req.FlowJobName, rawTableName, req.TableNameSchemaMapping, unchangedToastMergeChunking,
			&protos.PeerDBColumns{SoftDeleteColName: req.SoftDeleteColName, SyncedAtColName: req.SyncedAtColName},
		); err != nil {
			return model.NormalizeResponse{}, err
		}

		if err := c.UpdateNormalizeBatchID(ctx, req.FlowJobName, batchId); err != nil {
			return model.NormalizeResponse{}, err
		}
	}

	return model.NormalizeResponse{
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
	unchangedToastMergeChunking uint32,
	peerdbColumns *protos.PeerDBColumns,
) error {
	tableNames, err := c.getDistinctTableNamesInBatch(
		ctx,
		flowName,
		batchId,
		tableToSchema,
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
		dstDatasetTable, err := c.convertToDatasetTable(tableName)
		if err != nil {
			return err
		}

		// normalize anything between last normalized batch id to last sync batchid
		if len(unchangedToastColumns) == 0 {
			c.logger.Info("running single merge statement", slog.String("table", tableName))
			mergeStmt := mergeGen.generateMergeStmt(tableName, dstDatasetTable, nil)
			if err := c.runMergeStatement(ctx, dstDatasetTable.dataset, mergeStmt); err != nil {
				return err
			}
		} else {
			// This is so that the statement size for individual merge statements
			// doesn't exceed the limit
			chunkNumber := 0
			for chunk := range slices.Chunk(unchangedToastColumns, int(unchangedToastMergeChunking)) {
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
	if err := table.Create(ctx, metadata); err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", c.datasetID, rawTableName, err)
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: rawTableName,
	}, nil
}

func (c *BigQueryConnector) StartSetupNormalizedTables(_ context.Context) (any, error) {
	// needed since CreateNormalizedTable duplicate check isn't accurate enough
	return make(map[datasetTable]struct{}), nil
}

func (c *BigQueryConnector) FinishSetupNormalizedTables(_ context.Context, _ any) error {
	return nil
}

func (c *BigQueryConnector) CleanupSetupNormalizedTables(_ context.Context, _ any) {
}

// This runs CREATE TABLE IF NOT EXISTS on bigquery, using the schema and table name provided.
func (c *BigQueryConnector) SetupNormalizedTable(
	ctx context.Context,
	tx any,
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
) (bool, error) {
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
	// just assume this means dataset don't exist, and create it
	if _, err := dataset.Metadata(ctx); err != nil {
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
		if config.IsResync {
			c.logger.Info("[bigquery] deleting existing resync table",
				slog.String("table", tableIdentifier))
			deleteErr := table.Delete(ctx)
			if deleteErr != nil {
				return false, fmt.Errorf("failed to delete table %s: %w", tableIdentifier, deleteErr)
			}
		} else {
			// table exists, go to next table
			c.logger.Info("[bigquery] table already exists, skipping",
				slog.String("table", tableIdentifier),
				slog.Any("existingMetadata", existingMetadata))
			return true, nil
		}
	} else if !strings.Contains(err.Error(), "notFound") {
		return false, fmt.Errorf("error while checking metadata for BigQuery table existence %s: %w",
			tableIdentifier, err)
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

	timePartitionEnabled, err := internal.PeerDBBigQueryEnableSyncedAtPartitioning(ctx, config.Env)
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

	c.logger.Info("[bigquery] creating table",
		slog.String("table", tableIdentifier),
		slog.Any("metadata", metadata))
	if err := table.Create(ctx, metadata); err != nil {
		return false, fmt.Errorf("failed to create table %s: %w", tableIdentifier, err)
	}

	datasetTablesSet[datasetTable] = struct{}{}
	return false, nil
}

func (c *BigQueryConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	dataset := c.client.DatasetInProject(c.projectID, c.datasetID)
	rawTableHandle := dataset.Table(c.getRawTableName(jobName))
	// check if exists, then delete
	if _, err := rawTableHandle.Metadata(ctx); err == nil {
		c.logger.Info("[bigquery] deleting raw table", slog.String("table", rawTableHandle.FullyQualifiedName()))
		deleteErr := rawTableHandle.Delete(ctx)
		if deleteErr != nil {
			return fmt.Errorf("[bigquery] failed to delete raw table: %w", deleteErr)
		}
	}

	return nil
}

// getRawTableName returns the raw table name for the given table identifier.
func (c *BigQueryConnector) getRawTableName(flowJobName string) string {
	return "_peerdb_raw_" + shared.ReplaceIllegalCharactersWithUnderscores(flowJobName)
}

func (c *BigQueryConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
	// BigQuery doesn't really do transactions properly anyway so why bother?
	for _, renameRequest := range req.RenameTableOptions {
		srcDatasetTable, _ := c.convertToDatasetTable(renameRequest.CurrentName)
		dstDatasetTable, _ := c.convertToDatasetTable(renameRequest.NewName)
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", srcDatasetTable.string(),
			dstDatasetTable.string()))

		// if _resync table does not exist, log and continue.
		dataset := c.client.DatasetInProject(c.projectID, srcDatasetTable.dataset)
		if _, err := dataset.Table(srcDatasetTable.table).Metadata(ctx); err != nil {
			if !strings.Contains(err.Error(), "notFound") {
				return nil, fmt.Errorf("[renameTable] failed to get metadata for _resync table %s: %w", srcDatasetTable.string(), err)
			}
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping...", srcDatasetTable.string()))
			continue
		}

		// if the original table does not exist, log and skip soft delete step
		originalTableExists := true
		if _, err := dataset.Table(dstDatasetTable.table).Metadata(ctx); err != nil {
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
			tableSchema := tableNameSchemaMapping[renameRequest.CurrentName]
			columnIsJSON := make(map[string]bool, len(tableSchema.Columns))
			columnNames := make([]string, 0, len(tableSchema.Columns))
			for _, col := range tableSchema.Columns {
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

				pkeyCols := make([]string, 0, len(tableSchema.PrimaryKeyColumns))
				for _, pkeyCol := range tableSchema.PrimaryKeyColumns {
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
				if _, err := query.Read(ctx); err != nil {
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
			if _, err := query.Read(ctx); err != nil {
				return nil, fmt.Errorf("unable to set synced at column for table %s: %w", srcDatasetTable.string(), err)
			}
		}

		// drop the dst table if exists
		dropQuery := c.queryWithLogging("DROP TABLE IF EXISTS " + dstDatasetTable.string())
		dropQuery.DefaultProjectID = c.projectID
		dropQuery.DefaultDatasetID = c.datasetID
		if _, err := dropQuery.Read(ctx); err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dstDatasetTable.string(), err)
		}

		// rename the src table to dst
		query := c.queryWithLogging(fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			srcDatasetTable.string(), dstDatasetTable.table))
		query.DefaultProjectID = c.projectID
		query.DefaultDatasetID = c.datasetID
		if _, err := query.Read(ctx); err != nil {
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
		if _, err := query.Read(ctx); err != nil {
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
	req *protos.RemoveTablesFromRawTableInput,
) error {
	rawTableIdentifier := c.getRawTableName(req.FlowJobName)
	for _, tableName := range req.DestinationTableNames {
		c.logger.Info(fmt.Sprintf("removing entries for table '%s' from raw table...", tableName))
		deleteCmd := c.queryWithLogging(fmt.Sprintf("DELETE FROM `%s` WHERE _peerdb_destination_table_name = '%s'"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			rawTableIdentifier, tableName, req.NormalizeBatchId, req.SyncBatchId))
		deleteCmd.DefaultProjectID = c.projectID
		deleteCmd.DefaultDatasetID = c.datasetID
		if _, err := deleteCmd.Read(ctx); err != nil {
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

func (c *BigQueryConnector) PullQRepObjects(ctx context.Context, _ *otel_metrics.OtelManager, config *protos.QRepConfig, partition *protos.QRepPartition, stream *model.QObjectStream) (int64, int64, error) {
	stream.SetFormat("Avro")

	schema, err := c.getQRepSchema(ctx, config)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get QRep schema: %w", err)
	}
	stream.SetSchema(schema)

	// todo: refactor duplicate code
	stagingURL, err := url.Parse(config.StagingPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse staging path %s: %w", config.StagingPath, err)
	}
	tableURL := stagingURL.JoinPath(config.WatermarkTable) // source table identifier
	bucketName := stagingURL.Host
	prefix := strings.TrimPrefix(tableURL.Path, "/") + "/" // ensure: "aa/bb/"

	if partition == nil || partition.Range == nil {
		return 0, 0, fmt.Errorf("partition and partition range must be provided")
	}

	objectRange := partition.Range.GetObjectIdRange()
	if objectRange == nil {
		return 0, 0, fmt.Errorf("invalid partition range")
	}

	bucket := c.storageClient.Bucket(bucketName)

	startOffset := objectRange.Start
	endOffset := objectRange.End
	if endOffset == startOffset {
		// If startOffset and endOffset are the same,
		// we can assume it's the last partition,
		// and it consists of one object only.
		// Since EndOffset is exclusive,
		// we set it to empty string to ensure last
		// object is included in the listing.
		endOffset = ""
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		Delimiter:   "/",
		StartOffset: objectRange.Start,
		EndOffset:   objectRange.End,
	})

	accessBoundary := []downscope.AccessBoundaryRule{
		{
			AvailableResource:    fmt.Sprintf("//storage.googleapis.com/projects/_/buckets/%s", bucketName),
			AvailablePermissions: []string{"inRole:roles/storage.objectViewer"},
			Condition: &downscope.AvailabilityCondition{
				Expression: fmt.Sprintf("resource.name.startsWith('projects/_/buckets/%s/objects/%s')", bucketName, prefix),
			},
		},
	}

	// todo: token can expire during long listing operation, need to handle that
	//       by refreshing the token if needed.

	tp, err := downscope.NewCredentials(&downscope.Options{Credentials: c.credentials, Rules: accessBoundary})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate downscoped token provider: %w", err)
	}

	token, err := tp.Token(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate downscoped token: %w", err)
	}

	urlHeaders := make(http.Header)
	urlHeaders.Add("Authorization", "Bearer "+token.Value)

	var totalBytes int64
	var totalObjects int64
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			c.logger.Debug("finished listing objects in bucket",
				slog.String("bucket", bucketName),
				slog.String("prefix", prefix),
				slog.Int64("totalObjects", totalObjects),
				slog.Int64("totalBytes", totalBytes))
			break
		}
		if err != nil {
			return 0, 0, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		if attrs.Name == "" {
			continue // skip if no name, todo: figure out why this happens
		}

		stream.Objects <- &model.Object{
			URL:     fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucketName, url.PathEscape(attrs.Name)),
			Size:    attrs.Size,
			Headers: urlHeaders,
		}

		totalBytes += attrs.Size
		totalObjects += 1
	}

	close(stream.Objects)

	c.logger.Info("finished pulling downloadable objects",
		slog.String("bucket", bucketName),
		slog.String("prefix", prefix),
		slog.Int64("totalObjects", totalObjects),
		slog.Int64("totalBytes", totalBytes))

	return totalObjects, totalBytes, nil
}

func (c *BigQueryConnector) SetupReplication(ctx context.Context, input *protos.SetupReplicationInput) (model.SetupReplicationResult, error) {
	return model.SetupReplicationResult{}, nil
}

func (c *BigQueryConnector) GetQRepPartitions(ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	stagingURL, err := url.Parse(config.StagingPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse staging path %s: %w", config.StagingPath, err)
	}

	tableURL := stagingURL.JoinPath(config.WatermarkTable) // source table identifier

	bucketName := stagingURL.Host
	prefix := strings.TrimPrefix(tableURL.Path, "/") + "/" // ensure: "aa/bb/"

	bucket := c.storageClient.Bucket(bucketName)

	var startOffset string
	if last.Range != nil {
		objectRange := last.Range.GetObjectIdRange()
		if objectRange == nil {
			return nil, fmt.Errorf("invalid partition range type: %T", last.Range.Range)
		}
		startOffset = objectRange.Start
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		Delimiter:   "/",
		StartOffset: startOffset,
	})

	var partitions []*protos.QRepPartition
	var currentPartition *protos.QRepPartition
	var currentPartitionObjectsNum uint64

	// todo: from config.NumRowsPerPartition we know what is desired number of rows per partition
	//       ideally if we can estimate average object row count, we can set this dynamically
	//       to achieve the desired number of rows per partition.
	var numObjectPerPartition uint64 = 10

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s with prefix %s: %w", bucketName, prefix, err)
		}

		// we only want the first object after the start offset
		if attrs.Name == startOffset {
			continue
		}

		if currentPartition == nil {
			currentPartition = &protos.QRepPartition{
				PartitionId: uuid.NewString(),
				Range: &protos.PartitionRange{
					Range: &protos.PartitionRange_ObjectIdRange{
						ObjectIdRange: &protos.ObjectIdPartitionRange{
							Start: attrs.Name,
							End:   attrs.Name,
						},
					},
				},
			}
			currentPartitionObjectsNum = 0
		}

		currentPartitionObjectsNum++
		currentPartition.Range.GetObjectIdRange().End = attrs.Name

		if currentPartitionObjectsNum >= numObjectPerPartition {
			partitions = append(partitions, currentPartition)
			currentPartition = nil
			currentPartitionObjectsNum = 0
		}
	}

	if currentPartition != nil {
		partitions = append(partitions, currentPartition)
	}

	return partitions, nil
}

func (c *BigQueryConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}

func (c *BigQueryConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, tm, system, nullableEnabled)
		if err != nil {
			c.logger.Error("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}

	return res, nil
}

func (c *BigQueryConnector) getTableSchemaForTable(
	ctx context.Context,
	tm *protos.TableMapping,
	system protos.TypeSystem,
	nullableEnabled bool,
) (*protos.TableSchema, error) {
	dsTable, err := c.convertToDatasetTable(tm.SourceTableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table identifier %s: %w", tm.SourceTableIdentifier, err)
	}

	table := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for table %s: %w", tm.SourceTableIdentifier, err)
	}

	columns := make([]*protos.FieldDescription, 0, len(metadata.Schema))
	columnNames := make([]string, 0, len(metadata.Schema))

	excludedCols := make(map[string]struct{})
	for _, col := range tm.Exclude {
		excludedCols[col] = struct{}{}
	}

	for _, field := range metadata.Schema {
		if _, excluded := excludedCols[field.Name]; excluded {
			continue
		}

		var colType string
		switch system {
		case protos.TypeSystem_Q:
			colType = string(BigQueryTypeToQValueKind(field))
		default:
			colType = string(field.Type)
		}

		nullable := field.Required == false

		columns = append(columns, &protos.FieldDescription{
			Name:     field.Name,
			Type:     colType,
			Nullable: nullable,
		})
		columnNames = append(columnNames, field.Name)
	}

	// BigQuery doesn't have traditional primary keys, but we can use the table's clustering fields
	// or return empty primary key columns
	var primaryKeyColumns []string
	if metadata.Clustering != nil {
		// Use clustering fields as primary key columns if available
		for _, clusterField := range metadata.Clustering.Fields {
			// Only include if not excluded
			if _, excluded := excludedCols[clusterField]; !excluded {
				primaryKeyColumns = append(primaryKeyColumns, clusterField)
			}
		}
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		Columns:               columns,
		PrimaryKeyColumns:     primaryKeyColumns,
		IsReplicaIdentityFull: len(primaryKeyColumns) == 0, // True if no primary key columns
		System:                system,
		NullableEnabled:       nullableEnabled,
	}, nil
}

func (c *BigQueryConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

func (c *BigQueryConnector) ExportTxSnapshot(ctx context.Context, flowName string, _ map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	cfg, err := internal.FetchConfigFromDB(ctx, c.catalogPool, flowName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch flow config from db: %w", err)
	}

	jobs := make([]*bigquery.Job, 0, len(cfg.TableMappings))
	for _, tm := range cfg.TableMappings {
		uri := fmt.Sprintf("%s/%s/*.avro", cfg.SnapshotStagingPath, url.PathEscape(tm.SourceTableIdentifier))
		gcsRef := bigquery.NewGCSReference(uri)
		gcsRef.DestinationFormat = bigquery.Avro
		gcsRef.AvroOptions = &bigquery.AvroOptions{UseAvroLogicalTypes: true}
		gcsRef.Compression = bigquery.Deflate

		dsTable, err := c.convertToDatasetTable(tm.SourceTableIdentifier)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse table identifier %s: %w", tm.SourceTableIdentifier, err)
		}

		extractor := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table).ExtractorTo(gcsRef)
		job, err := extractor.Run(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to start export job for table %s: %w", tm.SourceTableIdentifier, err)
		}
		jobs = append(jobs, job)
	}
	for _, job := range jobs {
		if status, err := job.Wait(ctx); err != nil {
			return nil, nil, fmt.Errorf("error waiting for export job to complete: %w", err)
		} else if err := status.Err(); err != nil {
			return nil, nil, fmt.Errorf("export job completed with error: %w", err)
		}
	}

	return nil, nil, nil
}

func (c *BigQueryConnector) FinishExport(a any) error {
	return nil
}

func (c *BigQueryConnector) SetupReplConn(ctx context.Context) error {
	return nil
}

func (c *BigQueryConnector) ReplPing(ctx context.Context) error {
	return nil
}

func (c *BigQueryConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	return nil
}

func (c *BigQueryConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

func (c *BigQueryConnector) getQRepSchema(ctx context.Context, config *protos.QRepConfig) (types.QRecordSchema, error) {
	dsTable, err := c.convertToDatasetTable(config.WatermarkTable)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to parse table identifier %s: %w", config.WatermarkTable, err)
	}

	tableRef := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return types.QRecordSchema{}, fmt.Errorf("failed to get table metadata for %s.%s: %w", dsTable.dataset, dsTable.table, err)
	}

	return bigQuerySchemaToQRecordSchema(metadata.Schema)
}

func bigQuerySchemaToQRecordSchema(schema bigquery.Schema) (types.QRecordSchema, error) {
	fields := make([]types.QField, 0, len(schema))

	for _, field := range schema {
		qValueKind := BigQueryTypeToQValueKind(field)
		if qValueKind == types.QValueKindInvalid {
			return types.QRecordSchema{}, fmt.Errorf("unsupported BigQuery field type: %s for field %s", field.Type, field.Name) // todo: should we fail?
		}

		qField := types.QField{
			Name:      field.Name,
			Type:      qValueKind,
			Nullable:  !field.Required,
			Precision: int16(field.Precision),
			Scale:     int16(field.Scale),
		}

		fields = append(fields, qField)
	}

	return types.NewQRecordSchema(fields), nil
}
