package bigquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	storageapi "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// DatasetTable identifies a BigQuery table by dataset and table name.
type DatasetTable struct {
	Dataset string
	Table   string
}

func (d DatasetTable) String() string {
	return d.Dataset + "." + d.Table
}

func ResolveDatasetTable(identifier, defaultDataset string) (DatasetTable, error) {
	parts := strings.Split(identifier, ".")
	switch len(parts) {
	case 1:
		return DatasetTable{Dataset: defaultDataset, Table: parts[0]}, nil
	case 2:
		return DatasetTable{Dataset: parts[0], Table: parts[1]}, nil
	case 3:
		return DatasetTable{Dataset: parts[1], Table: parts[2]}, nil
	default:
		return DatasetTable{}, fmt.Errorf("invalid BigQuery table name: %q", identifier)
	}
}

// TablesHaveChangeHistoryEnabled checks whether change history is enabled via
// INFORMATION_SCHEMA.TABLES.is_change_history_enabled, one query per dataset
// for all tables in that dataset.
func TablesHaveChangeHistoryEnabled(
	ctx context.Context, client *bigquery.Client, projectID string, tables []DatasetTable,
) (map[DatasetTable]bool, error) {
	tablesByDataset := make(map[string][]string)
	for _, dt := range tables {
		tablesByDataset[dt.Dataset] = append(tablesByDataset[dt.Dataset], dt.Table)
	}

	result := make(map[DatasetTable]bool, len(tables))
	for dataset, tableNames := range tablesByDataset {
		q := client.Query(fmt.Sprintf(
			"SELECT table_name, is_change_history_enabled FROM `%s`.INFORMATION_SCHEMA.TABLES "+
				"WHERE table_name IN UNNEST(@table_names)",
			dataset))
		q.Parameters = []bigquery.QueryParameter{{Name: "table_names", Value: tableNames}}
		q.DefaultProjectID = projectID
		q.DefaultDatasetID = dataset

		it, err := q.Read(ctx)
		if err != nil {
			return nil, err
		}
		for {
			var row []bigquery.Value
			if err := it.Next(&row); err != nil {
				if errors.Is(err, iterator.Done) {
					break
				}
				return nil, err
			}
			tableName, _ := row[0].(string)
			isEnabled, _ := row[1].(string)
			result[DatasetTable{Dataset: dataset, Table: tableName}] = strings.EqualFold(isEnabled, "YES")
		}
	}
	return result, nil
}

// ColumnInfo describes a single column of a BigQuery table.
type ColumnInfo struct {
	// Type is the field type; for ARRAY columns, the element type.
	Type bigquery.FieldType
	// DataType is the INFORMATION_SCHEMA type, e.g. "NUMERIC(10, 2)".
	DataType   string
	IsRepeated bool
	IsNullable bool
	IsPK       bool // part of a real (NOT ENFORCED) primary key constraint
}

// TableInfo describes a BigQuery table's columns, keyed by column name.
type TableInfo struct {
	Columns map[string]ColumnInfo
	// ColumnNames lists the columns in table order.
	ColumnNames []string
	// PrimaryKey lists the primary key columns in constraint order.
	PrimaryKey []string
	// ClusteringKeys lists the clustering columns in clustering order.
	ClusteringKeys []string
	SizeBytes      int64
}

// HasPrimaryKey reports whether any column is part of a primary key constraint.
func (t TableInfo) HasPrimaryKey() bool {
	return len(t.PrimaryKey) > 0
}

// queryConcurrency bounds concurrent BigQuery query jobs issued by validation.
const queryConcurrency = 8

// tableColumnsQuery reads columns, keys and size of tables in one dataset,
// excluding pseudo columns such as _PARTITIONTIME.
const tableColumnsQuery = `SELECT
  c.table_name, c.column_name, c.is_nullable, c.data_type, c.clustering_ordinal_position,
  k.ordinal_position AS pk_position, s.size_bytes
FROM %[1]s.INFORMATION_SCHEMA.COLUMNS AS c
LEFT JOIN %[1]s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
  ON tc.table_name = c.table_name AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN %[1]s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
  ON k.constraint_name = tc.constraint_name AND k.table_name = c.table_name AND k.column_name = c.column_name
LEFT JOIN %[1]s.__TABLES__ AS s
  ON s.table_id = c.table_name
WHERE c.table_name IN UNNEST(@table_names) AND c.is_hidden = 'NO'
ORDER BY c.table_name, c.ordinal_position`

type tableColumnRow struct {
	TableName          string             `bigquery:"table_name"`
	ColumnName         string             `bigquery:"column_name"`
	IsNullable         string             `bigquery:"is_nullable"`
	DataType           string             `bigquery:"data_type"`
	ClusteringPosition bigquery.NullInt64 `bigquery:"clustering_ordinal_position"`
	PKPosition         bigquery.NullInt64 `bigquery:"pk_position"`
	SizeBytes          bigquery.NullInt64 `bigquery:"size_bytes"`
}

// GetTables reads table info with one INFORMATION_SCHEMA query per dataset.
// Missing tables and datasets yield a *common.SourceTablesMissingError.
func GetTables(
	ctx context.Context, client *bigquery.Client, projectID string, tables []DatasetTable,
) (map[DatasetTable]TableInfo, error) {
	tablesByDataset := make(map[string][]string)
	for _, dt := range tables {
		if !slices.Contains(tablesByDataset[dt.Dataset], dt.Table) {
			tablesByDataset[dt.Dataset] = append(tablesByDataset[dt.Dataset], dt.Table)
		}
	}

	var mu sync.Mutex
	result := make(map[DatasetTable]TableInfo, len(tables))
	var missingTables []common.QualifiedTable

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(queryConcurrency)
	for dataset, tableNames := range tablesByDataset {
		g.Go(func() error {
			datasetTables, err := getDatasetTables(gCtx, client, projectID, dataset, tableNames)
			if err != nil {
				if err := checkDatasetMissing(gCtx, client, projectID, dataset, err); err != nil {
					return err
				}
				// The dataset does not exist, so none of its tables do.
				datasetTables = nil
			}

			mu.Lock()
			defer mu.Unlock()
			for _, table := range tableNames {
				dt := DatasetTable{Dataset: dataset, Table: table}
				info, ok := datasetTables[table]
				if !ok {
					missingTables = append(missingTables, common.QualifiedTable{Namespace: dataset, Table: table})
					continue
				}
				result[dt] = info
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if len(missingTables) > 0 {
		slices.SortFunc(missingTables, func(a, b common.QualifiedTable) int {
			return strings.Compare(a.String(), b.String())
		})
		return nil, common.NewSourceTablesMissingError(missingTables)
	}

	return result, nil
}

// checkDatasetMissing returns nil if queryErr means the dataset does not exist.
// Queries return 403 for both missing and unreadable datasets; datasets.get
// tells them apart.
func checkDatasetMissing(ctx context.Context, client *bigquery.Client, projectID, dataset string, queryErr error) error {
	if isNotFoundError(queryErr) {
		return nil
	}
	if !isAccessDeniedError(queryErr) {
		return fmt.Errorf("failed to get tables of dataset %q: %w", dataset, queryErr)
	}
	if _, err := client.DatasetInProject(projectID, dataset).Metadata(ctx); isNotFoundError(err) {
		return nil
	}
	return fmt.Errorf("no permission to read dataset %q, grant roles/bigquery.dataViewer on it: %w", dataset, queryErr)
}

// isNotFoundError matches both the jobs.query and the job-polling error forms.
func isNotFoundError(err error) bool {
	if apiErr, ok := errors.AsType[*googleapi.Error](err); ok {
		return apiErr.Code == http.StatusNotFound
	}
	if bqErr, ok := errors.AsType[*bigquery.Error](err); ok {
		return bqErr.Reason == "notFound"
	}
	return false
}

// isAccessDeniedError checks the reason, not the code: 403 also means quota errors.
func isAccessDeniedError(err error) bool {
	if apiErr, ok := errors.AsType[*googleapi.Error](err); ok {
		return slices.ContainsFunc(apiErr.Errors, func(item googleapi.ErrorItem) bool {
			return item.Reason == "accessDenied"
		})
	}
	if bqErr, ok := errors.AsType[*bigquery.Error](err); ok {
		return bqErr.Reason == "accessDenied"
	}
	return false
}

// getDatasetTables returns the found tables of one dataset, keyed by name.
func getDatasetTables(
	ctx context.Context, client *bigquery.Client, projectID, dataset string, tableNames []string,
) (map[string]TableInfo, error) {
	q := client.Query(fmt.Sprintf(tableColumnsQuery, fmt.Sprintf("`%s.%s`", projectID, dataset)))
	q.Parameters = []bigquery.QueryParameter{{Name: "table_names", Value: tableNames}}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	type positionedColumn struct {
		column   string
		position int64
	}
	clustering := make(map[string][]positionedColumn)
	pks := make(map[string][]positionedColumn)

	result := make(map[string]TableInfo, len(tableNames))
	for {
		var row tableColumnRow
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}

		info, ok := result[row.TableName]
		if !ok {
			info = TableInfo{Columns: make(map[string]ColumnInfo)}
			if row.SizeBytes.Valid {
				info.SizeBytes = row.SizeBytes.Int64
			}
		}
		fieldType, repeated := fieldTypeFromDataType(row.DataType)
		info.Columns[row.ColumnName] = ColumnInfo{
			Type:       fieldType,
			DataType:   row.DataType,
			IsRepeated: repeated,
			IsNullable: strings.EqualFold(row.IsNullable, "YES"),
			IsPK:       row.PKPosition.Valid,
		}
		info.ColumnNames = append(info.ColumnNames, row.ColumnName)
		result[row.TableName] = info

		if row.PKPosition.Valid {
			pks[row.TableName] = append(pks[row.TableName], positionedColumn{row.ColumnName, row.PKPosition.Int64})
		}
		if row.ClusteringPosition.Valid {
			clustering[row.TableName] = append(clustering[row.TableName],
				positionedColumn{row.ColumnName, row.ClusteringPosition.Int64})
		}
	}

	sortedColumns := func(keys []positionedColumn) []string {
		slices.SortFunc(keys, func(a, b positionedColumn) int { return int(a.position - b.position) })
		columns := make([]string, 0, len(keys))
		for _, k := range keys {
			columns = append(columns, k.column)
		}
		return columns
	}
	for table, info := range result {
		if keys := pks[table]; len(keys) > 0 {
			info.PrimaryKey = sortedColumns(keys)
		}
		if keys := clustering[table]; len(keys) > 0 {
			info.ClusteringKeys = sortedColumns(keys)
		}
		result[table] = info
	}

	return result, nil
}

// fieldTypeFromDataType maps an INFORMATION_SCHEMA type to the table-API field
// type and whether it is an ARRAY.
func fieldTypeFromDataType(dataType string) (bigquery.FieldType, bool) {
	dataType = strings.TrimSpace(dataType)
	inner, repeated := strings.CutPrefix(dataType, "ARRAY<")
	if repeated {
		dataType = strings.TrimSuffix(inner, ">")
	}
	if i := strings.IndexAny(dataType, "<("); i >= 0 {
		dataType = dataType[:i]
	}
	switch dataType = strings.TrimSpace(dataType); dataType {
	case "INT64":
		return bigquery.IntegerFieldType, repeated
	case "FLOAT64":
		return bigquery.FloatFieldType, repeated
	case "BOOL":
		return bigquery.BooleanFieldType, repeated
	case "STRUCT":
		return bigquery.RecordFieldType, repeated
	default:
		return bigquery.FieldType(dataType), repeated
	}
}

// ReplicationMethod selects how CDC events are produced for a BigQuery source mirror.
type ReplicationMethod int

const (
	ReplicationMethodUnspecified ReplicationMethod = iota
	ReplicationMethodEvents
	ReplicationMethodQuery
)

// CDCEventsFunction selects which BigQuery CDC function backs a table's events.
type CDCEventsFunction int

const (
	CDCEventsFunctionUnspecified CDCEventsFunction = iota
	CDCEventsFunctionAppends
	CDCEventsFunctionChanges
)

// SourceTableConfig is validation input for a single mirror table.
type SourceTableConfig struct {
	// SourceTableIdentifier is "table", "dataset.table", or "project.dataset.table".
	SourceTableIdentifier string
	// WatermarkColumn is required when the mirror's ReplicationMethod is ReplicationMethodQuery.
	WatermarkColumn string
	// Include, if non-empty, restricts replication to these column names
	// ("selected_columns"). Mutually exclusive with Exclude.
	Include []string
	// Exclude lists column names excluded from replication for this table.
	Exclude           []string
	CDCEventsFunction CDCEventsFunction
	// HasOrderingKey reports whether the table mapping configures an explicit
	// ordering key (a PK substitute) via column settings.
	HasOrderingKey bool
	// RequiresOrderingKey reports whether the destination engine requires a
	// source PK or an explicit ordering key.
	RequiresOrderingKey bool
}

// SourceConfig holds everything ValidateSource needs to validate a BigQuery
// mirror source's tables. It intentionally avoids
// depending on generated protos so it can be built and called from outside
// the flow module.
//
//nolint:govet // logically grouped, fieldalignment confuses things
type SourceConfig struct {
	Client            *bigquery.Client
	StorageClient     *storage.Client
	StorageReadClient *storageapi.BigQueryReadClient
	ProjectID         string
	// DefaultDataset is used for table identifiers with no dataset qualifier.
	DefaultDataset    string
	Tables            []SourceTableConfig
	ReplicationMethod ReplicationMethod
	// HasSnapshot enables validation of snapshot staging access and export
	// permissions using SnapshotStagingPath.
	HasSnapshot         bool
	SnapshotStagingPath string
	// SnapshotOnly skips CDC-specific validation after the source tables have
	// been validated.
	SnapshotOnly bool
}

// ExternalError marks a failure as coming from a BigQuery API call rather
// than mirror configuration, so callers can classify it (e.g. as an
// infrastructure error) without this package depending on their error types.
type ExternalError struct {
	error
}

func (e *ExternalError) Unwrap() error {
	return e.error
}

// ValidateSource checks that every configured source table exists, that its
// column selection resolves to a non-empty set of real columns, and that the
// client can read data from it. It also validates destination ordering-key
// requirements, snapshot export permissions, and CDC requirements for mirrors
// with those phases enabled. The returned table metadata can be reused for
// validation that is specific to the caller.
func ValidateSource(ctx context.Context, cfg SourceConfig) (map[DatasetTable]TableInfo, error) {
	if cfg.HasSnapshot {
		if err := validateSnapshotStagingAccess(ctx, cfg); err != nil {
			return nil, err
		}
	}

	tablesByKey, err := validateSourceTables(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := validateSourceOrderingKeys(cfg, tablesByKey); err != nil {
		return nil, err
	}
	if !cfg.SnapshotOnly {
		if err := validateSourceCDC(ctx, cfg, tablesByKey); err != nil {
			return nil, err
		}
	}
	if cfg.HasSnapshot {
		if err := validateSnapshotExportAccess(ctx, cfg); err != nil {
			return nil, err
		}
	}
	return tablesByKey, nil
}

// validateSourceOrderingKeys performs the source-metadata portion of the
// destination ordering-key validation here as well, so an invalid mapping is
// rejected during source validation instead of waiting for destination
// validation.
func validateSourceOrderingKeys(cfg SourceConfig, tablesByKey map[DatasetTable]TableInfo) error {
	for _, table := range cfg.Tables {
		if !table.RequiresOrderingKey || table.HasOrderingKey {
			continue
		}

		key, err := ResolveDatasetTable(table.SourceTableIdentifier, cfg.DefaultDataset)
		if err != nil {
			return err
		}
		if !tablesByKey[key].HasPrimaryKey() {
			return fmt.Errorf("cannot determine sort key from source table %s; empty sort key is not supported", key)
		}
	}
	return nil
}

// validateSnapshotStagingAccess checks the snapshot staging path and verifies
// the bucket operations used by snapshots.
func validateSnapshotStagingAccess(ctx context.Context, cfg SourceConfig) error {
	if cfg.SnapshotStagingPath == "" {
		return fmt.Errorf("snapshot bucket is required for BigQuery source connector")
	}

	stagingURL, err := url.Parse(cfg.SnapshotStagingPath)
	if err != nil {
		return fmt.Errorf("invalid snapshot bucket: %w", err)
	}
	if stagingURL.Scheme != "gs" {
		return fmt.Errorf("invalid snapshot bucket: invalid scheme %q, expected gs", stagingURL.Scheme)
	}
	if stagingURL.Host == "" {
		return fmt.Errorf("invalid snapshot bucket: bucket name is required")
	}

	bucket := cfg.StorageClient.Bucket(stagingURL.Host)
	prefix := strings.Trim(stagingURL.Path, "/")
	if prefix != "" {
		prefix += "/"
	}
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	if _, err := it.Next(); err != nil && !errors.Is(err, iterator.Done) {
		return fmt.Errorf("failed to access staging bucket: %w", &ExternalError{err})
	}

	var randomSuffix [16]byte
	if _, err := rand.Read(randomSuffix[:]); err != nil {
		return fmt.Errorf("failed to generate staging bucket access-check object name: %w", err)
	}
	testObject := bucket.Object(prefix + ".peerdb_access_check_" + hex.EncodeToString(randomSuffix[:]))
	writer := testObject.NewWriter(ctx)
	if _, err := writer.Write([]byte("PeerDB snapshot staging access check")); err != nil {
		_ = writer.Close()
		_ = testObject.Delete(ctx)
		return fmt.Errorf("failed to write to staging bucket: %w", &ExternalError{err})
	}
	if err := writer.Close(); err != nil {
		_ = testObject.Delete(ctx)
		return fmt.Errorf("failed to write to staging bucket: %w", &ExternalError{err})
	}
	if err := testObject.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete from staging bucket: %w", &ExternalError{err})
	}
	return nil
}

// validateSnapshotExportAccess checks that BigQuery can create export jobs
// for the first source table. The query is a dry run and exports no data.
func validateSnapshotExportAccess(ctx context.Context, cfg SourceConfig) error {
	if len(cfg.Tables) == 0 {
		return nil
	}

	key, err := ResolveDatasetTable(cfg.Tables[0].SourceTableIdentifier, cfg.DefaultDataset)
	if err != nil {
		return err
	}
	query := cfg.Client.Query(fmt.Sprintf(
		"EXPORT DATA OPTIONS(uri='%s*.avro', format='Avro') AS SELECT * FROM `%s.%s.%s` LIMIT 0",
		cfg.SnapshotStagingPath, cfg.ProjectID, key.Dataset, key.Table))
	query.DryRun = true
	if _, err := query.Run(ctx); err != nil {
		return fmt.Errorf("failed to validate export job permission for table %q: %w", key, &ExternalError{err})
	}
	return nil
}

func validateSourceTables(ctx context.Context, cfg SourceConfig) (map[DatasetTable]TableInfo, error) {
	validateTables := make([]DatasetTable, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		dt, err := ResolveDatasetTable(t.SourceTableIdentifier, cfg.DefaultDataset)
		if err != nil {
			return nil, err
		}
		validateTables = append(validateTables, dt)
	}

	tablesByKey, err := GetTables(ctx, cfg.Client, cfg.ProjectID, validateTables)
	if err != nil {
		if _, ok := errors.AsType[*common.SourceTablesMissingError](err); ok {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get columns for tables: %w", &ExternalError{err})
	}

	for i, t := range cfg.Tables {
		key := validateTables[i]
		if err := validateColumnSelection(key, tablesByKey[key], t.Include, t.Exclude); err != nil {
			return nil, err
		}
	}

	if err := validateTablesDataAccess(ctx, cfg.Client, cfg.ProjectID, validateTables); err != nil {
		return nil, err
	}

	return tablesByKey, nil
}

// validateColumnSelection checks that Include/Exclude reference real columns
// on the table and that at least one column remains for replication.
func validateColumnSelection(key DatasetTable, info TableInfo, include, exclude []string) error {
	switch {
	case len(include) > 0:
		for _, col := range include {
			if _, ok := info.Columns[col]; !ok {
				return fmt.Errorf("column %q does not exist in table %q", col, key)
			}
		}
	case len(exclude) > 0:
		for _, col := range exclude {
			if _, ok := info.Columns[col]; !ok {
				return fmt.Errorf("excluded column %q does not exist in table %q", col, key)
			}
		}
		if len(info.Columns) == len(exclude) {
			return fmt.Errorf("all columns are excluded for table %q, at least one column must remain", key)
		}
	default:
		if len(info.Columns) == 0 {
			return fmt.Errorf("table %q has no columns", key)
		}
	}
	return nil
}

// dataAccessTablesPerQuery stays under BigQuery's 1,000 tables per query.
const dataAccessTablesPerQuery = 500

// validateTablesDataAccess checks bigquery.tables.getData with batched
// UNION ALL dry-run queries.
func validateTablesDataAccess(ctx context.Context, client *bigquery.Client, projectID string, tables []DatasetTable) error {
	tables = slices.Clone(tables)
	slices.SortFunc(tables, func(a, b DatasetTable) int {
		return strings.Compare(a.String(), b.String())
	})
	tables = slices.Compact(tables)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(queryConcurrency)
	for batch := range slices.Chunk(tables, dataAccessTablesPerQuery) {
		g.Go(func() error {
			selects := make([]string, 0, len(batch))
			for _, key := range batch {
				selects = append(selects, fmt.Sprintf("(SELECT TO_JSON_STRING(t) FROM `%s.%s.%s` AS t LIMIT 0)",
					projectID, key.Dataset, key.Table))
			}
			query := client.Query(strings.Join(selects, " UNION ALL "))
			query.DryRun = true
			if _, err := query.Run(gCtx); err != nil {
				return fmt.Errorf("failed to validate data access for tables %q..%q: %w",
					batch[0].String(), batch[len(batch)-1].String(), &ExternalError{err})
			}
			return nil
		})
	}
	return g.Wait()
}

// validateSourceCDC checks that every configured source table meets the
// requirements of its replication mode and CDC events function.
func validateSourceCDC(ctx context.Context, cfg SourceConfig, tablesByKey map[DatasetTable]TableInfo) error {
	validateTables := make([]DatasetTable, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		dt, err := ResolveDatasetTable(t.SourceTableIdentifier, cfg.DefaultDataset)
		if err != nil {
			return err
		}
		validateTables = append(validateTables, dt)
	}

	needsChangeHistory := false
	for _, t := range cfg.Tables {
		if t.CDCEventsFunction == CDCEventsFunctionChanges {
			needsChangeHistory = true
			break
		}
	}
	var changeHistoryByTable map[DatasetTable]bool
	if needsChangeHistory {
		var err error
		changeHistoryByTable, err = TablesHaveChangeHistoryEnabled(ctx, cfg.Client, cfg.ProjectID, validateTables)
		if err != nil {
			return fmt.Errorf("failed to check enable_change_history option: %w", &ExternalError{err})
		}
	}

	if err := validateStorageReadAPI(ctx, cfg); err != nil {
		return err
	}

	for i, t := range cfg.Tables {
		key := validateTables[i]

		switch cfg.ReplicationMethod {
		case ReplicationMethodQuery:
			if t.WatermarkColumn == "" {
				return fmt.Errorf("table %q has no watermark_column configured; QUERY replication mode requires "+
					"one TIMESTAMP column per table to incrementally scan", key)
			}
			if slices.Contains(t.Exclude, t.WatermarkColumn) {
				return fmt.Errorf("watermark column %q on table %q is excluded from replication; "+
					"QUERY replication mode requires the watermark column to be synced so per-row "+
					"commit times can be tracked", t.WatermarkColumn, key)
			}
			column, ok := tablesByKey[key].Columns[t.WatermarkColumn]
			if !ok {
				return fmt.Errorf("watermark column %q does not exist on table %q", t.WatermarkColumn, key)
			}
			if column.Type != bigquery.TimestampFieldType {
				return fmt.Errorf("watermark column %q on table %q must be TIMESTAMP, got %s",
					t.WatermarkColumn, key, column.Type)
			}
		case ReplicationMethodEvents:
			switch t.CDCEventsFunction {
			case CDCEventsFunctionChanges:
				if !tablesByKey[key].HasPrimaryKey() && !t.HasOrderingKey {
					return fmt.Errorf("table %q has no primary key constraint configured on BigQuery; "+
						"CHANGES mode requires either a real (NOT ENFORCED) PK constraint on the source table "+
						"or an explicit ordering key configured via column settings on the table mapping", key)
				}
				if !changeHistoryByTable[key] {
					return fmt.Errorf("table %q does not have enable_change_history=TRUE set; "+
						"CHANGES mode requires it (run ALTER TABLE ... SET OPTIONS(enable_change_history=true) on the source table)",
						key)
				}
			case CDCEventsFunctionAppends:
			default:
				return fmt.Errorf("table %q has no cdc_events_function configured; "+
					"REPLICATION_MODE_EVENTS replication mode requires one per table to select the CDC function to use",
					key)
			}
		default:
			return fmt.Errorf("no replication mode configured for table %q; a CDC mirror must select "+
				"either EVENTS or QUERY replication mode", key)
		}
	}

	return nil
}

// validateStorageReadAPI creates a read session for the first source table to
// verify that the BigQuery Storage Read API is enabled and accessible. Creating
// the session does not read any table data.
func validateStorageReadAPI(ctx context.Context, cfg SourceConfig) error {
	if len(cfg.Tables) == 0 {
		return nil
	}
	if cfg.StorageReadClient == nil {
		return errors.New("BigQuery Storage Read client is required when the Storage Read API is enabled")
	}

	key, err := ResolveDatasetTable(cfg.Tables[0].SourceTableIdentifier, cfg.DefaultDataset)
	if err != nil {
		return err
	}
	_, err = cfg.StorageReadClient.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/" + cfg.ProjectID,
		ReadSession: &storagepb.ReadSession{
			Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", cfg.ProjectID, key.Dataset, key.Table),
			DataFormat: storagepb.DataFormat_ARROW,
		},
		MaxStreamCount: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to validate BigQuery Storage Read API: %w; enable the API by following %s",
			&ExternalError{err},
			"https://docs.cloud.google.com/bigquery/docs/reference/storage#enabling_the_api")
	}
	return nil
}
