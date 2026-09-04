package bigquery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
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
	Type bigquery.FieldType // BigQuery data type, e.g. "TIMESTAMP"
	IsPK bool               // part of a real (NOT ENFORCED) primary key constraint
}

// TableInfo describes a BigQuery table's columns, keyed by column name.
type TableInfo struct {
	Columns map[string]ColumnInfo
}

// HasPrimaryKey reports whether any column is part of a primary key constraint.
func (t TableInfo) HasPrimaryKey() bool {
	for _, col := range t.Columns {
		if col.IsPK {
			return true
		}
	}
	return false
}

// GetTables returns information about the specified tables
func GetTables(
	ctx context.Context, client *bigquery.Client, projectID string, tables []DatasetTable,
) (map[DatasetTable]TableInfo, error) {
	tablesByDataset := make(map[string][]string)
	for _, dt := range tables {
		tablesByDataset[dt.Dataset] = append(tablesByDataset[dt.Dataset], dt.Table)
	}

	result := make(map[DatasetTable]TableInfo, len(tables))
	for dataset, tableNames := range tablesByDataset {
		ds := client.Dataset(dataset)
		tables := ds.Tables(ctx)
		for {
			table, err := tables.Next()
			if err != nil {
				if errors.Is(err, iterator.Done) {
					break
				}
				return nil, err
			}
			if !slices.Contains(tableNames, table.TableID) {
				continue
			}

			tableInfo := TableInfo{Columns: make(map[string]ColumnInfo)}
			tableMeta, err := table.Metadata(ctx)
			if err != nil {
				return nil, err
			}
			var primaryKeyColumns []string
			if tableMeta.TableConstraints != nil && tableMeta.TableConstraints.PrimaryKey != nil {
				primaryKeyColumns = tableMeta.TableConstraints.PrimaryKey.Columns
			}
			for _, col := range tableMeta.Schema {
				tableInfo.Columns[col.Name] = ColumnInfo{
					Type: col.Type,
					IsPK: slices.Contains(primaryKeyColumns, col.Name),
				}
			}

			result[DatasetTable{Dataset: dataset, Table: table.TableID}] = tableInfo
		}
	}

	return result, nil
}

// ReplicationMode selects how CDC events are produced for a BigQuery source mirror.
type ReplicationMode int

const (
	ReplicationModeUnspecified ReplicationMode = iota
	ReplicationModeEvents
	ReplicationModeQuery
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
	// WatermarkColumn is required when the mirror's ReplicationMode is ReplicationModeQuery.
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
	// RequiresOrderingKey reports whether the destination engine is a
	// ReplacingMergeTree variant, which collapses rows on write when ordered
	// by an empty tuple, so it needs either a source PK or an explicit
	// ordering key.
	RequiresOrderingKey bool
}

// SourceConfig holds everything the ValidateSource* functions need to
// validate a BigQuery mirror source's tables. It intentionally avoids
// depending on generated protos so it can be built and called from outside
// the flow module.
type SourceConfig struct {
	Client    *bigquery.Client
	ProjectID string
	// DefaultDataset is used for table identifiers with no dataset qualifier.
	DefaultDataset  string
	Tables          []SourceTableConfig
	ReplicationMode ReplicationMode
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

// ValidateSourceTables checks that every configured source table exists, that
// its column selection (Include/Exclude) resolves to a non-empty set of real
// columns, and that the client can read data from it. It returns the tables'
// columns for reuse by ValidateSourceCDC.
func ValidateSourceTables(ctx context.Context, cfg SourceConfig) (map[DatasetTable]TableInfo, error) {
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
		return nil, fmt.Errorf("failed to get columns for tables: %w", &ExternalError{err})
	}

	var missingTables []common.QualifiedTable
	for _, key := range validateTables {
		if _, ok := tablesByKey[key]; !ok {
			missingTables = append(missingTables, common.QualifiedTable{Namespace: key.Dataset, Table: key.Table})
		}
	}
	if len(missingTables) > 0 {
		return nil, common.NewSourceTablesMissingError(missingTables)
	}

	for i, t := range cfg.Tables {
		key := validateTables[i]

		if err := validateColumnSelection(key, tablesByKey[key], t.Include, t.Exclude); err != nil {
			return nil, err
		}

		if err := validateTableDataAccess(ctx, cfg.Client, cfg.ProjectID, key); err != nil {
			return nil, err
		}
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

// validateTableDataAccess checks bigquery.tables.getData permission via a
// zero-row dry-run query, without actually executing it.
func validateTableDataAccess(ctx context.Context, client *bigquery.Client, projectID string, key DatasetTable) error {
	query := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.%s` LIMIT 0", projectID, key.Dataset, key.Table))
	query.DryRun = true
	if _, err := query.Run(ctx); err != nil {
		return fmt.Errorf("failed to validate data access for table %q: %w", key, &ExternalError{err})
	}
	return nil
}

// ValidateSourceCDC checks that every configured source table meets the
// requirements of its replication mode and CDC events function. tablesByKey
// is the result of a prior, successful ValidateSourceTables call.
func ValidateSourceCDC(ctx context.Context, cfg SourceConfig, tablesByKey map[DatasetTable]TableInfo) error {
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

	for i, t := range cfg.Tables {
		key := validateTables[i]

		switch cfg.ReplicationMode {
		case ReplicationModeQuery:
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
		case ReplicationModeEvents:
			destinationHasOrderingKey := tablesByKey[key].HasPrimaryKey() || t.HasOrderingKey

			switch t.CDCEventsFunction {
			case CDCEventsFunctionChanges:
				if !destinationHasOrderingKey {
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
				if t.RequiresOrderingKey && !destinationHasOrderingKey {
					return fmt.Errorf("table %q has no primary key configured on BigQuery and no ordering key configured "+
						"via column settings, and the destination engine is a ReplacingMergeTree variant (the plain form "+
						"is the default); ORDER BY tuple() on a keyless ReplacingMergeTree collapses the table on writes, "+
						"so such a table must use an explicit CH_ENGINE_MERGE_TREE engine instead, or have an ordering key "+
						"configured", key)
				}
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
