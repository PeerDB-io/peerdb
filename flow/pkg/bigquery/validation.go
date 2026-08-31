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
		return DatasetTable{}, fmt.Errorf("invalid BigQuery table name: %s", identifier)
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
	Type string // BigQuery data type, e.g. "TIMESTAMP"
	IsPK bool   // part of a real (NOT ENFORCED) primary key constraint
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
			for _, col := range tableMeta.Schema {
				tableInfo.Columns[col.Name] = ColumnInfo{
					Type: string(col.Type),
					IsPK: slices.Contains(tableMeta.TableConstraints.PrimaryKey.Columns, col.Name),
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
	ReplicationModeEvents ReplicationMode = iota
	ReplicationModeQuery
)

// CDCEventsFunction selects which BigQuery CDC function backs a table's events.
type CDCEventsFunction int

const (
	CDCEventsFunctionAppends CDCEventsFunction = iota
	CDCEventsFunctionChanges
)

// SourceTableConfig is validation input for a single mirror table.
type SourceTableConfig struct {
	// SourceTableIdentifier is "table", "dataset.table", or "project.dataset.table".
	SourceTableIdentifier string
	// WatermarkColumn is required when the mirror's ReplicationMode is ReplicationModeQuery.
	WatermarkColumn   string
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

// ValidateSourceTables checks that every configured source table exists and
// returns their columns for reuse by ValidateSourceCDC.
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

	return tablesByKey, nil
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

		if cfg.ReplicationMode == ReplicationModeQuery {
			if t.WatermarkColumn == "" {
				return fmt.Errorf("table %s has no watermark_column configured; QUERY replication mode requires "+
					"one TIMESTAMP column per table to incrementally scan", key)
			}
			column, ok := tablesByKey[key].Columns[t.WatermarkColumn]
			if !ok {
				return fmt.Errorf("watermark column %s does not exist on table %s", t.WatermarkColumn, key)
			}
			if column.Type != string(bigquery.TimestampFieldType) {
				return fmt.Errorf("watermark column %s on table %s must be TIMESTAMP, got %s",
					t.WatermarkColumn, key, column.Type)
			}
		}

		destinationHasOrderingKey := tablesByKey[key].HasPrimaryKey() || t.HasOrderingKey

		switch t.CDCEventsFunction {
		case CDCEventsFunctionChanges:
			if !destinationHasOrderingKey {
				return fmt.Errorf("table %s has no primary key constraint configured on BigQuery; "+
					"CHANGES mode requires either a real (NOT ENFORCED) PK constraint on the source table "+
					"or an explicit ordering key configured via column settings on the table mapping", key)
			}
			if !changeHistoryByTable[key] {
				return fmt.Errorf("table %s does not have enable_change_history=TRUE set; "+
					"CHANGES mode requires it (run ALTER TABLE ... SET OPTIONS(enable_change_history=true) on the source table)",
					key)
			}
		case CDCEventsFunctionAppends:
			if t.RequiresOrderingKey && !destinationHasOrderingKey {
				return fmt.Errorf("table %s has no primary key configured on BigQuery and no ordering key configured "+
					"via column settings, and the destination engine is a ReplacingMergeTree variant (the plain form "+
					"is the default); ORDER BY tuple() on a keyless ReplacingMergeTree collapses the table on writes, "+
					"so such a table must use an explicit CH_ENGINE_MERGE_TREE engine instead, or have an ordering key "+
					"configured", key)
			}
		}
	}

	return nil
}
