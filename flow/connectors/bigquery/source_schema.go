package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

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

	columns := make([]*protos.ColumnsItem, 0, len(metadata.Schema))
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

		nullable := !field.Required

		columns = append(columns, &protos.FieldDescription{
			Name:     field.Name,
			Type:     colType,
			Nullable: nullable,
		})
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

func (c *BigQueryConnector) GetDefaultPartitionKeyForTables(
	_ context.Context,
	_ *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}
