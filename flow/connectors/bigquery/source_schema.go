package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"google.golang.org/api/iterator"
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
