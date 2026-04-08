package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
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

		tablesIter := dataset.Tables(ctx)
		for {
			table, err := tablesIter.Next()
			if errors.Is(err, iterator.Done) || c.isApiErrorWithStatusCode(err, 404) {
				// in rare cases, like in e2e tests, a dataset may be deleted
				// between the time we list datasets and list tables
				break
			}

			if err != nil {
				return nil, fmt.Errorf("failed to list tables in dataset %s: %w", dataset.DatasetID, err)
			}

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

	var primaryKeys []string
	if metadata.TableConstraints != nil && metadata.TableConstraints.PrimaryKey != nil {
		primaryKeys = metadata.TableConstraints.PrimaryKey.Columns
	}

	columns := make([]*protos.ColumnsItem, 0, len(metadata.Schema))
	for _, field := range metadata.Schema {
		qkind := string(BigQueryTypeToQValueKind(field))

		columns = append(columns, &protos.ColumnsItem{
			Name:  field.Name,
			Type:  fieldNormalizedTypeName(field),
			IsKey: slices.Contains(primaryKeys, field.Name),
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

		metadata, err := table.Metadata(ctx)
		var tableSize string
		if err != nil {
			c.logger.Warn("failed to get table metadata for size calculation",
				slog.String("table", table.TableID),
				slog.Any("error", err))
			tableSize = "Unknown"
		} else {
			tableSize = utils.FormatTableSize(metadata.NumBytes)
		}

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

	var primaryKeyColumns []string
	if metadata.TableConstraints != nil && metadata.TableConstraints.PrimaryKey != nil {
		primaryKeyColumns = metadata.TableConstraints.PrimaryKey.Columns
	}

	return &protos.TableSchema{
		TableIdentifier:   tm.SourceTableIdentifier,
		Columns:           columns,
		PrimaryKeyColumns: primaryKeyColumns,
		System:            system,
		NullableEnabled:   nullableEnabled,
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
