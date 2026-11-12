package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	bqvalidate "github.com/PeerDB-io/peerdb/flow/pkg/bigquery"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
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
			Name:     field.Name,
			Type:     fieldNormalizedTypeName(field),
			IsKey:    slices.Contains(primaryKeys, field.Name),
			Qkind:    qkind,
			Nullable: !field.Required,
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
			tableSize = utils.HumanReadableBytes(metadata.NumBytes)
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

// GetTableSchema reads the source schemas with one INFORMATION_SCHEMA query per
// dataset, the same source mirror validation reads.
func (c *BigQueryConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	datasetTables := make([]bqvalidate.DatasetTable, 0, len(tableMappings))
	for _, tm := range tableMappings {
		dt, err := bqvalidate.ResolveDatasetTable(tm.SourceTableIdentifier, c.datasetID)
		if err != nil {
			return nil, err
		}
		datasetTables = append(datasetTables, dt)
	}

	tables, err := bqvalidate.GetTables(ctx, c.client, c.projectID, datasetTables)
	if err != nil {
		c.logger.Error("error fetching schemas", slog.Any("error", err))
		return nil, fmt.Errorf("failed to get source table schemas: %w", err)
	}

	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for i, tm := range tableMappings {
		res[tm.SourceTableIdentifier] = tableSchemaFromTableInfo(tm, tables[datasetTables[i]], system, nullableEnabled)
	}
	c.logger.Info("fetched schemas", slog.Int("tables", len(res)))

	return res, nil
}

func tableSchemaFromTableInfo(
	tm *protos.TableMapping,
	info bqvalidate.TableInfo,
	system protos.TypeSystem,
	nullableEnabled bool,
) *protos.TableSchema {
	columns := make([]*protos.FieldDescription, 0, len(info.ColumnNames))
	for _, name := range info.ColumnNames {
		if slices.Contains(tm.Exclude, name) {
			continue
		}

		col := info.Columns[name]
		// INFORMATION_SCHEMA reports ARRAY columns as not nullable, while the
		// table API reports them as REPEATED, which is not REQUIRED.
		field := &bigquery.FieldSchema{
			Name:      name,
			Type:      col.Type,
			Repeated:  col.IsRepeated,
			Required:  !col.IsNullable && !col.IsRepeated,
			Precision: col.Precision,
			Scale:     col.Scale,
		}

		var colType string
		switch system {
		case protos.TypeSystem_Q:
			colType = string(BigQueryTypeToQValueKind(field))
		default:
			colType = string(field.Type)
		}

		precision, scale := bigQueryNumericPrecisionAndScale(field)

		columns = append(columns, &protos.FieldDescription{
			Name:         name,
			Type:         colType,
			Nullable:     !field.Required,
			TypeModifier: datatypes.MakeNumericTypmod(int32(precision), int32(scale)),
		})
	}

	return &protos.TableSchema{
		TableIdentifier:   tm.SourceTableIdentifier,
		Columns:           columns,
		PrimaryKeyColumns: info.PrimaryKey,
		System:            system,
		NullableEnabled:   nullableEnabled,
	}
}

func (c *BigQueryConnector) GetDefaultPartitionKeyForTables(
	_ context.Context,
	_ *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}
