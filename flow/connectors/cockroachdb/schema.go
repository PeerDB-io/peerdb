package conncockroachdb

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func (c *CockroachDBConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema)

	for _, tableMapping := range tableMappings {
		parsedTable, err := common.ParseTableIdentifier(tableMapping.SourceTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("unable to parse table identifier: %w", err)
		}

		// Query table schema from information_schema
		rows, err := c.conn.Query(ctx, `
			SELECT 
				column_name,
				data_type,
				is_nullable
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position
		`, parsedTable.Namespace, parsedTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema for table %s: %w", parsedTable, err)
		}

		var columns []*protos.FieldDescription
		for rows.Next() {
			var colName, dataType, isNullable string
			if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan column info: %w", err)
			}

			columns = append(columns, &protos.FieldDescription{
				Name:     colName,
				Type:     dataType,
				Nullable: isNullable == "YES",
			})
		}
		rows.Close()

		// Get primary key columns
		var pkCols []string
		pkRows, err := c.conn.Query(ctx, `
			SELECT column_name
			FROM information_schema.key_column_usage
			WHERE table_schema = $1 AND table_name = $2
			AND constraint_name = (
				SELECT constraint_name
				FROM information_schema.table_constraints
				WHERE table_schema = $1 AND table_name = $2
				AND constraint_type = 'PRIMARY KEY'
			)
			ORDER BY ordinal_position
		`, parsedTable.Namespace, parsedTable.Table)
		if err == nil {
			for pkRows.Next() {
				var colName string
				if err := pkRows.Scan(&colName); err == nil {
					pkCols = append(pkCols, colName)
				}
			}
			pkRows.Close()
		}

		res[tableMapping.SourceTableIdentifier] = &protos.TableSchema{
			TableIdentifier:       tableMapping.SourceTableIdentifier,
			PrimaryKeyColumns:     pkCols,
			IsReplicaIdentityFull: false,
			System:                system,
			Columns:               columns,
		}
	}

	return res, nil
}

func (c *CockroachDBConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
		AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get all tables: %w", err)
	}
	defer rows.Close()

	var tables []*protos.TableResponse
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tables = append(tables, &protos.TableResponse{
			TableName: fmt.Sprintf("%s.%s", schema, table),
		})
	}

	var tableNames []string
	for _, t := range tables {
		tableNames = append(tableNames, t.TableName)
	}
	return &protos.AllTablesResponse{Tables: tableNames}, nil
}

func (c *CockroachDBConnector) GetColumns(
	ctx context.Context,
	version uint32,
	schema string,
	table string,
) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	var columns []*protos.ColumnsItem
	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns = append(columns, &protos.ColumnsItem{
			Name: colName,
			Type: dataType,
		})
	}

	return &protos.TableColumnsResponse{Columns: columns}, nil
}

func (c *CockroachDBConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
		ORDER BY schema_name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("failed to scan schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (c *CockroachDBConnector) GetTablesInSchema(
	ctx context.Context,
	schema string,
	cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables in schema: %w", err)
	}
	defer rows.Close()

	var tables []*protos.TableResponse
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tables = append(tables, &protos.TableResponse{
			TableName: fmt.Sprintf("%s.%s", schema, table),
		})
	}

	return &protos.SchemaTablesResponse{Tables: tables}, nil
}
