package conncockroachdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *CockroachDBConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	return c.getTableSchema(ctx, env, system, tableMappings, crdbHLC{})
}

// getTableSchema resolves table schemas, optionally AS OF SYSTEM TIME asOf
// (zero reads the latest schema). The changefeed path passes a row's commit
// timestamp so added columns are typed by the schema that produced the row,
// not by whatever the schema has become since.
func (c *CockroachDBConnector) getTableSchema(
	ctx context.Context,
	env map[string]string,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
	asOf crdbHLC,
) (map[string]*protos.TableSchema, error) {
	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	// the rendered HLC is digits and a dot only, so the clause is injection safe
	var aostClause string
	if asOf != (crdbHLC{}) {
		aostClause = " AS OF SYSTEM TIME '" + asOf.String() + "'"
	}

	res := make(map[string]*protos.TableSchema)

	for _, tableMapping := range tableMappings {
		parsedTable, err := common.ParseTableIdentifier(tableMapping.SourceTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("unable to parse table identifier: %w", err)
		}

		// virtual computed columns are excluded: changefeeds omit them (default
		// virtual_columns='omitted'), so replicating them from the snapshot
		// would leave values CDC can never update. Stored computed columns are
		// emitted by changefeeds and stay included.
		rows, err := c.conn.Query(ctx, `
			SELECT
				c.column_name,
				c.data_type,
				c.udt_name,
				c.is_nullable,
				c.numeric_precision,
				c.numeric_scale
			FROM information_schema.columns c`+aostClause+`
			WHERE c.table_schema = $1 AND c.table_name = $2
			AND NOT EXISTS (
				SELECT 1 FROM pg_catalog.pg_class pc
				JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
				JOIN pg_catalog.pg_attribute pa ON pa.attrelid = pc.oid
				WHERE pn.nspname = c.table_schema AND pc.relname = c.table_name
				AND pa.attname = c.column_name AND pa.attgenerated = 'v'
			)
			ORDER BY c.ordinal_position
		`, parsedTable.Namespace, parsedTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema for table %s: %w", parsedTable, err)
		}

		columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.FieldDescription, error) {
			var colName, dataType, udtName, isNullable string
			var numericPrecision, numericScale *int32
			if err := row.Scan(&colName, &dataType, &udtName, &isNullable, &numericPrecision, &numericScale); err != nil {
				return nil, fmt.Errorf("failed to scan column info: %w", err)
			}

			qkind := crdbTypeToQValueKind(dataType, udtName)
			colType := udtName
			if system == protos.TypeSystem_Q {
				colType = string(qkind)
			}
			// capture DECIMAL(p,s) so destinations pick a matching decimal type;
			// -1 means unbounded (like Postgres atttypmod). DECIMAL(p) means
			// DECIMAL(p,0) and CockroachDB reports numeric_scale = 0 for it, so
			// precision never arrives without a scale; defaulting a nil scale to
			// 0 is only a guard so precision is never dropped
			typeModifier := int32(-1)
			if (qkind == types.QValueKindNumeric || qkind == types.QValueKindArrayNumeric) && numericPrecision != nil {
				var scale int32
				if numericScale != nil {
					scale = *numericScale
				}
				typeModifier = datatypes.MakeNumericTypmod(*numericPrecision, scale)
			}
			return &protos.FieldDescription{
				Name:         colName,
				Type:         colType,
				TypeModifier: typeModifier,
				Nullable:     isNullable == "YES",
			}, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read schema for table %s: %w", parsedTable, err)
		}

		pkRows, err := c.conn.Query(ctx, `
			SELECT column_name
			FROM information_schema.key_column_usage`+aostClause+`
			WHERE table_schema = $1 AND table_name = $2
			AND constraint_name = (
				SELECT constraint_name
				FROM information_schema.table_constraints
				WHERE table_schema = $1 AND table_name = $2
				AND constraint_type = 'PRIMARY KEY'
			)
			ORDER BY ordinal_position
		`, parsedTable.Namespace, parsedTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %s: %w", parsedTable, err)
		}
		pkCols, err := pgx.CollectRows[string](pkRows, pgx.RowTo)
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key for table %s: %w", parsedTable, err)
		}

		res[tableMapping.SourceTableIdentifier] = &protos.TableSchema{
			TableIdentifier:       tableMapping.SourceTableIdentifier,
			PrimaryKeyColumns:     pkCols,
			IsReplicaIdentityFull: false,
			System:                system,
			NullableEnabled:       nullableEnabled,
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

	tableNames, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var schema, table string
		if err := row.Scan(&schema, &table); err != nil {
			return "", fmt.Errorf("failed to scan table: %w", err)
		}
		return schema + "." + table, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read all tables: %w", err)
	}

	return &protos.AllTablesResponse{Tables: tableNames}, nil
}

func (c *CockroachDBConnector) GetColumns(
	ctx context.Context,
	version uint32,
	schema string,
	table string,
) (*protos.TableColumnsResponse, error) {
	// virtual computed columns are hidden here too, matching GetTableSchema:
	// they never replicate, so offering them in column pickers only misleads
	rows, err := c.conn.Query(ctx, `
		SELECT c.column_name, c.data_type, c.is_nullable
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		AND NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_class pc
			JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
			JOIN pg_catalog.pg_attribute pa ON pa.attrelid = pc.oid
			WHERE pn.nspname = c.table_schema AND pc.relname = c.table_name
			AND pa.attname = c.column_name AND pa.attgenerated = 'v'
		)
		ORDER BY c.ordinal_position
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.ColumnsItem, error) {
		var colName, dataType, isNullable string
		if err := row.Scan(&colName, &dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		return &protos.ColumnsItem{
			Name:     colName,
			Type:     dataType,
			Nullable: isNullable == "YES",
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read columns: %w", err)
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

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
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

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table string
		if err := row.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		return &protos.TableResponse{
			// bare table name: unlike GetAllTables, callers qualify it with the
			// schema themselves
			TableName: table,
			// every CockroachDB table is mirrorable: tables without an explicit
			// primary key get the hidden rowid one
			CanMirror: true,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read tables in schema: %w", err)
	}

	return &protos.SchemaTablesResponse{Tables: tables}, nil
}
