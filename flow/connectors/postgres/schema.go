package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *PostgresConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.conn.Query(ctx, "SELECT n.nspname || '.' || c.relname AS schema_table "+
		"FROM pg_class c "+
		"JOIN pg_namespace n ON c.relnamespace = n.oid "+
		"WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'"+
		" AND c.relkind IN ('r', 'v', 'm', 'f', 'p')")
	if err != nil {
		return nil, err
	}

	tables, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (c *PostgresConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rows, err := c.conn.Query(ctx, "SELECT nspname"+
		" FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema';")
	if err != nil {
		return nil, err
	}

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (c *PostgresConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	pgVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		c.logger.Error("unable to get pgversion for schema tables", slog.Any("error", err))
		return nil, err
	}

	relKindFilterExpr := "t.relkind IN ('r', 'p')"
	// publish_via_partition_root is only available in PG13 and above
	if pgVersion < shared.POSTGRES_13 {
		relKindFilterExpr = "t.relkind = 'r'"
	}

	rows, err := c.conn.Query(ctx, `SELECT DISTINCT ON (t.relname)
		t.relname,
		(con.contype = 'p' OR t.relreplident in ('i', 'f')) AS can_mirror,
		pg_size_pretty(pg_total_relation_size(t.oid))::text AS table_size
	FROM pg_class t
	LEFT JOIN pg_namespace n ON t.relnamespace = n.oid
	LEFT JOIN pg_constraint con ON con.conrelid = t.oid
	WHERE n.nspname = $1 AND `+
		relKindFilterExpr+
		`AND t.relispartition IS NOT TRUE ORDER BY t.relname, can_mirror DESC;`, schema)
	if err != nil {
		c.logger.Info("failed to fetch tables", slog.Any("error", err))
		return nil, err
	}

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table pgtype.Text
		var hasPkeyOrReplica pgtype.Bool
		var tableSize pgtype.Text
		if err := rows.Scan(&table, &hasPkeyOrReplica, &tableSize); err != nil {
			return nil, err
		}
		var sizeOfTable string
		if tableSize.Valid {
			sizeOfTable = tableSize.String
		}
		canMirror := !cdcEnabled || (hasPkeyOrReplica.Valid && hasPkeyOrReplica.Bool)

		return &protos.TableResponse{
			TableName: table.String,
			CanMirror: canMirror,
			TableSize: sizeOfTable,
		}, nil
	})
	if err != nil {
		slog.InfoContext(ctx, "failed to fetch publications", slog.Any("error", err))
		return nil, err
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

func (c *PostgresConnector) GetColumns(ctx context.Context, version uint32, schema string, table string) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.Query(ctx, `SELECT
    DISTINCT attname AS column_name,
    atttypid AS oid,
    format_type(atttypid, atttypmod) AS data_type,
    (pg_constraint.contype = 'p') AS is_primary_key
	FROM pg_attribute
	JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
	JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
	LEFT JOIN pg_constraint ON pg_attribute.attrelid = pg_constraint.conrelid
		AND pg_attribute.attnum = ANY(pg_constraint.conkey)
		AND pg_constraint.contype = 'p'
	WHERE pg_namespace.nspname = $1
		AND relname = $2
		AND pg_attribute.attnum > 0
		AND NOT attisdropped
	ORDER BY column_name;`, schema, table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.ColumnsItem, error) {
		var columnName pgtype.Text
		var oid uint32
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		if err := rows.Scan(&columnName, &oid, &datatype, &isPkey); err != nil {
			return nil, err
		}
		return &protos.ColumnsItem{
			Name:  columnName.String,
			Type:  datatype.String,
			IsKey: isPkey.Bool,
			Qkind: string(c.postgresOIDToQValueKind(oid, c.customTypeMapping, version)),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}

// GetTableIndexes extracts all indexes for a given table
func (c *PostgresConnector) GetTableIndexes(
	ctx context.Context,
	schemaName, tableName string,
) ([]*protos.IndexDefinition, error) {
	query := `
		SELECT
			ic.relname::text AS index_name,
			am.amname AS index_type,
			i.indisunique AS is_unique,
			i.indisprimary AS is_primary,
			pg_get_indexdef(i.indexrelid) AS index_definition,
			array(
				SELECT a.attname
				FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND a.attnum > 0
				ORDER BY k.ord
			) AS columns
		FROM pg_index i
		JOIN pg_class t ON i.indrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		JOIN pg_class ic ON i.indexrelid = ic.oid
		JOIN pg_am am ON ic.relam = am.oid
		WHERE n.nspname = $1 AND t.relname = $2 AND i.indisvalid
		ORDER BY ic.relname;`

	rows, err := c.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes: %w", err)
	}
	defer rows.Close()

	var indexes []*protos.IndexDefinition
	for rows.Next() {
		var indexName, indexType, indexDef pgtype.Text
		var isUnique, isPrimary pgtype.Bool
		var columns []string

		if err := rows.Scan(&indexName, &indexType, &isUnique, &isPrimary, &indexDef, &columns); err != nil {
			return nil, fmt.Errorf("error scanning index row: %w", err)
		}

		// Skip primary key indexes as they're handled by table creation
		if isPrimary.Bool {
			continue
		}

		// Skip UNIQUE indexes that have a corresponding UNIQUE constraint with the same name.
		// PostgreSQL automatically creates an index when you create a UNIQUE constraint,
		// so we should only migrate the constraint, not the index, to avoid conflicts.
		if isUnique.Bool {
			var constraintExists bool
			checkConstraintQuery := `SELECT EXISTS(
				SELECT 1 FROM pg_constraint con
				JOIN pg_class t ON con.conrelid = t.oid
				JOIN pg_namespace n ON t.relnamespace = n.oid
				WHERE n.nspname = $1 AND t.relname = $2 AND con.conname = $3 AND con.contype = 'u'
			)`
			checkErr := c.conn.QueryRow(ctx, checkConstraintQuery, schemaName, tableName, indexName.String).Scan(&constraintExists)
			if checkErr != nil {
				c.logger.Warn(fmt.Sprintf("[GetTableIndexes] failed to check if constraint exists for UNIQUE index %s: %v - including index", indexName.String, checkErr))
			} else if constraintExists {
				c.logger.Info(fmt.Sprintf("[GetTableIndexes] skipping UNIQUE index %s because UNIQUE constraint with same name exists", indexName.String))
				continue
			}
		}

		// Extract just the index name without schema qualification
		nameParts := strings.Split(indexName.String, ".")
		indexNameOnly := nameParts[len(nameParts)-1]

		indexes = append(indexes, &protos.IndexDefinition{
			Name:       indexNameOnly,
			TableName:  tableName,
			Definition: indexDef.String,
			IsUnique:   isUnique.Bool,
			IsPrimary:  isPrimary.Bool,
			Columns:    columns,
			IndexType:  indexType.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("[GetTableIndexes] found %d indexes for table %s.%s", len(indexes), schemaName, tableName))

	return indexes, nil
}

// GetTableTriggers extracts all triggers for a given table
func (c *PostgresConnector) GetTableTriggers(
	ctx context.Context,
	schemaName, tableName string,
) ([]*protos.TriggerDefinition, error) {
	query := `
		SELECT
			t.tgname AS trigger_name,
			pg_get_triggerdef(t.oid) AS trigger_definition,
			CASE t.tgtype::int & 2
				WHEN 0 THEN 'AFTER'
				ELSE 'BEFORE'
			END AS timing,
			CASE
				WHEN t.tgtype::int & 4 = 4 THEN ARRAY['INSERT']
				WHEN t.tgtype::int & 8 = 8 THEN ARRAY['UPDATE']
				WHEN t.tgtype::int & 16 = 16 THEN ARRAY['DELETE']
				ELSE ARRAY[]::text[]
			END AS events,
			p.proname AS function_name,
			pg_get_functiondef(p.oid) AS function_definition,
			t.tgenabled != 'D' AS enabled
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_proc p ON t.tgfoid = p.oid
		WHERE n.nspname = $1
			AND c.relname = $2
			AND NOT t.tgisinternal
		ORDER BY t.tgname;`

	rows, err := c.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %w", err)
	}
	defer rows.Close()

	var triggers []*protos.TriggerDefinition
	for rows.Next() {
		var triggerName, triggerDef, timing, functionName, functionDef pgtype.Text
		var events []string
		var enabled pgtype.Bool

		if err := rows.Scan(&triggerName, &triggerDef, &timing, &events, &functionName, &functionDef, &enabled); err != nil {
			return nil, fmt.Errorf("error scanning trigger row: %w", err)
		}

		triggers = append(triggers, &protos.TriggerDefinition{
			Name:               triggerName.String,
			TableName:          tableName,
			Definition:         triggerDef.String,
			Timing:             timing.String,
			Events:             events,
			FunctionName:       functionName.String,
			FunctionDefinition: functionDef.String,
			Enabled:            enabled.Bool,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trigger rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("[GetTableTriggers] found %d triggers for table %s.%s", len(triggers), schemaName, tableName))

	return triggers, nil
}

// GetTableConstraints extracts all constraints for a given table
func (c *PostgresConnector) GetTableConstraints(
	ctx context.Context,
	schemaName, tableName string,
) ([]*protos.ConstraintDefinition, error) {
	query := `
		SELECT
			con.conname AS constraint_name,
			con.contype AS constraint_type,
			pg_get_constraintdef(con.oid) AS constraint_definition,
			array_agg(DISTINCT a.attname ORDER BY a.attname) FILTER (WHERE a.attname IS NOT NULL) AS columns,
			CASE 
				WHEN con.confrelid != 0 THEN (SELECT relname FROM pg_class WHERE oid = con.confrelid)
				ELSE NULL
			END AS referenced_table,
			CASE 
				WHEN con.confrelid != 0 THEN (
					SELECT array_agg(attname ORDER BY attnum)
					FROM pg_attribute
					WHERE attrelid = con.confrelid AND attnum = ANY(con.confkey)
				)
				ELSE NULL
			END AS referenced_columns
		FROM pg_constraint con
		JOIN pg_class t ON con.conrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		LEFT JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
		WHERE n.nspname = $1 AND t.relname = $2
			AND con.contype IN ('p', 'f', 'u', 'c', 'n') -- p=primary key, f=foreign key, u=unique, c=check, n=not null
		GROUP BY con.oid, con.conname, con.contype, con.confrelid, con.confkey
		ORDER BY con.conname;`

	rows, err := c.conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying constraints: %w", err)
	}
	defer rows.Close()

	var constraints []*protos.ConstraintDefinition
	for rows.Next() {
		var constraintName, constraintType, constraintDef pgtype.Text
		var columns []string
		var referencedTable pgtype.Text
		var referencedColumns []string

		if err := rows.Scan(&constraintName, &constraintType, &constraintDef, &columns, &referencedTable, &referencedColumns); err != nil {
			return nil, fmt.Errorf("error scanning constraint row: %w", err)
		}

		// Map constraint type codes to readable names
		constraintTypeName := ""
		switch constraintType.String {
		case "p":
			constraintTypeName = "PRIMARY KEY"
		case "f":
			constraintTypeName = "FOREIGN KEY"
		case "u":
			constraintTypeName = "UNIQUE"
		case "c":
			constraintTypeName = "CHECK"
		case "n":
			constraintTypeName = "NOT NULL"
		}

		// Skip primary key constraints as they're handled by table creation
		if constraintType.String == "p" {
			continue
		}

		constraints = append(constraints, &protos.ConstraintDefinition{
			Name:              constraintName.String,
			TableName:         tableName,
			Type:              constraintTypeName,
			Definition:        constraintDef.String,
			Columns:           columns,
			ReferencedTable:   referencedTable.String,
			ReferencedColumns: referencedColumns,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating constraint rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("[GetTableConstraints] found %d constraints for table %s.%s", len(constraints), schemaName, tableName))

	return constraints, nil
}
