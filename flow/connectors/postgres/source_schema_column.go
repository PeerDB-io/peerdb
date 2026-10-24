package connpostgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// isExcludedColumn reports whether a source column is left out of replicated rows: excluded in the table mapping,
// or carrying the name PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN reserves for its own column.
func isExcludedColumn(mapping model.SourceTableMapping, sourceSchemaAsDestinationColumn bool, column string) bool {
	if _, ok := mapping.Exclude[column]; ok {
		return true
	}
	return sourceSchemaAsDestinationColumn && column == internal.SourceSchemaColumnName
}

// reservedSourceSchemaColumnQuery finds tables among the mirrored ones ($1 schemas, $2 names) and all their
// inheritance descendants or partitions that have a column named like the source schema column ($3). It follows
// every pg_inherits edge, deliberately more than CDC routing does, as the snapshot reads every descendant. The
// planner overestimates the closure, so LIMIT 1 keeps the per-relation probe from becoming a scan of the whole
// pg_attribute index, and MATERIALIZED keeps it from joining every relation to pg_class first; the probes run in
// OID order for locality.
const reservedSourceSchemaColumnQuery = `
	WITH RECURSIVE mirrored AS (
		SELECT c.oid
		FROM unnest($1::text[], $2::text[]) AS t(nspname, relname)
		JOIN pg_catalog.pg_namespace n ON n.nspname = t.nspname
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.relname
	), candidates AS (
		SELECT oid FROM mirrored
		UNION
		SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN candidates t ON i.inhparent = t.oid
	), offending AS MATERIALIZED (
		SELECT t.oid
		FROM (SELECT oid FROM candidates ORDER BY oid) t
		CROSS JOIN LATERAL (
			SELECT FROM pg_catalog.pg_attribute a
			WHERE a.attrelid = t.oid AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped
			LIMIT 1
		) a
	)
	SELECT n.nspname, c.relname
	FROM offending o
	JOIN pg_catalog.pg_class c ON c.oid = o.oid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	ORDER BY 1, 2`

// checkReservedSourceSchemaColumn rejects mirrored tables, and their descendants or partitions, that have a
// column named like the source schema column while PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN is on: its values
// would be silently replaced by the source schema.
func (c *PostgresConnector) checkReservedSourceSchemaColumn(
	ctx context.Context, env map[string]string, tables []*common.QualifiedTable,
) error {
	if enabled, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, env); err != nil || !enabled {
		return err
	}
	schemas, names := splitQualifiedTables(tables)
	rows, err := c.conn.Query(ctx, reservedSourceSchemaColumnQuery, schemas, names, internal.SourceSchemaColumnName)
	if err != nil {
		return fmt.Errorf("failed to check for columns named %s: %w", internal.SourceSchemaColumnName, err)
	}
	offending, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var schema, table string
		err := row.Scan(&schema, &table)
		return common.QuoteIdentifier(schema) + "." + common.QuoteIdentifier(table), err
	})
	if err != nil {
		return fmt.Errorf("failed to read tables with columns named %s: %w", internal.SourceSchemaColumnName, err)
	}
	if len(offending) > 0 {
		return fmt.Errorf("column %s is reserved while PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN is enabled, "+
			"rename it in: %s", internal.SourceSchemaColumnName, strings.Join(offending, ", "))
	}
	return nil
}
