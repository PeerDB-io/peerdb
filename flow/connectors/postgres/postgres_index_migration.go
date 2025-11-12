package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// getTableIndexes retrieves all non-primary-key indexes for a table from the source
// Primary keys are already handled in generateCreateTableSQLForNormalizedTable
// Returns proto IndexDescription for direct use in TableSchema
func (c *PostgresConnector) getTableIndexes(
	ctx context.Context,
	schemaTable *utils.SchemaTable,
) ([]*protos.IndexDescription, error) {
	query := `
		SELECT 
			i.relname AS index_name,
			pg_get_indexdef(idx.indexrelid) AS index_def,
			idx.indisprimary AS is_primary,
			idx.indisunique AS is_unique,
			idx.indisreplident AS is_replica
		FROM pg_index idx
		JOIN pg_class t ON idx.indrelid = t.oid
		JOIN pg_class i ON idx.indexrelid = i.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE n.nspname = $1 
		AND t.relname = $2
		AND idx.indisprimary = false
		ORDER BY i.relname
	`

	rows, err := c.conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	var indexes []*protos.IndexDescription
	for rows.Next() {
		var indexName, indexDef string
		var isPrimary, isUnique, isReplica bool
		if err := rows.Scan(&indexName, &indexDef, &isPrimary, &isUnique, &isReplica); err != nil {
			return nil, fmt.Errorf("error scanning index definition: %w", err)
		}
		indexes = append(indexes, &protos.IndexDescription{
			IndexName: indexName,
			IndexDef:  indexDef,
			IsPrimary: isPrimary,
			IsUnique:  isUnique,
			IsReplica: isReplica,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Found %d indexes to migrate for table %s", len(indexes), schemaTable),
		slog.String("schema", schemaTable.Schema),
		slog.String("table", schemaTable.Table))

	return indexes, nil
}

// createTableIndexes creates indexes on the destination table
func (c *PostgresConnector) createTableIndexes(
	ctx context.Context,
	tx pgx.Tx,
	sourceTable *utils.SchemaTable,
	destTable *utils.SchemaTable,
) error {
	indexes, err := c.getTableIndexes(ctx, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to fetch indexes: %w", err)
	}

	if len(indexes) == 0 {
		c.logger.Info("No secondary indexes to create for table", slog.String("table", destTable.String()))
		return nil
	}

	for _, idx := range indexes {
		// Replace source schema.table with destination schema.table in the index definition
		// pg_get_indexdef returns: CREATE [UNIQUE] INDEX index_name ON schema.table USING ...
		modifiedIndexDef := strings.Replace(
			idx.IndexDef,
			fmt.Sprintf(" ON %s.%s ",
				utils.QuoteIdentifier(sourceTable.Schema),
				utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf(" ON %s ", destTable.String()),
			1,
		)

		// Also handle case without schema qualification
		modifiedIndexDef = strings.Replace(
			modifiedIndexDef,
			fmt.Sprintf(" ON %s ", utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf(" ON %s ", destTable.String()),
			1,
		)

		// Add CONCURRENTLY if not in transaction (but we're in a transaction during setup, so this won't apply)
		// Just create the index normally during initial setup
		c.logger.Info("Creating index on destination table",
			slog.String("indexName", idx.IndexName),
			slog.String("table", destTable.String()),
			slog.Bool("isUnique", idx.IsUnique))

		_, err := tx.Exec(ctx, modifiedIndexDef)
		if err != nil {
			// Don't fail the entire migration if one index fails
			// Some indexes might have dependencies or use extensions not available
			c.logger.Warn("Failed to create index, skipping",
				slog.String("indexName", idx.IndexName),
				slog.String("table", destTable.String()),
				slog.String("indexDef", modifiedIndexDef),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created index",
			slog.String("indexName", idx.IndexName),
			slog.String("table", destTable.String()))
	}

	return nil
}

// getTableIndexCount returns the count of non-primary indexes for a table
// Useful for logging and validation
func (c *PostgresConnector) getTableIndexCount(
	ctx context.Context,
	schemaTable *utils.SchemaTable,
) (int, error) {
	var count int
	query := `
		SELECT COUNT(*)
		FROM pg_index idx
		JOIN pg_class t ON idx.indrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE n.nspname = $1 
		AND t.relname = $2
		AND idx.indisprimary = false
	`
	err := c.conn.QueryRow(ctx, query, schemaTable.Schema, schemaTable.Table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error counting indexes: %w", err)
	}
	return count, nil
}

// createTableIndexesFromSchema creates indexes on the destination table from the TableSchema
// This method uses the indexes that were fetched from the source during SetupTableSchema
func (c *PostgresConnector) createTableIndexesFromSchema(
	ctx context.Context,
	tx pgx.Tx,
	tableSchema *protos.TableSchema,
	destTable *utils.SchemaTable,
) error {
	if len(tableSchema.Indexes) == 0 {
		c.logger.Info("No secondary indexes to create for table", slog.String("table", destTable.String()))
		return nil
	}

	// Parse source table from the table schema identifier
	sourceTable, err := utils.ParseSchemaTable(tableSchema.TableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to parse source table identifier: %w", err)
	}

	for _, idx := range tableSchema.Indexes {
		// Replace source schema.table with destination schema.table in the index definition
		// pg_get_indexdef returns: CREATE [UNIQUE] INDEX index_name ON schema.table USING ...
		modifiedIndexDef := strings.Replace(
			idx.IndexDef,
			fmt.Sprintf(" ON %s.%s ",
				utils.QuoteIdentifier(sourceTable.Schema),
				utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf(" ON %s ", destTable.String()),
			1,
		)

		// Also handle case without schema qualification
		modifiedIndexDef = strings.Replace(
			modifiedIndexDef,
			fmt.Sprintf(" ON %s ", utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf(" ON %s ", destTable.String()),
			1,
		)

		c.logger.Info("Creating index on destination table",
			slog.String("indexName", idx.IndexName),
			slog.String("table", destTable.String()),
			slog.String("indexDef", modifiedIndexDef),
			slog.Bool("isUnique", idx.IsUnique),
			slog.Bool("isPrimary", idx.IsPrimary),
			slog.Bool("isReplica", idx.IsReplica),
		)

		_, err := tx.Exec(ctx, modifiedIndexDef)
		if err != nil {
			// Don't fail the entire migration if one index fails
			// Some indexes might have dependencies or use extensions not available
			c.logger.Warn("Failed to create index, skipping",
				slog.String("indexName", idx.IndexName),
				slog.String("table", destTable.String()),
				slog.String("indexDef", modifiedIndexDef),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created index",
			slog.String("indexName", idx.IndexName),
			slog.String("table", destTable.String()),
			slog.String("indexDef", modifiedIndexDef),
			slog.Bool("isUnique", idx.IsUnique),
			slog.Bool("isPrimary", idx.IsPrimary),
			slog.Bool("isReplica", idx.IsReplica),
		)
	}

	return nil
}
