package connpostgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// GetIndexesForTable retrieves all indexes for a given table
func (c *PostgresConnector) GetIndexesForTable(ctx context.Context, schemaTable *utils.SchemaTable) ([]*protos.IndexDefinition, error) {
	query := `
		SELECT
			i.relname AS index_name,
			pg_get_indexdef(ix.indexrelid) AS index_def,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary
		FROM pg_index ix
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = $1
		AND t.relname = $2
		AND t.relkind = 'r'
		ORDER BY i.relname
	`

	rows, err := c.conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	indexes := make([]*protos.IndexDefinition, 0)
	for rows.Next() {
		var indexName, indexDef string
		var isUnique, isPrimary bool

		if err := rows.Scan(&indexName, &indexDef, &isUnique, &isPrimary); err != nil {
			return nil, fmt.Errorf("error scanning index row: %w", err)
		}

		// Skip primary key indexes as they're handled separately
		if isPrimary {
			continue
		}

		indexes = append(indexes, &protos.IndexDefinition{
			IndexName: indexName,
			IndexDef:  indexDef,
			IsUnique:  isUnique,
			IsPrimary: isPrimary,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over index rows: %w", err)
	}

	return indexes, nil
}

// GetTriggersForTable retrieves all triggers for a given table
func (c *PostgresConnector) GetTriggersForTable(ctx context.Context, schemaTable *utils.SchemaTable) ([]*protos.TriggerDefinition, error) {
	query := `
		SELECT
			t.tgname AS trigger_name,
			pg_get_triggerdef(t.oid) AS trigger_def,
			CASE t.tgtype & CAST(2 AS int2)
				WHEN 0 THEN 'AFTER'
				ELSE 'BEFORE'
			END AS timing,
			CASE
				WHEN t.tgtype & CAST(4 AS int2) = 4 THEN 'INSERT'
				WHEN t.tgtype & CAST(8 AS int2) = 8 THEN 'DELETE'
				WHEN t.tgtype & CAST(16 AS int2) = 16 THEN 'UPDATE'
				WHEN t.tgtype & CAST(32 AS int2) = 32 THEN 'TRUNCATE'
			END AS events,
			CASE t.tgtype & CAST(1 AS int2)
				WHEN 0 THEN 'STATEMENT'
				ELSE 'ROW'
			END AS for_each
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		AND c.relname = $2
		AND NOT t.tgisinternal
		ORDER BY t.tgname
	`

	rows, err := c.conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	triggers := make([]*protos.TriggerDefinition, 0)
	for rows.Next() {
		var triggerName, triggerDef, timing, events, forEach string

		if err := rows.Scan(&triggerName, &triggerDef, &timing, &events, &forEach); err != nil {
			return nil, fmt.Errorf("error scanning trigger row: %w", err)
		}

		triggers = append(triggers, &protos.TriggerDefinition{
			TriggerName: triggerName,
			TriggerDef:  triggerDef,
			Timing:      timing,
			Events:      events,
			ForEach:     forEach,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over trigger rows: %w", err)
	}

	return triggers, nil
}

// CompareAndGenerateIndexDeltas compares source and destination indexes and generates deltas
func (c *PostgresConnector) CompareAndGenerateIndexDeltas(
	ctx context.Context,
	srcSchemaTable *utils.SchemaTable,
	dstSchemaTable *utils.SchemaTable,
) (addedIndexes []*protos.IndexDefinition, droppedIndexes []string, err error) {
	srcIndexes, err := c.GetIndexesForTable(ctx, srcSchemaTable)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting source indexes: %w", err)
	}

	dstIndexes, err := c.GetIndexesForTable(ctx, dstSchemaTable)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting destination indexes: %w", err)
	}

	// Create maps for comparison by index definition (columns, uniqueness)
	// We extract the column/type part from the index definition to compare structure, not names
	srcIndexMap := make(map[string]*protos.IndexDefinition)
	for _, idx := range srcIndexes {
		// Extract the key part: "USING btree (column1, column2)" from the definition
		key := extractIndexStructure(idx.IndexDef, idx.IsUnique)
		srcIndexMap[key] = idx
	}

	dstIndexMap := make(map[string]*protos.IndexDefinition)
	dstIndexNameMap := make(map[string]bool)
	for _, idx := range dstIndexes {
		key := extractIndexStructure(idx.IndexDef, idx.IsUnique)
		dstIndexMap[key] = idx
		dstIndexNameMap[idx.IndexName] = true
	}

	// Find added indexes (indexes in source that don't exist in destination by structure)
	addedIndexes = make([]*protos.IndexDefinition, 0)
	for key, idx := range srcIndexMap {
		if _, exists := dstIndexMap[key]; !exists {
			addedIndexes = append(addedIndexes, idx)
		}
	}

	// Find dropped indexes (indexes in destination that don't exist in source by structure)
	droppedIndexes = make([]string, 0)
	for key, idx := range dstIndexMap {
		if _, exists := srcIndexMap[key]; !exists {
			droppedIndexes = append(droppedIndexes, idx.IndexName)
		}
	}

	return addedIndexes, droppedIndexes, nil
}

// extractIndexStructure extracts the structural part of an index definition for comparison
// Returns a normalized string like "UNIQUE:btree:email" or "btree:name,email"
func extractIndexStructure(indexDef string, isUnique bool) string {
	// Extract the USING clause and columns
	// Example: "CREATE INDEX idx_name ON schema.table USING btree (column1, column2)"
	usingPos := strings.Index(indexDef, " USING ")
	if usingPos == -1 {
		return indexDef // fallback to full definition
	}
	
	structure := indexDef[usingPos+7:] // Skip " USING "
	if isUnique {
		return "UNIQUE:" + structure
	}
	return structure
}

// CompareAndGenerateTriggerDeltas compares source and destination triggers and generates deltas
func (c *PostgresConnector) CompareAndGenerateTriggerDeltas(
	ctx context.Context,
	srcSchemaTable *utils.SchemaTable,
	dstSchemaTable *utils.SchemaTable,
) (addedTriggers []*protos.TriggerDefinition, droppedTriggers []string, err error) {
	srcTriggers, err := c.GetTriggersForTable(ctx, srcSchemaTable)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting source triggers: %w", err)
	}

	dstTriggers, err := c.GetTriggersForTable(ctx, dstSchemaTable)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting destination triggers: %w", err)
	}

	// Create maps for easy comparison
	srcTriggerMap := make(map[string]*protos.TriggerDefinition)
	for _, trg := range srcTriggers {
		srcTriggerMap[trg.TriggerName] = trg
	}

	dstTriggerMap := make(map[string]*protos.TriggerDefinition)
	for _, trg := range dstTriggers {
		dstTriggerMap[trg.TriggerName] = trg
	}

	// Find added triggers
	addedTriggers = make([]*protos.TriggerDefinition, 0)
	for name, trg := range srcTriggerMap {
		if _, exists := dstTriggerMap[name]; !exists {
			addedTriggers = append(addedTriggers, trg)
		}
	}

	// Find dropped triggers
	droppedTriggers = make([]string, 0)
	for name := range dstTriggerMap {
		if _, exists := srcTriggerMap[name]; !exists {
			droppedTriggers = append(droppedTriggers, name)
		}
	}

	return addedTriggers, droppedTriggers, nil
}
