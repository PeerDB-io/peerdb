package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// IndexInfo represents information about a PostgreSQL index
type IndexInfo struct {
	IndexName    string
	TableSchema  string
	TableName    string
	IndexDef     string
	IsUnique     bool
	IsPrimary    bool
	IndexColumns []string
}

// TriggerInfo represents information about a PostgreSQL trigger
type TriggerInfo struct {
	TriggerName       string
	TableSchema       string
	TableName         string
	TriggerDef        string
	EventManipulation string
	ActionTiming      string
	ActionStatement   string
}

// ConstraintInfo represents information about a PostgreSQL constraint
type ConstraintInfo struct {
	ConstraintName string
	TableSchema    string
	TableName      string
	ConstraintType string // 'c' for check, 'f' for foreign key, 'u' for unique, 'p' for primary key
	ConstraintDef  string // Full constraint definition from pg_get_constraintdef
	IsDeferrable   bool
	IsDeferred     bool
}

// SyncIndexesAndTriggers syncs indexes, triggers, and constraints from source to destination.
// This is called once during initial setup, not for on-the-fly changes.
//
// Features:
//   - Syncs all non-primary-key indexes from source to destination
//   - Syncs all triggers from source to destination
//   - Syncs check constraints and foreign key constraints
//   - Automatically syncs trigger functions if they don't exist on destination
//   - Skips indexes/triggers/constraints that already exist on destination
//
// Limitations:
//   - Only runs during initial setup (not for on-the-fly changes)
//   - Requires trigger functions to exist on source (or will attempt to sync them)
//   - Primary key indexes and constraints are skipped (already exist)
//   - Foreign key constraints referencing tables not in the sync are skipped
func (c *PostgresConnector) SyncIndexesAndTriggers(
	ctx context.Context,
	tableMappings []*protos.TableMapping,
	sourceConn *PostgresConnector,
) error {
	c.logger.Info("Starting index and trigger synchronization",
		slog.Int("tableCount", len(tableMappings)))

	for _, tableMapping := range tableMappings {
		srcTable, err := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("error parsing source table %s: %w", tableMapping.SourceTableIdentifier, err)
		}

		dstTable, err := utils.ParseSchemaTable(tableMapping.DestinationTableIdentifier)
		if err != nil {
			return fmt.Errorf("error parsing destination table %s: %w", tableMapping.DestinationTableIdentifier, err)
		}

		// Sync indexes
		if err := c.syncIndexesForTable(ctx, srcTable, dstTable, sourceConn); err != nil {
			c.logger.Warn("Failed to sync indexes for table",
				slog.String("srcTable", srcTable.String()),
				slog.String("dstTable", dstTable.String()),
				slog.Any("error", err))
			// Continue with other tables even if one fails
		}

		// Sync triggers
		if err := c.syncTriggersForTable(ctx, srcTable, dstTable, sourceConn); err != nil {
			c.logger.Warn("Failed to sync triggers for table",
				slog.String("srcTable", srcTable.String()),
				slog.String("dstTable", dstTable.String()),
				slog.Any("error", err))
			// Continue with other tables even if one fails
		}

		// Sync constraints (check constraints and foreign keys)
		if err := c.syncConstraintsForTable(ctx, srcTable, dstTable, sourceConn, tableMappings); err != nil {
			c.logger.Warn("Failed to sync constraints for table",
				slog.String("srcTable", srcTable.String()),
				slog.String("dstTable", dstTable.String()),
				slog.Any("error", err))
			// Continue with other tables even if one fails
		}
	}

	c.logger.Info("Completed index, trigger, and constraint synchronization")
	return nil
}

// syncIndexesForTable syncs indexes for a specific table
func (c *PostgresConnector) syncIndexesForTable(
	ctx context.Context,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
	sourceConn *PostgresConnector,
) error {
	// Get indexes from source
	srcIndexes, err := sourceConn.getIndexesForTable(ctx, srcTable)
	if err != nil {
		return fmt.Errorf("error getting source indexes: %w", err)
	}

	// Get indexes from destination
	dstIndexes, err := c.getIndexesForTable(ctx, dstTable)
	if err != nil {
		return fmt.Errorf("error getting destination indexes: %w", err)
	}

	// Create a map of destination indexes by name for quick lookup
	dstIndexMap := make(map[string]*IndexInfo, len(dstIndexes))
	for _, idx := range dstIndexes {
		dstIndexMap[idx.IndexName] = idx
	}

	// Find missing indexes and create them
	createdCount := 0
	for _, srcIdx := range srcIndexes {
		// Skip primary key indexes - they should already exist
		if srcIdx.IsPrimary {
			continue
		}

		// Check if index already exists in destination
		if _, exists := dstIndexMap[srcIdx.IndexName]; exists {
			c.logger.Debug("Index already exists in destination",
				slog.String("indexName", srcIdx.IndexName),
				slog.String("dstTable", dstTable.String()))
			continue
		}

		// Create the index
		// Replace source schema/table names with destination
		indexSQL := c.adaptIndexSQL(srcIdx.IndexDef, srcTable, dstTable)

		c.logger.Info("Creating index on destination",
			slog.String("indexName", srcIdx.IndexName),
			slog.String("srcTable", srcTable.String()),
			slog.String("dstTable", dstTable.String()),
			slog.String("indexSQL", indexSQL))

		if _, err := c.conn.Exec(ctx, indexSQL); err != nil {
			c.logger.Error("Failed to create index",
				slog.String("indexName", srcIdx.IndexName),
				slog.String("indexSQL", indexSQL),
				slog.Any("error", err))
			// Continue with other indexes even if one fails
			continue
		}

		createdCount++
		c.logger.Info("Successfully created index",
			slog.String("indexName", srcIdx.IndexName),
			slog.String("dstTable", dstTable.String()))
	}

	if createdCount > 0 {
		c.logger.Info("Created indexes for table",
			slog.String("dstTable", dstTable.String()),
			slog.Int("createdCount", createdCount))
	}

	return nil
}

// syncTriggersForTable syncs triggers for a specific table
func (c *PostgresConnector) syncTriggersForTable(
	ctx context.Context,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
	sourceConn *PostgresConnector,
) error {
	c.logger.Info("Starting trigger sync for table",
		slog.String("srcTable", srcTable.String()),
		slog.String("dstTable", dstTable.String()))

	// Get triggers from source
	srcTriggers, err := sourceConn.getTriggersForTable(ctx, srcTable)
	if err != nil {
		return fmt.Errorf("error getting source triggers: %w", err)
	}

	c.logger.Info("Retrieved source triggers",
		slog.String("srcTable", srcTable.String()),
		slog.Int("triggerCount", len(srcTriggers)))

	// Get triggers from destination
	dstTriggers, err := c.getTriggersForTable(ctx, dstTable)
	if err != nil {
		return fmt.Errorf("error getting destination triggers: %w", err)
	}

	c.logger.Info("Retrieved destination triggers",
		slog.String("dstTable", dstTable.String()),
		slog.Int("triggerCount", len(dstTriggers)))

	// Create a map of destination triggers by name for quick lookup
	dstTriggerMap := make(map[string]*TriggerInfo, len(dstTriggers))
	for _, trig := range dstTriggers {
		dstTriggerMap[trig.TriggerName] = trig
	}

	// Find missing triggers and create them
	createdCount := 0
	for _, srcTrig := range srcTriggers {
		c.logger.Info("Processing source trigger",
			slog.String("triggerName", srcTrig.TriggerName),
			slog.String("triggerDef", srcTrig.TriggerDef),
			slog.String("srcTable", srcTable.String()))

		// Check if trigger already exists in destination
		if _, exists := dstTriggerMap[srcTrig.TriggerName]; exists {
			c.logger.Info("Trigger already exists in destination, skipping",
				slog.String("triggerName", srcTrig.TriggerName),
				slog.String("dstTable", dstTable.String()))
			continue
		}

		// Extract function name from trigger definition
		funcName, funcSchema := c.extractFunctionFromTriggerDef(srcTrig.TriggerDef)
		c.logger.Info("Extracted function from trigger definition",
			slog.String("triggerName", srcTrig.TriggerName),
			slog.String("functionName", funcName),
			slog.String("functionSchema", funcSchema))

		// Check if function exists on destination
		// Try multiple schemas if function name doesn't have schema qualification
		funcExists := false
		if funcName != "" {
			schemasToCheck := []string{funcSchema}
			// If no schema was specified, try public schema and table's schema
			if funcSchema == "public" || funcSchema == "" {
				schemasToCheck = []string{"public", dstTable.Schema, srcTable.Schema}
			}

			for _, schema := range schemasToCheck {
				exists, err := c.checkFunctionExists(ctx, schema, funcName)
				if err != nil {
					c.logger.Warn("Failed to check if function exists",
						slog.String("functionName", funcName),
						slog.String("functionSchema", schema),
						slog.Any("error", err))
					continue
				}
				if exists {
					funcExists = true
					funcSchema = schema // Update to the actual schema where function exists
					c.logger.Info("Found function on destination",
						slog.String("functionName", funcName),
						slog.String("functionSchema", schema))
					break
				}
			}

			if !funcExists {
				// Try to sync the function from source
				// Try multiple schemas on source to find the function
				sourceSchemasToCheck := []string{funcSchema, "public", srcTable.Schema}
				if funcSchema == "public" || funcSchema == "" {
					sourceSchemasToCheck = []string{"public", srcTable.Schema}
				}

				funcSynced := false
				for _, sourceSchema := range sourceSchemasToCheck {
					c.logger.Info("Attempting to sync trigger function from source",
						slog.String("functionName", funcName),
						slog.String("sourceSchema", sourceSchema),
						slog.String("targetSchema", funcSchema))

					if err := c.syncTriggerFunction(ctx, sourceSchema, funcName, funcSchema, sourceConn); err != nil {
						c.logger.Debug("Failed to sync function from this schema, trying next",
							slog.String("functionName", funcName),
							slog.String("sourceSchema", sourceSchema),
							slog.Any("error", err))
						continue
					}

					// Verify function was created
					funcExists, err = c.checkFunctionExists(ctx, funcSchema, funcName)
					if err == nil && funcExists {
						funcSynced = true
						c.logger.Info("Successfully synced trigger function",
							slog.String("functionName", funcName),
							slog.String("sourceSchema", sourceSchema),
							slog.String("targetSchema", funcSchema))
						break
					}
				}

				if !funcSynced {
					c.logger.Warn("Failed to sync trigger function from source, skipping trigger",
						slog.String("triggerName", srcTrig.TriggerName),
						slog.String("functionName", funcName),
						slog.String("checkedSchemas", fmt.Sprintf("%v", sourceSchemasToCheck)),
						slog.String("hint", "Create the function manually on destination"))
					continue
				}
			}
		}

		// Create the trigger
		// pg_get_triggerdef already gives us the full CREATE TRIGGER statement
		// We just need to replace source schema/table names with destination
		triggerSQL := c.adaptTriggerSQL(srcTrig.TriggerDef, srcTable, dstTable)

		c.logger.Info("Creating trigger on destination",
			slog.String("triggerName", srcTrig.TriggerName),
			slog.String("srcTable", srcTable.String()),
			slog.String("dstTable", dstTable.String()),
			slog.String("triggerSQL", triggerSQL))

		if _, err := c.conn.Exec(ctx, triggerSQL); err != nil {
			c.logger.Error("Failed to create trigger",
				slog.String("triggerName", srcTrig.TriggerName),
				slog.String("triggerSQL", triggerSQL),
				slog.Any("error", err))
			// Continue with other triggers even if one fails
			continue
		}

		createdCount++
		c.logger.Info("Successfully created trigger",
			slog.String("triggerName", srcTrig.TriggerName),
			slog.String("dstTable", dstTable.String()))
	}

	if createdCount > 0 {
		c.logger.Info("Created triggers for table",
			slog.String("dstTable", dstTable.String()),
			slog.Int("createdCount", createdCount))
	}

	return nil
}

// getIndexesForTable retrieves all indexes for a given table
func (c *PostgresConnector) getIndexesForTable(
	ctx context.Context,
	table *utils.SchemaTable,
) ([]*IndexInfo, error) {
	// Use pg_indexes view which is simpler and more reliable
	query := `
		SELECT
			indexname,
			schemaname,
			tablename,
			indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
		ORDER BY indexname
	`

	rows, err := c.conn.Query(ctx, query, table.Schema, table.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes: %w", err)
	}
	defer rows.Close()

	var indexes []*IndexInfo
	for rows.Next() {
		var idx IndexInfo
		err := rows.Scan(
			&idx.IndexName,
			&idx.TableSchema,
			&idx.TableName,
			&idx.IndexDef,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning index row: %w", err)
		}

		// Determine if index is unique or primary key
		idx.IsUnique = strings.Contains(strings.ToUpper(idx.IndexDef), "UNIQUE")

		// Check if it's a primary key constraint
		// Primary keys are typically named like tablename_pkey
		idx.IsPrimary = strings.HasSuffix(idx.IndexName, "_pkey") ||
			strings.Contains(strings.ToUpper(idx.IndexDef), "PRIMARY KEY")

		// Extract column names from index definition
		// This is a simple extraction - may need refinement for complex cases
		idx.IndexColumns = c.extractColumnsFromIndexDef(idx.IndexDef)

		indexes = append(indexes, &idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	return indexes, nil
}

// extractColumnsFromIndexDef extracts column names from index definition
func (c *PostgresConnector) extractColumnsFromIndexDef(indexDef string) []string {
	// This is a simplified extraction - looks for patterns like (col1, col2)
	// For more complex cases, we might need to parse the SQL properly
	var columns []string

	// Find the part between parentheses
	start := strings.Index(indexDef, "(")
	end := strings.LastIndex(indexDef, ")")
	if start >= 0 && end > start {
		colPart := indexDef[start+1 : end]
		// Split by comma and clean up
		parts := strings.Split(colPart, ",")
		for _, part := range parts {
			col := strings.TrimSpace(part)
			// Remove function calls, operators, etc. - just get column name
			// Remove quotes if present
			col = strings.Trim(col, `"'`)
			// Take only the column name part (before any operators or functions)
			if spaceIdx := strings.Index(col, " "); spaceIdx > 0 {
				col = col[:spaceIdx]
			}
			if len(col) > 0 {
				columns = append(columns, col)
			}
		}
	}

	return columns
}

// getTriggersForTable retrieves all triggers for a given table
func (c *PostgresConnector) getTriggersForTable(
	ctx context.Context,
	table *utils.SchemaTable,
) ([]*TriggerInfo, error) {
	// Use pg_trigger and pg_proc to get full trigger definition
	query := `
		SELECT
			t.tgname as trigger_name,
			n.nspname as schema_name,
			c.relname as table_name,
			pg_get_triggerdef(t.oid) as trigger_def,
			CASE 
				WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
				WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END as action_timing,
			CASE 
				WHEN t.tgtype & 4 = 4 THEN 'INSERT'
				WHEN t.tgtype & 8 = 8 THEN 'DELETE'
				WHEN t.tgtype & 16 = 16 THEN 'UPDATE'
				ELSE 'UNKNOWN'
			END as event_manipulation
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 
			AND c.relname = $2
			AND NOT t.tgisinternal
		ORDER BY t.tgname
	`

	rows, err := c.conn.Query(ctx, query, table.Schema, table.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %w", err)
	}
	defer rows.Close()

	var triggers []*TriggerInfo
	for rows.Next() {
		var trig TriggerInfo
		err := rows.Scan(
			&trig.TriggerName,
			&trig.TableSchema,
			&trig.TableName,
			&trig.TriggerDef,
			&trig.ActionTiming,
			&trig.EventManipulation,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning trigger row: %w", err)
		}

		// Extract action statement from trigger definition
		// The trigger_def from pg_get_triggerdef already contains the full CREATE TRIGGER statement
		// We just need to extract the EXECUTE FUNCTION part
		trig.ActionStatement = c.extractActionStatement(trig.TriggerDef)

		triggers = append(triggers, &trig)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trigger rows: %w", err)
	}

	return triggers, nil
}

// extractActionStatement extracts the action statement (EXECUTE FUNCTION ...) from trigger definition
func (c *PostgresConnector) extractActionStatement(triggerDef string) string {
	// pg_get_triggerdef returns something like:
	// CREATE TRIGGER trigger_name BEFORE INSERT ON schema.table FOR EACH ROW EXECUTE FUNCTION function_name()
	// We want to extract the EXECUTE FUNCTION part
	executeIdx := strings.Index(strings.ToUpper(triggerDef), "EXECUTE")
	if executeIdx >= 0 {
		return triggerDef[executeIdx:]
	}
	return ""
}

// adaptIndexSQL adapts index SQL from source to destination table
func (c *PostgresConnector) adaptIndexSQL(
	indexSQL string,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
) string {
	// Replace source schema.table with destination schema.table
	adapted := strings.ReplaceAll(indexSQL,
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(srcTable.Schema), utils.QuoteIdentifier(srcTable.Table)),
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(dstTable.Schema), utils.QuoteIdentifier(dstTable.Table)))

	// Also handle unquoted versions
	adapted = strings.ReplaceAll(adapted,
		fmt.Sprintf("%s.%s", srcTable.Schema, srcTable.Table),
		fmt.Sprintf("%s.%s", dstTable.Schema, dstTable.Table))

	return adapted
}

// extractFunctionFromTriggerDef extracts function name and schema from trigger definition
func (c *PostgresConnector) extractFunctionFromTriggerDef(triggerDef string) (funcName, funcSchema string) {
	// pg_get_triggerdef returns something like:
	// CREATE TRIGGER trigger_name BEFORE INSERT ON schema.table FOR EACH ROW EXECUTE FUNCTION schema.function_name()
	// We need to extract the function name and schema

	// Find "EXECUTE FUNCTION" or "EXECUTE PROCEDURE"
	executeIdx := strings.Index(strings.ToUpper(triggerDef), "EXECUTE")
	if executeIdx < 0 {
		return "", ""
	}

	// Get the part after EXECUTE
	executePart := triggerDef[executeIdx:]

	// Look for FUNCTION or PROCEDURE keyword
	funcKeywordIdx := strings.Index(strings.ToUpper(executePart), "FUNCTION")
	if funcKeywordIdx < 0 {
		funcKeywordIdx = strings.Index(strings.ToUpper(executePart), "PROCEDURE")
		if funcKeywordIdx < 0 {
			return "", ""
		}
	}

	// Get the function part (after FUNCTION/PROCEDURE keyword)
	funcPart := strings.TrimSpace(executePart[funcKeywordIdx+8:]) // 8 = len("FUNCTION") or len("PROCEDURE")

	// Remove trailing parentheses and whitespace
	funcPart = strings.TrimSpace(strings.TrimSuffix(funcPart, "()"))
	funcPart = strings.TrimSpace(strings.TrimSuffix(funcPart, ")"))

	// Check if it has schema qualification (schema.function)
	if dotIdx := strings.LastIndex(funcPart, "."); dotIdx >= 0 {
		funcSchema = funcPart[:dotIdx]
		funcName = funcPart[dotIdx+1:]
		// Remove quotes if present
		funcSchema = strings.Trim(funcSchema, `"'`)
		funcName = strings.Trim(funcName, `"'`)
	} else {
		// No schema, function is in current schema or public
		funcName = strings.Trim(funcPart, `"'`)
		funcSchema = "public" // Default to public schema
	}

	return funcName, funcSchema
}

// checkFunctionExists checks if a function exists in the specified schema
func (c *PostgresConnector) checkFunctionExists(ctx context.Context, schema, funcName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1 AND p.proname = $2
		)
	`

	var exists bool
	err := c.conn.QueryRow(ctx, query, schema, funcName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking function existence: %w", err)
	}

	return exists, nil
}

// syncTriggerFunction syncs a trigger function from source to destination
func (c *PostgresConnector) syncTriggerFunction(
	ctx context.Context,
	sourceSchema, funcName, targetSchema string,
	sourceConn *PostgresConnector,
) error {
	// Get function definition from source
	query := `
		SELECT pg_get_functiondef(p.oid) as function_def
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = $1 AND p.proname = $2
		LIMIT 1
	`

	var funcDef string
	err := sourceConn.conn.QueryRow(ctx, query, sourceSchema, funcName).Scan(&funcDef)
	if err != nil {
		return fmt.Errorf("error getting function definition from source schema %s: %w", sourceSchema, err)
	}

	if funcDef == "" {
		return fmt.Errorf("function definition is empty")
	}

	c.logger.Info("Retrieved function definition from source",
		slog.String("functionName", funcName),
		slog.String("sourceSchema", sourceSchema),
		slog.String("functionDef", funcDef))

	// Adapt function definition to use target schema if different
	// Replace source schema with target schema in the function definition
	if sourceSchema != targetSchema {
		funcDef = strings.ReplaceAll(funcDef,
			fmt.Sprintf("%s.%s", utils.QuoteIdentifier(sourceSchema), utils.QuoteIdentifier(funcName)),
			fmt.Sprintf("%s.%s", utils.QuoteIdentifier(targetSchema), utils.QuoteIdentifier(funcName)))
		funcDef = strings.ReplaceAll(funcDef,
			fmt.Sprintf("%s.%s", sourceSchema, funcName),
			fmt.Sprintf("%s.%s", targetSchema, funcName))
	}

	// Create function on destination
	// The function definition from pg_get_functiondef already includes CREATE OR REPLACE FUNCTION
	// We just need to execute it
	c.logger.Info("Creating function on destination",
		slog.String("functionName", funcName),
		slog.String("targetSchema", targetSchema))

	if _, err := c.conn.Exec(ctx, funcDef); err != nil {
		return fmt.Errorf("error creating function on destination: %w", err)
	}

	c.logger.Info("Successfully created function on destination",
		slog.String("functionName", funcName),
		slog.String("targetSchema", targetSchema))

	return nil
}

// adaptTriggerSQL adapts trigger SQL from source to destination table
func (c *PostgresConnector) adaptTriggerSQL(
	triggerSQL string,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
) string {
	// Replace source schema.table with destination schema.table
	adapted := strings.ReplaceAll(triggerSQL,
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(srcTable.Schema), utils.QuoteIdentifier(srcTable.Table)),
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(dstTable.Schema), utils.QuoteIdentifier(dstTable.Table)))

	// Also handle unquoted versions
	adapted = strings.ReplaceAll(adapted,
		fmt.Sprintf("%s.%s", srcTable.Schema, srcTable.Table),
		fmt.Sprintf("%s.%s", dstTable.Schema, dstTable.Table))

	return adapted
}

// syncConstraintsForTable syncs constraints (check and foreign key) for a specific table
func (c *PostgresConnector) syncConstraintsForTable(
	ctx context.Context,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
	sourceConn *PostgresConnector,
	tableMappings []*protos.TableMapping,
) error {
	c.logger.Info("Starting constraint sync for table",
		slog.String("srcTable", srcTable.String()),
		slog.String("dstTable", dstTable.String()))

	// Get constraints from source
	srcConstraints, err := sourceConn.getConstraintsForTable(ctx, srcTable)
	if err != nil {
		return fmt.Errorf("error getting source constraints: %w", err)
	}

	c.logger.Info("Retrieved source constraints",
		slog.String("srcTable", srcTable.String()),
		slog.Int("constraintCount", len(srcConstraints)))

	// Get constraints from destination
	dstConstraints, err := c.getConstraintsForTable(ctx, dstTable)
	if err != nil {
		return fmt.Errorf("error getting destination constraints: %w", err)
	}

	c.logger.Info("Retrieved destination constraints",
		slog.String("dstTable", dstTable.String()),
		slog.Int("constraintCount", len(dstConstraints)))

	// Create a map of destination constraints by name for quick lookup
	dstConstraintMap := make(map[string]*ConstraintInfo, len(dstConstraints))
	for _, constraint := range dstConstraints {
		dstConstraintMap[constraint.ConstraintName] = constraint
	}

	// Build a mapping of source table names to destination table names for FK resolution
	tableNameMap := make(map[string]string)
	for _, tm := range tableMappings {
		src, err := utils.ParseSchemaTable(tm.SourceTableIdentifier)
		if err != nil {
			continue
		}
		dst, err := utils.ParseSchemaTable(tm.DestinationTableIdentifier)
		if err != nil {
			continue
		}
		// Map both qualified and unqualified names
		tableNameMap[src.String()] = dst.String()
		tableNameMap[fmt.Sprintf("%s.%s", src.Schema, src.Table)] = fmt.Sprintf("%s.%s", dst.Schema, dst.Table)
	}

	// Find missing constraints and create them
	createdCount := 0
	for _, srcConstraint := range srcConstraints {
		c.logger.Info("Processing source constraint",
			slog.String("constraintName", srcConstraint.ConstraintName),
			slog.String("constraintType", srcConstraint.ConstraintType),
			slog.String("constraintDef", srcConstraint.ConstraintDef),
			slog.String("srcTable", srcTable.String()))

		// Skip primary key constraints - they should already exist
		if srcConstraint.ConstraintType == "p" {
			c.logger.Debug("Skipping primary key constraint",
				slog.String("constraintName", srcConstraint.ConstraintName))
			continue
		}

		// Skip unique constraints that are already covered by unique indexes
		if srcConstraint.ConstraintType == "u" {
			c.logger.Debug("Skipping unique constraint (handled by unique index)",
				slog.String("constraintName", srcConstraint.ConstraintName))
			continue
		}

		// Check if constraint already exists in destination
		if _, exists := dstConstraintMap[srcConstraint.ConstraintName]; exists {
			c.logger.Info("Constraint already exists in destination, skipping",
				slog.String("constraintName", srcConstraint.ConstraintName),
				slog.String("dstTable", dstTable.String()))
			continue
		}

		// Adapt constraint definition for destination
		constraintSQL := c.adaptConstraintSQL(srcConstraint.ConstraintDef, srcTable, dstTable, tableNameMap, srcConstraint.ConstraintName)

		c.logger.Info("Creating constraint on destination",
			slog.String("constraintName", srcConstraint.ConstraintName),
			slog.String("constraintType", srcConstraint.ConstraintType),
			slog.String("srcTable", srcTable.String()),
			slog.String("dstTable", dstTable.String()),
			slog.String("constraintSQL", constraintSQL))

		if _, err := c.conn.Exec(ctx, constraintSQL); err != nil {
			c.logger.Error("Failed to create constraint",
				slog.String("constraintName", srcConstraint.ConstraintName),
				slog.String("constraintType", srcConstraint.ConstraintType),
				slog.String("constraintSQL", constraintSQL),
				slog.Any("error", err))
			// Continue with other constraints even if one fails
			continue
		}

		createdCount++
		c.logger.Info("Successfully created constraint",
			slog.String("constraintName", srcConstraint.ConstraintName),
			slog.String("constraintType", srcConstraint.ConstraintType),
			slog.String("dstTable", dstTable.String()))
	}

	if createdCount > 0 {
		c.logger.Info("Created constraints for table",
			slog.String("dstTable", dstTable.String()),
			slog.Int("createdCount", createdCount))
	}

	return nil
}

// getConstraintsForTable retrieves all constraints for a given table
func (c *PostgresConnector) getConstraintsForTable(
	ctx context.Context,
	table *utils.SchemaTable,
) ([]*ConstraintInfo, error) {
	// Query constraints from pg_constraint
	query := `
		SELECT
			con.conname as constraint_name,
			n.nspname as schema_name,
			c.relname as table_name,
			con.contype::text as constraint_type,
			pg_get_constraintdef(con.oid) as constraint_def,
			con.condeferrable as is_deferrable,
			con.condeferred as is_deferred
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 
			AND c.relname = $2
			AND con.contype IN ('c', 'f') -- 'c' for check, 'f' for foreign key
		ORDER BY con.conname
	`

	rows, err := c.conn.Query(ctx, query, table.Schema, table.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying constraints: %w", err)
	}
	defer rows.Close()

	var constraints []*ConstraintInfo
	for rows.Next() {
		var constraint ConstraintInfo
		err := rows.Scan(
			&constraint.ConstraintName,
			&constraint.TableSchema,
			&constraint.TableName,
			&constraint.ConstraintType,
			&constraint.ConstraintDef,
			&constraint.IsDeferrable,
			&constraint.IsDeferred,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning constraint row: %w", err)
		}

		constraints = append(constraints, &constraint)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating constraint rows: %w", err)
	}

	return constraints, nil
}

// adaptConstraintSQL adapts constraint SQL from source to destination table
func (c *PostgresConnector) adaptConstraintSQL(
	constraintDef string,
	srcTable *utils.SchemaTable,
	dstTable *utils.SchemaTable,
	tableNameMap map[string]string,
	constraintName string,
) string {
	// The constraint definition from pg_get_constraintdef is already in the format:
	// For check: CHECK (expression)
	// For foreign key: FOREIGN KEY (columns) REFERENCES table(columns)
	// We need to:
	// 1. For foreign keys, replace referenced table names using tableNameMap FIRST
	// 2. Then replace source table name with destination table name (for self-referencing FKs)

	adapted := constraintDef

	// For foreign key constraints, replace referenced table names
	// Handle both cross-table and self-referencing foreign keys
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(adapted)), "FOREIGN KEY") {
		// First, add the current table to the tableNameMap if not already present
		// This ensures self-referencing FKs are handled
		srcTableStr := fmt.Sprintf("%s.%s", srcTable.Schema, srcTable.Table)
		dstTableStr := fmt.Sprintf("%s.%s", dstTable.Schema, dstTable.Table)
		if _, exists := tableNameMap[srcTableStr]; !exists {
			tableNameMap[srcTableStr] = dstTableStr
		}
		// Also add unqualified table name
		if _, exists := tableNameMap[srcTable.Table]; !exists {
			tableNameMap[srcTable.Table] = dstTable.Table
		}

		// Look for REFERENCES clause and replace table names
		for srcTableName, dstTableName := range tableNameMap {
			// Handle schema-qualified table names in REFERENCES
			if strings.Contains(srcTableName, ".") {
				parts := strings.Split(srcTableName, ".")
				if len(parts) == 2 {
					srcSchema, srcTbl := parts[0], parts[1]
					dstParts := strings.Split(dstTableName, ".")
					if len(dstParts) == 2 {
						dstSchema, dstTbl := dstParts[0], dstParts[1]
						// Replace schema.table references (quoted)
						adapted = strings.ReplaceAll(adapted,
							fmt.Sprintf("REFERENCES %s.%s", utils.QuoteIdentifier(srcSchema), utils.QuoteIdentifier(srcTbl)),
							fmt.Sprintf("REFERENCES %s.%s", utils.QuoteIdentifier(dstSchema), utils.QuoteIdentifier(dstTbl)))
						// Replace schema.table references (unquoted)
						adapted = strings.ReplaceAll(adapted,
							fmt.Sprintf("REFERENCES %s.%s", srcSchema, srcTbl),
							fmt.Sprintf("REFERENCES %s.%s", dstSchema, dstTbl))
					}
				}
			} else {
				// Handle unqualified table names in REFERENCES
				// Replace unqualified table name (quoted)
				adapted = strings.ReplaceAll(adapted,
					fmt.Sprintf("REFERENCES %s", utils.QuoteIdentifier(srcTableName)),
					fmt.Sprintf("REFERENCES %s.%s", utils.QuoteIdentifier(dstTable.Schema), utils.QuoteIdentifier(dstTableName)))
				// Replace unqualified table name (unquoted)
				adapted = strings.ReplaceAll(adapted,
					fmt.Sprintf("REFERENCES %s", srcTableName),
					fmt.Sprintf("REFERENCES %s.%s", utils.QuoteIdentifier(dstTable.Schema), utils.QuoteIdentifier(dstTableName)))
			}
		}
	}

	// Replace source schema.table with destination schema.table in CHECK constraints
	// (For FKs, we've already handled the REFERENCES clause above)
	adapted = strings.ReplaceAll(adapted,
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(srcTable.Schema), utils.QuoteIdentifier(srcTable.Table)),
		fmt.Sprintf("%s.%s", utils.QuoteIdentifier(dstTable.Schema), utils.QuoteIdentifier(dstTable.Table)))

	// Also handle unquoted versions
	adapted = strings.ReplaceAll(adapted,
		fmt.Sprintf("%s.%s", srcTable.Schema, srcTable.Table),
		fmt.Sprintf("%s.%s", dstTable.Schema, dstTable.Table))

	// Build the full ALTER TABLE statement
	// For check constraints: ALTER TABLE ... ADD CONSTRAINT ... CHECK ...
	// For foreign keys: ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ...
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(adapted)), "CHECK") ||
		strings.HasPrefix(strings.ToUpper(strings.TrimSpace(adapted)), "FOREIGN KEY") {
		return fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s %s",
			utils.QuoteIdentifier(dstTable.Schema),
			utils.QuoteIdentifier(dstTable.Table),
			utils.QuoteIdentifier(constraintName),
			adapted)
	}

	return adapted
}
