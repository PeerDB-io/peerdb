package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// TableSchema represents the full schema of a table
type TableSchema struct {
	SchemaName  string
	TableName   string
	Columns     []ColumnDefinition
	PrimaryKey  []string
	Constraints []ConstraintDefinition
}

// ColumnDefinition represents a column in a table
type ColumnDefinition struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	IsPrimaryKey bool
}

// ConstraintDefinition represents a constraint on a table
type ConstraintDefinition struct {
	Name       string
	Type       string // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	Definition string
}

// TriggerDefinition represents a trigger
type TriggerDefinition struct {
	Name       string
	Event      string // INSERT, UPDATE, DELETE, TRUNCATE
	Timing     string // BEFORE, AFTER, INSTEAD OF
	Function   string
	Condition  *string
	Definition string // Full CREATE TRIGGER statement
}

// IndexDefinition represents an index
type IndexDefinition struct {
	Name        string
	TableSchema string
	TableName   string
	Columns     []string
	IsUnique    bool
	Method      string // btree, hash, gist, etc.
	Definition  string // Full CREATE INDEX statement
}

// MigrateSchemaFromSource migrates the full schema from source to target Postgres database
// sourceConnector is the connector for the source database
// targetConnector is the connector for the target database
func MigrateSchemaFromSource(
	ctx context.Context,
	sourceConnector *PostgresConnector,
	targetConnector *PostgresConnector,
	tableMappings []*protos.TableMapping,
) error {
	sourceConn := sourceConnector.conn
	targetConn := targetConnector.conn
	logger := sourceConnector.logger
	logger.Info("[migration] Starting schema migration")

	for _, mapping := range tableMappings {
		sourceTable := mapping.SourceTableIdentifier
		targetTable := mapping.DestinationTableIdentifier

		if targetTable == "" {
			targetTable = sourceTable
		}

		logger.Info("[migration] Migrating schema",
			slog.String("sourceTable", sourceTable),
			slog.String("targetTable", targetTable))

		// Get source schema
		sourceSchema, err := getTableSchema(ctx, sourceConn, sourceTable)
		if err != nil {
			return fmt.Errorf("failed to get source schema for %s: %w", sourceTable, err)
		}

		// Check if target table exists
		targetSchemaTable, err := utils.ParseSchemaTable(targetTable)
		if err != nil {
			return fmt.Errorf("error parsing target table %s: %w", targetTable, err)
		}

		tableExists, err := tableExists(ctx, targetConn, targetSchemaTable)
		if err != nil {
			return fmt.Errorf("failed to check if target table exists: %w", err)
		}

		if !tableExists {
			// Create table with full schema
			if err := createTableFromSchema(ctx, targetConn, sourceSchema, targetSchemaTable, logger); err != nil {
				return fmt.Errorf("failed to create table %s: %w", targetTable, err)
			}
		} else {
			// Alter table to match source schema
			if err := alterTableToMatchSchema(ctx, targetConn, sourceSchema, targetSchemaTable, logger); err != nil {
				return fmt.Errorf("failed to alter table %s: %w", targetTable, err)
			}
		}
	}

	logger.Info("[migration] Schema migration completed")
	return nil
}

// MigrateTriggersFromSource migrates triggers from source to target Postgres database
func MigrateTriggersFromSource(
	ctx context.Context,
	sourceConnector *PostgresConnector,
	targetConnector *PostgresConnector,
	tableMappings []*protos.TableMapping,
) error {
	sourceConn := sourceConnector.conn
	targetConn := targetConnector.conn
	logger := sourceConnector.logger
	logger.Info("[migration] Starting trigger migration")

	for _, mapping := range tableMappings {
		sourceTable := mapping.SourceTableIdentifier
		targetTable := mapping.DestinationTableIdentifier

		if targetTable == "" {
			targetTable = sourceTable
		}

		logger.Info("[migration] Migrating triggers",
			slog.String("sourceTable", sourceTable),
			slog.String("targetTable", targetTable))

		// Get source triggers
		sourceTriggers, err := getTableTriggers(ctx, sourceConn, sourceTable)
		if err != nil {
			return fmt.Errorf("failed to get source triggers for %s: %w", sourceTable, err)
		}

		// Get target triggers
		targetTriggers, err := getTableTriggers(ctx, targetConn, targetTable)
		if err != nil {
			return fmt.Errorf("failed to get target triggers for %s: %w", targetTable, err)
		}

		// Create a map of existing target triggers
		targetTriggerMap := make(map[string]*TriggerDefinition)
		for _, trigger := range targetTriggers {
			targetTriggerMap[trigger.Name] = trigger
		}

		// Migrate each source trigger
		tx, err := targetConn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer shared.RollbackTx(tx, logger)

		for _, sourceTrigger := range sourceTriggers {
			if _, exists := targetTriggerMap[sourceTrigger.Name]; exists {
				// Drop existing trigger if it exists
				_, err = tx.Exec(ctx, fmt.Sprintf(
					"DROP TRIGGER IF EXISTS %s ON %s",
					utils.QuoteIdentifier(sourceTrigger.Name),
					utils.QuoteIdentifier(targetTable)))
				if err != nil {
					return fmt.Errorf("failed to drop existing trigger %s: %w", sourceTrigger.Name, err)
				}
			}

			// Create trigger
			triggerSQL := buildCreateTriggerSQL(sourceTrigger, targetTable)
			_, err = tx.Exec(ctx, triggerSQL)
			if err != nil {
				return fmt.Errorf("failed to create trigger %s: %w", sourceTrigger.Name, err)
			}

			logger.Info("[migration] Created trigger",
				slog.String("trigger", sourceTrigger.Name),
				slog.String("table", targetTable))
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit trigger migration: %w", err)
		}
	}

	logger.Info("[migration] Trigger migration completed")
	return nil
}

// MigrateIndexesFromSource migrates indexes from source to target Postgres database
func MigrateIndexesFromSource(
	ctx context.Context,
	sourceConnector *PostgresConnector,
	targetConnector *PostgresConnector,
	tableMappings []*protos.TableMapping,
) error {
	sourceConn := sourceConnector.conn
	targetConn := targetConnector.conn
	logger := sourceConnector.logger
	logger.Info("[migration] Starting index migration")

	for _, mapping := range tableMappings {
		sourceTable := mapping.SourceTableIdentifier
		targetTable := mapping.DestinationTableIdentifier

		if targetTable == "" {
			targetTable = sourceTable
		}

		logger.Info("[migration] Migrating indexes",
			slog.String("sourceTable", sourceTable),
			slog.String("targetTable", targetTable))

		// Get source indexes
		sourceIndexes, err := getTableIndexes(ctx, sourceConn, sourceTable)
		if err != nil {
			return fmt.Errorf("failed to get source indexes for %s: %w", sourceTable, err)
		}

		// Get target indexes
		targetIndexes, err := getTableIndexes(ctx, targetConn, targetTable)
		if err != nil {
			return fmt.Errorf("failed to get target indexes for %s: %w", targetTable, err)
		}

		// Create a map of existing target indexes
		targetIndexMap := make(map[string]*IndexDefinition)
		for _, index := range targetIndexes {
			targetIndexMap[index.Name] = index
		}

		// Migrate each source index
		tx, err := targetConn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer shared.RollbackTx(tx, logger)

		for _, sourceIndex := range sourceIndexes {
			// Skip primary key indexes as they're handled by schema migration
			if strings.HasSuffix(sourceIndex.Name, "_pkey") {
				continue
			}

			if _, exists := targetIndexMap[sourceIndex.Name]; exists {
				// Drop existing index if it exists
				_, err = tx.Exec(ctx, fmt.Sprintf(
					"DROP INDEX IF EXISTS %s.%s",
					utils.QuoteIdentifier(sourceIndex.TableSchema),
					utils.QuoteIdentifier(sourceIndex.Name)))
				if err != nil {
					return fmt.Errorf("failed to drop existing index %s: %w", sourceIndex.Name, err)
				}
			}

			// Create index (use the definition from source, but replace table name)
			indexSQL := buildCreateIndexSQL(sourceIndex, targetTable)
			_, err = tx.Exec(ctx, indexSQL)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", sourceIndex.Name, err)
			}

			logger.Info("[migration] Created index",
				slog.String("index", sourceIndex.Name),
				slog.String("table", targetTable))
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit index migration: %w", err)
		}
	}

	logger.Info("[migration] Index migration completed")
	return nil
}

// Helper functions

func getTableSchema(ctx context.Context, conn *pgx.Conn, tableName string) (*TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("error parsing table name: %w", err)
	}

	// Get columns
	columnsQuery := `
		SELECT 
			a.attname AS column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			a.attnotnull AS not_null,
			pg_get_expr(adbin, adrelid) AS default_value
		FROM pg_attribute a
		LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 AND c.relname = $2
			AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`

	rows, err := conn.Query(ctx, columnsQuery, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnDefinition
	var columnNames []string
	for rows.Next() {
		var colName, dataType pgtype.Text
		var notNull pgtype.Bool
		var defaultValue pgtype.Text

		if err := rows.Scan(&colName, &dataType, &notNull, &defaultValue); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		var defValue *string
		if defaultValue.Valid {
			defValue = &defaultValue.String
		}

		columns = append(columns, ColumnDefinition{
			Name:         colName.String,
			DataType:     dataType.String,
			IsNullable:   !notNull.Bool,
			DefaultValue: defValue,
		})
		columnNames = append(columnNames, colName.String)
	}

	// Get primary key
	pkQuery := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON i.indrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	pkRows, err := conn.Query(ctx, pkQuery, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer pkRows.Close()

	var primaryKey []string
	for pkRows.Next() {
		var pkCol pgtype.Text
		if err := pkRows.Scan(&pkCol); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		primaryKey = append(primaryKey, pkCol.String)
	}

	// Mark primary key columns
	for i := range columns {
		for _, pkCol := range primaryKey {
			if columns[i].Name == pkCol {
				columns[i].IsPrimaryKey = true
				break
			}
		}
	}

	return &TableSchema{
		SchemaName:  schemaTable.Schema,
		TableName:   schemaTable.Table,
		Columns:     columns,
		PrimaryKey:  primaryKey,
		Constraints: []ConstraintDefinition{}, // Can be extended later
	}, nil
}

func tableExists(ctx context.Context, conn *pgx.Conn, schemaTable *utils.SchemaTable) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = $1 AND c.relname = $2
		)`, schemaTable.Schema, schemaTable.Table).Scan(&exists)
	return exists, err
}

func createTableFromSchema(
	ctx context.Context,
	conn *pgx.Conn,
	schema *TableSchema,
	targetSchemaTable *utils.SchemaTable,
	logger log.Logger,
) error {
	var columnDefs []string
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("%s %s", utils.QuoteIdentifier(col.Name), col.DataType)
		if !col.IsNullable {
			colDef += " NOT NULL"
		}
		if col.DefaultValue != nil {
			colDef += " DEFAULT " + *col.DefaultValue
		}
		columnDefs = append(columnDefs, colDef)
	}

	var pkDef string
	if len(schema.PrimaryKey) > 0 {
		pkCols := make([]string, len(schema.PrimaryKey))
		for i, pk := range schema.PrimaryKey {
			pkCols[i] = utils.QuoteIdentifier(pk)
		}
		pkDef = fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(pkCols, ", "))
	}

	createTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (%s%s)",
		utils.QuoteIdentifier(targetSchemaTable.Schema),
		utils.QuoteIdentifier(targetSchemaTable.Table),
		strings.Join(columnDefs, ", "),
		pkDef,
	)

	_, err := conn.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	logger.Info("[migration] Created table",
		slog.String("table", targetSchemaTable.String()))
	return nil
}

func alterTableToMatchSchema(
	ctx context.Context,
	conn *pgx.Conn,
	sourceSchema *TableSchema,
	targetSchemaTable *utils.SchemaTable,
	logger log.Logger,
) error {
	// Get target schema
	targetSchema, err := getTableSchema(ctx, conn, targetSchemaTable.String())
	if err != nil {
		return fmt.Errorf("failed to get target schema: %w", err)
	}

	// Create maps for comparison
	sourceColMap := make(map[string]ColumnDefinition)
	for _, col := range sourceSchema.Columns {
		sourceColMap[col.Name] = col
	}

	targetColMap := make(map[string]ColumnDefinition)
	for _, col := range targetSchema.Columns {
		targetColMap[col.Name] = col
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	// Add missing columns
	for _, sourceCol := range sourceSchema.Columns {
		if _, exists := targetColMap[sourceCol.Name]; !exists {
			colDef := fmt.Sprintf("%s %s", utils.QuoteIdentifier(sourceCol.Name), sourceCol.DataType)
			if !sourceCol.IsNullable {
				colDef += " NOT NULL"
			}
			if sourceCol.DefaultValue != nil {
				colDef += " DEFAULT " + *sourceCol.DefaultValue
			}

			alterSQL := fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN %s",
				utils.QuoteIdentifier(targetSchemaTable.Schema),
				utils.QuoteIdentifier(targetSchemaTable.Table),
				colDef,
			)

			_, err = tx.Exec(ctx, alterSQL)
			if err != nil {
				return fmt.Errorf("failed to add column %s: %w", sourceCol.Name, err)
			}

			logger.Info("[migration] Added column",
				slog.String("column", sourceCol.Name),
				slog.String("table", targetSchemaTable.String()))
		}
	}

	// Note: We don't drop columns or change column types to avoid data loss
	// This can be extended based on requirements

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit schema changes: %w", err)
	}

	return nil
}

func getTableTriggers(ctx context.Context, conn *pgx.Conn, tableName string) ([]*TriggerDefinition, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("error parsing table name: %w", err)
	}

	query := `
		SELECT 
			t.tgname AS trigger_name,
			pg_get_triggerdef(t.oid) AS trigger_definition,
			CASE 
				WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
				WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS timing,
			CASE 
				WHEN t.tgtype & 4 = 4 THEN 'INSERT'
				WHEN t.tgtype & 8 = 8 THEN 'DELETE'
				WHEN t.tgtype & 16 = 16 THEN 'UPDATE'
				WHEN t.tgtype & 32 = 32 THEN 'TRUNCATE'
				ELSE 'UNKNOWN'
			END AS event,
			pg_get_function_identity_arguments(t.tgfoid) AS function_args
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_proc p ON t.tgfoid = p.oid
		JOIN pg_namespace pn ON p.pronamespace = pn.oid
		WHERE n.nspname = $1 AND c.relname = $2
			AND NOT t.tgisinternal
		ORDER BY t.tgname`

	rows, err := conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []*TriggerDefinition
	for rows.Next() {
		var triggerName, triggerDef, timing, event, functionArgs pgtype.Text
		if err := rows.Scan(&triggerName, &triggerDef, &timing, &event, &functionArgs); err != nil {
			return nil, fmt.Errorf("failed to scan trigger: %w", err)
		}

		// Extract function name from trigger definition
		functionName := ""
		if functionArgs.Valid {
			// The function name is typically in the trigger definition
			// We'll extract it from pg_get_triggerdef output
			functionName = functionArgs.String
		}

		triggers = append(triggers, &TriggerDefinition{
			Name:       triggerName.String,
			Event:      event.String,
			Timing:     timing.String,
			Function:   functionName,
			Definition: triggerDef.String,
		})
	}

	return triggers, nil
}

func buildCreateTriggerSQL(trigger *TriggerDefinition, targetTable string) string {
	// Use the full trigger definition from pg_get_triggerdef
	// It already contains the complete CREATE TRIGGER statement
	// We just need to replace the table reference
	schemaTable, err := utils.ParseSchemaTable(targetTable)
	if err != nil {
		// Fallback to using the definition as-is
		return trigger.Definition
	}

	// Extract the table name from the definition and replace it
	// pg_get_triggerdef returns: CREATE TRIGGER name ... ON table_name ...
	// We need to find and replace the table reference
	def := trigger.Definition

	// Try to find "ON schema.table" pattern and replace
	// This is a simplified approach - a full SQL parser would be better
	parts := strings.Split(def, " ON ")
	if len(parts) >= 2 {
		// Find the table part (might be schema.table or just table)
		tablePart := strings.Fields(parts[1])[0]
		newTableRef := fmt.Sprintf("%s.%s", utils.QuoteIdentifier(schemaTable.Schema), utils.QuoteIdentifier(schemaTable.Table))
		def = strings.Replace(def, tablePart, newTableRef, 1)
	}

	return def
}

func getTableIndexes(ctx context.Context, conn *pgx.Conn, tableName string) ([]*IndexDefinition, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("error parsing table name: %w", err)
	}

	query := `
		SELECT 
			i.indexname AS index_name,
			i.indexdef AS index_definition,
			i.indexdef LIKE '%UNIQUE%' AS is_unique
		FROM pg_indexes i
		WHERE i.schemaname = $1 AND i.tablename = $2
			AND i.indexname NOT LIKE '%_pkey'
		ORDER BY i.indexname`

	rows, err := conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []*IndexDefinition
	for rows.Next() {
		var indexName, indexDef pgtype.Text
		var isUnique pgtype.Bool
		if err := rows.Scan(&indexName, &indexDef, &isUnique); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		// Extract columns from index definition (simplified)
		columns := extractIndexColumns(indexDef.String)

		indexes = append(indexes, &IndexDefinition{
			Name:        indexName.String,
			TableSchema: schemaTable.Schema,
			TableName:   schemaTable.Table,
			Columns:     columns,
			IsUnique:    isUnique.Bool,
			Definition:  indexDef.String,
		})
	}

	return indexes, nil
}

func extractIndexColumns(indexDef string) []string {
	// Simple extraction - find columns in parentheses
	// This is simplified - in production, use proper SQL parsing
	start := strings.Index(indexDef, "(")
	end := strings.Index(indexDef, ")")
	if start == -1 || end == -1 {
		return []string{}
	}

	colsStr := indexDef[start+1 : end]
	cols := strings.Split(colsStr, ",")
	var result []string
	for _, col := range cols {
		col = strings.TrimSpace(col)
		col = strings.Trim(col, "\"")
		if col != "" {
			result = append(result, col)
		}
	}
	return result
}

func buildCreateIndexSQL(index *IndexDefinition, targetTable string) string {
	// Use the definition from source, but replace table name
	schemaTable, _ := utils.ParseSchemaTable(targetTable)
	oldTableRef := fmt.Sprintf("%s.%s", index.TableSchema, index.TableName)
	newTableRef := fmt.Sprintf("%s.%s", utils.QuoteIdentifier(schemaTable.Schema), utils.QuoteIdentifier(schemaTable.Table))
	return strings.Replace(index.Definition, oldTableRef, newTableRef, 1)
}
