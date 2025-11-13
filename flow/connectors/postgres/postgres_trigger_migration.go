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

// getTableTriggers retrieves all triggers associated with a specific table
// Returns proto TriggerDescription for direct use in TableSchema
func (c *PostgresConnector) getTableTriggers(
	ctx context.Context,
	schemaTable *utils.SchemaTable,
) ([]*protos.TriggerDescription, error) {
	query := `
		SELECT 
			t.tgname AS trigger_name,
			pg_get_triggerdef(t.oid) AS trigger_def,
			CASE 
				WHEN t.tgtype & 4 = 4 THEN 'UPDATE'
				WHEN t.tgtype & 8 = 8 THEN 'DELETE'
				WHEN t.tgtype & 16 = 16 THEN 'INSERT'
				WHEN t.tgtype & 32 = 32 THEN 'TRUNCATE'
				ELSE 'UNKNOWN'
			END AS event_manipulation,
			CASE 
				WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
				WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS action_timing,
			CASE 
				WHEN t.tgtype & 1 = 1 THEN 'ROW'
				ELSE 'STATEMENT'
			END AS action_orientation
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 
		AND c.relname = $2
		AND NOT t.tgisinternal
		ORDER BY t.tgname
	`

	rows, err := c.conn.Query(ctx, query, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers for table %s.%s: %w",
			schemaTable.Schema, schemaTable.Table, err)
	}
	defer rows.Close()

	var triggers []*protos.TriggerDescription
	for rows.Next() {
		var triggerName, triggerDef, eventManipulation, actionTiming, actionOrientation string
		if err := rows.Scan(&triggerName, &triggerDef, &eventManipulation, &actionTiming, &actionOrientation); err != nil {
			return nil, fmt.Errorf("error scanning trigger definition: %w", err)
		}
		triggers = append(triggers, &protos.TriggerDescription{
			TriggerName:       triggerName,
			TriggerDef:        triggerDef,
			EventManipulation: eventManipulation,
			ActionTiming:      actionTiming,
			ActionOrientation: actionOrientation,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trigger rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Found %d triggers to migrate for table %s.%s",
		len(triggers), schemaTable.Schema, schemaTable.Table),
		slog.String("schema", schemaTable.Schema),
		slog.String("table", schemaTable.Table))

	return triggers, nil
}

// createTableTriggers creates triggers on the destination table
func (c *PostgresConnector) createTableTriggers(
	ctx context.Context,
	tx pgx.Tx,
	sourceTable *utils.SchemaTable,
	destTable *utils.SchemaTable,
) error {
	triggers, err := c.getTableTriggers(ctx, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to fetch triggers: %w", err)
	}

	if len(triggers) == 0 {
		c.logger.Info("No triggers to create for table",
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table))
		return nil
	}

	for _, trig := range triggers {
		// Replace source table reference with destination table in the trigger definition
		// pg_get_triggerdef returns: CREATE TRIGGER trigger_name ... ON schema.table_name ...
		modifiedTriggerDef := strings.Replace(
			trig.TriggerDef,
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(sourceTable.Schema), utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(destTable.Schema), utils.QuoteIdentifier(destTable.Table)),
			1,
		)

		// Also handle case without schema qualification
		modifiedTriggerDef = strings.Replace(
			modifiedTriggerDef,
			fmt.Sprintf("ON %s", utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(destTable.Schema), utils.QuoteIdentifier(destTable.Table)),
			1,
		)

		// Replace schema references in function calls within the trigger
		modifiedTriggerDef = strings.ReplaceAll(
			modifiedTriggerDef,
			fmt.Sprintf("%s.", sourceTable.Schema),
			fmt.Sprintf("%s.", destTable.Schema),
		)

		c.logger.Info("Creating trigger on destination table",
			slog.String("triggerName", trig.TriggerName),
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table),
			slog.String("eventManipulation", trig.EventManipulation),
			slog.String("actionTiming", trig.ActionTiming),
			slog.String("actionOrientation", trig.ActionOrientation))

		_, err := tx.Exec(ctx, modifiedTriggerDef)
		if err != nil {
			// Don't fail the entire migration if one trigger fails
			// Log as warning and continue
			c.logger.Warn("Failed to create trigger on destination, continuing...",
				slog.String("triggerName", trig.TriggerName),
				slog.String("schema", destTable.Schema),
				slog.String("table", destTable.Table),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created trigger",
			slog.String("triggerName", trig.TriggerName),
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table))
	}

	return nil
}

// createTableTriggersFromSchema creates triggers on the destination using pre-fetched trigger definitions
// This method uses the triggers that were fetched from the source during SetupTableSchema
func (c *PostgresConnector) createTableTriggersFromSchema(
	ctx context.Context,
	tx pgx.Tx,
	tableSchema *protos.TableSchema,
	destTable *utils.SchemaTable,
) error {
	if len(tableSchema.Triggers) == 0 {
		c.logger.Info("No triggers to create for table",
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table))
		return nil
	}

	// Parse source table from the table schema identifier
	sourceTable, err := utils.ParseSchemaTable(tableSchema.TableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to parse source table identifier: %w", err)
	}

	for _, trig := range tableSchema.Triggers {
		// Replace source table reference with destination table in the trigger definition
		// pg_get_triggerdef returns: CREATE TRIGGER trigger_name ... ON schema.table_name ...
		modifiedTriggerDef := strings.Replace(
			trig.TriggerDef,
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(sourceTable.Schema), utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(destTable.Schema), utils.QuoteIdentifier(destTable.Table)),
			1,
		)

		// Also handle case without schema qualification
		modifiedTriggerDef = strings.Replace(
			modifiedTriggerDef,
			fmt.Sprintf("ON %s", utils.QuoteIdentifier(sourceTable.Table)),
			fmt.Sprintf("ON %s.%s", utils.QuoteIdentifier(destTable.Schema), utils.QuoteIdentifier(destTable.Table)),
			1,
		)

		// Replace schema references in function calls within the trigger
		modifiedTriggerDef = strings.ReplaceAll(
			modifiedTriggerDef,
			fmt.Sprintf("%s.", sourceTable.Schema),
			fmt.Sprintf("%s.", destTable.Schema),
		)

		c.logger.Info("Creating trigger on destination table",
			slog.String("triggerName", trig.TriggerName),
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table),
			slog.String("triggerDef", modifiedTriggerDef),
			slog.String("eventManipulation", trig.EventManipulation),
			slog.String("actionTiming", trig.ActionTiming),
			slog.String("actionOrientation", trig.ActionOrientation))

		_, err := tx.Exec(ctx, modifiedTriggerDef)
		if err != nil {
			// Don't fail the entire migration if one trigger fails
			// Log as warning and continue
			c.logger.Warn("Failed to create trigger on destination, continuing...",
				slog.String("triggerName", trig.TriggerName),
				slog.String("schema", destTable.Schema),
				slog.String("table", destTable.Table),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created trigger",
			slog.String("triggerName", trig.TriggerName),
			slog.String("schema", destTable.Schema),
			slog.String("table", destTable.Table))
	}

	return nil
}
