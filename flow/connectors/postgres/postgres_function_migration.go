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

// getTableFunctions retrieves all functions associated with a table's schema
// This includes functions that might be used in triggers, constraints, or generated columns
// Returns proto FunctionDescription for direct use in TableSchema
func (c *PostgresConnector) getTableFunctions(
	ctx context.Context,
	schemaTable *utils.SchemaTable,
) ([]*protos.FunctionDescription, error) {
	query := `
		SELECT 
			p.proname AS function_name,
			pg_get_functiondef(p.oid) AS function_def,
			l.lanname AS language,
			pg_get_function_result(p.oid) AS return_type
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		WHERE n.nspname = $1
		AND l.lanname IN ('plpgsql', 'sql', 'plpython3u', 'plperl', 'pltcl')
		ORDER BY p.proname
	`

	rows, err := c.conn.Query(ctx, query, schemaTable.Schema)
	if err != nil {
		return nil, fmt.Errorf("error querying functions for schema %s: %w", schemaTable.Schema, err)
	}
	defer rows.Close()

	var functions []*protos.FunctionDescription
	for rows.Next() {
		var functionName, functionDef, language, returnType string
		if err := rows.Scan(&functionName, &functionDef, &language, &returnType); err != nil {
			return nil, fmt.Errorf("error scanning function definition: %w", err)
		}
		functions = append(functions, &protos.FunctionDescription{
			FunctionName: functionName,
			FunctionDef:  functionDef,
			Language:     language,
			ReturnType:   returnType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating function rows: %w", err)
	}

	c.logger.Info(fmt.Sprintf("Found %d functions to migrate for schema %s", len(functions), schemaTable.Schema),
		slog.String("schema", schemaTable.Schema),
		slog.String("table", schemaTable.Table))

	return functions, nil
}

// createTableFunctions creates functions on the destination schema
func (c *PostgresConnector) createTableFunctions(
	ctx context.Context,
	tx pgx.Tx,
	sourceTable *utils.SchemaTable,
	destTable *utils.SchemaTable,
) error {
	functions, err := c.getTableFunctions(ctx, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to fetch functions: %w", err)
	}

	if len(functions) == 0 {
		c.logger.Info("No functions to create for schema", slog.String("schema", destTable.Schema))
		return nil
	}

	for _, fn := range functions {
		// Replace source schema with destination schema in the function definition
		// pg_get_functiondef returns: CREATE OR REPLACE FUNCTION schema.function_name(...) ...
		modifiedFunctionDef := strings.Replace(
			fn.FunctionDef,
			fmt.Sprintf("FUNCTION %s.", utils.QuoteIdentifier(sourceTable.Schema)),
			fmt.Sprintf("FUNCTION %s.", utils.QuoteIdentifier(destTable.Schema)),
			1,
		)

		// Also handle case without schema qualification in function body
		// Replace references to source schema with destination schema
		modifiedFunctionDef = strings.ReplaceAll(
			modifiedFunctionDef,
			fmt.Sprintf("%s.", sourceTable.Schema),
			fmt.Sprintf("%s.", destTable.Schema),
		)

		c.logger.Info("Creating function on destination schema",
			slog.String("functionName", fn.FunctionName),
			slog.String("schema", destTable.Schema),
			slog.String("language", fn.Language),
			slog.String("returnType", fn.ReturnType))

		_, err := tx.Exec(ctx, modifiedFunctionDef)
		if err != nil {
			// Don't fail the entire migration if one function fails
			// Log as warning and continue
			c.logger.Warn("Failed to create function on destination, continuing...",
				slog.String("functionName", fn.FunctionName),
				slog.String("schema", destTable.Schema),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created function",
			slog.String("functionName", fn.FunctionName),
			slog.String("schema", destTable.Schema))
	}

	return nil
}

// createTableFunctionsFromSchema creates functions on the destination using pre-fetched function definitions
// This method uses the functions that were fetched from the source during SetupTableSchema
func (c *PostgresConnector) createTableFunctionsFromSchema(
	ctx context.Context,
	tx pgx.Tx,
	tableSchema *protos.TableSchema,
	destTable *utils.SchemaTable,
) error {
	if len(tableSchema.Functions) == 0 {
		c.logger.Info("No functions to create for schema", slog.String("schema", destTable.Schema))
		return nil
	}

	// Parse source table from the table schema identifier
	sourceTable, err := utils.ParseSchemaTable(tableSchema.TableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to parse source table identifier: %w", err)
	}

	for _, fn := range tableSchema.Functions {
		// Replace source schema with destination schema in the function definition
		// pg_get_functiondef returns: CREATE OR REPLACE FUNCTION schema.function_name(...) ...
		modifiedFunctionDef := strings.Replace(
			fn.FunctionDef,
			fmt.Sprintf("FUNCTION %s.", utils.QuoteIdentifier(sourceTable.Schema)),
			fmt.Sprintf("FUNCTION %s.", utils.QuoteIdentifier(destTable.Schema)),
			1,
		)

		// Also handle case without schema qualification in function body
		// Replace references to source schema with destination schema
		modifiedFunctionDef = strings.ReplaceAll(
			modifiedFunctionDef,
			fmt.Sprintf("%s.", sourceTable.Schema),
			fmt.Sprintf("%s.", destTable.Schema),
		)

		c.logger.Info("Creating function on destination schema",
			slog.String("functionName", fn.FunctionName),
			slog.String("schema", destTable.Schema),
			slog.String("functionDef", modifiedFunctionDef),
			slog.String("language", fn.Language),
			slog.String("returnType", fn.ReturnType))

		_, err := tx.Exec(ctx, modifiedFunctionDef)
		if err != nil {
			// Don't fail the entire migration if one function fails
			// Log as warning and continue
			c.logger.Warn("Failed to create function on destination, continuing...",
				slog.String("functionName", fn.FunctionName),
				slog.String("schema", destTable.Schema),
				slog.Any("error", err))
			continue
		}

		c.logger.Info("Successfully created function",
			slog.String("functionName", fn.FunctionName),
			slog.String("schema", destTable.Schema))
	}

	return nil
}
