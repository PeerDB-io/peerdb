package connpostgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type TriggerInfo struct {
	TriggerName    string
	TableSchema    string
	TableName      string
	FunctionSchema string
	FunctionName   string
	Timing         string
	Events         []string
	Enabled        bool
	Definition     string
}

// IndexInfo represents a PostgreSQL index definition
type IndexInfo struct {
	IndexName   string
	TableSchema string
	TableName   string
	Columns     []string // Column names in the index
	IsUnique    bool
	IsPrimary   bool
	Definition  string // Full CREATE INDEX statement
}

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

func (c *PostgresConnector) GetTriggersForTables(
	ctx context.Context,
	tableMappings []*protos.TableMapping,
) (map[string][]*TriggerInfo, error) {
	if len(tableMappings) == 0 {
		return make(map[string][]*TriggerInfo), nil
	}

	// Build a map of source table identifiers for quick lookup
	sourceTableMap := make(map[string]struct{})
	var tableConditions []string

	for _, tm := range tableMappings {
		schemaTable, err := utils.ParseSchemaTable(tm.SourceTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error parsing source table identifier %s: %w", tm.SourceTableIdentifier, err)
		}
		key := fmt.Sprintf("%s.%s", schemaTable.Schema, schemaTable.Table)
		sourceTableMap[key] = struct{}{}
		tableConditions = append(tableConditions, fmt.Sprintf("(n.nspname = %s AND c.relname = %s)",
			utils.QuoteLiteral(schemaTable.Schema),
			utils.QuoteLiteral(schemaTable.Table)))
	}

	// Query to get all triggers for the specified tables
	// This query joins pg_trigger, pg_class, pg_namespace, and pg_proc to get complete trigger information
	// Note: A trigger can have multiple events (INSERT OR UPDATE OR DELETE), so we extract all events from tgtype
	query := fmt.Sprintf(`
		SELECT
			t.tgname AS trigger_name,
			n.nspname AS table_schema,
			c.relname AS table_name,
			pn.nspname AS function_schema,
			p.proname AS function_name,
			t.tgtype AS trigger_type,
			t.tgenabled AS trigger_enabled
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_proc p ON t.tgfoid = p.oid
		JOIN pg_namespace pn ON p.pronamespace = pn.oid
		WHERE t.tgname NOT LIKE 'pg_%%'
			AND NOT t.tgisinternal
			AND (%s)
		ORDER BY n.nspname, c.relname, t.tgname`,
		strings.Join(tableConditions, " OR "))

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %w", err)
	}
	defer rows.Close()

	// Group triggers by table
	triggersByTable := make(map[string][]*TriggerInfo)

	for rows.Next() {
		var triggerName, tableSchema, tableName, functionSchema, functionName pgtype.Text
		var triggerType int32
		var triggerEnabled pgtype.Text

		if err := rows.Scan(&triggerName, &tableSchema, &tableName, &functionSchema, &functionName, &triggerType, &triggerEnabled); err != nil {
			return nil, fmt.Errorf("error scanning trigger row: %w", err)
		}

		tableKey := fmt.Sprintf("%s.%s", tableSchema.String, tableName.String)
		// Only include triggers for tables in our mapping
		if _, ok := sourceTableMap[tableKey]; !ok {
			continue
		}

		// Extract timing from trigger type bits
		// Bit 1 (0x02) = BEFORE, Bit 6 (0x40) = INSTEAD OF, otherwise AFTER
		var timing string
		if triggerType&2 == 2 {
			timing = "BEFORE"
		} else if triggerType&64 == 64 {
			timing = "INSTEAD OF"
		} else {
			timing = "AFTER"
		}

		// Extract events from trigger type bits
		// Bit 2 (0x04) = INSERT, Bit 3 (0x08) = DELETE, Bit 4 (0x10) = UPDATE, Bit 5 (0x20) = TRUNCATE
		var events []string
		if triggerType&4 == 4 {
			events = append(events, "INSERT")
		}
		if triggerType&8 == 8 {
			events = append(events, "DELETE")
		}
		if triggerType&16 == 16 {
			events = append(events, "UPDATE")
		}
		if triggerType&32 == 32 {
			events = append(events, "TRUNCATE")
		}

		// Check if trigger is enabled (D = disabled, O = origin, R = replica, A = always)
		enabled := triggerEnabled.String != "D"

		// Build CREATE TRIGGER statement for definition
		eventsStr := strings.Join(events, " OR ")
		definition := fmt.Sprintf(
			"CREATE TRIGGER %s %s %s ON %s.%s FOR EACH ROW EXECUTE FUNCTION %s.%s()",
			utils.QuoteIdentifier(triggerName.String),
			timing,
			eventsStr,
			utils.QuoteIdentifier(tableSchema.String),
			utils.QuoteIdentifier(tableName.String),
			utils.QuoteIdentifier(functionSchema.String),
			utils.QuoteIdentifier(functionName.String),
		)

		triggerInfo := &TriggerInfo{
			TriggerName:    triggerName.String,
			TableSchema:    tableSchema.String,
			TableName:      tableName.String,
			FunctionSchema: functionSchema.String,
			FunctionName:   functionName.String,
			Timing:         timing,
			Events:         events,
			Enabled:        enabled,
			Definition:     definition,
		}

		triggersByTable[tableKey] = append(triggersByTable[tableKey], triggerInfo)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trigger rows: %w", err)
	}

	return triggersByTable, nil
}

type TriggerFunctionInfo struct {
	FunctionSchema string
	FunctionName   string
	Definition     string
}

func (c *PostgresConnector) GetTriggerFunctionDefinitions(
	ctx context.Context,
	triggersByTable map[string][]*TriggerInfo,
) (map[string]*TriggerFunctionInfo, error) {
	functionMap := make(map[string]struct{})
	var functionIdentifiers []struct {
		schema string
		name   string
	}

	for _, triggers := range triggersByTable {
		for _, trigger := range triggers {
			key := fmt.Sprintf("%s.%s", trigger.FunctionSchema, trigger.FunctionName)
			if _, exists := functionMap[key]; !exists {
				functionMap[key] = struct{}{}
				functionIdentifiers = append(functionIdentifiers, struct {
					schema string
					name   string
				}{schema: trigger.FunctionSchema, name: trigger.FunctionName})
			}
		}
	}

	if len(functionIdentifiers) == 0 {
		return make(map[string]*TriggerFunctionInfo), nil
	}

	var functionConditions []string
	for _, fn := range functionIdentifiers {
		functionConditions = append(functionConditions,
			fmt.Sprintf("(pn.nspname = %s AND p.proname = %s)",
				utils.QuoteLiteral(fn.schema),
				utils.QuoteLiteral(fn.name)))
	}

	query := fmt.Sprintf(`
		SELECT
			pn.nspname AS function_schema,
			p.proname AS function_name,
			pg_get_functiondef(p.oid) AS function_definition
		FROM pg_proc p
		JOIN pg_namespace pn ON p.pronamespace = pn.oid
		WHERE (%s)
		ORDER BY pn.nspname, p.proname`,
		strings.Join(functionConditions, " OR "))

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying trigger function definitions: %w", err)
	}
	defer rows.Close()

	functionDefinitions := make(map[string]*TriggerFunctionInfo)
	for rows.Next() {
		var functionSchema, functionName, functionDef pgtype.Text

		if err := rows.Scan(&functionSchema, &functionName, &functionDef); err != nil {
			return nil, fmt.Errorf("error scanning function definition row: %w", err)
		}

		key := fmt.Sprintf("%s.%s", functionSchema.String, functionName.String)
		functionDefinitions[key] = &TriggerFunctionInfo{
			FunctionSchema: functionSchema.String,
			FunctionName:   functionName.String,
			Definition:     functionDef.String,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating function definition rows: %w", err)
	}

	return functionDefinitions, nil
}

// GetIndexesForTables queries indexes for the specified tables from PostgreSQL system catalogs.
// Only queries tables that are part of the mirror (from tableMappings).
// Returns a map keyed by table identifier (schema.table) to list of indexes.
func (c *PostgresConnector) GetIndexesForTables(
	ctx context.Context,
	tableMappings []*protos.TableMapping,
) (map[string][]*IndexInfo, error) {
	if len(tableMappings) == 0 {
		return make(map[string][]*IndexInfo), nil
	}

	// Build a map of source table identifiers for quick lookup
	sourceTableMap := make(map[string]struct{})
	var tableConditions []string

	for _, tm := range tableMappings {
		schemaTable, err := utils.ParseSchemaTable(tm.SourceTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error parsing source table identifier %s: %w", tm.SourceTableIdentifier, err)
		}
		key := fmt.Sprintf("%s.%s", schemaTable.Schema, schemaTable.Table)
		sourceTableMap[key] = struct{}{}
		tableConditions = append(tableConditions, fmt.Sprintf("(n.nspname = %s AND t.relname = %s)",
			utils.QuoteLiteral(schemaTable.Schema),
			utils.QuoteLiteral(schemaTable.Table)))
	}

	// Query to get all indexes for the specified tables
	// This query joins pg_index, pg_class (for index), pg_class (for table), and pg_namespace
	// We exclude primary key indexes as they're already handled during table creation
	// We also exclude indexes that are part of constraints (unique constraints create indexes)
	query := fmt.Sprintf(`
		SELECT
			i.relname AS index_name,
			n.nspname AS table_schema,
			t.relname AS table_name,
			idx.indisunique AS is_unique,
			idx.indisprimary AS is_primary,
			pg_get_indexdef(idx.indexrelid) AS index_definition
		FROM pg_index idx
		JOIN pg_class i ON idx.indexrelid = i.oid
		JOIN pg_class t ON idx.indrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE (%s)
			AND NOT idx.indisprimary
			AND i.relkind = 'i'
		ORDER BY n.nspname, t.relname, i.relname`,
		strings.Join(tableConditions, " OR "))

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes: %w", err)
	}
	defer rows.Close()

	// Group indexes by table
	indexesByTable := make(map[string][]*IndexInfo)

	for rows.Next() {
		var indexName, tableSchema, tableName, indexDef pgtype.Text
		var isUnique, isPrimary pgtype.Bool

		if err := rows.Scan(&indexName, &tableSchema, &tableName, &isUnique, &isPrimary, &indexDef); err != nil {
			return nil, fmt.Errorf("error scanning index row: %w", err)
		}

		tableKey := fmt.Sprintf("%s.%s", tableSchema.String, tableName.String)
		// Only include indexes for tables in our mapping
		if _, ok := sourceTableMap[tableKey]; !ok {
			continue
		}

		// Extract column names from index definition
		// The index definition from pg_get_indexdef includes column names
		// We'll parse them from the definition string
		columns := extractColumnsFromIndexDef(indexDef.String)

		indexInfo := &IndexInfo{
			IndexName:   indexName.String,
			TableSchema: tableSchema.String,
			TableName:   tableName.String,
			Columns:     columns,
			IsUnique:    isUnique.Bool,
			IsPrimary:   isPrimary.Bool,
			Definition:  indexDef.String,
		}

		indexesByTable[tableKey] = append(indexesByTable[tableKey], indexInfo)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	return indexesByTable, nil
}

// extractColumnsFromIndexDef extracts column names from a CREATE INDEX statement.
// Example: "CREATE INDEX idx_name ON schema.table (col1, col2)" -> ["col1", "col2"]
func extractColumnsFromIndexDef(indexDef string) []string {
	// Find the part after the opening parenthesis
	startIdx := strings.Index(indexDef, "(")
	if startIdx == -1 {
		return []string{}
	}

	// Find the matching closing parenthesis
	parenCount := 0
	endIdx := -1
	for i := startIdx; i < len(indexDef); i++ {
		if indexDef[i] == '(' {
			parenCount++
		} else if indexDef[i] == ')' {
			parenCount--
			if parenCount == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx == -1 {
		return []string{}
	}

	// Extract the column list
	columnList := indexDef[startIdx+1 : endIdx]

	// Split by comma and clean up each column name
	parts := strings.Split(columnList, ",")
	columns := make([]string, 0, len(parts))

	for _, part := range parts {
		// Remove whitespace and quotes
		col := strings.TrimSpace(part)
		col = strings.Trim(col, `"`)

		// Handle expressions (e.g., "LOWER(email)") - take the first part before (
		if idx := strings.Index(col, "("); idx != -1 {
			col = strings.TrimSpace(col[:idx])
		}

		if col != "" {
			columns = append(columns, col)
		}
	}

	return columns
}
