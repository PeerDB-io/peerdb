package connpostgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type normalizeStmtGenerator struct {
	// to log fallback statement selection
	log.Logger
	// _PEERDB_RAW_...
	rawTableName string
	// the schema of the table to merge into
	tableSchemaMapping map[string]*protos.TableSchema
	// array of toast column combinations that are unchanged
	unchangedToastColumnsMap map[string][]string
	// _PEERDB_IS_DELETED and _SYNCED_AT columns
	peerdbCols *protos.PeerDBColumns
	// Postgres metadata schema
	metadataSchema string
	// Postgres version 15 introduced MERGE, fallback statements before that
	supportsMerge bool
}

func (n *normalizeStmtGenerator) columnTypeToPg(schema *protos.TableSchema, column *protos.FieldDescription) string {
	var pgType string
	switch schema.System {
	case protos.TypeSystem_Q:
		pgType = qValueKindToPostgresType(column.Type)
	case protos.TypeSystem_PG:
		pgType = column.Type
		// Add schema qualification for user-defined types
		if column.TypeSchemaName != "" {
			schemaQualifiedPgType := common.QualifiedTable{
				Namespace: column.TypeSchemaName,
				Table:     pgType,
			}
			return schemaQualifiedPgType.String()
		}
	default:
		panic(fmt.Sprintf("unsupported system %s", schema.System))
	}

	return pgType
}

func (n *normalizeStmtGenerator) generateExpr(
	normalizedTableSchema *protos.TableSchema,
	genericColumnType string,
	stringCol string,
	pgType string,
) string {
	if normalizedTableSchema.System == protos.TypeSystem_Q {
		qkind := types.QValueKind(genericColumnType)
		if qkind.IsArray() {
			return fmt.Sprintf("ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>%s)::JSON))::%s", stringCol, pgType)
		} else if qkind == types.QValueKindBytes {
			return fmt.Sprintf("decode(_peerdb_data->>%s, 'base64')::%s", stringCol, pgType)
		}
	}
	return fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
}

func (n *normalizeStmtGenerator) generateNormalizeStatements(dstTable string) []string {
	normalizedTableSchema := n.tableSchemaMapping[dstTable]

	if n.supportsMerge {
		unchangedToastColumns := n.unchangedToastColumnsMap[dstTable]
		stmts := []string{n.generateMergeStatement(dstTable, normalizedTableSchema, unchangedToastColumns)}
		// For partitioned destination tables where delete records lack the partition key
		// (REPLICA IDENTITY DEFAULT), split deletes into a separate statement that only
		// uses primary keys, so the MERGE can use partition key equality for pruning.
		if dstTable == "chat_messages.messages" {
			stmts = append(stmts, n.generatePartitionedDeleteStatement(dstTable, normalizedTableSchema))
		}
		return stmts
	}
	n.Warn("Postgres version is not high enough to support MERGE, falling back to UPSERT+DELETE")
	n.Warn("TOAST columns will not be updated properly, use REPLICA IDENTITY FULL or upgrade Postgres")
	if n.peerdbCols.SoftDeleteColName != "" {
		n.Warn("soft delete enabled with fallback statements! this combination is unsupported")
	}
	return n.generateFallbackStatements(dstTable, normalizedTableSchema)
}

func (n *normalizeStmtGenerator) generateFallbackStatements(
	dstTableName string,
	normalizedTableSchema *protos.TableSchema,
) []string {
	columnCount := len(normalizedTableSchema.Columns)
	columnNames := make([]string, 0, columnCount)
	flattenedCastsSQLArray := make([]string, 0, columnCount)
	primaryKeyColumnCasts := make(map[string]string, len(normalizedTableSchema.PrimaryKeyColumns))
	for _, column := range normalizedTableSchema.Columns {
		genericColumnType := column.Type
		quotedCol := common.QuoteIdentifier(column.Name)
		stringCol := utils.QuoteLiteral(column.Name)
		columnNames = append(columnNames, quotedCol)
		pgType := n.columnTypeToPg(normalizedTableSchema, column)
		expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)

		flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("%s AS %s", expr, quotedCol))
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = expr
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	parsedDstTable, _ := common.ParseTableIdentifier(dstTableName)

	insertColumnsSQL := strings.Join(columnNames, ",")
	updateColumnsSQLArray := make([]string, 0, columnCount)
	for _, column := range normalizedTableSchema.Columns {
		quotedCol := common.QuoteIdentifier(column.Name)
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`%s=EXCLUDED.%s`, quotedCol, quotedCol))
	}
	updateColumnsSQL := strings.Join(updateColumnsSQLArray, ",")
	deleteWhereClauseArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s.%s=%s`,
			parsedDstTable.String(), common.QuoteIdentifier(columnName), columnCast))
	}
	deleteWhereClauseSQL := strings.Join(deleteWhereClauseArray, " AND ")

	// make it update instead in case soft-delete is enabled
	deleteUpdate := fmt.Sprintf(`DELETE FROM %s USING `, parsedDstTable.String())
	if n.peerdbCols.SoftDeleteColName != "" {
		deleteUpdate = fmt.Sprintf(`UPDATE %s SET %s=TRUE`,
			parsedDstTable.String(), common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName))
		if n.peerdbCols.SyncedAtColName != "" {
			deleteUpdate += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
		deleteUpdate += " FROM"
	}
	fallbackUpsertStatement := fmt.Sprintf(fallbackUpsertStatementSQL,
		strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","), n.metadataSchema,
		n.rawTableName, parsedDstTable.String(), insertColumnsSQL, flattenedCastsSQL,
		strings.Join(normalizedTableSchema.PrimaryKeyColumns, ","), updateColumnsSQL)
	fallbackDeleteStatement := fmt.Sprintf(fallbackDeleteStatementSQL,
		strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","), n.metadataSchema,
		n.rawTableName, deleteUpdate, deleteWhereClauseSQL)

	return []string{fallbackUpsertStatement, fallbackDeleteStatement}
}

func (n *normalizeStmtGenerator) generateMergeStatement(
	dstTableName string,
	normalizedTableSchema *protos.TableSchema,
	unchangedToastColumns []string,
) string {
	columnCount := len(normalizedTableSchema.Columns)
	quotedColumnNames := make([]string, columnCount)

	parsedDstTable, _ := common.ParseTableIdentifier(dstTableName)

	primaryKeyColumnCasts := make(map[string]string, len(normalizedTableSchema.PrimaryKeyColumns))
	primaryKeySelectSQLArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))

	// For jsonb_to_record path we build:
	//   recordDefs  – column definitions for the AS clause of jsonb_to_record (in the CTE)
	//   selectExprs – the SELECT list in the USING subquery, referencing columns from src_rank.
	//                 json/jsonb columns are wrapped with _peerdb_parse_jsonb/_peerdb_parse_json
	//                 to unwrap PeerDB's stringified representation.
	// For legacy path we only build selectExprs (flattened casts via ->>).
	selectExprs := make([]string, 0, columnCount)
	recordDefs := make([]string, 0, columnCount)
	useJsonbToRecord := normalizedTableSchema.System == protos.TypeSystem_PG
	for i, column := range normalizedTableSchema.Columns {
		quotedCol := common.QuoteIdentifier(column.Name)
		stringCol := utils.QuoteLiteral(column.Name)
		quotedColumnNames[i] = quotedCol
		pgType := n.columnTypeToPg(normalizedTableSchema, column)

		if useJsonbToRecord {
			// json/jsonb columns are stored as stringified text inside _peerdb_data,
			// so jsonb_to_record extracts them as a jsonb string wrapper.
			// Include them in the record as jsonb, then unwrap in the USING SELECT.
			switch column.Type {
			case "json":
				recordDefs = append(recordDefs, quotedCol+" jsonb")
				selectExprs = append(selectExprs, fmt.Sprintf("(%s #>> '{}')::json AS %s", quotedCol, quotedCol))
			case "jsonb":
				recordDefs = append(recordDefs, quotedCol+" jsonb")
				selectExprs = append(selectExprs, fmt.Sprintf("(%s #>> '{}')::jsonb AS %s", quotedCol, quotedCol))
			default:
				recordDefs = append(recordDefs, fmt.Sprintf("%s %s", quotedCol, pgType))
				selectExprs = append(selectExprs, quotedCol)
			}
		} else {
			genericColumnType := column.Type
			expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)
			selectExprs = append(selectExprs, fmt.Sprintf("%s AS %s", expr, quotedCol))
		}

		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			if !useJsonbToRecord {
				primaryKeyColumnCasts[column.Name] = fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
			}
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s", quotedCol, quotedCol))
		}
	}

	// For partitioned destination tables, include the partition key in the MERGE ON clause
	// to enable partition pruning. Currently hardcoded for chat_messages.messages.
	if dstTableName == "chat_messages.messages" {
		partitionCol := common.QuoteIdentifier("created_at")
		joinClause := fmt.Sprintf("src.%s=dst.%s", partitionCol, partitionCol)
		if !slices.Contains(primaryKeySelectSQLArray, joinClause) {
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, joinClause)
		}
	}

	selectExprsSQL := strings.Join(selectExprs, ",")
	insertValuesSQLArray := make([]string, 0, columnCount+2)
	for _, quotedCol := range quotedColumnNames {
		insertValuesSQLArray = append(insertValuesSQLArray, "src."+quotedCol)
	}

	updateStatementsforToastCols := n.generateUpdateStatements(quotedColumnNames, unchangedToastColumns)
	// append synced_at column
	if n.peerdbCols.SyncedAtColName != "" {
		quotedColumnNames = append(quotedColumnNames, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	}
	insertColumnsSQL := strings.Join(quotedColumnNames, ",")
	insertValuesSQL := strings.Join(insertValuesSQLArray, ",")

	if n.peerdbCols.SoftDeleteColName != "" {
		softDeleteInsertColumnsSQL := strings.Join(
			append(quotedColumnNames, common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName)), ",")
		softDeleteInsertValuesSQL := strings.Join(append(insertValuesSQLArray, "TRUE"), ",")

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, "\n")

	conflictPart := "DELETE"
	if n.peerdbCols.SoftDeleteColName != "" {
		colName := n.peerdbCols.SoftDeleteColName
		conflictPart = fmt.Sprintf(`UPDATE SET %s=TRUE`, common.QuoteIdentifier(colName))
		if n.peerdbCols.SyncedAtColName != "" {
			conflictPart += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
	}

	var mergeStmt string
	if useJsonbToRecord {
		// PARTITION BY uses quoted PK column names directly — jsonb_to_record
		// already extracted them in the CTE via r.*
		primaryKeyQuotedNames := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
		for _, pkCol := range normalizedTableSchema.PrimaryKeyColumns {
			primaryKeyQuotedNames = append(primaryKeyQuotedNames, common.QuoteIdentifier(pkCol))
		}
		mergeStmt = fmt.Sprintf(
			mergeStatementSQLJsonbToRecord,
			strings.Join(primaryKeyQuotedNames, ","),
			n.metadataSchema,
			n.rawTableName,
			strings.Join(recordDefs, ","),
			parsedDstTable.String(),
			selectExprsSQL,
			strings.Join(primaryKeySelectSQLArray, " AND "),
			insertColumnsSQL,
			insertValuesSQL,
			updateStringToastCols,
			conflictPart,
		)
	} else {
		mergeStmt = fmt.Sprintf(
			mergeStatementSQL,
			strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","),
			n.metadataSchema,
			n.rawTableName,
			parsedDstTable.String(),
			selectExprsSQL,
			strings.Join(primaryKeySelectSQLArray, " AND "),
			insertColumnsSQL,
			insertValuesSQL,
			updateStringToastCols,
			conflictPart,
		)
	}

	return mergeStmt
}

// generatePartitionedDeleteStatement generates a separate DELETE for partitioned tables
// where delete records lack the partition key (e.g. REPLICA IDENTITY DEFAULT).
// This allows the MERGE to use partition key equality for pruning while deletes
// fall back to matching on primary keys only.
func (n *normalizeStmtGenerator) generatePartitionedDeleteStatement(
	dstTableName string,
	normalizedTableSchema *protos.TableSchema,
) string {
	parsedDstTable, _ := common.ParseTableIdentifier(dstTableName)
	useJsonbToRecord := normalizedTableSchema.System == protos.TypeSystem_PG

	pkJoinClauses := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	if useJsonbToRecord {
		// Build record defs for PK columns only
		pkRecordDefs := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
		for _, column := range normalizedTableSchema.Columns {
			if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
				quotedCol := common.QuoteIdentifier(column.Name)
				pgType := n.columnTypeToPg(normalizedTableSchema, column)
				pkRecordDefs = append(pkRecordDefs, fmt.Sprintf("%s %s", quotedCol, pgType))
				pkJoinClauses = append(pkJoinClauses, fmt.Sprintf("src.%s=dst.%s", quotedCol, quotedCol))
			}
		}
		primaryKeyQuotedNames := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
		for _, pkCol := range normalizedTableSchema.PrimaryKeyColumns {
			primaryKeyQuotedNames = append(primaryKeyQuotedNames, common.QuoteIdentifier(pkCol))
		}
		return fmt.Sprintf(
			`WITH src_rank AS (
				SELECT r.*,_peerdb_record_type,_peerdb_timestamp,
				RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
				FROM %s.%s, jsonb_to_record(_peerdb_data) AS r(%s)
				WHERE _peerdb_batch_id = $1 AND _peerdb_destination_table_name = $2
			)
			DELETE FROM %s dst USING (SELECT * FROM src_rank WHERE _peerdb_rank=1 AND _peerdb_record_type=2) src
			WHERE %s`,
			strings.Join(primaryKeyQuotedNames, ","),
			n.metadataSchema,
			n.rawTableName,
			strings.Join(pkRecordDefs, ","),
			parsedDstTable.String(),
			strings.Join(pkJoinClauses, " AND "),
		)
	}

	// Legacy ->> path
	for _, column := range normalizedTableSchema.Columns {
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			quotedCol := common.QuoteIdentifier(column.Name)
			stringCol := utils.QuoteLiteral(column.Name)
			pgType := n.columnTypeToPg(normalizedTableSchema, column)
			pkJoinClauses = append(pkJoinClauses,
				fmt.Sprintf("%s.%s=(_peerdb_data->>%s)::%s", parsedDstTable.String(), quotedCol, stringCol, pgType))
		}
	}
	primaryKeyColumnCasts := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for _, column := range normalizedTableSchema.Columns {
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			stringCol := utils.QuoteLiteral(column.Name)
			pgType := n.columnTypeToPg(normalizedTableSchema, column)
			primaryKeyColumnCasts = append(primaryKeyColumnCasts, fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType))
		}
	}
	return fmt.Sprintf(
		`WITH src_rank AS (
			SELECT _peerdb_data,_peerdb_record_type,_peerdb_timestamp,
			RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
			FROM %s.%s WHERE _peerdb_batch_id = $1 AND _peerdb_destination_table_name = $2
		)
		DELETE FROM %s USING src_rank WHERE %s AND src_rank._peerdb_rank=1 AND src_rank._peerdb_record_type=2`,
		strings.Join(primaryKeyColumnCasts, ","),
		n.metadataSchema,
		n.rawTableName,
		parsedDstTable.String(),
		strings.Join(pkJoinClauses, " AND "),
	)
}

func (n *normalizeStmtGenerator) generateUpdateStatements(quotedCols []string, unchangedToastColumns []string) []string {
	handleSoftDelete := n.peerdbCols.SoftDeleteColName != ""
	stmtCount := len(unchangedToastColumns)
	if handleSoftDelete {
		stmtCount *= 2
	}
	updateStmts := make([]string, 0, stmtCount)

	for _, cols := range unchangedToastColumns {
		unchangedColsArray := strings.Split(cols, ",")
		for i, unchangedToastCol := range unchangedColsArray {
			unchangedColsArray[i] = common.QuoteIdentifier(unchangedToastCol)
		}
		otherCols := shared.ArrayMinus(quotedCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("%s=src.%s", colName, colName))
		}
		// set the synced at column to the current timestamp
		if n.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, common.QuoteIdentifier(n.peerdbCols.SyncedAtColName)+`=CURRENT_TIMESTAMP`)
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName)+`=FALSE`)
		}

		quotedCols := utils.QuoteLiteral(cols)
		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray[len(tmpArray)-1] = common.QuoteIdentifier(n.peerdbCols.SoftDeleteColName) + `=TRUE`
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
