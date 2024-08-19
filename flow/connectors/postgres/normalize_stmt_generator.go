package connpostgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
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

func (n *normalizeStmtGenerator) columnTypeToPg(schema *protos.TableSchema, columnType string) string {
	switch schema.System {
	case protos.TypeSystem_Q:
		return qValueKindToPostgresType(columnType)
	case protos.TypeSystem_PG:
		return columnType
	default:
		panic(fmt.Sprintf("unsupported system %s", schema.System))
	}
}

func (n *normalizeStmtGenerator) generateExpr(
	normalizedTableSchema *protos.TableSchema,
	genericColumnType string,
	stringCol string,
	pgType string,
) string {
	if normalizedTableSchema.System == protos.TypeSystem_Q {
		qkind := qvalue.QValueKind(genericColumnType)
		if qkind.IsArray() {
			return fmt.Sprintf("ARRAY(SELECT JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>%s)::JSON))::%s", stringCol, pgType)
		} else if qkind == qvalue.QValueKindBytes {
			return fmt.Sprintf("decode(_peerdb_data->>%s, 'base64')::%s", stringCol, pgType)
		}
	}
	return fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
}

func (n *normalizeStmtGenerator) generateNormalizeStatements(dstTable string) []string {
	normalizedTableSchema := n.tableSchemaMapping[dstTable]
	if n.supportsMerge {
		unchangedToastColumns := n.unchangedToastColumnsMap[dstTable]
		return []string{n.generateMergeStatement(dstTable, normalizedTableSchema, unchangedToastColumns)}
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
		quotedCol := QuoteIdentifier(column.Name)
		stringCol := QuoteLiteral(column.Name)
		columnNames = append(columnNames, quotedCol)
		pgType := n.columnTypeToPg(normalizedTableSchema, genericColumnType)
		expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)

		flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("%s AS %s", expr, quotedCol))
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = expr
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	parsedDstTable, _ := utils.ParseSchemaTable(dstTableName)

	insertColumnsSQL := strings.Join(columnNames, ",")
	updateColumnsSQLArray := make([]string, 0, columnCount)
	for _, column := range normalizedTableSchema.Columns {
		quotedCol := QuoteIdentifier(column.Name)
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`%s=EXCLUDED.%s`, quotedCol, quotedCol))
	}
	updateColumnsSQL := strings.Join(updateColumnsSQLArray, ",")
	deleteWhereClauseArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s.%s=%s`,
			parsedDstTable.String(), QuoteIdentifier(columnName), columnCast))
	}
	deleteWhereClauseSQL := strings.Join(deleteWhereClauseArray, " AND ")

	// make it update instead in case soft-delete is enabled
	deleteUpdate := fmt.Sprintf(`DELETE FROM %s USING `, parsedDstTable.String())
	if n.peerdbCols.SoftDeleteColName != "" {
		deleteUpdate = fmt.Sprintf(`UPDATE %s SET %s=TRUE`,
			parsedDstTable.String(), QuoteIdentifier(n.peerdbCols.SoftDeleteColName))
		if n.peerdbCols.SyncedAtColName != "" {
			deleteUpdate += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
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

	flattenedCastsSQLArray := make([]string, 0, columnCount)
	parsedDstTable, _ := utils.ParseSchemaTable(dstTableName)

	primaryKeyColumnCasts := make(map[string]string)
	primaryKeySelectSQLArray := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	for i, column := range normalizedTableSchema.Columns {
		genericColumnType := column.Type
		quotedCol := QuoteIdentifier(column.Name)
		stringCol := QuoteLiteral(column.Name)
		quotedColumnNames[i] = quotedCol
		pgType := n.columnTypeToPg(normalizedTableSchema, genericColumnType)
		expr := n.generateExpr(normalizedTableSchema, genericColumnType, stringCol, pgType)

		flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("%s AS %s", expr, quotedCol))
		if slices.Contains(normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s", quotedCol, quotedCol))
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	insertValuesSQLArray := make([]string, 0, columnCount+2)
	for _, quotedCol := range quotedColumnNames {
		insertValuesSQLArray = append(insertValuesSQLArray, "src."+quotedCol)
	}

	updateStatementsforToastCols := n.generateUpdateStatements(quotedColumnNames, unchangedToastColumns)
	// append synced_at column
	if n.peerdbCols.SyncedAtColName != "" {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	}
	insertColumnsSQL := strings.Join(quotedColumnNames, ",")
	insertValuesSQL := strings.Join(insertValuesSQLArray, ",")

	if n.peerdbCols.SoftDeleteColName != "" {
		softDeleteInsertColumnsSQL := strings.Join(
			append(quotedColumnNames, QuoteIdentifier(n.peerdbCols.SoftDeleteColName)), ",")
		softDeleteInsertValuesSQL := strings.Join(append(insertValuesSQLArray, "TRUE"), ",")

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, "\n")

	conflictPart := "DELETE"
	if n.peerdbCols.SoftDeleteColName != "" {
		colName := n.peerdbCols.SoftDeleteColName
		conflictPart = fmt.Sprintf(`UPDATE SET %s=TRUE`, QuoteIdentifier(colName))
		if n.peerdbCols.SyncedAtColName != "" {
			conflictPart += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
	}

	mergeStmt := fmt.Sprintf(
		mergeStatementSQL,
		strings.Join(slices.Collect(maps.Values(primaryKeyColumnCasts)), ","),
		n.metadataSchema,
		n.rawTableName,
		parsedDstTable.String(),
		flattenedCastsSQL,
		strings.Join(primaryKeySelectSQLArray, " AND "),
		insertColumnsSQL,
		insertValuesSQL,
		updateStringToastCols,
		conflictPart,
	)

	return mergeStmt
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
			unchangedColsArray[i] = QuoteIdentifier(unchangedToastCol)
		}
		otherCols := shared.ArrayMinus(quotedCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("%s=src.%s", colName, colName))
		}
		// set the synced at column to the current timestamp
		if n.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, QuoteIdentifier(n.peerdbCols.SyncedAtColName)+`=CURRENT_TIMESTAMP`)
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, QuoteIdentifier(n.peerdbCols.SoftDeleteColName)+`=FALSE`)
		}

		quotedCols := QuoteLiteral(cols)
		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray[len(tmpArray)-1] = QuoteIdentifier(n.peerdbCols.SoftDeleteColName) + `=TRUE`
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns=%s
			THEN UPDATE SET %s`, quotedCols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
