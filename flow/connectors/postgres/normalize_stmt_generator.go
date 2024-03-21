package connpostgres

import (
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/sdk/log"
	"golang.org/x/exp/maps"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type normalizeStmtGenerator struct {
	rawTableName string
	// destination table name, used to retrieve records from raw table
	dstTableName string
	// the schema of the table to merge into
	normalizedTableSchema *protos.TableSchema
	// array of toast column combinations that are unchanged
	unchangedToastColumns []string
	// _PEERDB_IS_DELETED and _SYNCED_AT columns
	peerdbCols *protos.PeerDBColumns
	// Postgres version 15 introduced MERGE, fallback statements before that
	supportsMerge bool
	// Postgres metadata schema
	metadataSchema string
	// to log fallback statement selection
	logger log.Logger
}

func (n *normalizeStmtGenerator) generateNormalizeStatements() []string {
	if n.supportsMerge {
		return []string{n.generateMergeStatement()}
	}
	n.logger.Warn("Postgres version is not high enough to support MERGE, falling back to UPSERT+DELETE")
	n.logger.Warn("TOAST columns will not be updated properly, use REPLICA IDENTITY FULL or upgrade Postgres")
	if n.peerdbCols.SoftDelete {
		n.logger.Warn("soft delete enabled with fallback statements! this combination is unsupported")
	}
	return n.generateFallbackStatements()
}

func (n *normalizeStmtGenerator) generateFallbackStatements() []string {
	columnCount := len(n.normalizedTableSchema.Columns)
	columnNames := make([]string, 0, columnCount)
	flattenedCastsSQLArray := make([]string, 0, columnCount)
	primaryKeyColumnCasts := make(map[string]string, len(n.normalizedTableSchema.PrimaryKeyColumns))
	for _, column := range n.normalizedTableSchema.Columns {
		genericColumnType := column.Type
		quotedCol := QuoteIdentifier(column.Name)
		stringCol := QuoteLiteral(column.Name)
		columnNames = append(columnNames, quotedCol)
		pgType := qValueKindToPostgresType(genericColumnType)
		if qvalue.QValueKind(genericColumnType).IsArray() {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>%s)::JSON))::%s AS %s",
					stringCol, pgType, quotedCol))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>%s)::%s AS %s",
				stringCol, pgType, quotedCol))
		}
		if slices.Contains(n.normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	parsedDstTable, _ := utils.ParseSchemaTable(n.dstTableName)

	insertColumnsSQL := strings.Join(columnNames, ",")
	updateColumnsSQLArray := make([]string, 0, columnCount)
	for _, column := range n.normalizedTableSchema.Columns {
		quotedCol := QuoteIdentifier(column.Name)
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`%s=EXCLUDED.%s`, quotedCol, quotedCol))
	}
	updateColumnsSQL := strings.Join(updateColumnsSQLArray, ",")
	deleteWhereClauseArray := make([]string, 0, len(n.normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s.%s=%s`,
			parsedDstTable.String(), QuoteIdentifier(columnName), columnCast))
	}
	deleteWhereClauseSQL := strings.Join(deleteWhereClauseArray, " AND ")

	// make it update instead in case soft-delete is enabled
	deleteUpdate := fmt.Sprintf(`DELETE FROM %s USING `, parsedDstTable.String())
	if n.peerdbCols.SoftDelete {
		deleteUpdate = fmt.Sprintf(`UPDATE %s SET %s=TRUE`,
			parsedDstTable.String(), QuoteIdentifier(n.peerdbCols.SoftDeleteColName))
		if n.peerdbCols.SyncedAtColName != "" {
			deleteUpdate += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
		deleteUpdate += " FROM"
	}
	fallbackUpsertStatement := fmt.Sprintf(fallbackUpsertStatementSQL,
		strings.Join(maps.Values(primaryKeyColumnCasts), ","), n.metadataSchema,
		n.rawTableName, parsedDstTable.String(), insertColumnsSQL, flattenedCastsSQL,
		strings.Join(n.normalizedTableSchema.PrimaryKeyColumns, ","), updateColumnsSQL)
	fallbackDeleteStatement := fmt.Sprintf(fallbackDeleteStatementSQL,
		strings.Join(maps.Values(primaryKeyColumnCasts), ","), n.metadataSchema,
		n.rawTableName, deleteUpdate, deleteWhereClauseSQL)

	return []string{fallbackUpsertStatement, fallbackDeleteStatement}
}

func (n *normalizeStmtGenerator) generateMergeStatement() string {
	columnCount := len(n.normalizedTableSchema.Columns)
	quotedColumnNames := make([]string, columnCount)

	flattenedCastsSQLArray := make([]string, 0, columnCount)
	parsedDstTable, _ := utils.ParseSchemaTable(n.dstTableName)

	primaryKeyColumnCasts := make(map[string]string)
	primaryKeySelectSQLArray := make([]string, 0, len(n.normalizedTableSchema.PrimaryKeyColumns))
	for i, column := range n.normalizedTableSchema.Columns {
		genericColumnType := column.Type
		quotedCol := QuoteIdentifier(column.Name)
		stringCol := QuoteLiteral(column.Name)
		quotedColumnNames[i] = quotedCol

		pgType := qValueKindToPostgresType(genericColumnType)
		if qvalue.QValueKind(genericColumnType).IsArray() {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>%s)::JSON))::%s AS %s",
					stringCol, pgType, quotedCol))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>%s)::%s AS %s",
				stringCol, pgType, quotedCol))
		}
		if slices.Contains(n.normalizedTableSchema.PrimaryKeyColumns, column.Name) {
			primaryKeyColumnCasts[column.Name] = fmt.Sprintf("(_peerdb_data->>%s)::%s", stringCol, pgType)
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s",
				quotedCol, quotedCol))
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")
	insertValuesSQLArray := make([]string, 0, columnCount+2)
	for _, quotedCol := range quotedColumnNames {
		insertValuesSQLArray = append(insertValuesSQLArray, "src."+quotedCol)
	}

	updateStatementsforToastCols := n.generateUpdateStatements(quotedColumnNames)
	// append synced_at column
	if n.peerdbCols.SyncedAtColName != "" {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	}
	insertColumnsSQL := strings.Join(quotedColumnNames, ",")
	insertValuesSQL := strings.Join(insertValuesSQLArray, ",")

	if n.peerdbCols.SoftDelete {
		softDeleteInsertColumnsSQL := strings.Join(
			append(quotedColumnNames, QuoteIdentifier(n.peerdbCols.SoftDeleteColName)), ",")
		softDeleteInsertValuesSQL := strings.Join(append(insertValuesSQLArray, "TRUE"), ",")

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, "\n")

	conflictPart := "DELETE"
	if n.peerdbCols.SoftDelete {
		colName := n.peerdbCols.SoftDeleteColName
		conflictPart = fmt.Sprintf(`UPDATE SET %s=TRUE`, QuoteIdentifier(colName))
		if n.peerdbCols.SyncedAtColName != "" {
			conflictPart += fmt.Sprintf(`,%s=CURRENT_TIMESTAMP`, QuoteIdentifier(n.peerdbCols.SyncedAtColName))
		}
	}

	mergeStmt := fmt.Sprintf(
		mergeStatementSQL,
		strings.Join(maps.Values(primaryKeyColumnCasts), ","),
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

func (n *normalizeStmtGenerator) generateUpdateStatements(quotedCols []string) []string {
	handleSoftDelete := n.peerdbCols.SoftDelete && (n.peerdbCols.SoftDeleteColName != "")
	// weird way of doing it but avoids prealloc lint
	updateStmts := make([]string, 0, func() int {
		if handleSoftDelete {
			return 2 * len(n.unchangedToastColumns)
		}
		return len(n.unchangedToastColumns)
	}())

	for _, cols := range n.unchangedToastColumns {
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
