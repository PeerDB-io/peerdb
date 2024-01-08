package connpostgres

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"golang.org/x/exp/maps"
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
	logger slog.Logger
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
	columnCount := utils.TableSchemaColumns(n.normalizedTableSchema)
	columnNames := make([]string, 0, columnCount)
	flattenedCastsSQLArray := make([]string, 0, columnCount)
	primaryKeyColumnCasts := make(map[string]string)
	utils.IterColumns(n.normalizedTableSchema, func(columnName, genericColumnType string) {
		columnNames = append(columnNames, fmt.Sprintf("\"%s\"", columnName))
		pgType := qValueKindToPostgresType(genericColumnType)
		if qvalue.QValueKind(genericColumnType).IsArray() {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'%s')::JSON))::%s AS \"%s\"",
					strings.Trim(columnName, "\""), pgType, columnName))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>'%s')::%s AS \"%s\"",
				strings.Trim(columnName, "\""), pgType, columnName))
		}
		if slices.Contains(n.normalizedTableSchema.PrimaryKeyColumns, columnName) {
			primaryKeyColumnCasts[columnName] = fmt.Sprintf("(_peerdb_data->>'%s')::%s", columnName, pgType)
		}
	})
	flattenedCastsSQL := strings.TrimSuffix(strings.Join(flattenedCastsSQLArray, ","), ",")
	parsedDstTable, _ := utils.ParseSchemaTable(n.dstTableName)

	insertColumnsSQL := strings.TrimSuffix(strings.Join(columnNames, ","), ",")
	updateColumnsSQLArray := make([]string, 0, utils.TableSchemaColumns(n.normalizedTableSchema))
	utils.IterColumns(n.normalizedTableSchema, func(columnName, _ string) {
		updateColumnsSQLArray = append(updateColumnsSQLArray, fmt.Sprintf(`"%s"=EXCLUDED."%s"`, columnName, columnName))
	})
	updateColumnsSQL := strings.TrimSuffix(strings.Join(updateColumnsSQLArray, ","), ",")
	deleteWhereClauseArray := make([]string, 0, len(n.normalizedTableSchema.PrimaryKeyColumns))
	for columnName, columnCast := range primaryKeyColumnCasts {
		deleteWhereClauseArray = append(deleteWhereClauseArray, fmt.Sprintf(`%s."%s"=%s AND `,
			parsedDstTable.String(), columnName, columnCast))
	}
	deleteWhereClauseSQL := strings.TrimSuffix(strings.Join(deleteWhereClauseArray, ""), "AND ")
	deletePart := fmt.Sprintf(
		"DELETE FROM %s USING",
		parsedDstTable.String())

	if n.peerdbCols.SoftDelete {
		deletePart = fmt.Sprintf(`UPDATE %s SET "%s"=TRUE`,
			parsedDstTable.String(), n.peerdbCols.SoftDeleteColName)
		if n.peerdbCols.SyncedAtColName != "" {
			deletePart = fmt.Sprintf(`%s,"%s"=CURRENT_TIMESTAMP`,
				deletePart, n.peerdbCols.SyncedAtColName)
		}
		deletePart += " FROM"
	}
	fallbackUpsertStatement := fmt.Sprintf(fallbackUpsertStatementSQL,
		strings.TrimSuffix(strings.Join(maps.Values(primaryKeyColumnCasts), ","), ","), n.metadataSchema,
		n.rawTableName, parsedDstTable.String(), insertColumnsSQL, flattenedCastsSQL,
		strings.Join(n.normalizedTableSchema.PrimaryKeyColumns, ","), updateColumnsSQL)
	fallbackDeleteStatement := fmt.Sprintf(fallbackDeleteStatementSQL,
		strings.Join(maps.Values(primaryKeyColumnCasts), ","), n.metadataSchema,
		n.rawTableName, deletePart, deleteWhereClauseSQL)

	return []string{fallbackUpsertStatement, fallbackDeleteStatement}
}

func (n *normalizeStmtGenerator) generateMergeStatement() string {
	columnNames := utils.TableSchemaColumnNames(n.normalizedTableSchema)
	for i, columnName := range columnNames {
		columnNames[i] = fmt.Sprintf("\"%s\"", columnName)
	}

	flattenedCastsSQLArray := make([]string, 0, utils.TableSchemaColumns(n.normalizedTableSchema))
	parsedDstTable, _ := utils.ParseSchemaTable(n.dstTableName)

	primaryKeyColumnCasts := make(map[string]string)
	primaryKeySelectSQLArray := make([]string, 0, len(n.normalizedTableSchema.PrimaryKeyColumns))
	utils.IterColumns(n.normalizedTableSchema, func(columnName, genericColumnType string) {
		pgType := qValueKindToPostgresType(genericColumnType)
		if qvalue.QValueKind(genericColumnType).IsArray() {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS_TEXT((_peerdb_data->>'%s')::JSON))::%s AS \"%s\"",
					strings.Trim(columnName, "\""), pgType, columnName))
		} else {
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("(_peerdb_data->>'%s')::%s AS \"%s\"",
				strings.Trim(columnName, "\""), pgType, columnName))
		}
		if slices.Contains(n.normalizedTableSchema.PrimaryKeyColumns, columnName) {
			primaryKeyColumnCasts[columnName] = fmt.Sprintf("(_peerdb_data->>'%s')::%s", columnName, pgType)
			primaryKeySelectSQLArray = append(primaryKeySelectSQLArray, fmt.Sprintf("src.%s=dst.%s",
				columnName, columnName))
		}
	})
	flattenedCastsSQL := strings.TrimSuffix(strings.Join(flattenedCastsSQLArray, ","), ",")
	insertValuesSQLArray := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		insertValuesSQLArray = append(insertValuesSQLArray, fmt.Sprintf("src.%s", columnName))
	}

	updateStatementsforToastCols := n.generateUpdateStatements(columnNames)
	// append synced_at column
	columnNames = append(columnNames, fmt.Sprintf(`"%s"`, n.peerdbCols.SyncedAtColName))
	insertColumnsSQL := strings.Join(columnNames, ",")
	// fill in synced_at column
	insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	insertValuesSQL := strings.TrimSuffix(strings.Join(insertValuesSQLArray, ","), ",")

	if n.peerdbCols.SoftDelete {
		softDeleteInsertColumnsSQL := strings.TrimSuffix(strings.Join(append(columnNames,
			fmt.Sprintf(`"%s"`, n.peerdbCols.SoftDeleteColName)), ","), ",")
		softDeleteInsertValuesSQL := strings.Join(append(insertValuesSQLArray, "TRUE"), ",")

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (src._peerdb_record_type=2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, "\n")

	deletePart := "DELETE"
	if n.peerdbCols.SoftDelete {
		colName := n.peerdbCols.SoftDeleteColName
		deletePart = fmt.Sprintf(`UPDATE SET "%s"=TRUE`, colName)
		if n.peerdbCols.SyncedAtColName != "" {
			deletePart = fmt.Sprintf(`%s,"%s"=CURRENT_TIMESTAMP`,
				deletePart, n.peerdbCols.SyncedAtColName)
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
		deletePart,
	)

	return mergeStmt
}

func (n *normalizeStmtGenerator) generateUpdateStatements(allCols []string) []string {
	handleSoftDelete := n.peerdbCols.SoftDelete && (n.peerdbCols.SoftDeleteColName != "")
	// weird way of doing it but avoids prealloc lint
	updateStmts := make([]string, 0, func() int {
		if handleSoftDelete {
			return 2 * len(n.unchangedToastColumns)
		}
		return len(n.unchangedToastColumns)
	}())

	for _, cols := range n.unchangedToastColumns {
		unquotedUnchangedColsArray := strings.Split(cols, ",")
		unchangedColsArray := make([]string, 0, len(unquotedUnchangedColsArray))
		for _, unchangedToastCol := range unquotedUnchangedColsArray {
			unchangedColsArray = append(unchangedColsArray, unchangedToastCol)
		}
		otherCols := utils.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("%s=src.%s", colName, colName))
		}
		// set the synced at column to the current timestamp
		if n.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, fmt.Sprintf(`"%s"=CURRENT_TIMESTAMP`,
				n.peerdbCols.SyncedAtColName))
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, fmt.Sprintf(`"%s"=FALSE`,
				n.peerdbCols.SoftDeleteColName))
		}

		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type!=2 AND _peerdb_unchanged_toast_columns='%s'
			THEN UPDATE SET %s`, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray = append(tmpArray[:len(tmpArray)-1],
				fmt.Sprintf(`"%s"=TRUE`, n.peerdbCols.SoftDeleteColName))
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			src._peerdb_record_type=2 AND _peerdb_unchanged_toast_columns='%s'
			THEN UPDATE SET %s `, cols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
