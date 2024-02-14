package connsnowflake

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/numeric"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type mergeStmtGenerator struct {
	rawTableName string
	// destination table name, used to retrieve records from raw table
	dstTableName string
	// last synced batchID.
	syncBatchID int64
	// last normalized batchID.
	normalizeBatchID int64
	// the schema of the table to merge into
	normalizedTableSchema *protos.TableSchema
	// array of toast column combinations that are unchanged
	unchangedToastColumns []string
	// _PEERDB_IS_DELETED and _SYNCED_AT columns
	peerdbCols *protos.PeerDBColumns
}

func (m *mergeStmtGenerator) generateMergeStmt() (string, error) {
	parsedDstTable, _ := utils.ParseSchemaTable(m.dstTableName)
	columns := m.normalizedTableSchema.Columns

	flattenedCastsSQLArray := make([]string, 0, len(columns))
	for _, column := range columns {
		genericColumnType := column.Type
		qvKind := qvalue.QValueKind(genericColumnType)
		sfType, err := qValueKindToSnowflakeType(qvKind)
		if err != nil {
			return "", fmt.Errorf("failed to convert column type %s to snowflake type: %w", genericColumnType, err)
		}

		targetColumnName := SnowflakeIdentifierNormalize(column.Name)
		switch qvKind {
		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("BASE64_DECODE_BINARY(%s:\"%s\") "+
				"AS %s", toVariantColumnName, column.Name, targetColumnName))
		case qvalue.QValueKindGeography:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("TO_GEOGRAPHY(CAST(%s:\"%s\" AS STRING),true) AS %s",
					toVariantColumnName, column.Name, targetColumnName))
		case qvalue.QValueKindGeometry:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("TO_GEOMETRY(CAST(%s:\"%s\" AS STRING),true) AS %s",
					toVariantColumnName, column.Name, targetColumnName))
		case qvalue.QValueKindJSON, qvalue.QValueKindHStore:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("PARSE_JSON(CAST(%s:\"%s\" AS STRING)) AS %s",
					toVariantColumnName, column.Name, targetColumnName))
		// TODO: https://github.com/PeerDB-io/peerdb/issues/189 - handle time types and interval types
		// case model.ColumnTypeTime:
		// 	flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("TIME_FROM_PARTS(0,0,0,%s:%s:"+
		// 		"Microseconds*1000) "+
		// 		"AS %s", toVariantColumnName, columnName, columnName))
		case qvalue.QValueKindNumeric:
			precision, scale := numeric.ParseNumericTypmod(column.TypeModifier)
			if column.TypeModifier == -1 || precision > 38 || scale > 37 {
				precision = numeric.PeerDBNumericPrecision
				scale = numeric.PeerDBNumericScale
			}
			numericType := fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
			flattenedCastsSQLArray = append(flattenedCastsSQLArray,
				fmt.Sprintf("TRY_CAST((%s:\"%s\")::text AS %s) AS %s",
					toVariantColumnName, column.Name, numericType, targetColumnName))
		default:
			flattenedCastsSQLArray = append(flattenedCastsSQLArray, fmt.Sprintf("CAST(%s:\"%s\" AS %s) AS %s",
				toVariantColumnName, column.Name, sfType, targetColumnName))
		}
	}
	flattenedCastsSQL := strings.Join(flattenedCastsSQLArray, ",")

	quotedUpperColNames := make([]string, 0, len(columns))
	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedUpperColNames = append(quotedUpperColNames, SnowflakeIdentifierNormalize(column.Name))
		columnNames = append(columnNames, column.Name)
	}
	// append synced_at column
	quotedUpperColNames = append(quotedUpperColNames,
		fmt.Sprintf(`"%s"`, strings.ToUpper(m.peerdbCols.SyncedAtColName)),
	)

	insertColumnsSQL := strings.Join(quotedUpperColNames, ",")

	insertValuesSQLArray := make([]string, 0, len(columns))
	for _, column := range columns {
		normalizedColName := SnowflakeIdentifierNormalize(column.Name)
		insertValuesSQLArray = append(insertValuesSQLArray, fmt.Sprintf("SOURCE.%s", normalizedColName))
	}
	// fill in synced_at column
	insertValuesSQLArray = append(insertValuesSQLArray, "CURRENT_TIMESTAMP")
	insertValuesSQL := strings.Join(insertValuesSQLArray, ",")
	updateStatementsforToastCols := m.generateUpdateStatements(columnNames)

	// handling the case when an insert and delete happen in the same batch, with updates in the middle
	// with soft-delete, we want the row to be in the destination with SOFT_DELETE true
	// the current merge statement doesn't do that, so we add another case to insert the DeleteRecord
	if m.peerdbCols.SoftDelete && (m.peerdbCols.SoftDeleteColName != "") {
		softDeleteInsertColumnsSQL := strings.Join(append(quotedUpperColNames,
			m.peerdbCols.SoftDeleteColName), ",")
		softDeleteInsertValuesSQL := insertValuesSQL + ",TRUE"
		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (SOURCE._PEERDB_RECORD_TYPE = 2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, " ")

	normalizedpkeyColsArray := make([]string, 0, len(m.normalizedTableSchema.PrimaryKeyColumns))
	pkeySelectSQLArray := make([]string, 0, len(m.normalizedTableSchema.PrimaryKeyColumns))
	for _, pkeyColName := range m.normalizedTableSchema.PrimaryKeyColumns {
		normalizedPkeyColName := SnowflakeIdentifierNormalize(pkeyColName)
		normalizedpkeyColsArray = append(normalizedpkeyColsArray, normalizedPkeyColName)
		pkeySelectSQLArray = append(pkeySelectSQLArray, fmt.Sprintf("TARGET.%s = SOURCE.%s",
			normalizedPkeyColName, normalizedPkeyColName))
	}
	// TARGET.<pkey1> = SOURCE.<pkey1> AND TARGET.<pkey2> = SOURCE.<pkey2> ...
	pkeySelectSQL := strings.Join(pkeySelectSQLArray, " AND ")

	deletePart := "DELETE"
	if m.peerdbCols.SoftDelete {
		colName := m.peerdbCols.SoftDeleteColName
		deletePart = fmt.Sprintf("UPDATE SET %s = TRUE", colName)
		if m.peerdbCols.SyncedAtColName != "" {
			deletePart = fmt.Sprintf("%s, %s = CURRENT_TIMESTAMP", deletePart, m.peerdbCols.SyncedAtColName)
		}
	}

	mergeStatement := fmt.Sprintf(mergeStatementSQL, snowflakeSchemaTableNormalize(parsedDstTable),
		toVariantColumnName, m.rawTableName, m.normalizeBatchID, m.syncBatchID, flattenedCastsSQL,
		fmt.Sprintf("(%s)", strings.Join(normalizedpkeyColsArray, ",")),
		pkeySelectSQL, insertColumnsSQL, insertValuesSQL, updateStringToastCols, deletePart)

	return mergeStatement, nil
}

/*
This function generates UPDATE statements for a MERGE operation based on the provided inputs.

Inputs:
1. allCols: An array of all column names.
2. unchangedToastCols: An array capturing unique sets of unchanged toast column groups.
3. softDeleteCol: just set to false in the case we see an insert after a soft-deleted column
4. syncedAtCol: set to the CURRENT_TIMESTAMP

Algorithm:
1. Iterate over each unique set of unchanged toast column groups.
2. For each group, split it into individual column names.
3. Calculate the other columns by finding the set difference between allCols and the unchanged columns.
4. Generate an update statement for the current group by setting the appropriate conditions
and updating the other columns.
  - The condition includes checking if the _PEERDB_RECORD_TYPE is not 2 (not a DELETE) and if the
    _PEERDB_UNCHANGED_TOAST_COLUMNS match the current group.
  - The update sets the other columns to their corresponding values
    from the SOURCE table. It doesn't set (make null the Unchanged toast columns.

5. Append the update statement to the list of generated statements.
6. Repeat steps 1-5 for each unique set of unchanged toast column groups.
7. Return the list of generated update statements.
*/
func (m *mergeStmtGenerator) generateUpdateStatements(allCols []string) []string {
	handleSoftDelete := m.peerdbCols.SoftDelete && (m.peerdbCols.SoftDeleteColName != "")
	// weird way of doing it but avoids prealloc lint
	updateStmts := make([]string, 0, func() int {
		if handleSoftDelete {
			return 2 * len(m.unchangedToastColumns)
		}
		return len(m.unchangedToastColumns)
	}())

	for _, cols := range m.unchangedToastColumns {
		unchangedColsArray := strings.Split(cols, ",")
		otherCols := utils.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols)+2)
		for _, colName := range otherCols {
			normalizedColName := SnowflakeIdentifierNormalize(colName)
			tmpArray = append(tmpArray, fmt.Sprintf("%s = SOURCE.%s", normalizedColName, normalizedColName))
		}

		// set the synced at column to the current timestamp
		if m.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, fmt.Sprintf(`"%s" = CURRENT_TIMESTAMP`,
				m.peerdbCols.SyncedAtColName))
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, fmt.Sprintf(`"%s" = FALSE`,
				m.peerdbCols.SoftDeleteColName))
		}

		ssep := strings.Join(tmpArray, ", ")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
		(SOURCE._PEERDB_RECORD_TYPE != 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='%s'
		THEN UPDATE SET %s `, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray = append(tmpArray[:len(tmpArray)-1], fmt.Sprintf(`"%s" = TRUE`,
				m.peerdbCols.SoftDeleteColName))
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			(SOURCE._PEERDB_RECORD_TYPE = 2) AND _PEERDB_UNCHANGED_TOAST_COLUMNS='%s'
			THEN UPDATE SET %s `, cols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
