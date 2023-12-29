package connbigquery

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type mergeStmtGenerator struct {
	// dataset + raw table
	rawDatasetTable *datasetTable
	// destination table name, used to retrieve records from raw table
	dstTableName string
	// dataset + destination table
	dstDatasetTable *datasetTable
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

// generateFlattenedCTE generates a flattened CTE.
func (m *mergeStmtGenerator) generateFlattenedCTE() string {
	// for each column in the normalized table, generate CAST + JSON_EXTRACT_SCALAR
	// statement.
	flattenedProjs := make([]string, 0, utils.TableSchemaColumns(m.normalizedTableSchema)+3)
	utils.IterColumns(m.normalizedTableSchema, func(colName, colType string) {
		bqType := qValueKindToBigQueryType(colType)
		// CAST doesn't work for FLOAT, so rewrite it to FLOAT64.
		if bqType == bigquery.FloatFieldType {
			bqType = "FLOAT64"
		}
		var castStmt string

		switch qvalue.QValueKind(colType) {
		case qvalue.QValueKindJSON:
			// if the type is JSON, then just extract JSON
			castStmt = fmt.Sprintf("CAST(JSON_VALUE(_peerdb_data, '$.%s') AS %s) AS `%s`",
				colName, bqType, colName)
		// expecting data in BASE64 format
		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			castStmt = fmt.Sprintf("FROM_BASE64(JSON_VALUE(_peerdb_data, '$.%s')) AS `%s`",
				colName, colName)
		case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64,
			qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64, qvalue.QValueKindArrayString:
			castStmt = fmt.Sprintf("ARRAY(SELECT CAST(element AS %s) FROM "+
				"UNNEST(CAST(JSON_VALUE_ARRAY(_peerdb_data, '$.%s') AS ARRAY<STRING>)) AS element) AS `%s`",
				bqType, colName, colName)
		// MAKE_INTERVAL(years INT64, months INT64, days INT64, hours INT64, minutes INT64, seconds INT64)
		// Expecting interval to be in the format of {"Microseconds":2000000,"Days":0,"Months":0,"Valid":true}
		// json.Marshal in SyncRecords for Postgres already does this - once new data-stores are added,
		// this needs to be handled again
		// TODO add interval types again
		// case model.ColumnTypeInterval:
		// castStmt = fmt.Sprintf("MAKE_INTERVAL(0,CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Months') AS INT64),"+
		// 	"CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Days') AS INT64),0,0,"+
		// 	"CAST(CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Microseconds') AS INT64)/1000000 AS  INT64)) AS %s",
		// 	colName, colName, colName, colName)
		// TODO add proper granularity for time types, then restore this
		// case model.ColumnTypeTime:
		// 	castStmt = fmt.Sprintf("time(timestamp_micros(CAST(JSON_EXTRACT(_peerdb_data, '$.%s.Microseconds')"+
		// 		" AS int64))) AS %s",
		// 		colName, colName)
		default:
			castStmt = fmt.Sprintf("CAST(JSON_VALUE(_peerdb_data, '$.%s') AS %s) AS `%s`",
				colName, bqType, colName)
		}
		flattenedProjs = append(flattenedProjs, castStmt)
	})
	flattenedProjs = append(
		flattenedProjs,
		"_peerdb_timestamp",
		"_peerdb_record_type",
		"_peerdb_unchanged_toast_columns",
	)

	// normalize anything between last normalized batch id to last sync batchid
	return fmt.Sprintf(`WITH _peerdb_flattened AS
	 (SELECT %s FROM %s WHERE _peerdb_batch_id > %d and _peerdb_batch_id <= %d and
	 _peerdb_destination_table_name='%s')`,
		strings.Join(flattenedProjs, ", "), m.rawDatasetTable.string(), m.normalizeBatchID,
		m.syncBatchID, m.dstTableName)
}

// generateDeDupedCTE generates a de-duped CTE.
func (m *mergeStmtGenerator) generateDeDupedCTE() string {
	const cte = `_peerdb_de_duplicated_data_res AS (
		SELECT _peerdb_ranked.*
			FROM (
				SELECT RANK() OVER (
					PARTITION BY %s ORDER BY _peerdb_timestamp DESC
				) as _peerdb_rank, * FROM _peerdb_flattened
			) _peerdb_ranked
			WHERE _peerdb_rank = 1
	) SELECT * FROM _peerdb_de_duplicated_data_res`
	pkeyColsStr := fmt.Sprintf("(CONCAT(%s))", strings.Join(m.normalizedTableSchema.PrimaryKeyColumns,
		", '_peerdb_concat_', "))
	return fmt.Sprintf(cte, pkeyColsStr)
}

// generateMergeStmt generates a merge statement.
func (m *mergeStmtGenerator) generateMergeStmt() string {
	// comma separated list of column names
	columnCount := utils.TableSchemaColumns(m.normalizedTableSchema)
	backtickColNames := make([]string, 0, columnCount)
	pureColNames := make([]string, 0, columnCount)
	utils.IterColumns(m.normalizedTableSchema, func(colName, _ string) {
		backtickColNames = append(backtickColNames, fmt.Sprintf("`%s`", colName))
		pureColNames = append(pureColNames, colName)
	})
	csep := strings.Join(backtickColNames, ", ")
	insertColumnsSQL := csep + fmt.Sprintf(", `%s`", m.peerdbCols.SyncedAtColName)
	insertValuesSQL := csep + ",CURRENT_TIMESTAMP"

	updateStatementsforToastCols := m.generateUpdateStatements(pureColNames,
		m.unchangedToastColumns, m.peerdbCols)
	if m.peerdbCols.SoftDelete {
		softDeleteInsertColumnsSQL := insertColumnsSQL + fmt.Sprintf(", `%s`", m.peerdbCols.SoftDeleteColName)
		softDeleteInsertValuesSQL := insertValuesSQL + ", TRUE"

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND (_peerdb_deduped._PEERDB_RECORD_TYPE = 2) THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, " ")

	pkeySelectSQLArray := make([]string, 0, len(m.normalizedTableSchema.PrimaryKeyColumns))
	for _, pkeyColName := range m.normalizedTableSchema.PrimaryKeyColumns {
		pkeySelectSQLArray = append(pkeySelectSQLArray, fmt.Sprintf("_peerdb_target.%s = _peerdb_deduped.%s",
			pkeyColName, pkeyColName))
	}
	// _peerdb_target.<pkey1> = _peerdb_deduped.<pkey1> AND _peerdb_target.<pkey2> = _peerdb_deduped.<pkey2> ...
	pkeySelectSQL := strings.Join(pkeySelectSQLArray, " AND ")

	deletePart := "DELETE"
	if m.peerdbCols.SoftDelete {
		colName := m.peerdbCols.SoftDeleteColName
		deletePart = fmt.Sprintf("UPDATE SET %s = TRUE", colName)
		if m.peerdbCols.SyncedAtColName != "" {
			deletePart = fmt.Sprintf("%s, %s = CURRENT_TIMESTAMP",
				deletePart, m.peerdbCols.SyncedAtColName)
		}
	}

	return fmt.Sprintf(`
	MERGE %s _peerdb_target USING (%s,%s) _peerdb_deduped
	ON %s
		WHEN NOT MATCHED and (_peerdb_deduped._peerdb_record_type != 2) THEN
			INSERT (%s) VALUES (%s)
		%s
		WHEN MATCHED AND (_peerdb_deduped._peerdb_record_type = 2) THEN
	%s;
	`, m.dstDatasetTable.string(), m.generateFlattenedCTE(), m.generateDeDupedCTE(),
		pkeySelectSQL, insertColumnsSQL, insertValuesSQL, updateStringToastCols, deletePart)
}

/*
This function takes an array of unique unchanged toast column groups and an array of all column names,
and returns suitable UPDATE statements as part of a MERGE operation.

Algorithm:
1. Iterate over each unique unchanged toast column group.
2. Split the group into individual column names.
3. Calculate the other columns by finding the set difference between all column names
and the unchanged columns.
4. Generate an update statement for the current group by setting the appropriate conditions
and updating the other columns (not the unchanged toast columns)
5. Append the update statement to the list of generated statements.
6. Repeat steps 1-5 for each unique unchanged toast column group.
7. Return the list of generated update statements.
*/
func (m *mergeStmtGenerator) generateUpdateStatements(
	allCols []string,
	unchangedToastCols []string,
	peerdbCols *protos.PeerDBColumns,
) []string {
	updateStmts := make([]string, 0, len(unchangedToastCols))

	for _, cols := range unchangedToastCols {
		unchangedColsArray := strings.Split(cols, ", ")
		otherCols := utils.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s` = _peerdb_deduped.%s", colName, colName))
		}

		// set the synced at column to the current timestamp
		if peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s` = CURRENT_TIMESTAMP",
				peerdbCols.SyncedAtColName))
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if peerdbCols.SoftDeleteColName != "" {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s` = FALSE",
				peerdbCols.SoftDeleteColName))
		}

		ssep := strings.Join(tmpArray, ", ")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
		(_peerdb_deduped._peerdb_record_type != 2) AND _peerdb_unchanged_toast_columns='%s'
		THEN UPDATE SET %s `, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if peerdbCols.SoftDelete && (peerdbCols.SoftDeleteColName != "") {
			tmpArray = append(tmpArray[:len(tmpArray)-1],
				fmt.Sprintf("`%s` = TRUE", peerdbCols.SoftDeleteColName))
			ssep := strings.Join(tmpArray, ", ")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			(_peerdb_deduped._peerdb_record_type = 2) AND _peerdb_unchanged_toast_columns='%s'
			THEN UPDATE SET %s `, cols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
