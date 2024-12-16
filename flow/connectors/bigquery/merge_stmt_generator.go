package connbigquery

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type mergeStmtGenerator struct {
	// the schema of the table to merge into
	tableSchemaMapping map[string]*protos.TableSchema
	// _PEERDB_IS_DELETED and _SYNCED_AT columns
	peerdbCols *protos.PeerDBColumns
	// map for shorter columns
	shortColumn map[string]string
	// dataset + raw table
	rawDatasetTable datasetTable
	// batch id currently to be merged
	mergeBatchId int64
}

// generateFlattenedCTE generates a flattened CTE.
func (m *mergeStmtGenerator) generateFlattenedCTE(dstTable string, normalizedTableSchema *protos.TableSchema) string {
	// for each column in the normalized table, generate CAST + JSON_EXTRACT_SCALAR
	// statement.
	flattenedProjs := make([]string, 0, len(normalizedTableSchema.Columns)+3)

	for _, column := range normalizedTableSchema.Columns {
		colType := column.Type
		bqTypeString := qValueKindToBigQueryTypeString(column, normalizedTableSchema.NullableEnabled, true)
		var castStmt string
		shortCol := m.shortColumn[column.Name]
		switch qvalue.QValueKind(colType) {
		case qvalue.QValueKindJSON, qvalue.QValueKindJSONB, qvalue.QValueKindHStore:
			// if the type is JSON, then just extract JSON
			castStmt = fmt.Sprintf("CAST(PARSE_JSON(JSON_VALUE(_peerdb_data, '$.%s'),wide_number_mode=>'round') AS %s) AS `%s`",
				column.Name, bqTypeString, shortCol)
		// expecting data in BASE64 format
		case qvalue.QValueKindBytes:
			castStmt = fmt.Sprintf("FROM_BASE64(JSON_VALUE(_peerdb_data,'$.%s')) AS `%s`",
				column.Name, shortCol)
		case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64, qvalue.QValueKindArrayInt16,
			qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64, qvalue.QValueKindArrayString,
			qvalue.QValueKindArrayBoolean, qvalue.QValueKindArrayTimestamp, qvalue.QValueKindArrayTimestampTZ,
			qvalue.QValueKindArrayDate, qvalue.QValueKindArrayUUID:
			castStmt = fmt.Sprintf("ARRAY(SELECT CAST(element AS %s) FROM "+
				"UNNEST(CAST(JSON_VALUE_ARRAY(_peerdb_data, '$.%s') AS ARRAY<STRING>)) AS element WHERE element IS NOT null) AS `%s`",
				bqTypeString, column.Name, shortCol)
		case qvalue.QValueKindGeography, qvalue.QValueKindGeometry, qvalue.QValueKindPoint:
			castStmt = fmt.Sprintf("CAST(ST_GEOGFROMTEXT(JSON_VALUE(_peerdb_data, '$.%s')) AS %s) AS `%s`",
				column.Name, bqTypeString, shortCol)
		// MAKE_INTERVAL(years INT64, months INT64, days INT64, hours INT64, minutes INT64, seconds INT64)
		// Expecting interval to be in the format of {"Microseconds":2000000,"Days":0,"Months":0,"Valid":true}
		// json.Marshal in SyncRecords for Postgres already does this - once new data-stores are added,
		// this needs to be handled again
		// TODO add interval types again
		// case model.ColumnTypeInterval:
		// castStmt = fmt.Sprintf("MAKE_INTERVAL(0,CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Months') AS INT64),"+
		// 	"CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Days') AS INT64),0,0,"+
		// 	"CAST(CAST(JSON_EXTRACT_SCALAR(_peerdb_data, '$.%s.Microseconds') AS INT64)/1000000 AS  INT64)) AS %s",
		// 	column.Name, column.Name, column.Name, column.Name)
		// TODO add proper granularity for time types, then restore this
		// case model.ColumnTypeTime:
		// 	castStmt = fmt.Sprintf("time(timestamp_micros(CAST(JSON_EXTRACT(_peerdb_data, '$.%s.Microseconds')"+
		// 		" AS int64))) AS %s",
		// 		column.Name, column.Name)
		default:
			castStmt = fmt.Sprintf("CAST(JSON_VALUE(_peerdb_data, '$.%s') AS %s) AS `%s`",
				column.Name, bqTypeString, shortCol)
		}
		flattenedProjs = append(flattenedProjs, castStmt)
	}
	flattenedProjs = append(
		flattenedProjs,
		"_peerdb_timestamp",
		"_peerdb_record_type AS _rt",
		"_peerdb_unchanged_toast_columns AS _ut",
	)

	// normalize anything between last normalized batch id to last sync batchid
	return fmt.Sprintf("WITH _f AS "+
		"(SELECT %s FROM `%s` WHERE _peerdb_batch_id=%d AND "+
		"_peerdb_destination_table_name='%s')",
		strings.Join(flattenedProjs, ","), m.rawDatasetTable.string(), m.mergeBatchId, dstTable)
}

// This function is to support datatypes like JSON which cannot be partitioned by or compared by BigQuery
func (m *mergeStmtGenerator) transformedPkeyStrings(normalizedTableSchema *protos.TableSchema, forPartition bool) []string {
	pkeys := make([]string, 0, len(normalizedTableSchema.PrimaryKeyColumns))
	columnNameTypeMap := make(map[string]qvalue.QValueKind, len(normalizedTableSchema.Columns))
	for _, col := range normalizedTableSchema.Columns {
		columnNameTypeMap[col.Name] = qvalue.QValueKind(col.Type)
	}

	for _, pkeyCol := range normalizedTableSchema.PrimaryKeyColumns {
		pkeyColType, ok := columnNameTypeMap[pkeyCol]
		if !ok {
			continue
		}
		switch pkeyColType {
		case qvalue.QValueKindJSON:
			if forPartition {
				pkeys = append(pkeys, fmt.Sprintf("TO_JSON_STRING(%s)", m.shortColumn[pkeyCol]))
			} else {
				pkeys = append(pkeys, fmt.Sprintf("TO_JSON_STRING(_t.`%s`)=TO_JSON_STRING(_d.%s)",
					pkeyCol, m.shortColumn[pkeyCol]))
			}
		case qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
			if forPartition {
				pkeys = append(pkeys, fmt.Sprintf("CAST(%s as STRING)", m.shortColumn[pkeyCol]))
			} else {
				pkeys = append(pkeys, fmt.Sprintf("_t.`%s`=_d.%s", pkeyCol, m.shortColumn[pkeyCol]))
			}
		default:
			if forPartition {
				pkeys = append(pkeys, m.shortColumn[pkeyCol])
			} else {
				pkeys = append(pkeys, fmt.Sprintf("_t.`%s`=_d.%s", pkeyCol, m.shortColumn[pkeyCol]))
			}
		}
	}
	return pkeys
}

// generateDeDupedCTE generates a de-duped CTE.
func (m *mergeStmtGenerator) generateDeDupedCTE(normalizedTableSchema *protos.TableSchema) string {
	const cte = `_dd AS (
		SELECT _peerdb_ranked.* FROM(
				SELECT RANK() OVER(
					PARTITION BY %s ORDER BY _peerdb_timestamp DESC
				) AS _peerdb_rank,* FROM _f
			) _peerdb_ranked
			WHERE _peerdb_rank=1
	) SELECT * FROM _dd`

	shortPkeys := m.transformedPkeyStrings(normalizedTableSchema, true)
	pkeyColsStr := strings.Join(shortPkeys, ",")
	return fmt.Sprintf(cte, pkeyColsStr)
}

// generateMergeStmt generates a merge statement.
func (m *mergeStmtGenerator) generateMergeStmt(dstTable string, dstDatasetTable datasetTable, unchangedToastColumns []string) string {
	normalizedTableSchema := m.tableSchemaMapping[dstTable]
	// comma separated list of column names
	columnCount := len(normalizedTableSchema.Columns)
	backtickColNames := make([]string, 0, columnCount)
	shortBacktickColNames := make([]string, 0, columnCount)
	pureColNames := make([]string, 0, columnCount)
	for i, col := range normalizedTableSchema.Columns {
		shortCol := fmt.Sprintf("_c%d", i)
		m.shortColumn[col.Name] = shortCol
		backtickColNames = append(backtickColNames, fmt.Sprintf("`%s`", col.Name))
		shortBacktickColNames = append(shortBacktickColNames, fmt.Sprintf("`%s`", shortCol))
		pureColNames = append(pureColNames, col.Name)
	}
	insertColumnsSQL := strings.Join(backtickColNames, ", ")
	insertValuesSQL := strings.Join(shortBacktickColNames, ", ")
	if m.peerdbCols.SyncedAtColName != "" {
		insertColumnsSQL += fmt.Sprintf(", `%s`", m.peerdbCols.SyncedAtColName)
		insertValuesSQL += ",CURRENT_TIMESTAMP"
	}

	updateStatementsforToastCols := m.generateUpdateStatements(pureColNames, unchangedToastColumns)
	if m.peerdbCols.SoftDeleteColName != "" {
		softDeleteInsertColumnsSQL := insertColumnsSQL + fmt.Sprintf(",`%s`", m.peerdbCols.SoftDeleteColName)
		softDeleteInsertValuesSQL := insertValuesSQL + ",TRUE"

		updateStatementsforToastCols = append(updateStatementsforToastCols,
			fmt.Sprintf("WHEN NOT MATCHED AND _d._rt=2 THEN INSERT (%s) VALUES(%s)",
				softDeleteInsertColumnsSQL, softDeleteInsertValuesSQL))
	}
	updateStringToastCols := strings.Join(updateStatementsforToastCols, " ")

	pkeySelectSQLArray := m.transformedPkeyStrings(normalizedTableSchema, false)
	// t.<pkey1> = d.<pkey1> AND t.<pkey2> = d.<pkey2> ...
	pkeySelectSQL := strings.Join(pkeySelectSQLArray, " AND ")

	deletePart := "DELETE"
	if m.peerdbCols.SoftDeleteColName != "" {
		colName := m.peerdbCols.SoftDeleteColName
		deletePart = fmt.Sprintf("UPDATE SET %s=TRUE", colName)
		if m.peerdbCols.SyncedAtColName != "" {
			deletePart = fmt.Sprintf("%s,%s=CURRENT_TIMESTAMP",
				deletePart, m.peerdbCols.SyncedAtColName)
		}
	}

	return fmt.Sprintf("MERGE `%s` _t USING(%s,%s) _d"+
		" ON %s WHEN NOT MATCHED AND _d._rt!=2 THEN "+
		"INSERT (%s) VALUES(%s) "+
		"%s WHEN MATCHED AND _d._rt=2 THEN %s;",
		dstDatasetTable.table, m.generateFlattenedCTE(dstTable, normalizedTableSchema), m.generateDeDupedCTE(normalizedTableSchema),
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
func (m *mergeStmtGenerator) generateUpdateStatements(allCols []string, unchangedToastColumns []string) []string {
	handleSoftDelete := m.peerdbCols.SoftDeleteColName != ""
	// weird way of doing it but avoids prealloc lint
	updateStmts := make([]string, 0, func() int {
		if handleSoftDelete {
			return 2 * len(unchangedToastColumns)
		}
		return len(unchangedToastColumns)
	}())

	for _, cols := range unchangedToastColumns {
		unchangedColsArray := strings.Split(cols, ",")
		otherCols := shared.ArrayMinus(allCols, unchangedColsArray)
		tmpArray := make([]string, 0, len(otherCols))
		for _, colName := range otherCols {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s`=_d.%s", colName, m.shortColumn[colName]))
		}

		// set the synced at column to the current timestamp
		if m.peerdbCols.SyncedAtColName != "" {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s`=CURRENT_TIMESTAMP",
				m.peerdbCols.SyncedAtColName))
		}
		// set soft-deleted to false, tackles insert after soft-delete
		if handleSoftDelete {
			tmpArray = append(tmpArray, fmt.Sprintf("`%s`=FALSE",
				m.peerdbCols.SoftDeleteColName))
		}

		ssep := strings.Join(tmpArray, ",")
		updateStmt := fmt.Sprintf(`WHEN MATCHED AND
		_rt!=2 AND _ut='%s'
		THEN UPDATE SET %s`, cols, ssep)
		updateStmts = append(updateStmts, updateStmt)

		// generates update statements for the case where updates and deletes happen in the same branch
		// the backfill has happened from the pull side already, so treat the DeleteRecord as an update
		// and then set soft-delete to true.
		if handleSoftDelete {
			tmpArray = append(tmpArray[:len(tmpArray)-1],
				fmt.Sprintf("`%s`=TRUE", m.peerdbCols.SoftDeleteColName))
			ssep := strings.Join(tmpArray, ",")
			updateStmt := fmt.Sprintf(`WHEN MATCHED AND
			_rt=2 AND _ut='%s'
			THEN UPDATE SET %s`, cols, ssep)
			updateStmts = append(updateStmts, updateStmt)
		}
	}
	return updateStmts
}
