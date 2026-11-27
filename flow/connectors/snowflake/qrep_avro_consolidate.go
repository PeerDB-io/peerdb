package connsnowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type SnowflakeAvroConsolidateHandler struct {
	connector    *SnowflakeConnector
	config       *protos.QRepConfig
	dstTableName string
	stage        string
	// avroFieldByCol maps a destination column to the exact field name in the staged Avro files
	avroFieldByCol map[string]string
	allColNames    []string
	allColTypes    []string
	// resolveAvroFields looks up the staged Avro field names instead of deriving them from the destination column names.
	// Only set for QRep snapshots, where the staged files hold the source column names.
	resolveAvroFields bool
}

// NewSnowflakeAvroConsolidateHandler creates a new SnowflakeAvroWriteHandler
func NewSnowflakeAvroConsolidateHandler(
	connector *SnowflakeConnector,
	config *protos.QRepConfig,
	dstTableName string,
	stage string,
) *SnowflakeAvroConsolidateHandler {
	return &SnowflakeAvroConsolidateHandler{
		connector:    connector,
		config:       config,
		dstTableName: dstTableName,
		stage:        stage,
	}
}

func (s *SnowflakeAvroConsolidateHandler) CopyStageToDestination(ctx context.Context) error {
	s.connector.logger.Info("Copying stage to destination " + s.dstTableName)

	columns, colsErr := s.connector.getColsFromTable(ctx, s.dstTableName)
	if colsErr != nil {
		return fmt.Errorf("failed to get columns from destination table: %w", colsErr)
	}

	colNames := make([]string, 0, len(columns))
	colTypes := make([]string, 0, len(columns))
	for _, col := range columns {
		colNames = append(colNames, col.ColumnName)
		colTypes = append(colTypes, col.ColumnType)
	}
	s.allColNames = colNames
	s.allColTypes = colTypes

	if s.resolveAvroFields {
		if err := s.resolveAvroFieldNames(ctx); err != nil {
			return err
		}
	}

	appendMode := true
	if s.config.WriteMode != nil {
		writeType := s.config.WriteMode.WriteType
		if writeType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
			appendMode = false
		}
	}

	if appendMode {
		err := s.handleAppendMode(ctx)
		if err != nil {
			return fmt.Errorf("failed to handle append mode: %w", err)
		}
	} else {
		err := s.handleUpsertMode(ctx)
		if err != nil {
			return fmt.Errorf("failed to handle upsert mode: %w", err)
		}
	}

	return nil
}

// matchAvroFieldNames maps destination columns to the Avro field holding their values.
// An exact match wins; otherwise a case-insensitive match is used, because Snowflake can store a column
// with different case than the source name (e.g. QUOTED_IDENTIFIERS_IGNORE_CASE stores "partProjectId" as PARTPROJECTID).
// Columns without any matching field are left out of the result.
// A column which matches several fields case-insensitively and none exactly is an error rather than a guess.
func matchAvroFieldNames(colNames []string, avroFields []string) (map[string]string, error) {
	matched := make(map[string]string, len(colNames))
	for _, col := range colNames {
		var foldMatches []string
		exact := false
		for _, field := range avroFields {
			if field == col {
				exact = true
				break
			}
			if strings.EqualFold(field, col) {
				foldMatches = append(foldMatches, field)
			}
		}
		switch {
		case exact:
			matched[col] = col
		case len(foldMatches) == 1:
			matched[col] = foldMatches[0]
		case len(foldMatches) > 1:
			return nil, fmt.Errorf("destination column %q matches multiple Avro fields case-insensitively: %q",
				col, foldMatches)
		}
	}
	return matched, nil
}

// getAvroFieldNames reads the field names of the first record in the stage.
// All files of a QRep run come from the same source query, so they share one schema.
func (s *SnowflakeAvroConsolidateHandler) getAvroFieldNames(ctx context.Context) ([]string, error) {
	var fieldsJSON sql.NullString
	err := s.connector.QueryRowContext(ctx, "SELECT OBJECT_KEYS($1) FROM @"+s.stage+" LIMIT 1").Scan(&fieldsJSON)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !fieldsJSON.Valid) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to read Avro field names from stage: %w", err)
	}
	var fields []string
	if err := json.Unmarshal([]byte(fieldsJSON.String), &fields); err != nil {
		return nil, fmt.Errorf("failed to parse Avro field names from stage: %w", err)
	}
	return fields, nil
}

func (s *SnowflakeAvroConsolidateHandler) resolveAvroFieldNames(ctx context.Context) error {
	avroFields, err := s.getAvroFieldNames(ctx)
	if err != nil {
		// without the field names the COPY uses the destination column names, as before this lookup existed
		s.connector.logger.Warn("could not read Avro field names from stage, matching on destination column names",
			slog.Any("error", err))
		return nil
	}

	colNames := make([]string, 0, len(s.allColNames))
	for _, col := range s.allColNames {
		if col != s.config.SyncedAtColName && col != s.config.SoftDeleteColName {
			colNames = append(colNames, col)
		}
	}
	s.avroFieldByCol, err = matchAvroFieldNames(colNames, avroFields)
	return err
}

// getTransformSQL builds the COPY transformation. avroFieldByCol, when it has an entry for a column,
// gives the exact Avro field name for it; other columns derive the name from the destination column as before.
func getTransformSQL(
	colNames []string, colTypes []string, syncedAtCol string, isDeletedCol string, avroFieldByCol map[string]string,
) (string, string) {
	transformations := make([]string, 0, len(colNames))
	columnOrder := make([]string, 0, len(colNames))
	for idx, avroColName := range colNames {
		colType := colTypes[idx]
		normalizedColName := SnowflakeIdentifierNormalize(avroColName)
		columnOrder = append(columnOrder, normalizedColName)
		if avroColName == syncedAtCol {
			transformations = append(transformations, "CURRENT_TIMESTAMP AS "+normalizedColName)
			continue
		}

		if avroColName == isDeletedCol {
			transformations = append(transformations, "FALSE AS "+normalizedColName)
			continue
		}

		if avroField, ok := avroFieldByCol[avroColName]; ok {
			avroColName = avroField
		} else if utils.IsUpper(avroColName) {
			avroColName = strings.ToLower(avroColName)
		}
		// Avro files are written with lowercase in mind, so don't normalize it like everything else
		switch colType {
		case "GEOGRAPHY":
			transformations = append(transformations,
				fmt.Sprintf("TO_GEOGRAPHY($1:\"%s\"::string, true) AS %s", avroColName, normalizedColName))
		case "GEOMETRY":
			transformations = append(transformations,
				fmt.Sprintf("TO_GEOMETRY($1:\"%s\"::string, true) AS %s", avroColName, normalizedColName))
		case "NUMBER":
			transformations = append(transformations,
				fmt.Sprintf("$1:\"%s\" AS %s", avroColName, normalizedColName))
		case "DATE":
			transformations = append(transformations,
				fmt.Sprintf("TO_DATE($1:\"%s\") AS %s", avroColName, normalizedColName))
		case "TIME":
			transformations = append(transformations,
				fmt.Sprintf("TO_TIME(SPLIT($1:\"%s\",'+')[0]) AS %s", avroColName, normalizedColName))
		case "VARIANT":
			transformations = append(transformations,
				fmt.Sprintf("PARSE_JSON($1:\"%s\") AS %s", avroColName, normalizedColName))

		default:
			transformations = append(transformations,
				fmt.Sprintf("($1:\"%s\")::%s AS %s", avroColName, colType, normalizedColName))
		}
	}
	transformationSQL := strings.Join(transformations, ",")
	columnsSQL := strings.Join(columnOrder, ",")

	return transformationSQL, columnsSQL
}

// copy to either the actual destination table or a tempTable
func (s *SnowflakeAvroConsolidateHandler) getCopyTransformation(copyDstTable string) string {
	transformationSQL, columnsSQL := getTransformSQL(
		s.allColNames,
		s.allColTypes,
		s.config.SyncedAtColName,
		s.config.SoftDeleteColName,
		s.avroFieldByCol,
	)
	onErrorStr := ""
	if onError := internal.GetEnvString("PEERDB_SNOWFLAKE_ON_ERROR", ""); onError != "" {
		onErrorStr = ", ON_ERROR=" + onError
	}
	return fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) FILE_FORMAT=(TYPE=AVRO), PURGE=TRUE%s",
		copyDstTable, columnsSQL, transformationSQL, s.stage, onErrorStr)
}

func (s *SnowflakeAvroConsolidateHandler) handleAppendMode(ctx context.Context) error {
	parsedDstTable, _ := common.ParseTableIdentifier(s.dstTableName)
	copyCmd := s.getCopyTransformation(snowflakeSchemaTableNormalize(parsedDstTable))
	s.connector.logger.Info("running copy command: " + copyCmd)
	if _, err := s.connector.ExecContext(ctx, copyCmd); err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}

	s.connector.logger.Info("copied file from stage " + s.stage + " to table " + s.dstTableName)
	return nil
}

func (s *SnowflakeAvroConsolidateHandler) generateUpsertMergeCommand(
	tempTableName string,
) string {
	upsertKeyCols := s.config.WriteMode.UpsertKeyColumns
	// all cols are acquired from snowflake schema, so let us try to make upsert key cols match the case
	// and also the watermark col, then the quoting should be fine
	caseMatchedCols := make(map[string]string, len(s.allColNames))
	for _, col := range s.allColNames {
		caseMatchedCols[strings.ToLower(col)] = col
	}

	for i, col := range upsertKeyCols {
		upsertKeyCols[i] = caseMatchedCols[strings.ToLower(col)]
	}

	upsertKeys := make([]string, 0, len(upsertKeyCols))
	partitionKeyCols := make([]string, 0, len(upsertKeyCols))
	for _, key := range upsertKeyCols {
		quotedKey := common.QuoteIdentifier(key)
		upsertKeys = append(upsertKeys, fmt.Sprintf("dst.%s = src.%s", quotedKey, quotedKey))
		partitionKeyCols = append(partitionKeyCols, quotedKey)
	}
	upsertKeyClause := strings.Join(upsertKeys, " AND ")

	updateSetClauses := make([]string, 0, len(s.allColNames))
	insertColumnsClauses := make([]string, 0, len(s.allColNames))
	insertValuesClauses := make([]string, 0, len(s.allColNames))
	for _, column := range s.allColNames {
		quotedColumn := common.QuoteIdentifier(column)
		updateSetClauses = append(updateSetClauses, fmt.Sprintf("%s = src.%s", quotedColumn, quotedColumn))
		insertColumnsClauses = append(insertColumnsClauses, quotedColumn)
		insertValuesClauses = append(insertValuesClauses, "src."+quotedColumn)
	}
	updateSetClause := strings.Join(updateSetClauses, ", ")
	insertColumnsClause := strings.Join(insertColumnsClauses, ", ")
	insertValuesClause := strings.Join(insertValuesClauses, ", ")
	selectCmd := fmt.Sprintf(`
		SELECT *
		FROM %s
		QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) = 1
	`, tempTableName, strings.Join(partitionKeyCols, ","), partitionKeyCols[0])

	mergeCmd := fmt.Sprintf(`
			MERGE INTO %s dst
			USING (%s) src
			ON %s
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
		`, s.dstTableName, selectCmd, upsertKeyClause,
		updateSetClause, insertColumnsClause, insertValuesClause)

	return mergeCmd
}

// handleUpsertMode handles the upsert mode
func (s *SnowflakeAvroConsolidateHandler) handleUpsertMode(ctx context.Context) error {
	//nolint:gosec // number has no cryptographic significance
	runID := rand.Uint64()

	tempTableName := fmt.Sprintf("%s_temp_%d", s.dstTableName, runID)

	createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
		tempTableName, s.dstTableName)
	if _, err := s.connector.ExecContext(ctx, createTempTableCmd); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	s.connector.logger.Info("created temp table " + tempTableName)

	copyCmd := s.getCopyTransformation(tempTableName)

	if _, err := s.connector.ExecContext(ctx, copyCmd); err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	s.connector.logger.Info("copied file from stage " + s.stage + " to temp table " + tempTableName)

	mergeCmd := s.generateUpsertMergeCommand(tempTableName)

	startTime := time.Now()
	rows, err := s.connector.ExecContext(ctx, mergeCmd)
	if err != nil {
		return fmt.Errorf("failed to merge data into destination table '%s': %w", mergeCmd, err)
	}
	rowCount, err := rows.RowsAffected()
	if err == nil {
		var totalRowsAtTarget int64
		if err := s.connector.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.dstTableName).Scan(&totalRowsAtTarget); err != nil {
			return fmt.Errorf("failed to get count for table %s: %w", s.dstTableName, err)
		}
		s.connector.logger.Info(fmt.Sprintf("merged %d rows into destination table %s, total rows at target: %d",
			rowCount, s.dstTableName, totalRowsAtTarget))
	} else {
		s.connector.logger.Error("failed to get rows affected", slog.Any("error", err))
	}

	s.connector.logger.Info(fmt.Sprintf("merged data from temp table %s into destination table %s, time taken %v",
		tempTableName, s.dstTableName, time.Since(startTime)))
	return nil
}
