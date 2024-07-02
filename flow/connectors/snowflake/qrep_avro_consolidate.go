package connsnowflake

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SnowflakeAvroConsolidateHandler struct {
	connector    *SnowflakeConnector
	config       *protos.QRepConfig
	dstTableName string
	stage        string
	allColNames  []string
	allColTypes  []string
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

func getTransformSQL(colNames []string, colTypes []string, syncedAtCol string, isDeletedCol string) (string, string) {
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

		if utils.IsUpper(avroColName) {
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
	)
	return fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) FILE_FORMAT=(TYPE=AVRO), PURGE=TRUE, ON_ERROR=CONTINUE",
		copyDstTable, columnsSQL, transformationSQL, s.stage)
}

func (s *SnowflakeAvroConsolidateHandler) handleAppendMode(ctx context.Context) error {
	parsedDstTable, _ := utils.ParseSchemaTable(s.dstTableName)
	copyCmd := s.getCopyTransformation(snowflakeSchemaTableNormalize(parsedDstTable))
	s.connector.logger.Info("running copy command: " + copyCmd)
	_, err := s.connector.database.ExecContext(ctx, copyCmd)
	if err != nil {
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
		quotedKey := utils.QuoteIdentifier(key)
		upsertKeys = append(upsertKeys, fmt.Sprintf("dst.%s = src.%s", quotedKey, quotedKey))
		partitionKeyCols = append(partitionKeyCols, quotedKey)
	}
	upsertKeyClause := strings.Join(upsertKeys, " AND ")

	updateSetClauses := make([]string, 0, len(s.allColNames))
	insertColumnsClauses := make([]string, 0, len(s.allColNames))
	insertValuesClauses := make([]string, 0, len(s.allColNames))
	for _, column := range s.allColNames {
		quotedColumn := utils.QuoteIdentifier(column)
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
	runID, err := shared.RandomUInt64()
	if err != nil {
		return fmt.Errorf("failed to generate run ID: %w", err)
	}

	tempTableName := fmt.Sprintf("%s_temp_%d", s.dstTableName, runID)

	//nolint:gosec
	createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
		tempTableName, s.dstTableName)
	if _, err := s.connector.database.ExecContext(ctx, createTempTableCmd); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	s.connector.logger.Info("created temp table " + tempTableName)

	copyCmd := s.getCopyTransformation(tempTableName)
	_, err = s.connector.database.ExecContext(ctx, copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	s.connector.logger.Info("copied file from stage " + s.stage + " to temp table " + tempTableName)

	mergeCmd := s.generateUpsertMergeCommand(tempTableName)

	startTime := time.Now()
	rows, err := s.connector.database.ExecContext(ctx, mergeCmd)
	if err != nil {
		return fmt.Errorf("failed to merge data into destination table '%s': %w", mergeCmd, err)
	}
	rowCount, err := rows.RowsAffected()
	if err == nil {
		totalRowsAtTarget, err := s.connector.getTableCounts(ctx, []string{s.dstTableName})
		if err != nil {
			return err
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
