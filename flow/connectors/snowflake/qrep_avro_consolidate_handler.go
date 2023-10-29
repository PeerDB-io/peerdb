package connsnowflake

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
	log "github.com/sirupsen/logrus"
)

type SnowflakeAvroConsolidateHandler struct {
	connector    *SnowflakeConnector
	dstTableName string
	stage        string
	copyOpts     []string
	colInfo      *model.ColumnInformation
}

// NewSnowflakeAvroConsolidateHandler creates a new SnowflakeAvroWriteHandler
func NewSnowflakeAvroConsolidateHandler(
	connector *SnowflakeConnector,
	dstTableName string,
	stage string,
	copyOpts []string,
	colInfo *model.ColumnInformation,
) *SnowflakeAvroConsolidateHandler {
	return &SnowflakeAvroConsolidateHandler{
		connector:    connector,
		dstTableName: dstTableName,
		stage:        stage,
		copyOpts:     copyOpts,
		colInfo:      colInfo,
	}
}

func (s *SnowflakeAvroConsolidateHandler) generateCopyTransformation() *CopyInfo {
	var transformations []string
	var columnOrder []string
	for avroColName, colType := range s.colInfo.ColumnMap {
		if avroColName == "_PEERDB_IS_DELETED" {
			continue
		}
		if strings.ToUpper(avroColName) == avroColName {
			avroColName = strings.ToLower(avroColName)
		}
		normalizedColName := snowflakeIdentifierNormalize(avroColName)
		columnOrder = append(columnOrder, normalizedColName)
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
		default:
			transformations = append(transformations,
				fmt.Sprintf("($1:\"%s\")::%s AS %s", avroColName, colType, normalizedColName))
		}
	}
	transformationSQL := strings.Join(transformations, ",")
	columnsSQL := strings.Join(columnOrder, ",")
	return &CopyInfo{transformationSQL, columnsSQL}
}

func (s *SnowflakeAvroConsolidateHandler) HandleAppendMode(
	flowJobName string) error {
	copyInfo := s.generateCopyTransformation()
	parsedDstTable, err := utils.ParseSchemaTable(s.dstTableName)
	if err != nil {
		return fmt.Errorf("failed to parse source table '%s'", s.dstTableName)
	}
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) %s",
		snowflakeSchemaTableNormalize(parsedDstTable), copyInfo.columnsSQL,
		copyInfo.transformationSQL, s.stage, strings.Join(s.copyOpts, ","))
	log.Infof("running copy command: %s", copyCmd)
	_, err = s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}

	log.Infof("copied file from stage %s to table %s", s.stage, s.dstTableName)
	return nil
}

// HandleUpsertMode handles the upsert mode
func (s *SnowflakeAvroConsolidateHandler) HandleUpsertMode(
	upsertKeyCols []string,
	watermarkCol string,
	flowJobName string,
) error {
	runID, err := util.RandomUInt64()
	if err != nil {
		return fmt.Errorf("failed to generate run ID: %w", err)
	}

	tempTableName := fmt.Sprintf("%s_temp_%d", s.dstTableName, runID)
	copyInfo := s.generateCopyTransformation()

	//nolint:gosec
	createTempTableCmd := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s LIMIT 0",
		tempTableName, s.dstTableName)
	if _, err := s.connector.database.Exec(createTempTableCmd); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("created temp table %s", tempTableName)

	parsedDstTable, err := utils.ParseSchemaTable(s.dstTableName)
	if err != nil {
		return fmt.Errorf("failed to parse source table '%s'", s.dstTableName)
	}
	//nolint:gosec
	copyCmd := fmt.Sprintf("COPY INTO %s(%s) FROM (SELECT %s FROM @%s) %s",
		tempTableName, copyInfo.columnsSQL, copyInfo.transformationSQL, s.stage, strings.Join(s.copyOpts, ","))
	_, err = s.connector.database.Exec(copyCmd)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO command: %w", err)
	}
	log.Infof("copied file from stage %s to temp table %s", s.stage, tempTableName)

	mergeCmd := s.generateUpsertMergeCommand(upsertKeyCols, tempTableName,
		snowflakeSchemaTableNormalize(parsedDstTable))

	_, err = s.connector.database.Exec(mergeCmd)
	if err != nil {
		return fmt.Errorf("failed to merge data into destination table '%s': %w", mergeCmd, err)
	}

	log.WithFields(log.Fields{
		"flowName": flowJobName,
	}).Infof("merged data from temp table %s into destination table %s",
		tempTableName, s.dstTableName)
	return nil
}

func (s *SnowflakeAvroConsolidateHandler) generateUpsertMergeCommand(
	upsertKeyCols []string,
	tempTableName string,
	dstTable string,
) string {
	// all cols are acquired from snowflake schema, so let us try to make upsert key cols match the case
	// and also the watermark col, then the quoting should be fine
	caseMatchedCols := map[string]string{}
	for _, col := range s.colInfo.Columns {
		caseMatchedCols[strings.ToLower(col)] = col
	}

	for i, col := range upsertKeyCols {
		upsertKeyCols[i] = caseMatchedCols[strings.ToLower(col)]
	}

	upsertKeys := []string{}
	partitionKeyCols := []string{}
	for _, key := range upsertKeyCols {
		quotedKey := utils.QuoteIdentifier(key)
		upsertKeys = append(upsertKeys, fmt.Sprintf("dst.%s = src.%s", quotedKey, quotedKey))
		partitionKeyCols = append(partitionKeyCols, quotedKey)
	}
	upsertKeyClause := strings.Join(upsertKeys, " AND ")

	updateSetClauses := []string{}
	insertColumnsClauses := []string{}
	insertValuesClauses := []string{}
	for _, column := range s.colInfo.Columns {
		quotedColumn := utils.QuoteIdentifier(column)
		updateSetClauses = append(updateSetClauses, fmt.Sprintf("%s = src.%s", quotedColumn, quotedColumn))
		insertColumnsClauses = append(insertColumnsClauses, quotedColumn)
		insertValuesClauses = append(insertValuesClauses, fmt.Sprintf("src.%s", quotedColumn))
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
		`, dstTable, selectCmd, upsertKeyClause,
		updateSetClause, insertColumnsClause, insertValuesClause)

	return mergeCmd
}

func copyStageToDestination(
	connector *SnowflakeConnector,
	config *protos.QRepConfig,
	dstTableName string,
	stage string,
	colInfo *model.ColumnInformation,
) error {
	log.WithFields(log.Fields{
		"flowName": config.FlowJobName,
	}).Infof("Copying stage to destination %s", dstTableName)
	copyOpts := []string{
		"FILE_FORMAT = (TYPE = AVRO)",
		"PURGE = TRUE",
		"ON_ERROR = 'CONTINUE'",
	}

	writeHandler := NewSnowflakeAvroConsolidateHandler(connector, dstTableName, stage, copyOpts, colInfo)

	appendMode := true
	if config.WriteMode != nil {
		writeType := config.WriteMode.WriteType
		if writeType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
			appendMode = false
		}
	}

	switch appendMode {
	case true:
		err := writeHandler.HandleAppendMode(config.FlowJobName)
		if err != nil {
			return fmt.Errorf("failed to handle append mode: %w", err)
		}

	case false:
		upsertKeyCols := config.WriteMode.UpsertKeyColumns
		err := writeHandler.HandleUpsertMode(upsertKeyCols, config.WatermarkColumn,
			config.FlowJobName)
		if err != nil {
			return fmt.Errorf("failed to handle upsert mode: %w", err)
		}
	}

	return nil
}
