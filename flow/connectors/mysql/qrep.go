package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MySqlConnector) tableRowEstimate(ctx context.Context, schema string, table string) (int64, error) {
	rs, err := c.Execute(ctx, fmt.Sprintf("select table_rows from information_schema.tables where table_schema='%s' and table_name='%s'",
		mysql.Escape(schema), mysql.Escape(table)))
	if err != nil {
		return 0, fmt.Errorf("failed to query information schema for row count estimate: %w", err)
	}
	defer rs.Close()
	return rs.GetInt(0, 0)
}

func (c *MySqlConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		// if no watermark column is specified, return a single partition
		return []*protos.QRepPartition{
			{
				PartitionId:        utils.FullTablePartitionID,
				Range:              nil,
				FullTablePartition: true,
			},
		}, nil
	}

	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, fmt.Errorf("num rows per partition must be greater than 0")
	}

	numPartitions := int64(config.NumPartitionsOverride)
	numRowsPerPartition := int64(config.NumRowsPerPartition)

	parsedWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	minmaxQuery := fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`) FROM %[1]s",
		parsedWatermarkTable.MySQL(), config.WatermarkColumn)
	var minmaxHasCount bool
	if last != nil && last.Range != nil && numPartitions == 0 {
		// we resume replication from the last partition, we need to include count of the remaining rows in the query
		minmaxHasCount = true
		minmaxQuery = fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`),COUNT(*) FROM %[1]s",
			parsedWatermarkTable.MySQL(), config.WatermarkColumn)
	} else if numPartitions == 0 {
		// we are starting replication from the beginning and need to estimate approximate rows count to calculate partitions
		totalRows, err := c.tableRowEstimate(ctx, parsedWatermarkTable.Namespace, parsedWatermarkTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}

		if totalRows == 0 {
			c.logger.Warn("estimating no records to replicate, only using 1 partition")
			numPartitions = 1
		} else {
			// Calculate the number of partitions
			adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
			c.logger.Info("[mysql] partition details",
				slog.Int64("totalRowsEstimate", totalRows),
				slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
				slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
				slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

			numPartitions = adjustedPartitions.AdjustedNumPartitions
		}
	}

	var rs *mysql.Result
	if last != nil && last.Range != nil {
		var minVal string
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = strconv.FormatInt(lastRange.IntRange.End, 10)
		case *protos.PartitionRange_UintRange:
			minVal = strconv.FormatUint(lastRange.UintRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			time := lastRange.TimestampRange.End.AsTime()
			minVal = "'" + time.Format("2006-01-02 15:04:05.999999") + "'"
		case *protos.PartitionRange_NullRange:
			// this case should never happen because we only add null partition for InitialCopyOnly replication
			// (so there shouldn't be a resume scenario with null range)
			return nil, errors.New("unexpected null range in last partition after resuming QRep")
		}

		minmaxQuery = fmt.Sprintf("%s WHERE `%s` > %s", minmaxQuery, config.WatermarkColumn, minVal)
	}
	c.logger.Info("querying min/max", slog.String("query", minmaxQuery))
	rs, err = c.Execute(ctx, minmaxQuery)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	if minmaxHasCount {
		totalRows, err := rs.GetInt(0, 2)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}

		if totalRows == 0 {
			c.logger.Warn("no records to replicate, returning")
			return make([]*protos.QRepPartition, 0), nil
		}

		// Calculate the number of partitions
		adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
		c.logger.Info("[mysql] partition details",
			slog.Int64("totalRows", totalRows),
			slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
			slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
			slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))

		numPartitions = adjustedPartitions.AdjustedNumPartitions
	}

	watermarkField := rs.Fields[1]
	watermarkMyType := watermarkField.Type
	watermarkUnsigned := (watermarkField.Flag & mysql.UNSIGNED_FLAG) != 0
	watermarkQKind, err := qkindFromMysqlType(watermarkField.Type, watermarkUnsigned, watermarkField.Charset)
	if err != nil {
		return nil, fmt.Errorf("failed to convert mysql type to qvaluekind: %w", err)
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	val1, err := QValueFromMysqlFieldValue(watermarkQKind, watermarkMyType, rs.Values[0][0])
	if err != nil {
		return nil, fmt.Errorf("failed to convert partition minimum to qvalue: %w", err)
	}
	val2, err := QValueFromMysqlFieldValue(watermarkQKind, watermarkMyType, rs.Values[0][1])
	if err != nil {
		return nil, fmt.Errorf("failed to convert partition maximum to qvalue: %w", err)
	}
	if err := partitionHelper.AddPartitionsWithRange(val1.Value(), val2.Value(), numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}

	// add null values partition to the end, if nulls aren't present it will be an empty partition
	// that gets skipped during replication
	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}

	return partitionHelper.GetPartitions(), nil
}

// supportsRangePartition reports whether a primary key column with this
// information_schema DATA_TYPE can drive range-partitioned snapshots through
// PartitionHelper.AddPartitionsWithRange. Integer and temporal types only; other
// types fall back to a full table snapshot. Low-cardinality columns are still
// accepted, as it would be no worse than a single full table snapshot.
func supportsRangePartition(dataType string) bool {
	switch strings.ToLower(dataType) {
	// integer types
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year",
		return true
	// temporal types
	case "date", "datetime", "timestamp":
		return true
	case "varchar":
	default:
		return false
	}
}

// mysqlQuotedList renders values as a comma-separated list of escaped string literals
// for IN clauses. Literals are used instead of placeholders because go-mysql args switch
// to the binary prepared-statement protocol, which MySQL-compatible frontends and
// proxies often support poorly.
func mysqlQuotedList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = "'" + mysql.Escape(value) + "'"
	}
	return strings.Join(quoted, ",")
}

// GetDefaultPartitionKeyForTables returns the first primary key column for each table
// whose type supports range partitioning. Tables without a primary key or have type that
// does not support range partition fall back to full table partitions.
func (c *MySqlConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	c.logger.Info("Evaluating if tables can perform parallel load")

	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}

	if len(input.TableMappings) == 0 {
		c.logger.Warn("no table mappings found")
		return output, nil
	}

	type tableKey struct {
		schema string
		table  string
	}
	sourcesByTable := make(map[tableKey][]string, len(input.TableMappings))
	var schemas, tables []string
	for _, tm := range input.TableMappings {
		parsed, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("failed to parse source table %s: %w", tm.SourceTableIdentifier, err)
		}
		key := tableKey{schema: parsed.Namespace, table: parsed.Table}
		if _, ok := sourcesByTable[key]; !ok {
			schemas = append(schemas, parsed.Namespace)
			tables = append(tables, parsed.Table)
		}
		sourcesByTable[key] = append(sourcesByTable[key], tm.SourceTableIdentifier)
	}

	// using cross-product of schema/table is safe here because information_schema can be
	// case-insensitive in MySQL (when lower_case_table_names = 1 in MySQL, and always the
	// case in MariaDB). We will filter in Go code below to skip irrelevant ones.
	tableFilter := fmt.Sprintf("TABLE_SCHEMA IN (%s) AND TABLE_NAME IN (%s)",
		mysqlQuotedList(schemas), mysqlQuotedList(tables))

	pkRows, err := c.Execute(ctx,
		`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX
				FROM information_schema.STATISTICS
				WHERE INDEX_NAME = 'PRIMARY' AND `+tableFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.statistics for primary keys: %w", err)
	}
	defer pkRows.Close()
	firstPKColumn := make(map[tableKey]string)
	pkColumnCount := make(map[tableKey]int)
	for i := range pkRows.Values {
		schema, err := pkRows.GetString(i, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key schema: %w", err)
		}
		table, err := pkRows.GetString(i, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key table: %w", err)
		}
		column, err := pkRows.GetString(i, 2)
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key column: %w", err)
		}
		seqInIndex, err := pkRows.GetInt(i, 3)
		if err != nil {
			return nil, fmt.Errorf("failed to read primary key column position: %w", err)
		}
		key := tableKey{schema: schema, table: table}
		if _, relevant := sourcesByTable[key]; !relevant {
			continue
		}
		pkColumnCount[key]++
		if seqInIndex == 1 {
			firstPKColumn[key] = column
		}
	}

	typeRows, err := c.Execute(ctx,
		`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
				FROM information_schema.COLUMNS
				WHERE `+tableFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.columns for primary key types: %w", err)
	}
	defer typeRows.Close()
	type columnKey struct {
		tableKey
		column string
	}
	dataTypes := make(map[columnKey]string)
	for i := range typeRows.Values {
		schema, err := typeRows.GetString(i, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to read column schema: %w", err)
		}
		table, err := typeRows.GetString(i, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to read column table: %w", err)
		}
		column, err := typeRows.GetString(i, 2)
		if err != nil {
			return nil, fmt.Errorf("failed to read column name: %w", err)
		}
		dataType, err := typeRows.GetString(i, 3)
		if err != nil {
			return nil, fmt.Errorf("failed to read column type: %w", err)
		}
		dataTypes[columnKey{tableKey{schema: schema, table: table}, column}] = dataType
	}

	for key, sources := range sourcesByTable {
		tableName := key.schema + "." + key.table
		pkColumn, hasPK := firstPKColumn[key]
		if !hasPK {
			c.logger.Info("[mysql] table has no primary key, defaulting to full table snapshot",
				slog.String("table", tableName))
			continue
		}
		dataType := dataTypes[columnKey{key, pkColumn}]
		if !supportsRangePartition(dataType) {
			c.logger.Info("[mysql] primary key type does not support range partitioning, defaulting to full table snapshot",
				slog.String("table", tableName),
				slog.String("column", pkColumn),
				slog.String("dataType", dataType))
			continue
		}
		c.logger.Info("[mysql] using primary key as default partition key",
			slog.String("table", tableName),
			slog.String("column", pkColumn),
			slog.Bool("compositeKey", pkColumnCount[key] > 1))
		for _, source := range sources {
			output.TableDefaultPartitionKeyMapping[source] = pkColumn
		}
	}
	return output, nil
}

func buildSelectedColumns(cols []*protos.FieldDescription, exclude []string) string {
	columns := []string{}
	selectAsterisk := true
	for _, col := range cols {
		if slices.Contains(exclude, col.Name) {
			selectAsterisk = false
			continue
		}

		converted := common.QuoteMySQLIdentifier(col.Name)
		if col.Type == string(types.QValueKindUint16Enum) {
			converted = fmt.Sprintf("CAST(%s AS UNSIGNED) AS %s", converted, converted)
			selectAsterisk = false
		}
		columns = append(columns, converted)
	}

	selectedColumns := "*"
	if !selectAsterisk {
		selectedColumns = strings.Join(columns, ", ")
	}

	return selectedColumns
}

func (c *MySqlConnector) PullQRepRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	tableSchema, err := c.getTableSchemaForTable(ctx, config.Env,
		&protos.TableMapping{SourceTableIdentifier: config.WatermarkTable}, protos.TypeSystem_Q,
		config.Version)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}

	selectedColumns := buildSelectedColumns(tableSchema.Columns, config.Exclude)
	parsedSrcTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		c.logger.Error("unable to parse source table", slog.Any("error", err))
		return 0, 0, fmt.Errorf("unable to parse source table: %w", err)
	}

	c.logger.Info("[mysql] pulling records start")

	c.totalBytesRead.Store(0)
	c.deltaBytesRead.Store(0)
	totalRecords := int64(0)
	onResult := func(rs *mysql.Result) error {
		schema, err := QRecordSchemaFromMysqlFields(tableSchema, rs.Fields)
		if err != nil {
			return err
		}
		stream.SetSchema(schema)
		return nil
	}
	var rs mysql.Result
	onRow := func(row []mysql.FieldValue) error {
		totalRecords += 1
		schema, err := stream.Schema()
		if err != nil {
			return err
		}
		record := make([]types.QValue, 0, len(row))
		for idx, val := range row {
			qv, err := QValueFromMysqlFieldValue(schema.Fields[idx].Type, rs.Fields[idx].Type, val)
			if err != nil {
				return fmt.Errorf("could not convert mysql value for %s: %w", schema.Fields[idx].Name, err)
			}
			record = append(record, qv)
		}

		if err := stream.Send(ctx, record); err != nil {
			return fmt.Errorf("failed to send record to stream: %w", err)
		}

		if totalRecords%50000 == 0 {
			c.logger.Info("[mysql] pulling records",
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", c.totalBytesRead.Load()),
				slog.Int("channelLen", len(stream.Records)))
		}

		return nil
	}

	shutDown := common.Interval(ctx, time.Minute, func() {
		read := c.deltaBytesRead.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
	})
	defer shutDown()

	if partition.FullTablePartition {
		query := config.Query
		if query == "" {
			query = fmt.Sprintf("SELECT %s FROM %s", selectedColumns, parsedSrcTable.MySQL())
		}

		if err := c.ExecuteSelectStreaming(ctx, query, &rs, onRow, onResult); err != nil {
			return 0, 0, err
		}
	} else {
		var rangeStart string
		var rangeEnd string

		queryTemplate := config.Query
		if queryTemplate == "" {
			queryTemplate = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
				selectedColumns, parsedSrcTable.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn))
		}
		// Depending on the type of the range, convert the range into the correct type
		switch x := partition.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			rangeStart = strconv.FormatInt(x.IntRange.Start, 10)
			rangeEnd = strconv.FormatInt(x.IntRange.End, 10)
		case *protos.PartitionRange_UintRange:
			rangeStart = strconv.FormatUint(x.UintRange.Start, 10)
			rangeEnd = strconv.FormatUint(x.UintRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			rangeStart = "'" + x.TimestampRange.Start.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
			rangeEnd = "'" + x.TimestampRange.End.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
		case *protos.PartitionRange_NullRange:
			if config.Query != "" {
				return 0, 0, errors.New("can't construct a null range partition for custom queries")
			}
			queryTemplate = fmt.Sprintf(
				"SELECT %s FROM %s WHERE %s IS NULL",
				selectedColumns, parsedSrcTable.MySQL(), common.QuoteMySQLIdentifier(config.WatermarkColumn),
			)
		default:
			return 0, 0, fmt.Errorf("unknown range type: %v", x)
		}

		templateParams := map[string]string{}
		if rangeStart != "" && rangeEnd != "" {
			templateParams["start"] = rangeStart
			templateParams["end"] = rangeEnd
		}

		// Build the query to pull records within the range from the source table
		// Be sure to order the results by the watermark column to ensure consistency across pulls
		query, err := utils.ExecuteTemplate(queryTemplate, templateParams)
		if err != nil {
			return 0, 0, err
		}

		if err := c.ExecuteSelectStreaming(ctx, query, &rs, onRow, onResult); err != nil {
			return 0, 0, err
		}
	}

	c.logger.Info("[mysql] pulled records",
		slog.Int64("records", totalRecords),
		slog.Int64("bytes", c.totalBytesRead.Load()),
		slog.Int("channelLen", len(stream.Records)))
	return totalRecords, c.deltaBytesRead.Swap(0), nil
}
