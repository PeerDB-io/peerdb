package connmysql

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

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

	var minmaxQuery string
	var minmaxHasCount bool
	if last != nil && last.Range != nil {
		// partial query, append minVal later
		if numPartitions == 0 {
			minmaxHasCount = true
			minmaxQuery = fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`),COUNT(*) FROM %[1]s WHERE `%[2]s` > ",
				parsedWatermarkTable.MySQL(), config.WatermarkColumn)
		} else {
			minmaxQuery = fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`) FROM %[1]s WHERE `%[2]s` > ",
				parsedWatermarkTable.MySQL(), config.WatermarkColumn)
		}
	} else if numPartitions == 0 {
		minmaxQuery = fmt.Sprintf("SELECT MIN(`%[2]s`),MAX(`%[2]s`) FROM %[1]s",
			parsedWatermarkTable.MySQL(), config.WatermarkColumn)

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
		}

		c.logger.Info("querying min/max", slog.String("query", minmaxQuery), slog.String("minVal", minVal))
		rs, err = c.Execute(ctx, minmaxQuery+minVal)
	} else {
		c.logger.Info("querying min/max", slog.String("query", minmaxQuery))
		rs, err = c.Execute(ctx, minmaxQuery)
	}
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

	return partitionHelper.GetPartitions(), nil
}

func (c *MySqlConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}

// parseEnumOptions parses the column type string for an enum column and returns the possible options for the enum
func parseEnumOptions(columnType string) []string {
	// columnType typically looks like: enum('a','b','c')
	// but enum labels may contain commas and escaped quotes, so we cannot just strings.Split on ",".
	// Extract the contents between the first '(' and the last ')'.
	start := strings.Index(columnType, "(")
	end := strings.LastIndex(columnType, ")")
	if start == -1 || end == -1 || end <= start+1 {
		return nil
	}
	optionsStr := columnType[start+1 : end]
	var (
		options  []string
		current  bytes.Buffer
		inQuotes bool
		hadQuote bool
	)
	for i := 0; i < len(optionsStr); i++ {
		ch := optionsStr[i]
		// Handle backslash-escaped characters: treat the backslash and the next
		// character as literal content so they don't affect parsing (e.g., an
		// escaped comma or quote).
		if ch == '\\' {
			current.WriteByte(ch)
			if i+1 < len(optionsStr) {
				i++
				current.WriteByte(optionsStr[i])
			}
			continue
		}
		// Toggle quote state on unescaped single quotes, but don't include them
		// in the final option values.
		if ch == '\'' {
			inQuotes = !inQuotes
			hadQuote = true
			continue
		}
		// Comma outside of quotes separates enum options.
		if ch == ',' && !inQuotes {
			option := strings.TrimSpace(current.String())
			if option != "" || hadQuote {
				options = append(options, option)
			}
			current.Reset()
			hadQuote = false
			continue
		}
		current.WriteByte(ch)
	}

	// Flush the final option, if any.
	option := strings.TrimSpace(current.String())
	if option != "" || hadQuote {
		options = append(options, option)
	}

	return options
}

// GetEnumColumnsInfo queries the information schema to get the possible options for enum columns in the specified table and columns
func (c *MySqlConnector) GetEnumColumnsInfo(ctx context.Context, table string, cols []string) (map[string][]string, error) {
	if len(cols) == 0 {
		return make(map[string][]string), nil
	}

	qualifiedTable, err := common.ParseTableIdentifier(table)
	if err != nil {
		return nil, err
	}

	quotedColumns := make([]string, len(cols))
	for i, col := range cols {
		quotedColumns[i] = fmt.Sprintf("'%s'", mysql.Escape(col))
	}

	rs, err := c.Execute(ctx, fmt.Sprintf(`select column_name, column_type 
		from information_schema.columns where table_schema = '%s' and table_name = '%s' and column_name in (%s)`,
		mysql.Escape(qualifiedTable.Namespace), mysql.Escape(qualifiedTable.Table), strings.Join(quotedColumns, ",")))
	if err != nil {
		return nil, fmt.Errorf("could not query information schema for enum options: %w", err)
	}
	defer rs.Close()

	res := make(map[string][]string)
	for idx := range rs.RowNumber() {
		columnName, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}

		dataType, err := rs.GetString(idx, 1)
		if err != nil {
			return nil, err
		}
		res[columnName] = parseEnumOptions(dataType)
	}

	return res, nil
}

// mapQValue maps QValue for certain mysql versions and types.
// For example, for mysql versions that do not support binlog row metadata,
// we need to map enums to integers to align with how mysql streams enum values in the binlog.
func (c *MySqlConnector) mapQValue(
	qv types.QValue, field types.QField, enumMap map[string][]string, binlogMetadataSupported bool,
) (types.QValue, error) {
	switch qvTyped := qv.(type) {
	// for enum types, we need to map the string to it's index since mysql versions prior to 8.0 stream enum values as integers during CDC
	case types.QValueEnum:
		if binlogMetadataSupported {
			break
		}

		enumOptions, ok := enumMap[field.Name]
		if !ok {
			return nil, fmt.Errorf("could not find enum options for column %q", field.Name)
		}

		enumToInt := func(val string) (string, error) {
			// MySQL enum index 0 is the empty string (invalid/unset enum value)
			if val == "" {
				return "", nil
			}

			matchingEnumIdx := slices.Index(enumOptions, val)
			if matchingEnumIdx == -1 {
				return "", fmt.Errorf("could not find matching enum option for value %q in column %q", qv.Value(), field.Name)
			}

			// MySQL enum indexes are 1-based, so add 1 to the index
			return strconv.Itoa(matchingEnumIdx + 1), nil
		}

		intVal, err := enumToInt(qvTyped.Val)
		if err != nil {
			return nil, err
		}
		qv = types.QValueEnum{
			Val: intVal,
		}
	}

	return qv, nil
}

func (c *MySqlConnector) PullQRepRecords(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	tableSchema, err := c.getTableSchemaForTable(ctx, config.Env,
		&protos.TableMapping{SourceTableIdentifier: config.WatermarkTable}, protos.TypeSystem_Q)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}

	var enumColNames []string
	for _, col := range tableSchema.Columns {
		if col.Type == string(types.QValueKindEnum) {
			enumColNames = append(enumColNames, col.Name)
		}
	}

	enumMap, err := c.GetEnumColumnsInfo(ctx, config.WatermarkTable, enumColNames)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get enum column options: %w", err)
	}

	// Pre-compute before the streaming query — can't issue queries inside row callbacks
	binlogMetadataSupported, err := c.IsBinlogMetadataSupported(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to determine if binlog metadata is supported: %w", err)
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
			field := schema.Fields[idx]
			qv, err := QValueFromMysqlFieldValue(field.Type, rs.Fields[idx].Type, val)
			if err != nil {
				return fmt.Errorf("could not convert mysql value for %s: %w", field.Name, err)
			}

			mapped, err := c.mapQValue(qv, field, enumMap, binlogMetadataSupported)
			if err != nil {
				return fmt.Errorf("failed to map qvalue for column %s: %w", field.Name, err)
			}

			record = append(record, mapped)
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
		// this is a full table partition, so just run the query
		if err := c.ExecuteSelectStreaming(ctx, config.Query, &rs, onRow, onResult); err != nil {
			return 0, 0, err
		}
	} else {
		var rangeStart string
		var rangeEnd string

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
		default:
			return 0, 0, fmt.Errorf("unknown range type: %v", x)
		}

		// Build the query to pull records within the range from the source table
		// Be sure to order the results by the watermark column to ensure consistency across pulls
		query, err := BuildQuery(c.logger, config.Query, rangeStart, rangeEnd)
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

func BuildQuery(logger log.Logger, query string, start string, end string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, map[string]string{
		"start": start,
		"end":   end,
	}); err != nil {
		return "", err
	}
	res := buf.String()

	logger.Info("[mysql] templated query", slog.String("query", res))
	return res, nil
}
