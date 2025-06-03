package connmysql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"text/template"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	shared_mysql "github.com/PeerDB-io/peerdb/flow/shared/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MySqlConnector) GetDataTypeOfWatermarkColumn(
	ctx context.Context,
	watermarkTableName string,
	watermarkColumn string,
) (qvalue.QValueKind, error) {
	if watermarkColumn == "" {
		return "", fmt.Errorf("watermark column is not specified in the config")
	}

	query := fmt.Sprintf("SELECT `%s` FROM %s LIMIT 0", watermarkColumn, watermarkTableName)
	rs, err := c.Execute(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to execute query for watermark column type: %w", err)
	}

	if len(rs.Fields) == 0 {
		return "", errors.New("no fields returned from select query: " + query)
	}

	return qkindFromMysql(rs.Fields[0])
}

func (c *MySqlConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" {
		// if no watermark column is specified, return a single partition
		return []*protos.QRepPartition{
			{
				PartitionId:        shared_mysql.MYSQL_FULL_TABLE_PARTITION_ID,
				Range:              nil,
				FullTablePartition: true,
			},
		}, nil
	}

	if config.NumRowsPerPartition <= 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	}

	var err error
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := fmt.Sprintf("`%s`", config.WatermarkColumn)

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf("WHERE %s > $1", quotedWatermarkColumn)
	}
	parsedWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	// Query to get the total number of rows in the table
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", parsedWatermarkTable.MySQL(), whereClause)
	var minVal any
	var totalRows int64
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_UintRange:
			minVal = lastRange.UintRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime()
		}
		c.logger.Info(fmt.Sprintf("count query: %s - minVal: %v", countQuery, minVal))

		rs, err := c.Execute(ctx, countQuery, minVal)
		if err != nil {
			return nil, err
		}

		totalRows, err = rs.GetInt(0, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}
	} else {
		rs, err := c.Execute(ctx, countQuery)
		if err != nil {
			return nil, err
		}

		totalRows, err = rs.GetInt(0, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}
	}

	if totalRows == 0 {
		c.logger.Warn("no records to replicate, returning")
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows / numRowsPerPartition
	if totalRows%numRowsPerPartition != 0 {
		numPartitions++
	}

	watermarkQKind, err := c.GetDataTypeOfWatermarkColumn(ctx, parsedWatermarkTable.MySQL(), config.WatermarkColumn)
	if err != nil {
		return nil, fmt.Errorf("failed to get data type of watermark column %s: %w", config.WatermarkColumn, err)
	}

	c.logger.Info(fmt.Sprintf("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows, numPartitions, numRowsPerPartition))
	var rs *mysql.Result

	switch watermarkQKind {
	case qvalue.QValueKindInt8, qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64,
		qvalue.QValueKindUInt8, qvalue.QValueKindUInt16, qvalue.QValueKindUInt32, qvalue.QValueKindUInt64:
		if minVal != nil {
			partitionsQuery := fmt.Sprintf(
				`WITH stats AS (
				SELECT MIN(%[2]s) AS min_watermark,
				1.0 * (MAX(%[2]s) - MIN(%[2]s)) / (%[1]d) AS range_size
				FROM %[3]s WHERE %[2]s > $1
			)
			SELECT FLOOR((w.%[2]s - s.min_watermark) / s.range_size) AS bucket,
			MIN(w.%[2]s) AS start, MAX(w.%[2]s) AS end
			FROM %[3]s AS w
			CROSS JOIN stats AS s
			WHERE w.%[2]s > $1
			GROUP BY bucket
			ORDER BY start;`,
				numPartitions,
				quotedWatermarkColumn,
				parsedWatermarkTable.MySQL(),
			)
			c.logger.Info("partitions query", slog.String("query", partitionsQuery), slog.Any("minVal", minVal))
			rs, err = c.Execute(ctx, partitionsQuery, minVal)
		} else {
			partitionsQuery := fmt.Sprintf(
				`WITH stats AS (
				SELECT MIN(%[2]s) AS min_watermark,
				1.0 * (MAX(%[2]s) - MIN(%[2]s)) / (%[1]d) AS range_size
				FROM %[3]s
			)
			SELECT FLOOR((w.%[2]s - s.min_watermark) / s.range_size) AS bucket,
			MIN(w.%[2]s) AS start,
			MAX(w.%[2]s) AS end
			FROM %[3]s AS w
			CROSS JOIN stats AS s
			GROUP BY bucket
			ORDER BY start;`,
				numPartitions,
				quotedWatermarkColumn,
				parsedWatermarkTable.MySQL(),
			)
			c.logger.Info("partitions query", slog.String("query", partitionsQuery))
			rs, err = c.Execute(ctx, partitionsQuery)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query for partitions: %w", err)
		}
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ:
		if minVal != nil {
			partitionsQuery := fmt.Sprintf(
				`WITH stats AS (
				SELECT MIN(%[2]s) AS min_watermark,
				1.0 * (TIMESTAMPDIFF(MICROSECOND, MAX(%[2]s), MIN(%[2]s)) / (%[1]d)) AS range_size
				FROM %[3]s WHERE %[2]s > $1
			)
			SELECT FLOOR(TIMESTAMPDIFF(MICROSECOND, w.%[2]s, s.min_watermark) / s.range_size) AS bucket,
			MIN(w.%[2]s) AS start,
			MAX(w.%[2]s) AS end
			FROM %[3]s AS w
			CROSS JOIN stats AS s
			WHERE w.%[2]s > $1
			GROUP BY bucket
			ORDER BY start;`,
				numPartitions,
				quotedWatermarkColumn,
				parsedWatermarkTable.MySQL(),
			)
			c.logger.Info("partitions query", slog.String("query", partitionsQuery), slog.Any("minVal", minVal))
			rs, err = c.Execute(ctx, partitionsQuery, minVal)
		} else {
			partitionsQuery := fmt.Sprintf(
				`WITH stats AS (
				SELECT MIN(%[2]s) AS min_watermark,
				1.0 * (TIMESTAMPDIFF(MICROSECOND, MAX(%[2]s), MIN(%[2]s)) / (%[1]d)) AS range_size
				FROM %[3]s
			)
			SELECT FLOOR(TIMESTAMPDIFF(MICROSECOND, w.%[2]s, s.min_watermark) / s.range_size) AS bucket,
			MIN(w.%[2]s) AS start,
			MAX(w.%[2]s) AS end
			FROM %[3]s AS w
			CROSS JOIN stats AS s
			GROUP BY bucket
			ORDER BY start;`,
				numPartitions,
				quotedWatermarkColumn,
				parsedWatermarkTable.MySQL(),
			)
			c.logger.Info("partitions query", slog.String("query", partitionsQuery))
			rs, err = c.Execute(ctx, partitionsQuery)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query for partitions: %w", err)
		}
	}

	qk1, err := qkindFromMysql(rs.Fields[1])
	if err != nil {
		return nil, err
	}
	qk2, err := qkindFromMysql(rs.Fields[2])
	if err != nil {
		return nil, err
	}
	if qk1 != qk2 {
		return nil, fmt.Errorf("low/high of partition range should be same type, got low:%s, high:%s", qk1, qk2)
	}
	partitionHelper := utils.NewPartitionHelper(c.logger)
	for _, row := range rs.Values {
		val1, err := QValueFromMysqlFieldValue(qk1, row[1])
		if err != nil {
			return nil, err
		}
		val2, err := QValueFromMysqlFieldValue(qk2, row[2])
		if err != nil {
			return nil, err
		}
		if err := partitionHelper.AddPartition(val1.Value(), val2.Value()); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *MySqlConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	tableSchema, err := c.getTableSchemaForTable(ctx, config.Env,
		&protos.TableMapping{SourceTableIdentifier: config.WatermarkTable}, protos.TypeSystem_Q)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}

	var totalRecords int64
	var totalBytes int64
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
		totalBytes += int64(len(row) / 8) // null bitmap
		for idx, val := range row {
			// TODO ideally go-mysql would give us row buffer, need upstream PR
			// see mysql/rowdata.go in go-mysql for field sizes
			// unfortunately we're using text protocol, so this is a weak estimate
			switch rs.Fields[idx].Type {
			case mysql.MYSQL_TYPE_NULL:
				// 0
			case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
				var v uint64
				if val.Type == mysql.FieldValueTypeUnsigned {
					v = val.AsUint64()
				} else {
					signed := val.AsInt64()
					if signed < 0 {
						v = uint64(-signed)
					} else {
						v = uint64(signed)
					}
				}
				if v < 10 {
					totalBytes += 1
				} else if v > 99999999999999 {
					// math.log10(10**15-1) == 15.0, so pick boundary where we're accurate, cap at 15 for simplicity
					totalBytes += 15
				} else {
					totalBytes += 1 + int64(math.Log10(float64(val.AsUint64())))
				}
			case mysql.MYSQL_TYPE_YEAR, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE:
				totalBytes += 4
			default:
				totalBytes += int64(len(val.AsString()))
			}
		}
		schema, err := stream.Schema()
		if err != nil {
			return err
		}
		record := make([]types.QValue, 0, len(row))
		for idx, val := range row {
			qv, err := QValueFromMysqlFieldValue(schema.Fields[idx].Type, val)
			if err != nil {
				return fmt.Errorf("could not convert mysql value for %s: %w", schema.Fields[idx].Name, err)
			}
			record = append(record, qv)
		}
		stream.Records <- record
		return nil
	}

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

	close(stream.Records)
	return totalRecords, totalBytes, nil
}

func BuildQuery(logger log.Logger, query string, start string, end string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	data := map[string]any{
		"start": start,
		"end":   end,
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return "", err
	}
	res := buf.String()

	logger.Info("[mysql] templated query", slog.String("query", res))
	return res, nil
}
