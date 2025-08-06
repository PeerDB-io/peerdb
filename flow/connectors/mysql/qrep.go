package connmysql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"text/template"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	shared_mysql "github.com/PeerDB-io/peerdb/flow/shared/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MySqlConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		// if no watermark column is specified, return a single partition
		return []*protos.QRepPartition{
			{
				PartitionId:        shared_mysql.MYSQL_FULL_TABLE_PARTITION_ID,
				Range:              nil,
				FullTablePartition: true,
			},
		}, nil
	}

	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	}

	numPartitions := int64(config.NumPartitionsOverride)
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := fmt.Sprintf("`%s`", config.WatermarkColumn)

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf("WHERE %s > ?", quotedWatermarkColumn)
	}
	parsedWatermarkTable, err := utils.ParseSchemaTable(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	// Query to get the total number of rows in the table
	var countQuery string
	if numPartitions == 0 {
		countQuery = fmt.Sprintf("SELECT MIN(%[2]s),MAX(%[2]s),COUNT(*) FROM %[1]s %[3]s",
			parsedWatermarkTable.MySQL(), config.WatermarkColumn, whereClause)
	} else {
		countQuery = fmt.Sprintf("SELECT MIN(%[2]s),MAX(%[2]s) FROM %[1]s %[3]s",
			parsedWatermarkTable.MySQL(), config.WatermarkColumn, whereClause)
	}
	var minVal any
	var rs *mysql.Result
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_UintRange:
			minVal = lastRange.UintRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime().String()
		}
		c.logger.Info("querying count", slog.String("query", countQuery), slog.Any("minVal", minVal))

		rs, err = c.Execute(ctx, countQuery, minVal)
	} else {
		rs, err = c.Execute(ctx, countQuery)
	}
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	if numPartitions == 0 {
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

	watermarkMyType := rs.Fields[1].Type
	watermarkQKind, err := qkindFromMysql(rs.Fields[1])
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

func (c *MySqlConnector) GetParallelLoadKeyForTables(
	ctx context.Context,
	input *protos.GetParallelLoadKeyForTablesInput,
) (*protos.GetParallelLoadKeyForTablesOutput, error) {
	return &protos.GetParallelLoadKeyForTablesOutput{
		TableParallelLoadKeyMapping: make(map[string]string),
	}, nil
}

func (c *MySqlConnector) PullQRepRecords(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
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
		stream.Records <- record
		return nil
	}

	c.bytesRead.Store(0)
	shutDown := shared.Interval(ctx, time.Minute, func() {
		if read := c.bytesRead.Swap(0); read != 0 {
			otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		}
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

	close(stream.Records)
	return totalRecords, c.bytesRead.Swap(0), nil
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
