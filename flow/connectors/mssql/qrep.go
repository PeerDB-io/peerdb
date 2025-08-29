package connmssql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MsSqlConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	return &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: nil,
	}, nil
}

func (c *MsSqlConnector) tableRowEstimate(ctx context.Context, schema, table string) (int64, error) {
	var rows int64
	err := c.conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT SUM(p.rows) FROM sys.partitions p"+
			" WHERE p.object_id = OBJECT_ID(%s) AND p.index_id IN (0, 1)",
		sanitize.QuoteString(schema+"."+table))).Scan(&rows)
	return rows, err
}

func (c *MsSqlConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
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

	parsedTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	quotedWatermark := common.QuoteMsSqlIdentifier(config.WatermarkColumn)
	minmaxQuery := "SELECT MIN(" + quotedWatermark + "), MAX(" + quotedWatermark +
		") FROM " + parsedTable.MsSql()

	if last != nil && last.Range != nil {
		var minVal string
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = strconv.FormatInt(lastRange.IntRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			minVal = "'" + lastRange.TimestampRange.End.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
		}
		if minVal != "" {
			minmaxQuery += fmt.Sprintf(" WHERE %s > %s", common.QuoteMsSqlIdentifier(config.WatermarkColumn), minVal)
		}
	}

	if numPartitions == 0 {
		totalRows, err := c.tableRowEstimate(ctx, parsedTable.Namespace, parsedTable.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate rows: %w", err)
		}
		if totalRows == 0 {
			c.logger.Warn("[mssql] estimating no records, using 1 partition")
			numPartitions = 1
		} else {
			adjusted := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
			c.logger.Info("[mssql] partition details",
				slog.Int64("totalRowsEstimate", totalRows),
				slog.Int64("numPartitions", adjusted.AdjustedNumPartitions))
			numPartitions = adjusted.AdjustedNumPartitions
		}
	}

	c.logger.Info("[mssql] querying min/max", slog.String("query", minmaxQuery))
	var minVal, maxVal any
	if err := c.conn.QueryRowContext(ctx, minmaxQuery).Scan(&minVal, &maxVal); err != nil {
		return nil, fmt.Errorf("failed to query min/max: %w", err)
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	if err := partitionHelper.AddPartitionsWithRange(minVal, maxVal, numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}

	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *MsSqlConnector) PullQRepRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	c.logger.Info("[mssql] pulling records start")

	// build query
	var query string
	if partition.FullTablePartition {
		query = config.Query
		if query == "" {
			parsedTable, err := common.ParseTableIdentifier(config.WatermarkTable)
			if err != nil {
				return 0, 0, fmt.Errorf("[mssql] failed to parse table: %w", err)
			}
			selectedColumns := c.buildSelectedColumns(ctx, config)
			query = fmt.Sprintf("SELECT %s FROM %s", selectedColumns, parsedTable.MsSql())
		}
	} else {
		parsedTable, err := common.ParseTableIdentifier(config.WatermarkTable)
		if err != nil {
			return 0, 0, fmt.Errorf("[mssql] failed to parse table: %w", err)
		}
		selectedColumns := c.buildSelectedColumns(ctx, config)

		queryTemplate := config.Query
		if queryTemplate == "" {
			queryTemplate = fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN {{.start}} AND {{.end}}",
				selectedColumns, parsedTable.MsSql(), common.QuoteMsSqlIdentifier(config.WatermarkColumn))
		}

		templateParams := map[string]string{}
		switch x := partition.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			templateParams["start"] = strconv.FormatInt(x.IntRange.Start, 10)
			templateParams["end"] = strconv.FormatInt(x.IntRange.End, 10)
		case *protos.PartitionRange_TimestampRange:
			templateParams["start"] = "'" + x.TimestampRange.Start.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
			templateParams["end"] = "'" + x.TimestampRange.End.AsTime().Format("2006-01-02 15:04:05.999999") + "'"
		case *protos.PartitionRange_NullRange:
			if config.Query != "" {
				return 0, 0, errors.New("can't construct null range partition for custom queries")
			}
			queryTemplate = fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NULL",
				selectedColumns, parsedTable.MsSql(), common.QuoteMsSqlIdentifier(config.WatermarkColumn))
		default:
			return 0, 0, fmt.Errorf("unknown range type: %v", x)
		}

		if len(templateParams) > 0 {
			query, err = utils.ExecuteTemplate(queryTemplate, templateParams)
			if err != nil {
				return 0, 0, err
			}
		} else {
			query = queryTemplate
		}
	}

	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, 0, err
	}

	schema := types.QRecordSchema{Fields: make([]types.QField, 0, len(colTypes))}
	qkinds := make([]types.QValueKind, len(colTypes))
	for i, ct := range colTypes {
		qkind, err := QkindFromMssqlColumnType(ct.DatabaseTypeName())
		if err != nil {
			return 0, 0, fmt.Errorf("[mssql] unmappable column %s type %s: %w", ct.Name(), ct.DatabaseTypeName(), err)
		}
		qkinds[i] = qkind
		nullable, _ := ct.Nullable()
		prec, scale, _ := ct.DecimalSize()
		schema.Fields = append(schema.Fields, types.QField{
			Name:      ct.Name(),
			Nullable:  nullable,
			Precision: int16(prec),
			Scale:     int16(scale),
			Type:      qkind,
		})
	}
	stream.SetSchema(schema)

	shutDown := common.Interval(ctx, time.Minute, func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, c.deltaBytesRead.Swap(0))
	})
	defer shutDown()

	var totalRecords int64
	for rows.Next() {
		scanArgs := make([]any, len(colTypes))
		for i := range scanArgs {
			scanArgs[i] = new(any)
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return 0, 0, err
		}

		record := make([]types.QValue, 0, len(scanArgs))
		for i, sa := range scanArgs {
			rawVal := *(sa.(*any))
			qv, err := QValueFromMssqlValue(qkinds[i], rawVal)
			if err != nil {
				return 0, 0, fmt.Errorf("[mssql] convert error for column %s: %w", colTypes[i].Name(), err)
			}
			record = append(record, qv)
		}
		if err := stream.Send(ctx, record); err != nil {
			return 0, 0, err
		}
		totalRecords++

		if totalRecords%50000 == 0 {
			c.logger.Info("[mssql] pulling records",
				slog.Int64("records", totalRecords),
				slog.Int("channelLen", len(stream.Records)))
		}
	}

	c.logger.Info("[mssql] pulled records", slog.Int64("records", totalRecords))
	return totalRecords, c.deltaBytesRead.Swap(0), rows.Err()
}

func (c *MsSqlConnector) buildSelectedColumns(ctx context.Context, config *protos.QRepConfig) string {
	if len(config.Exclude) == 0 {
		return "*"
	}
	tableSchema, err := c.getTableSchemaForTable(ctx, config.Env,
		&protos.TableMapping{SourceTableIdentifier: config.WatermarkTable}, protos.TypeSystem_Q)
	if err != nil {
		c.logger.Warn("[mssql] failed to get schema for column exclusion, falling back to *", slog.Any("error", err))
		return "*"
	}
	var quotedCols []string
	for _, col := range tableSchema.Columns {
		if !slices.Contains(config.Exclude, col.Name) {
			quotedCols = append(quotedCols, common.QuoteMsSqlIdentifier(col.Name))
		}
	}
	return strings.Join(quotedCols, ",")
}
