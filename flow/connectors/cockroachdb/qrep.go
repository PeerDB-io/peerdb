package conncockroachdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// crdbSystemTimeRegex matches a CockroachDB HLC timestamp, e.g. 1712345678901234567.0000000001
var crdbSystemTimeRegex = regexp.MustCompile(`^\d+(\.\d+)?$`)

func (c *CockroachDBConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		// if no watermark column is specified, return a single partition
		return utils.FullTablePartition(), nil
	}

	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	}

	numPartitions := int64(config.NumPartitionsOverride)
	numRowsPerPartition := int64(config.NumRowsPerPartition)

	parsedWatermarkTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	systemTime, err := c.snapshotSystemTime(ctx, config)
	if err != nil {
		return nil, err
	}

	var lastRangeEnd any
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			lastRangeEnd = lastRange.IntRange.End
		case *protos.PartitionRange_UintRange:
			lastRangeEnd = lastRange.UintRange.End
		case *protos.PartitionRange_TimestampRange:
			lastRangeEnd = lastRange.TimestampRange.End.AsTime()
		default:
			return nil, fmt.Errorf("unsupported range type %T in last partition after resuming QRep", lastRange)
		}
	}

	if numPartitions == 0 && lastRangeEnd == nil {
		// starting replication from the beginning: estimate row count to calculate partitions
		totalRows, err := c.tableRowEstimate(ctx, parsedWatermarkTable)
		if err != nil {
			c.logger.Warn("[cockroachdb] failed to estimate row count, falling back to precise count", slog.Any("error", err))
			totalRows = 0
		}
		if totalRows <= 0 {
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s AS OF SYSTEM TIME '%s'", parsedWatermarkTable.String(), systemTime)
			if err := c.conn.QueryRow(ctx, countQuery).Scan(&totalRows); err != nil {
				return nil, fmt.Errorf("failed to query for total rows: %w", err)
			}
		}

		if totalRows == 0 {
			c.logger.Warn("estimating no records to replicate, only using 1 partition")
			numPartitions = 1
		} else {
			adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
			c.logger.Info("[cockroachdb] partition details",
				slog.Int64("totalRowsEstimate", totalRows),
				slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
				slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
				slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))
			numPartitions = adjustedPartitions.AdjustedNumPartitions
		}
	}

	// when resuming, count remaining rows in the same scan as min/max
	minmaxHasCount := lastRangeEnd != nil && numPartitions == 0
	minmaxQuery := buildMinMaxQuery(parsedWatermarkTable, config.WatermarkColumn, systemTime, lastRangeEnd != nil, minmaxHasCount)
	var queryArgs []any
	if lastRangeEnd != nil {
		queryArgs = []any{lastRangeEnd}
	}
	c.logger.Info("querying min/max", slog.String("query", minmaxQuery))

	var minVal, maxVal any
	var totalRows int64
	row := c.conn.QueryRow(ctx, minmaxQuery, queryArgs...)
	if minmaxHasCount {
		err = row.Scan(&minVal, &maxVal, &totalRows)
	} else {
		err = row.Scan(&minVal, &maxVal)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query for min/max: %w", err)
	}

	if minmaxHasCount {
		if totalRows == 0 {
			c.logger.Warn("no records to replicate, returning")
			return []*protos.QRepPartition{}, nil
		}

		adjustedPartitions := shared.AdjustNumPartitions(totalRows, numRowsPerPartition)
		c.logger.Info("[cockroachdb] partition details",
			slog.Int64("totalRows", totalRows),
			slog.Int64("desiredNumRowsPerPartition", numRowsPerPartition),
			slog.Int64("adjustedNumPartitions", adjustedPartitions.AdjustedNumPartitions),
			slog.Int64("adjustedNumRowsPerPartition", adjustedPartitions.AdjustedNumRowsPerPartition))
		numPartitions = adjustedPartitions.AdjustedNumPartitions
	}

	switch minVal.(type) {
	case nil, int16, int32, int64, time.Time:
	default:
		c.logger.Info("[cockroachdb] watermark column type does not support range partitioning,"+
			" falling back to full table partition",
			slog.String("type", fmt.Sprintf("%T", minVal)))
		return utils.FullTablePartition(), nil
	}

	partitionHelper := utils.NewPartitionHelper(c.logger)
	if err := partitionHelper.AddPartitionsWithRange(minVal, maxVal, numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}

	// add null values partition to the end, if nulls aren't present it will be an empty partition
	// that gets skipped during replication
	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *CockroachDBConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	c.logger.Info("Evaluating if tables can perform parallel load")

	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}
	for _, tm := range input.TableMappings {
		source := tm.SourceTableIdentifier
		schema, ok := input.TableSchemaMapping[source]
		if !ok {
			c.logger.Warn("[cockroachdb] table schema not found, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		if len(schema.PrimaryKeyColumns) == 0 {
			c.logger.Info("[cockroachdb] table has no primary key, defaulting to full table snapshot",
				slog.String("table", source))
			continue
		}
		pkColumn := schema.PrimaryKeyColumns[0]
		var pkQKind types.QValueKind
		for _, col := range schema.Columns {
			if col.Name == pkColumn {
				pkQKind = types.QValueKind(col.Type)
				break
			}
		}
		if !supportsRangePartition(pkQKind) {
			c.logger.Info("[cockroachdb] primary key type does not support range partitioning, defaulting to full table snapshot",
				slog.String("table", source),
				slog.String("column", pkColumn),
				slog.String("qkind", string(pkQKind)))
			continue
		}
		c.logger.Info("[cockroachdb] using primary key as default partition key",
			slog.String("table", source),
			slog.String("column", pkColumn),
			slog.String("qkind", string(pkQKind)))
		output.TableDefaultPartitionKeyMapping[source] = pkColumn
	}
	return output, nil
}

func (c *CockroachDBConnector) PullQRepRecords(
	ctx context.Context,
	_ shared.CatalogPool,
	_ *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	partitionIdLog := slog.String(string(shared.PartitionIDKey), partition.PartitionId)

	tableSchemas, err := c.GetTableSchema(ctx, config.Env, config.Version, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: config.WatermarkTable}})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get schema for watermark table %s: %w", config.WatermarkTable, err)
	}
	tableSchema := tableSchemas[config.WatermarkTable]

	selectedColumns := buildSelectedColumns(tableSchema.Columns, config.Exclude)
	if selectedColumns == "" {
		return 0, 0, fmt.Errorf("no columns selected for watermark table %s (check Exclude configuration)", config.WatermarkTable)
	}

	parsedSrcTable, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to parse source table: %w", err)
	}

	systemTime, err := c.snapshotSystemTime(ctx, config)
	if err != nil {
		return 0, 0, err
	}

	query, queryArgs, err := buildPullQuery(config, partition, selectedColumns, parsedSrcTable, systemTime)
	if err != nil {
		return 0, 0, err
	}

	c.logger.Info("[cockroachdb] pulling records start", partitionIdLog, slog.String("query", query))

	rows, err := c.conn.Query(ctx, query, queryArgs...)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fds := rows.FieldDescriptions()
	schema := qRecordSchemaFromFieldDescriptions(tableSchema, fds)
	stream.SetSchema(schema)

	var totalRecords, totalBytes int64
	for rows.Next() {
		record, err := c.mapRowToQRecord(rows, fds, schema, dstType)
		if err != nil {
			return totalRecords, totalBytes, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		if err := stream.Send(ctx, record); err != nil {
			return totalRecords, totalBytes, fmt.Errorf("failed to send record to stream: %w", err)
		}

		totalRecords++
		for _, val := range rows.RawValues() {
			totalBytes += int64(len(val))
		}
		if totalRecords%50000 == 0 {
			c.logger.Info("[cockroachdb] pulling records",
				partitionIdLog,
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", totalBytes),
				slog.Int("channelLen", len(stream.Records)))
		}
	}
	if err := rows.Err(); err != nil {
		return totalRecords, totalBytes, fmt.Errorf("row iteration failed: %w", err)
	}

	c.logger.Info("[cockroachdb] pulled records",
		partitionIdLog,
		slog.Int64("records", totalRecords),
		slog.Int64("bytes", totalBytes),
		slog.Int("channelLen", len(stream.Records)))
	return totalRecords, totalBytes, nil
}

// snapshotSystemTime returns the HLC timestamp QRep queries read at via AS OF SYSTEM TIME,
// pinning reads in the recent past so long scans neither block nor get pushed by writers.
// config.SnapshotName carries an exported system time when set, otherwise the current
// cluster timestamp is captured.
func (c *CockroachDBConnector) snapshotSystemTime(ctx context.Context, config *protos.QRepConfig) (string, error) {
	if config.SnapshotName != "" {
		if !crdbSystemTimeRegex.MatchString(config.SnapshotName) {
			return "", fmt.Errorf("invalid CockroachDB system time %q", config.SnapshotName)
		}
		return config.SnapshotName, nil
	}

	return c.clusterLogicalTimestamp(ctx)
}

func (c *CockroachDBConnector) tableRowEstimate(ctx context.Context, table *common.QualifiedTable) (int64, error) {
	var estimate int64
	if err := c.conn.QueryRow(ctx, `SELECT COALESCE(MAX(s.estimated_row_count), 0)
		FROM crdb_internal.table_row_statistics s
		JOIN crdb_internal.tables t ON s.table_id = t.table_id
		WHERE t.database_name = current_database() AND t.schema_name = $1 AND t.name = $2`,
		table.Namespace, table.Table).Scan(&estimate); err != nil {
		return 0, fmt.Errorf("failed to query for row count estimate: %w", err)
	}
	return estimate, nil
}

func (c *CockroachDBConnector) mapRowToQRecord(
	rows pgx.Rows,
	fds []pgconn.FieldDescription,
	schema types.QRecordSchema,
	dstType protos.DBType,
) ([]types.QValue, error) {
	record := make([]types.QValue, len(fds))
	rawValues := rows.RawValues()
	typeMap := c.conn.TypeMap()
	for i, fd := range fds {
		field := schema.Fields[i]
		buf := rawValues[i]
		if buf == nil {
			record[i] = types.QValueNull(field.Type)
			continue
		}

		switch fd.DataTypeOID {
		case pgtype.JSONOID, pgtype.JSONBOID:
			// keep raw JSON text to avoid a decode/encode round trip losing number precision
			var jsonVal string
			if err := typeMap.Scan(fd.DataTypeOID, fd.Format, buf, &jsonVal); err != nil {
				return nil, fmt.Errorf("failed to scan json for %s: %w", fd.Name, err)
			}
			record[i] = types.QValueJSON{Val: jsonVal}
		case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
			var textArr pgtype.FlatArray[pgtype.Text]
			if err := typeMap.Scan(fd.DataTypeOID, fd.Format, buf, &textArr); err != nil {
				return nil, fmt.Errorf("failed to scan json array for %s: %w", fd.Name, err)
			}
			elements := make([]json.RawMessage, len(textArr))
			for j, text := range textArr {
				if text.Valid {
					elements[j] = json.RawMessage(text.String)
				} else {
					elements[j] = json.RawMessage("null")
				}
			}
			arrayVal, err := json.Marshal(elements)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal json array for %s: %w", fd.Name, err)
			}
			record[i] = types.QValueJSON{Val: string(arrayVal), IsArray: true}
		default:
			var value any
			if dt, ok := typeMap.TypeForOID(fd.DataTypeOID); ok {
				decoded, err := dt.Codec.DecodeValue(typeMap, fd.DataTypeOID, fd.Format, buf)
				if err != nil {
					return nil, fmt.Errorf("failed to decode value for %s: %w", fd.Name, err)
				}
				value = decoded
			} else {
				// types unknown to pgx (e.g. enums) arrive in text format
				switch fd.Format {
				case pgtype.TextFormatCode:
					value = string(buf)
				case pgtype.BinaryFormatCode:
					value = slices.Clone(buf)
				default:
					return nil, fmt.Errorf("unknown format code %d for %s", fd.Format, fd.Name)
				}
			}

			qv, err := qvalueFromCrdbValue(field, dstType, fd.DataTypeOID, typeMap, value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value for %s: %w", fd.Name, err)
			}
			record[i] = qv
		}
	}
	return record, nil
}

func supportsRangePartition(qkind types.QValueKind) bool {
	switch qkind {
	case types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64,
		types.QValueKindDate, types.QValueKindTimestamp, types.QValueKindTimestampTZ:
		return true
	default:
		return false
	}
}

func buildSelectedColumns(cols []*protos.FieldDescription, exclude []string) string {
	columns := make([]string, 0, len(cols))
	for _, col := range cols {
		if slices.Contains(exclude, col.Name) {
			continue
		}
		columns = append(columns, common.QuoteIdentifier(col.Name))
	}
	return strings.Join(columns, ", ")
}

func buildMinMaxQuery(
	table *common.QualifiedTable,
	watermarkColumn string,
	systemTime string,
	resuming bool,
	withCount bool,
) string {
	quotedColumn := common.QuoteIdentifier(watermarkColumn)
	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT MIN(%[1]s),MAX(%[1]s)", quotedColumn)
	if withCount {
		sb.WriteString(",COUNT(*)")
	}
	fmt.Fprintf(&sb, " FROM %s AS OF SYSTEM TIME '%s'", table.String(), systemTime)
	if resuming {
		fmt.Fprintf(&sb, " WHERE %s > $1", quotedColumn)
	}
	return sb.String()
}

func buildPullQuery(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	selectedColumns string,
	srcTable *common.QualifiedTable,
	systemTime string,
) (string, []any, error) {
	if partition.FullTablePartition {
		if config.Query != "" {
			return config.Query, nil, nil
		}
		return fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME '%s'",
			selectedColumns, srcTable.String(), systemTime), nil, nil
	}

	queryTemplate := config.Query
	if queryTemplate == "" {
		queryTemplate = fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME '%s' WHERE %s BETWEEN {{.start}} AND {{.end}}",
			selectedColumns, srcTable.String(), systemTime, common.QuoteIdentifier(config.WatermarkColumn))
	}
	templateParams := map[string]string{"start": "$1", "end": "$2"}

	var queryArgs []any
	switch x := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		queryArgs = []any{x.IntRange.Start, x.IntRange.End}
	case *protos.PartitionRange_UintRange:
		queryArgs = []any{x.UintRange.Start, x.UintRange.End}
	case *protos.PartitionRange_TimestampRange:
		queryArgs = []any{x.TimestampRange.Start.AsTime(), x.TimestampRange.End.AsTime()}
	case *protos.PartitionRange_NullRange:
		if config.Query != "" {
			return "", nil, errors.New("can't construct a null range partition for custom queries")
		}
		queryTemplate = fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME '%s' WHERE %s IS NULL",
			selectedColumns, srcTable.String(), systemTime, common.QuoteIdentifier(config.WatermarkColumn))
		templateParams = map[string]string{}
	default:
		return "", nil, fmt.Errorf("unknown range type: %v", x)
	}

	query, err := utils.ExecuteTemplate(queryTemplate, templateParams)
	if err != nil {
		return "", nil, err
	}
	return query, queryArgs, nil
}

func qRecordSchemaFromFieldDescriptions(
	tableSchema *protos.TableSchema,
	fds []pgconn.FieldDescription,
) types.QRecordSchema {
	tableColumns := make(map[string]*protos.FieldDescription, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		tableColumns[col.Name] = col
	}

	fields := make([]types.QField, 0, len(fds))
	for _, fd := range fds {
		var qkind types.QValueKind
		nullable := true
		if col, ok := tableColumns[fd.Name]; ok {
			qkind = types.QValueKind(col.Type)
			nullable = col.Nullable
		} else {
			qkind = crdbOIDToQValueKind(fd.DataTypeOID)
		}

		var precision, scale int16
		if qkind == types.QValueKindNumeric || qkind == types.QValueKindArrayNumeric {
			precision, scale = common.ParseNumericTypmod(fd.TypeModifier)
		}
		fields = append(fields, types.QField{
			Name:      fd.Name,
			Type:      qkind,
			Nullable:  nullable,
			Precision: precision,
			Scale:     scale,
		})
	}
	return types.NewQRecordSchema(fields)
}
