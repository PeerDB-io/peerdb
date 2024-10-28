package connsqlserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"text/template"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/sdk/log"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

func (c *SQLServerConnector) GetQRepPartitions(
	ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkTable == "" {
		c.logger.Info("watermark table is empty, doing full table refresh")
		return []*protos.QRepPartition{
			{
				PartitionId:        uuid.New().String(),
				FullTablePartition: true,
			},
		}, nil
	}

	if config.NumRowsPerPartition <= 0 {
		return nil, errors.New("num rows per partition must be greater than 0 for sql server")
	}

	var err error
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := fmt.Sprintf("\"%s\"", config.WatermarkColumn)

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf(`WHERE %s > :minVal`, quotedWatermarkColumn)
	}

	// Query to get the total number of rows in the table
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", config.WatermarkTable, whereClause)
	var minVal interface{} = nil
	var totalRows pgtype.Int8
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime()
		}
		c.logger.Info(fmt.Sprintf("count query: %s - minVal: %v", countQuery, minVal))
		params := map[string]interface{}{
			"minVal": minVal,
		}

		err := func() error {
			//nolint:sqlclosecheck
			rows, err := c.db.NamedQuery(countQuery, params)
			if err != nil {
				return err
			}
			defer rows.Close()

			if rows.Next() {
				if err := rows.Scan(&totalRows); err != nil {
					return err
				}
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}
	} else {
		row := c.db.QueryRowContext(ctx, countQuery)
		if err = row.Scan(&totalRows); err != nil {
			return nil, fmt.Errorf("failed to query for total rows: %w", err)
		}
	}

	if totalRows.Int64 == 0 {
		c.logger.Warn("no records to replicate, returning")
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows.Int64 / numRowsPerPartition
	if totalRows.Int64%numRowsPerPartition != 0 {
		numPartitions++
	}
	c.logger.Info(fmt.Sprintf("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows.Int64, numPartitions, numRowsPerPartition))
	var rows *sqlx.Rows
	if minVal != nil {
		// Query to get partitions using window functions
		partitionsQuery := fmt.Sprintf(
			`SELECT bucket_v, MIN(v_from) AS start_v, MAX(v_from) AS end_v
					FROM (
						SELECT NTILE(%d) OVER (ORDER BY %s) AS bucket_v, %s as v_from
						FROM %s WHERE %s > :minVal
					) AS subquery
					GROUP BY bucket_v
					ORDER BY start_v`,
			numPartitions,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			config.WatermarkTable,
			quotedWatermarkColumn,
		)
		c.logger.Info(fmt.Sprintf("partitions query: %s - minVal: %v", partitionsQuery, minVal))
		params := map[string]interface{}{
			"minVal": minVal,
		}
		rows, err = c.db.NamedQuery(partitionsQuery, params)
	} else {
		partitionsQuery := fmt.Sprintf(
			`SELECT bucket_v, MIN(v_from) AS start_v, MAX(v_from) AS end_v
					FROM (
						SELECT NTILE(%d) OVER (ORDER BY %s) AS bucket_v, %s as v_from
						FROM %s
					) AS subquery
					GROUP BY bucket_v
					ORDER BY start_v`,
			numPartitions,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			config.WatermarkTable,
		)
		c.logger.Info("partitions query: " + partitionsQuery)
		rows, err = c.db.Queryx(partitionsQuery)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query for partitions: %w", err)
	}

	defer rows.Close()

	partitionHelper := utils.NewPartitionHelper()
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end interface{}
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		err = partitionHelper.AddPartition(start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *SQLServerConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(c.logger, config.Query)
	if err != nil {
		return 0, err
	}

	if partition.FullTablePartition {
		// this is a full table partition, so just run the query
		qbatch, err := c.ExecuteAndProcessQuery(ctx, query)
		if err != nil {
			return 0, err
		}
		qbatch.FeedToQRecordStream(stream)
		return len(qbatch.Records), nil
	}

	var rangeStart interface{}
	var rangeEnd interface{}

	// Depending on the type of the range, convert the range into the correct type
	switch x := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		rangeStart = x.IntRange.Start
		rangeEnd = x.IntRange.End
	case *protos.PartitionRange_TimestampRange:
		rangeStart = x.TimestampRange.Start.AsTime()
		rangeEnd = x.TimestampRange.End.AsTime()
	default:
		return 0, fmt.Errorf("unknown range type: %v", x)
	}

	rangeParams := map[string]interface{}{
		"startRange": rangeStart,
		"endRange":   rangeEnd,
	}

	qbatch, err := c.NamedExecuteAndProcessQuery(ctx, query, rangeParams)
	if err != nil {
		return 0, err
	}
	qbatch.FeedToQRecordStream(stream)
	return len(qbatch.Records), nil
}

func BuildQuery(logger log.Logger, query string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	data := map[string]interface{}{
		"start": ":startRange",
		"end":   ":endRange",
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return "", err
	}
	res := buf.String()

	logger.Info("[ss] templated query", slog.String("query", res))
	return res, nil
}
