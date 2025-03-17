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

	utils "github.com/PeerDB-io/peerdb/flow/connectors/utils/partition"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

func (c *SQLServerConnector) GetQRepPartitions(
	ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkTable == "" {
		// if no watermark column is specified, return a single partition
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
	var minVal any
	var totalRows pgtype.Int8
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime()
		}
		c.logger.Info(fmt.Sprintf("count query: %s - minVal: %v", countQuery, minVal))
		params := map[string]any{
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
	} else if err := c.db.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
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
			`SELECT bucket, MIN(%[2]s) AS start_v, MAX(%[2]s) AS end_v
			FROM (
				SELECT NTILE(%[1]d) OVER (ORDER BY %s) AS bucket, %[2]s
				FROM %[3]s WHERE %[2]s > :minVal
			) AS subquery
			GROUP BY bucket
			ORDER BY start_v`,
			numPartitions,
			quotedWatermarkColumn,
			config.WatermarkTable,
		)
		c.logger.Info(fmt.Sprintf("partitions query: %s - minVal: %v", partitionsQuery, minVal))
		params := map[string]any{
			"minVal": minVal,
		}
		rows, err = c.db.NamedQuery(partitionsQuery, params)
	} else {
		partitionsQuery := fmt.Sprintf(
			`SELECT bucket, MIN(%[2]s) AS start_v, MAX(%[2]s) AS end_v
			FROM (
				SELECT NTILE(%[1]d) OVER (ORDER BY %[2]s) AS bucket, %[2]s FROM %[3]s
			) AS subquery
			GROUP BY bucket
			ORDER BY start_v`,
			numPartitions,
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

	partitionHelper := utils.NewPartitionHelper(c.logger)
	for rows.Next() {
		var bucket pgtype.Int8
		var start, end any
		if err := rows.Scan(&bucket, &start, &end); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := partitionHelper.AddPartition(start, end); err != nil {
			return nil, fmt.Errorf("failed to add partition: %w", err)
		}
	}

	return partitionHelper.GetPartitions(), nil
}

func (c *SQLServerConnector) PullQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(c.logger, config.Query)
	if err != nil {
		return 0, 0, err
	}

	var qbatch *model.QRecordBatch
	if last.FullTablePartition {
		// this is a full table partition, so just run the query
		var err error
		qbatch, err = c.ExecuteAndProcessQuery(ctx, query)
		if err != nil {
			return 0, 0, err
		}
	} else {
		var rangeStart any
		var rangeEnd any

		// Depending on the type of the range, convert the range into the correct type
		switch x := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			rangeStart = x.IntRange.Start
			rangeEnd = x.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			rangeStart = x.TimestampRange.Start.AsTime()
			rangeEnd = x.TimestampRange.End.AsTime()
		default:
			return 0, 0, fmt.Errorf("unknown range type: %v", x)
		}

		var err error
		qbatch, err = c.NamedExecuteAndProcessQuery(ctx, query, map[string]any{
			"startRange": rangeStart,
			"endRange":   rangeEnd,
		})
		if err != nil {
			return 0, 0, err
		}
	}

	qbatch.FeedToQRecordStream(stream)
	return int64(len(qbatch.Records)), 0, nil
}

func BuildQuery(logger log.Logger, query string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	data := map[string]any{
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
