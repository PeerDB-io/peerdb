package connsqlserver

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/partition"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

func (c *SQLServerConnector) SetupQRepMetadataTables(config *protos.QRepConfig) error {
	log.Infof("Setting up metadata tables for query replication on sql server is a no-op")
	return nil
}

func (c *SQLServerConnector) GetQRepPartitions(
	config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error) {
	if config.NumRowsPerPartition <= 0 {
		return nil, fmt.Errorf("num rows per partition must be greater than 0 for sql server")
	}

	var err error
	numRowsPerPartition := int64(config.NumRowsPerPartition)
	quotedWatermarkColumn := fmt.Sprintf("\"%s\"", config.WatermarkColumn)

	whereClause := ""
	if last != nil && last.Range != nil {
		whereClause = fmt.Sprintf(`WHERE %s > $1`, quotedWatermarkColumn)
	}

	// Query to get the total number of rows in the table
	//nolint:gosec
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", config.WatermarkTable, whereClause)
	var row *sql.Row
	var minVal interface{} = nil
	if last != nil && last.Range != nil {
		switch lastRange := last.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			minVal = lastRange.IntRange.End
		case *protos.PartitionRange_TimestampRange:
			minVal = lastRange.TimestampRange.End.AsTime()
		}
		row = c.db.QueryRow(countQuery, minVal)
	} else {
		row = c.db.QueryRow(countQuery)
	}

	var totalRows int64
	if err = row.Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("failed to query for total rows: %w", err)
	}

	if totalRows == 0 {
		log.Warnf("no records to replicate for flow job %s, returning", config.FlowJobName)
		return make([]*protos.QRepPartition, 0), nil
	}

	// Calculate the number of partitions
	numPartitions := totalRows / numRowsPerPartition
	if totalRows%numRowsPerPartition != 0 {
		numPartitions++
	}
	log.Infof("total rows: %d, num partitions: %d, num rows per partition: %d",
		totalRows, numPartitions, numRowsPerPartition)

	var rows *sql.Rows
	if minVal != nil {
		// Query to get partitions using window functions
		//nolint:gosec
		partitionsQuery := fmt.Sprintf(
			`SELECT NTILE(%d) OVER (ORDER BY %s) AS bucket, MIN(%s) AS start, MAX(%s) AS end
								FROM %s WHERE %s > $1
								GROUP BY bucket
								ORDER BY start`,
			numPartitions,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			config.WatermarkTable,
			quotedWatermarkColumn,
		)
		log.Infof("partitions query: %s - minVal: %v", partitionsQuery, minVal)
		rows, err = c.db.Query(partitionsQuery, minVal)
	} else {
		//nolint:gosec
		partitionsQuery := fmt.Sprintf(
			`SELECT NTILE(%d) OVER (ORDER BY %s) AS bucket, MIN(%s) AS start, MAX(%s) AS end
								FROM %s
								GROUP BY bucket
								ORDER BY start`,
			numPartitions,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			quotedWatermarkColumn,
			config.WatermarkTable,
		)
		log.Infof("partitions query: %s", partitionsQuery)
		rows, err = c.db.Query(partitionsQuery)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query for partitions: %w", err)
	}

	defer rows.Close()

	partitionHelper := utils.NewPartitionHelper()
	for rows.Next() {
		var bucket int64
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
	config *protos.QRepConfig, partition *protos.QRepPartition) (*model.QRecordBatch, error) {
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
		return nil, fmt.Errorf("unknown range type: %v", x)
	}

	// Build the query to pull records within the range from the source table
	// Be sure to order the results by the watermark column to ensure consistency across pulls
	query, err := BuildQuery(config.Query)
	if err != nil {
		return nil, err
	}

	return c.ExecuteAndProcessQuery(query, rangeStart, rangeEnd)
}

func BuildQuery(query string) (string, error) {
	tmpl, err := template.New("query").Parse(query)
	if err != nil {
		return "", err
	}

	data := map[string]interface{}{
		"start": "$1",
		"end":   "$2",
	}

	buf := new(bytes.Buffer)

	err = tmpl.Execute(buf, data)
	if err != nil {
		return "", err
	}
	res := buf.String()

	log.Infof("templated query: %s", res)
	return res, nil
}

func (c *SQLServerConnector) SyncQRepRecords(
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	records *model.QRecordBatch,
) (int, error) {
	panic("not implemented")
}

func (c *SQLServerConnector) ConsolidateQRepPartitions(config *protos.QRepConfig) error {
	panic("not implemented")
}

func (c *SQLServerConnector) CleanupQRepFlow(config *protos.QRepConfig) error {
	panic("not implemented")
}
