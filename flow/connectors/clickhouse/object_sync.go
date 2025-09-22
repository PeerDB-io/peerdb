package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// SyncQRepDownloadableObjects syncs data from downloadable objects (URLs) to ClickHouse
// Supports all ClickHouse formats based on the stream's Format field
func (c *ClickHouseConnector) SyncQRepObjects(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QObjectStream,
) (int64, shared.QRepWarnings, error) {
	dstTableName := config.DestinationTableIdentifier
	startTime := time.Now()

	format, err := stream.Format()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get format from stream: %w", err)
	}

	c.logger.Info("Starting SyncQRepObjects",
		slog.String("dstTable", dstTableName),
		slog.String("partitionId", partition.PartitionId),
		slog.String("format", format))

	// Get schema from the stream
	schema, err := stream.Schema()

	if err != nil {
		return 0, nil, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	// Apply type conversions if configured
	destTypeConversions := findTypeConversions(schema, config.Columns)
	if len(destTypeConversions) > 0 {
		schema = applyTypeConversions(schema, destTypeConversions)
	}

	// Create column name mapping for the format fields
	columnNameFieldMap := c.constructColumnNameFieldMap(schema.Fields, format)

	var totalRecords int64
	warnings := shared.QRepWarnings{}

	// Process each downloadable object
	for object := range stream.Objects {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}

		c.logger.Debug("Processing downloadable object",
			slog.String("url", object.URL),
			slog.Int64("size", object.Size))

		// we should generate insert query once and use query params to insert multiple files
		// query param should be used for headers as well
		recordsInserted, err := c.insertFromURLWithMapping(ctx, config, object, schema, columnNameFieldMap, format)
		if err != nil {
			c.logger.Error("Failed to insert from URL",
				slog.String("url", object.URL),
				slog.Any("error", err))
			return 0, nil, fmt.Errorf("failed to insert from URL %s: %w", object.URL, err)
		}

		totalRecords += recordsInserted
		c.logger.Info("Successfully processed object",
			slog.String("url", object.URL),
			slog.Int64("recordsInserted", recordsInserted))
	}

	// Check for stream errors
	if err := stream.Err(); err != nil {
		return 0, nil, fmt.Errorf("stream error: %w", err)
	}

	if err := c.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		c.logger.Error("Failed to finish QRep partition", slog.Any("error", err))
		return 0, nil, err
	}

	c.logger.Info("Completed SyncQRepObjects",
		slog.String("dstTable", dstTableName),
		slog.String("partitionId", partition.PartitionId),
		slog.Int64("totalRecords", totalRecords))

	return totalRecords, warnings, nil
}

// constructColumnNameFieldMap creates a mapping between column names and source field names
// The mapping strategy depends on the format
func (c *ClickHouseConnector) constructColumnNameFieldMap(fields []types.QField, format string) map[string]string {
	columnNameFieldMap := make(map[string]string)

	for _, field := range fields {
		columnNameFieldMap[field.Name] = field.Name
	}

	return columnNameFieldMap
}

// insertFromURLWithMapping inserts data from a URL using proper column mapping
func (c *ClickHouseConnector) insertFromURLWithMapping(
	ctx context.Context,
	config *protos.QRepConfig,
	object *model.Object,
	schema types.QRecordSchema,
	columnNameFieldMap map[string]string,
	format string,
) (int64, error) {
	urlTableFunction := c.buildURLTableFunction(object, format)

	// Create configuration for the table function helper
	insertConfig := &InsertFromTableFunctionConfig{
		DestinationTable:    config.DestinationTableIdentifier,
		Schema:              schema,
		ColumnNameMap:       columnNameFieldMap,
		ExcludedColumns:     config.Exclude,
		Config:              config,
		ClickHouseConnector: c,
		Logger:              nil, // Will use default logging if needed
	}

	query, err := buildInsertFromTableFunctionQuery(ctx, insertConfig, urlTableFunction)
	if err != nil {
		return 0, fmt.Errorf("failed to build insert query: %w", err)
	}

	c.logger.Debug("Executing URL table function query",
		slog.String("url", object.URL),
		slog.String("format", format),
		slog.String("query", query))

	// todo: query insert with a profile to get actual records inserted
	if err := c.exec(ctx, query); err != nil {
		return 0, err
	}

	return 0, nil
}

// buildURLTableFunction builds a ClickHouse URL table function with headers and format
func (c *ClickHouseConnector) buildURLTableFunction(object *model.Object, format string) string {
	var expr strings.Builder

	// Start with url function: url(URL, [headers(...), ] format)
	expr.WriteString("url(")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(object.URL))

	// Add headers if present
	if len(object.Headers) > 0 {
		expr.WriteString(", headers(")
		first := true
		for name, values := range object.Headers {
			// Use the first value for each header name
			if len(values) > 0 {
				if !first {
					expr.WriteString(", ")
				}
				expr.WriteString(peerdb_clickhouse.QuoteIdentifier(name))
				expr.WriteString("=")
				expr.WriteString(peerdb_clickhouse.QuoteLiteral(values[0]))
				first = false
			}
		}
		expr.WriteString(")")
	}

	// Specify format
	expr.WriteString(", ")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(format))
	expr.WriteString(")")

	return expr.String()
}
