package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// objectBatch holds a group of objects that can be inserted together
// (same headers, cumulative size within limit)
type objectBatch struct {
	objects []*model.Object
	size    int64
}

func (b *objectBatch) headers() http.Header {
	return b.objects[len(b.objects)-1].Headers
}

const defaultBatchFlushInterval = 5 * time.Second

// collectAndBatchObjects collects objects from stream and groups them into batches
// based on size limits. It returns a channel that receives batches as they become
// ready (either full or after a flush interval).
// The channel is closed when the stream is exhausted or context is cancelled.
// Note: All objects in a batch are assumed to have the same headers; the batch
// uses headers from the most recent object.
func collectAndBatchObjects(
	ctx context.Context,
	stream *model.QObjectStream,
	maxBatchSize int64,
	flushInterval time.Duration,
) <-chan *objectBatch {
	batches := make(chan *objectBatch)

	go func() {
		defer close(batches)

		var currentBatch *objectBatch
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		flushCurrentBatch := func() {
			if currentBatch != nil {
				select {
				case batches <- currentBatch:
				case <-ctx.Done():
					return
				}
				currentBatch = nil
			}
			ticker.Reset(flushInterval)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Flush on interval even if batch is not full
				flushCurrentBatch()
			case object, ok := <-stream.Objects:
				if !ok {
					// Stream closed, flush remaining batch
					flushCurrentBatch()
					return
				}

				// Check if we can add to current batch (size limit check only)
				if currentBatch != nil && currentBatch.size+object.Size <= maxBatchSize {
					currentBatch.objects = append(currentBatch.objects, object)
					currentBatch.size += object.Size
				} else {
					flushCurrentBatch()
					currentBatch = &objectBatch{
						objects: []*model.Object{object},
						size:    object.Size,
					}
				}
			}
		}
	}()

	return batches
}

// SyncQRepObjects syncs data from downloadable objects (URLs) to ClickHouse
// Supports all ClickHouse formats based on the stream's Format field
// Objects are batched by matching headers and size limits to reduce INSERT operations
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
		slog.String("format", string(format)))

	schema, err := stream.Schema()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get schema from stream: %w", err)
	}

	// For ClickHouse records sync we have Avro file size limits.
	// We reuse the same value as a good heuristic for batching inserts.
	maxBatchSize, err := internal.PeerDBS3BytesPerAvroFile(ctx, config.Env)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get batch size config: %w", err)
	}

	batches := collectAndBatchObjects(ctx, stream, maxBatchSize, defaultBatchFlushInterval)

	c.logger.Info("Started batching objects for processing",
		slog.Int64("maxBatchSize", maxBatchSize),
		slog.Duration("flushInterval", defaultBatchFlushInterval))

	columnNameFieldMap := c.constructColumnNameFieldMap(schema.Fields)

	var totalRecords int64
	var totalBatches int
	warnings := shared.QRepWarnings{}

	for batch := range batches {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}

		c.logger.Debug("Processing batch",
			slog.Int("batchIndex", totalBatches),
			slog.Int("objectCount", len(batch.objects)),
			slog.Int64("batchSize", batch.size))

		recordsInserted, err := c.insertFromURLBatch(
			ctx,
			config,
			batch,
			schema,
			columnNameFieldMap,
			string(format),
			objectSyncBigQueryExportFieldExpressionConverters...,
		)
		if err != nil {
			urls := make([]string, len(batch.objects))
			for i, obj := range batch.objects {
				urls[i] = obj.URL
			}
			c.logger.Error("Failed to insert batch",
				slog.Any("urls", urls),
				slog.Any("error", err))
			return 0, nil, fmt.Errorf("failed to insert batch: %w", err)
		}

		totalRecords += recordsInserted
		totalBatches++
		c.logger.Info("Successfully processed batch",
			slog.Int("batchIndex", totalBatches-1),
			slog.Int("objectCount", len(batch.objects)),
			slog.Int64("recordsInserted", recordsInserted))
	}

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
		slog.Int64("totalRecords", totalRecords),
		slog.Int("totalBatches", totalBatches))

	return totalRecords, warnings, nil
}

func (c *ClickHouseConnector) constructColumnNameFieldMap(fields []types.QField) map[string]string {
	columnNameFieldMap := make(map[string]string)

	for _, field := range fields {
		columnNameFieldMap[field.Name] = field.Name
	}

	return columnNameFieldMap
}

// insertFromURLBatch inserts data from a batch of URLs using proper column mapping
// Uses ClickHouse's brace expansion for multiple URLs: url('{url1,url2}', ...)
func (c *ClickHouseConnector) insertFromURLBatch(
	ctx context.Context,
	config *protos.QRepConfig,
	batch *objectBatch,
	schema types.QRecordSchema,
	columnNameFieldMap map[string]string,
	format string,
	fieldExpressionConverters ...fieldExpressionConverter,
) (int64, error) {
	urlTableFunction := c.buildURLTableFunction(batch, format)

	insertConfig := &insertFromTableFunctionConfig{
		destinationTable:          config.DestinationTableIdentifier,
		schema:                    schema,
		columnNameMap:             columnNameFieldMap,
		excludedColumns:           config.Exclude,
		config:                    config,
		connector:                 c,
		logger:                    c.logger,
		fieldExpressionConverters: fieldExpressionConverters,
	}

	query, err := buildInsertFromTableFunctionQuery(ctx, insertConfig, urlTableFunction, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to build insert query: %w", err)
	}

	c.logger.Debug("Executing URL table function query",
		slog.Int("urlCount", len(batch.objects)),
		slog.String("format", format),
		slog.String("query", query))

	var totalRows int64
	execCtx := clickhouse.Context(ctx, clickhouse.WithProgress(func(info *clickhouse.Progress) {
		totalRows += int64(info.Rows)
	}))

	if err := c.exec(execCtx, query); err != nil {
		return 0, err
	}

	return totalRows, nil
}

// buildURLTableFunction builds a ClickHouse URL table function with headers and format
// Supports multiple URLs using brace expansion: url('{url1,url2,url3}', ...)
func (c *ClickHouseConnector) buildURLTableFunction(batch *objectBatch, format string) string {
	var expr strings.Builder

	// Start with url function: url(URL, [headers(...), ] format)
	expr.WriteString("url(")

	// Build URL string - use brace expansion for multiple URLs
	if len(batch.objects) == 1 {
		expr.WriteString(peerdb_clickhouse.QuoteLiteral(batch.objects[0].URL))
	} else {
		// Multiple URLs: use brace expansion {url1,url2,url3}
		var urlList strings.Builder
		urlList.WriteString("{")
		for i, obj := range batch.objects {
			if i > 0 {
				urlList.WriteString(",")
			}
			urlList.WriteString(obj.URL)
		}
		urlList.WriteString("}")
		expr.WriteString(peerdb_clickhouse.QuoteLiteral(urlList.String()))
	}

	if headers := batch.headers(); len(headers) > 0 {
		expr.WriteString(", headers(")
		first := true
		for name, values := range headers {
			// Use the first value for each header name
			if len(values) == 0 {
				continue
			}

			if !first {
				expr.WriteString(", ")
			}
			expr.WriteString(peerdb_clickhouse.QuoteIdentifier(name))
			expr.WriteString("=")
			expr.WriteString(peerdb_clickhouse.QuoteLiteral(values[0]))
			first = false
		}
		expr.WriteString(")")
	}

	// Specify format
	expr.WriteString(", ")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(format))
	expr.WriteString(")")

	return expr.String()
}
