package connsnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	fnv "hash/fnv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// maxStreamingChannelsPerFlow caps the channel pool to stay well under Snowflake's
// 2000-channels-per-pipe ceiling while leaving headroom for parallel mirrors sharing
// the same pipe.
const maxStreamingChannelsPerFlow = 64

// routeChannelIdx returns the channel index for a destination table. Hashing by
// destination table name (not primary key) preserves per-table ordering, which is
// required by NormalizeRecords MERGE semantics.
func routeChannelIdx(destinationTable string, channelCount int) int {
	if channelCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(destinationTable))
	return int(h.Sum32() % uint32(channelCount))
}

// rawTableColumns is the fixed column order for the raw table Snowpipe.
// It must match createRawTableSQL exactly and never changes — the raw table schema
// is stable regardless of destination table evolution.
var rawTableColumns = []string{
	"_PEERDB_UID",
	"_PEERDB_TIMESTAMP",
	"_PEERDB_DESTINATION_TABLE_NAME",
	"_PEERDB_DATA",
	"_PEERDB_RECORD_TYPE",
	"_PEERDB_MATCH_DATA",
	"_PEERDB_BATCH_ID",
	"_PEERDB_UNCHANGED_TOAST_COLUMNS",
}

// rawStreamingBuffer accumulates NDJSON rows destined for the raw table.
type rawStreamingBuffer struct {
	encodedRows [][]byte
	currentSize int
}

// rawRow is the JSON shape for each NDJSON line sent to the raw table via Snowpipe.
// Field names must match rawTableColumns exactly (Snowpipe maps $1:<field> by name).
type rawRow struct {
	UID              string `json:"_PEERDB_UID"`
	Timestamp        int64  `json:"_PEERDB_TIMESTAMP"`
	DestinationTable string `json:"_PEERDB_DESTINATION_TABLE_NAME"`
	Data             string `json:"_PEERDB_DATA"`
	RecordType       int64  `json:"_PEERDB_RECORD_TYPE"`
	MatchData        string `json:"_PEERDB_MATCH_DATA"`
	BatchID          int64  `json:"_PEERDB_BATCH_ID"`
	UnchangedToast   string `json:"_PEERDB_UNCHANGED_TOAST_COLUMNS"`
}

// buildRawStreamingRow converts a CDC record to an NDJSON line for the raw table.
//
// Field mapping follows the same convention as utils.RecordsToRawTableStream:
//   - INSERT:  data=Items,       matchData="",        recordType=0
//   - UPDATE:  data=NewItems,    matchData=OldItems,  recordType=1
//   - DELETE:  data=Items,       matchData=Items,     recordType=2
//
// This means NormalizeRecords / mergeTablesForBatch can run unchanged after streaming sync,
// since the raw table rows are identical in structure to the Avro/S3 path.
func buildRawStreamingRow(record model.Record[model.RecordItems], syncBatchID int64) ([]byte, error) {
	row := rawRow{
		UID:              uuid.New().String(),
		Timestamp:        time.Now().UnixNano(),
		DestinationTable: record.GetDestinationTableName(),
		BatchID:          syncBatchID,
	}

	var err error
	switch rec := record.(type) {
	case *model.InsertRecord[model.RecordItems]:
		row.Data, err = model.ItemsToJSON(rec.Items)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize insert items: %w", err)
		}
		row.RecordType = 0
		// MatchData and UnchangedToast default to "" (zero value)

	case *model.UpdateRecord[model.RecordItems]:
		row.Data, err = model.ItemsToJSON(rec.NewItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update new items: %w", err)
		}
		row.MatchData, err = model.ItemsToJSON(rec.OldItems)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize update old items: %w", err)
		}
		row.RecordType = 1
		row.UnchangedToast = utils.KeysToString(rec.UnchangedToastColumns)

	case *model.DeleteRecord[model.RecordItems]:
		row.Data, err = model.ItemsToJSON(rec.Items)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize delete items: %w", err)
		}
		row.RecordType = 2
		row.MatchData = row.Data // DELETE: match data is same as data
		row.UnchangedToast = utils.KeysToString(rec.UnchangedToastColumns)

	default:
		return nil, fmt.Errorf("unknown record type in streaming path: %T", record)
	}

	b, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal raw streaming row: %w", err)
	}
	return append(b, '\n'), nil // NDJSON: one JSON object per line
}

// syncRecordsViaStreaming streams CDC records to the _PEERDB_STREAMING_* table via the
// Snowpipe Streaming REST API. NormalizeRecords runs a MERGE afterwards — identical
// to the Avro/S3 path but without the staging hop.
//
// The streaming path uses a dedicated raw table (_PEERDB_STREAMING_{job}) distinct from
// the non-streaming raw table (_PEERDB_RAW_{job}), so the two paths never share rows.
//
//   - CDC records → _PEERDB_INTERNAL._PEERDB_STREAMING_<job> (via Snowpipe Streaming)
//   - NormalizeRecords → mergeTablesForBatch (unchanged, reads from _PEERDB_STREAMING_<job>)
//
// This preserves correct UPDATE/DELETE/TOAST semantics through the existing MERGE
// logic while eliminating the Avro → S3 → COPY INTO latency cost.
func (c *SnowflakeConnector) syncRecordsViaStreaming(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
	syncBatchID int64,
) (*model.SyncResponse, error) {
	// Idempotency guard: if this batch was already committed, skip re-sending rows.
	// This prevents duplicates when Temporal retries the activity after a heartbeat timeout.
	lastBatchID, err := c.GetLastSyncBatchID(ctx, req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get last sync batch ID: %w", err)
	}
	if syncBatchID <= lastBatchID {
		c.logger.Info("[streaming] batch already synced, skipping",
			"flowJobName", req.FlowJobName,
			"syncBatchID", syncBatchID,
			"lastCommittedBatchID", lastBatchID,
		)
		// Drain the records channel so the producer goroutine can finish cleanly.
		for range req.Records.GetRecords() {
		}
		return &model.SyncResponse{
			LastSyncedCheckpoint: req.Records.GetLastCheckpoint(),
			NumRecordsSynced:     0,
			CurrentSyncBatchID:   syncBatchID,
			TableNameRowsMapping: utils.InitialiseTableRowsMap(req.TableMappings),
			TableSchemaDeltas:    nil,
		}, nil
	}

	if err := c.streamingManager.refreshAuth(ctx); err != nil {
		return nil, fmt.Errorf("failed to refresh streaming auth: %w", err)
	}

	// Resolve raw table coordinates. The raw table lives in rawSchema, which defaults
	// to _PEERDB_INTERNAL. Both are uppercased to match Snowflake's identifier convention.
	db := strings.ToUpper(c.config.Database)
	rawSchema := strings.ToUpper(c.rawSchema)
	rawTable := strings.ToUpper(getStreamingRawTableIdentifier(req.FlowJobName))
	// Pipe and channel are scoped to the flow job to prevent conflicts when multiple
	// mirrors share the same Snowflake account.
	sanitizedJob := strings.ToUpper(shared.ReplaceIllegalCharactersWithUnderscores(req.FlowJobName))
	pipeName := sanitizedJob + "_STREAMING_PIPE"

	// Channel pool size: distribute load across N channels by destination-table-name
	// hash. Per-table ordering is preserved (a given destination table always routes
	// to the same channel within a batch). N=1 keeps the original channel name
	// "peerdb-{job}-raw-0" so existing mirrors retain their durable offsetTokens.
	channelsPerFlow, err := internal.PeerDBSnowflakeStreamingChannelsPerFlow(ctx, req.Env)
	if err != nil {
		return nil, fmt.Errorf("failed to read streaming channels-per-flow: %w", err)
	}
	if channelsPerFlow < 1 {
		channelsPerFlow = 1
	}
	if channelsPerFlow > maxStreamingChannelsPerFlow {
		c.logger.Warn("[streaming] channels-per-flow exceeds Snowflake limit, capping",
			"requested", channelsPerFlow, "cap", maxStreamingChannelsPerFlow)
		channelsPerFlow = maxStreamingChannelsPerFlow
	}

	// Ensure the raw table pipe exists. CREATE PIPE IF NOT EXISTS is atomic on Snowflake
	// and preserves existing channels (their continuationTokens stay valid for idempotency
	// on Temporal retries). Raw table columns are fixed at compile time, so the pipe never
	// needs schema-driven recreation.
	if err := c.createStreamingPipe(ctx, db, rawSchema, rawTable, pipeName, rawTableColumns); err != nil {
		return nil, fmt.Errorf("failed to create raw table streaming pipe: %w", err)
	}

	channels := make([]*StreamingChannel, channelsPerFlow)
	channelNames := make([]string, channelsPerFlow)
	for i := int64(0); i < channelsPerFlow; i++ {
		channelNames[i] = fmt.Sprintf("peerdb-%s-raw-%d", req.FlowJobName, i)
		ch, err := c.streamingManager.getOrCreateChannel(ctx, db, rawSchema, pipeName, channelNames[i], false)
		if err != nil {
			return nil, fmt.Errorf("failed to get/create streaming channel %s: %w", channelNames[i], err)
		}
		channels[i] = ch
	}

	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	bufs := make([]*rawStreamingBuffer, channelsPerFlow)
	flushIndices := make([]int, channelsPerFlow)
	for i := range bufs {
		bufs[i] = &rawStreamingBuffer{}
	}

	for record := range req.Records.GetRecords() {
		// MessageRecord carries no CDC data; skip it.
		if _, ok := record.(*model.MessageRecord[model.RecordItems]); ok {
			continue
		}

		rowBytes, err := buildRawStreamingRow(record, syncBatchID)
		if err != nil {
			return nil, fmt.Errorf("failed to build raw streaming row: %w", err)
		}

		// Route by destination-table hash so all rows for a given table land on the
		// same channel — preserves per-table ordering required by NormalizeRecords MERGE.
		idx := routeChannelIdx(record.GetDestinationTableName(), int(channelsPerFlow))
		bufs[idx].encodedRows = append(bufs[idx].encodedRows, rowBytes)
		bufs[idx].currentSize += len(rowBytes)
		record.PopulateCountMap(tableNameRowsMapping)

		// Flush mid-batch if any single buffer reaches the chunk size limit.
		if bufs[idx].currentSize >= c.streamingManager.maxChunkBytes {
			if err := c.flushRawBuffer(ctx, db, rawSchema, pipeName, channelNames[idx], channels[idx], bufs[idx], syncBatchID, &flushIndices[idx]); err != nil {
				return nil, fmt.Errorf("failed to flush raw buffer mid-batch on channel %s: %w", channelNames[idx], err)
			}
		}
	}

	for i, buf := range bufs {
		if len(buf.encodedRows) > 0 {
			if err := c.flushRawBuffer(ctx, db, rawSchema, pipeName, channelNames[i], channels[i], buf, syncBatchID, &flushIndices[i]); err != nil {
				return nil, fmt.Errorf("failed to flush final raw buffer on channel %s: %w", channelNames[i], err)
			}
		}
	}

	// Schema deltas come from req.Records.SchemaDeltas, populated by AddSchemaDelta in the
	// pull loop. RelationRecords are never added to the records channel — they go directly
	// to CDCStream.SchemaDeltas. This matches the non-streaming path.
	schemaDeltas := req.Records.SchemaDeltas
	if len(schemaDeltas) > 0 {
		if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.TableMappings, schemaDeltas, nil); err != nil {
			return nil, fmt.Errorf("failed to replay schema deltas: %w", err)
		}
	}

	var totalRows int64
	for _, counts := range tableNameRowsMapping {
		totalRows += int64(counts.InsertCount.Load()) +
			int64(counts.UpdateCount.Load()) +
			int64(counts.DeleteCount.Load())
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: req.Records.GetLastCheckpoint(),
		NumRecordsSynced:     totalRows,
		CurrentSyncBatchID:   syncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    schemaDeltas,
	}, nil
}

// flushRawBuffer sends buffered raw table rows to Snowpipe Streaming and waits for
// delivery confirmation. Resets encodedRows and currentSize after a successful flush.
//
// syncBatchID and a per-call flushIndex are combined to produce an offsetToken for
// each chunk. Snowflake persists the offsetToken durably on the channel and treats
// a second Append Rows call with the same token as a no-op, closing the narrow
// at-least-once window that exists between row confirmation and FinishBatch.
func (c *SnowflakeConnector) flushRawBuffer(
	ctx context.Context,
	db, schema, pipe, channelName string,
	ch *StreamingChannel,
	buf *rawStreamingBuffer,
	syncBatchID int64,
	flushIndex *int,
) error {
	if len(buf.encodedRows) == 0 {
		return nil
	}

	rowCount := len(buf.encodedRows)
	flushStart := time.Now()
	c.logger.Info("[streaming] flushing raw buffer",
		"pipe", pipe, "channel", channelName, "rows", rowCount)

	// Concatenate rows into NDJSON chunks, splitting at maxChunkBytes.
	// Each chunk gets a unique offsetToken: "<syncBatchID>-<chunkIndex>".
	var currentChunk bytes.Buffer
	chunkCount := 0
	totalBytes := 0
	for _, rowBytes := range buf.encodedRows {
		if currentChunk.Len()+len(rowBytes) > c.streamingManager.maxChunkBytes && currentChunk.Len() > 0 {
			offsetToken := fmt.Sprintf("%d-%d", syncBatchID, *flushIndex)
			*flushIndex++
			if err := c.streamingManager.sendChunkWithRetry(ctx, db, schema, pipe, channelName, ch, currentChunk.Bytes(), offsetToken); err != nil {
				return fmt.Errorf("failed to send raw chunk: %w", err)
			}
			totalBytes += currentChunk.Len()
			chunkCount++
			currentChunk.Reset()
		}
		currentChunk.Write(rowBytes)
	}
	if currentChunk.Len() > 0 {
		offsetToken := fmt.Sprintf("%d-%d", syncBatchID, *flushIndex)
		*flushIndex++
		if err := c.streamingManager.sendChunkWithRetry(ctx, db, schema, pipe, channelName, ch, currentChunk.Bytes(), offsetToken); err != nil {
			return fmt.Errorf("failed to send final raw chunk: %w", err)
		}
		totalBytes += currentChunk.Len()
		chunkCount++
	}

	c.logger.Info("[streaming] rows sent to raw table via Snowpipe",
		"pipe", pipe, "rows", rowCount, "chunks", chunkCount, "bytes", totalBytes)

	// Verify that Snowflake committed all rows. waitForChannelIngestion polls the
	// bulk channel status endpoint until RowsInserted >= baseline + rowCount.
	if err := c.waitForChannelIngestion(ctx, db, schema, pipe, channelName, ch, rowCount, flushStart); err != nil {
		return fmt.Errorf("raw table streaming ingestion not confirmed for pipe %s: %w", pipe, err)
	}

	buf.encodedRows = buf.encodedRows[:0]
	buf.currentSize = 0
	return nil
}

// waitForChannelIngestion polls the Snowpipe Streaming bulk channel status endpoint until
// the channel confirms rows were committed or reports ingestion errors.
//
// This replaces SYSTEM$PIPE_STATUS polling because the channel-level endpoint:
//   - Exposes LastErrorMessage for immediate error detection (e.g. schema/type mismatches)
//   - Reports RowsInserted so we can confirm our specific batch was committed
//   - Gives per-channel granularity instead of per-pipe
//
// Delivery is confirmed when RowsInserted >= baseline + rowsSent, where baseline is the
// cumulative count captured from the channel at the last successful flush.
func (c *SnowflakeConnector) waitForChannelIngestion(
	ctx context.Context,
	db, schema, pipe, channelName string,
	ch *StreamingChannel,
	rowsSent int,
	flushStart time.Time,
) error {
	baseline := ch.getRowsInserted()
	timeout := time.Duration(c.streamingManager.ingestTimeoutSeconds) * time.Second
	deadline := time.Now().Add(timeout)
	pollInterval := 3 * time.Second

	for time.Now().Before(deadline) {
		statuses, err := c.streamingManager.GetChannelStatus(ctx, db, schema, pipe, []string{channelName})
		if err != nil {
			// Transient network/auth error — log and retry rather than failing fast.
			c.logger.Warn("[streaming] failed to get channel status, retrying", "channel", channelName, "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		status, ok := statuses[channelName]
		if !ok {
			// Channel may not be visible yet; retry.
			c.logger.Warn("[streaming] channel not in status response, retrying", "channel", channelName)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		c.logger.Info("[streaming] channel status",
			"channel", channelName,
			"rowsInserted", status.RowsInserted,
			"rowsErrors", status.RowsErrorCount,
			"latencyMs", status.SnowflakeAvgProcessingLatencyMs,
		)

		// Fail fast: Snowflake reports a parsing or schema error for this batch.
		if status.RowsErrorCount > 0 && status.LastErrorMessage != "" {
			c.streamingManager.metrics.rowsErroredCounter.Add(ctx, int64(status.RowsErrorCount))
			return fmt.Errorf("snowpipe streaming ingestion error for channel %s: %s", channelName, status.LastErrorMessage)
		}

		// Success: all rows in this flush have been committed.
		if status.RowsInserted >= baseline+rowsSent {
			ch.setRowsInserted(status.RowsInserted)
			commitLagMs := time.Since(flushStart).Milliseconds()
			c.streamingManager.metrics.rowsConfirmedCounter.Add(ctx, int64(rowsSent))
			c.streamingManager.metrics.ingestionLatencyGauge.Record(ctx, int64(status.SnowflakeAvgProcessingLatencyMs))
			c.streamingManager.metrics.commitLagGauge.Record(ctx, commitLagMs)
			c.logger.Info("[streaming] ingestion confirmed",
				"channel", channelName,
				"rowsInserted", status.RowsInserted,
				"baseline", baseline,
				"commitLagMs", commitLagMs,
			)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}

	return fmt.Errorf("timed out waiting for channel ingestion on %s (pipe %s.%s.%s) after %v",
		channelName, db, schema, pipe, timeout)
}

// createStreamingPipe ensures the Snowpipe object exists for a given table using
// CREATE PIPE IF NOT EXISTS. The DDL is atomic on Snowflake and preserves existing
// channels — repeated calls across sync batches are safe and effectively no-ops.
//
// columnNames must be non-empty; the raw-table caller passes the fixed rawTableColumns.
func (c *SnowflakeConnector) createStreamingPipe(
	ctx context.Context,
	db, schema, table, pipe string,
	columnNames []string,
) error {
	tableFQN := fmt.Sprintf("%s.%s.%s", db, schema, table)
	pipeFQN := fmt.Sprintf("%s.%s.%s", db, schema, pipe)

	selectParts := make([]string, len(columnNames))
	colParts := make([]string, len(columnNames))
	for i, col := range columnNames {
		selectParts[i] = fmt.Sprintf(`$1:"%s"`, col)
		colParts[i] = fmt.Sprintf(`"%s"`, col)
	}
	createPipeSQL := fmt.Sprintf(
		`CREATE PIPE IF NOT EXISTS %s AS COPY INTO %s(%s) FROM (SELECT %s FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))`,
		pipeFQN, tableFQN, strings.Join(colParts, ", "), strings.Join(selectParts, ", "),
	)

	if _, err := c.ExecContext(ctx, createPipeSQL); err != nil {
		return fmt.Errorf("failed to execute CREATE PIPE for %s: %w", pipeFQN, err)
	}
	return nil
}
