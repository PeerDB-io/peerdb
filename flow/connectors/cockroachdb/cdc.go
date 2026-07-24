package conncockroachdb

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// changefeedSlotName is a placeholder slot identifier: CockroachDB has no
// replication slots, but a non-empty name makes the snapshot workflow carry
// the captured system time into the QRep AS OF SYSTEM TIME reads.
const changefeedSlotName = "changefeed"

const resolvedLagWarnThreshold = time.Hour

func (c *CockroachDBConnector) clusterLogicalTimestamp(ctx context.Context) (string, error) {
	var systemTime string
	if err := c.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()::text").Scan(&systemTime); err != nil {
		return "", fmt.Errorf("failed to get cluster logical timestamp: %w", err)
	}
	return systemTime, nil
}

func (c *CockroachDBConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

// ExportTxSnapshot captures the current HLC timestamp so that
// initial-snapshot-only mirrors read all tables at one consistent system time.
func (c *CockroachDBConnector) ExportTxSnapshot(
	ctx context.Context, flowName string, env map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	systemTime, err := c.clusterLogicalTimestamp(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &protos.ExportTxSnapshotOutput{SnapshotName: systemTime}, nil, nil
}

func (c *CockroachDBConnector) FinishExport(any) error {
	return nil
}

// SetupReplication captures the current HLC timestamp as the consistent
// handoff point: the initial snapshot reads AS OF SYSTEM TIME at it, and the
// changefeed later resumes WITH cursor at it, so there is no gap or overlap.
func (c *CockroachDBConnector) SetupReplication(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	systemTime, err := c.clusterLogicalTimestamp(ctx)
	if err != nil {
		return model.SetupReplicationResult{}, err
	}
	wallNanos, err := parseHLCWallNanos(systemTime)
	if err != nil {
		return model.SetupReplicationResult{}, err
	}
	if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{ID: wallNanos, Text: systemTime}); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to store initial changefeed cursor: %w", err)
	}
	c.logger.Info("[cockroachdb] SetupReplication stored initial changefeed cursor", slog.String("cursor", systemTime))
	return model.SetupReplicationResult{SlotName: changefeedSlotName, SnapshotName: systemTime}, nil
}

func (c *CockroachDBConnector) SetupReplConn(context.Context, map[string]string) error {
	// changefeeds open a dedicated connection per PullRecords call
	return nil
}

func (c *CockroachDBConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	if lastOffset.Text == "" {
		return nil
	}
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

func (c *CockroachDBConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	// sinkless changefeeds end with their connection; nothing to clean up
	return nil
}

// changefeedDedupKey identifies one changefeed data message: a reconnect
// replays from the last resolved timestamp, so messages between it and the
// failure point are delivered again within the same PullRecords batch.
type changefeedDedupKey struct {
	table   string
	key     string
	updated string
}

// maxChangefeedDedupEntries bounds the replay dedup set; overflowing it trades
// possible duplicates for bounded memory.
const maxChangefeedDedupEntries = 1 << 20

// changefeedPullState carries the per-batch state shared between changefeed
// sessions (a session being one sinkless changefeed query on one connection).
//
//nolint:govet // keeping related fields together over alignment
type changefeedPullState struct {
	req               *model.PullRecordsRequest[model.RecordItems]
	tables            []*common.QualifiedTable
	sourceByEmitted   map[string]string
	schemas           map[string]*changefeedTableSchema
	seenSinceResolved map[changefeedDedupKey]struct{}
	unmappedTables    map[string]struct{}
	cursor            string
	resolvedInterval  time.Duration
	recordCount       uint32
	messagesReceived  uint64
	batchDeadline     time.Time
	graceDeadline     time.Time
	lastLagWarnAt     time.Time
	deltaBytes        atomic.Int64
	totalBytes        atomic.Int64
}

// indexSource registers a source table under every normalized form of its
// emitted full_table_name so lookups tolerate quoting and case differences.
func (state *changefeedPullState) indexSource(emitted string, source string) {
	for _, key := range tableRoutingKeys(emitted) {
		if _, present := state.sourceByEmitted[key]; !present {
			state.sourceByEmitted[key] = source
		}
	}
}

func (state *changefeedPullState) lookupSource(emitted string) (string, bool) {
	for _, key := range tableRoutingKeys(emitted) {
		if source, ok := state.sourceByEmitted[key]; ok {
			return source, true
		}
	}
	return "", false
}

// dedupSeen reports whether this data message was already processed since the
// last resolved timestamp, recording it otherwise. handleResolved clears the
// set, since a reconnect never replays past the last resolved cursor.
func (state *changefeedPullState) dedupSeen(logger log.Logger, table string, key []byte, updated string) bool {
	dedupKey := changefeedDedupKey{table: table, key: string(key), updated: updated}
	if _, seen := state.seenSinceResolved[dedupKey]; seen {
		return true
	}
	if len(state.seenSinceResolved) >= maxChangefeedDedupEntries {
		logger.Warn("[cockroachdb] changefeed replay dedup set overflowed, clearing;"+
			" a reconnect before the next resolved timestamp may emit duplicates",
			slog.Int("entries", len(state.seenSinceResolved)))
		clear(state.seenSinceResolved)
	}
	state.seenSinceResolved[dedupKey] = struct{}{}
	return false
}

// watchdogWindow returns how long the stream may stay silent before the
// session is hard-cancelled. Resolved timestamps normally arrive every
// resolvedInterval, so silence beyond these windows means a stalled stream.
func (state *changefeedPullState) watchdogWindow() time.Duration {
	stallWindow := max(30*time.Second, 5*state.resolvedInterval)
	if !state.graceDeadline.IsZero() {
		return max(time.Until(state.graceDeadline), time.Second)
	}
	if !state.batchDeadline.IsZero() {
		return max(time.Until(state.batchDeadline)+2*state.resolvedInterval, time.Second)
	}
	return stallWindow
}

func (c *CockroachDBConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	cursor := req.LastOffset.Text
	if cursor == "" {
		// SetupReplication seeds the offset; this is a defensive fallback
		var err error
		if cursor, err = c.clusterLogicalTimestamp(ctx); err != nil {
			return err
		}
		c.logger.Warn("[cockroachdb] no stored changefeed cursor, starting from current system time",
			slog.String("cursor", cursor))
	}
	cursorWall, err := parseHLCWallNanos(cursor)
	if err != nil {
		return err
	}

	state := &changefeedPullState{
		req:               req,
		tables:            make([]*common.QualifiedTable, 0, len(req.TableNameMapping)),
		sourceByEmitted:   make(map[string]string, len(req.TableNameMapping)),
		schemas:           make(map[string]*changefeedTableSchema, len(req.TableNameMapping)),
		seenSinceResolved: make(map[changefeedDedupKey]struct{}),
		unmappedTables:    make(map[string]struct{}),
		cursor:            cursor,
		resolvedInterval:  changefeedResolvedInterval(req.IdleTimeout),
	}
	for source, target := range req.TableNameMapping {
		parsed, err := common.ParseTableIdentifier(source)
		if err != nil {
			return fmt.Errorf("invalid source table %s: %w", source, err)
		}
		state.tables = append(state.tables, parsed)
		// full_table_name makes the changefeed emit db.schema.table
		state.indexSource(fmt.Sprintf("%s.%s.%s", c.Config.Database, parsed.Namespace, parsed.Table), source)
		if schema, ok := req.TableNameSchemaMapping[target.Name]; ok {
			state.schemas[source] = newChangefeedTableSchema(schema)
		}
	}
	slices.SortFunc(state.tables, func(a, b *common.QualifiedTable) int {
		return strings.Compare(a.String(), b.String())
	})
	if len(state.tables) >= changefeedManyTablesThreshold {
		c.logger.Warn("[cockroachdb] a single changefeed covers many tables, coupling their throughput and checkpointing",
			slog.Int("tables", len(state.tables)))
	}

	// seed the checkpoint so a batch that ends without a fresh resolved
	// timestamp re-persists the prior cursor instead of clearing it
	req.RecordStream.UpdateLatestCheckpointText(cursor)
	req.RecordStream.UpdateLatestCheckpointID(cursorWall)

	c.logger.Info("[cockroachdb] started PullRecords for mirror "+req.FlowJobName,
		slog.String("cursor", cursor),
		slog.Uint64("maxBatchSize", uint64(req.MaxBatchSize)),
		slog.Duration("syncInterval", req.IdleTimeout))

	pullStart := time.Now()
	defer func() {
		if state.recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64(otel_metrics.RowsInBatchKey, int64(state.recordCount)),
			attribute.Int64(otel_metrics.BytesPulledKey, state.totalBytes.Load()),
		)
		c.logger.Info("[cockroachdb] PullRecords finished streaming",
			slog.Uint64("records", uint64(state.recordCount)),
			slog.Int64("bytes", state.totalBytes.Load()),
			slog.String("cursor", state.cursor),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()

	reportBytesShutdown := common.Interval(ctx, 10*time.Second, func() {
		read := state.deltaBytes.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	})
	defer func() {
		reportBytesShutdown()
		read := state.deltaBytes.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	}()

	maxRetries, baseDelay := changefeedRetryConfig(c.Config)
	var attempt uint32
	for {
		messagesBefore := state.messagesReceived
		done, err := c.runChangefeedSession(ctx, otelManager, state)
		if done {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if state.messagesReceived > messagesBefore {
			attempt = 0
		}
		if err == nil {
			// watchdog fired without records: reconnect and keep waiting
			continue
		}
		if isCursorTooOldError(err) {
			return fmt.Errorf("changefeed cursor %s is older than the replica GC threshold and cannot resume;"+
				" increase gc.ttlseconds on the source tables or resync the mirror: %w", state.cursor, err)
		}
		if isPermanentChangefeedError(err) {
			return err
		}
		attempt++
		if attempt > maxRetries {
			return fmt.Errorf("changefeed failed after %d retries: %w", maxRetries, err)
		}
		delay := changefeedBackoff(baseDelay, attempt)
		c.logger.Warn("[cockroachdb] changefeed interrupted, reconnecting",
			slog.Uint64("attempt", uint64(attempt)),
			slog.Duration("backoff", delay),
			slog.String("cursor", state.cursor),
			slog.Any("error", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// runChangefeedSession executes one sinkless changefeed query on a dedicated
// connection and consumes messages until the batch completes (done=true) or
// the stream fails. Sinkless changefeeds never finish on their own and are
// only terminated by killing their connection, hence the watchdog + cancel.
func (c *CockroachDBConnector) runChangefeedSession(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	state *changefeedPullState,
) (bool, error) {
	stmt, err := buildChangefeedStatement(state.tables, changefeedOptions{
		Cursor:           state.cursor,
		ResolvedInterval: state.resolvedInterval,
	})
	if err != nil {
		return false, err
	}

	queryCtx, cancelQuery := context.WithCancel(ctx)
	defer cancelQuery()

	connConfig, err := ParseConfig(c.connStr, c.Config)
	if err != nil {
		return false, err
	}
	conn, err := NewCockroachDBConnFromConfig(queryCtx, connConfig, c.ssh)
	if err != nil {
		return false, fmt.Errorf("failed to open changefeed connection: %w", err)
	}
	defer func() {
		cancelQuery()
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := conn.Close(closeCtx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close changefeed connection", slog.Any("error", err))
		}
	}()

	watchdog := time.AfterFunc(state.watchdogWindow(), cancelQuery)
	defer watchdog.Stop()

	rows, err := conn.Query(queryCtx, stmt)
	if err != nil {
		return false, fmt.Errorf("failed to create changefeed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName pgtype.Text
		var key, value []byte
		if err := rows.Scan(&tableName, &key, &value); err != nil {
			return false, fmt.Errorf("failed to scan changefeed row: %w", err)
		}
		state.messagesReceived++
		messageBytes := int64(len(value))
		state.deltaBytes.Add(messageBytes)
		state.totalBytes.Add(messageBytes)

		envelope, err := parseChangefeedEnvelope(value)
		if err != nil {
			return false, err
		}

		if envelope.Resolved != "" {
			done, err := c.handleResolved(ctx, state, envelope.Resolved)
			if done || err != nil {
				return done, err
			}
		} else if tableName.Valid {
			if err := c.processChangefeedRow(ctx, otelManager, state, tableName.String, key, envelope); err != nil {
				return false, err
			}
		} else {
			c.logger.Warn("[cockroachdb] skipping changefeed message without table name")
		}

		now := time.Now()
		if !state.graceDeadline.IsZero() && !now.Before(state.graceDeadline) {
			return true, nil
		}
		if state.recordCount > 0 && !now.Before(state.batchDeadline.Add(2*state.resolvedInterval)) {
			// records are flowing but resolved timestamps stalled past the
			// idle deadline: cut the batch with the last known checkpoint
			return true, nil
		}
		watchdog.Reset(state.watchdogWindow())
	}

	err = rows.Err()
	switch {
	case ctx.Err() != nil:
		return false, ctx.Err()
	case queryCtx.Err() != nil:
		// watchdog fired: with records the batch is complete on the last
		// checkpoint, without records reconnect and keep waiting
		return state.recordCount > 0, nil
	case err != nil:
		return false, err
	default:
		return false, fmt.Errorf("changefeed stream ended unexpectedly")
	}
}

// handleResolved advances the checkpoint: resolved timestamps are the only
// safe resume points, as row timestamps between them may arrive out of order.
func (c *CockroachDBConnector) handleResolved(
	ctx context.Context, state *changefeedPullState, resolved string,
) (bool, error) {
	wallNanos, err := parseHLCWallNanos(resolved)
	if err != nil {
		return false, err
	}
	state.cursor = resolved
	state.req.RecordStream.UpdateLatestCheckpointText(resolved)
	state.req.RecordStream.UpdateLatestCheckpointID(wallNanos)
	// a reconnect replays at most back to this resolved timestamp, so older
	// dedup entries can never be delivered again
	clear(state.seenSinceResolved)

	if lag := time.Since(time.Unix(0, wallNanos)); lag > resolvedLagWarnThreshold &&
		time.Since(state.lastLagWarnAt) > time.Minute {
		state.lastLagWarnAt = time.Now()
		c.logger.Warn("[cockroachdb] changefeed resolved timestamp lags far behind;"+
			" if it falls behind gc.ttlseconds the mirror cannot resume",
			slog.Duration("lag", lag), slog.String("resolved", resolved))
	}

	now := time.Now()
	switch {
	case !state.graceDeadline.IsZero():
		return true, nil
	case state.recordCount > 0:
		return !now.Before(state.batchDeadline), nil
	default:
		// nothing handed to the sync flow yet, safe to persist directly so an
		// idle mirror's cursor keeps advancing ahead of the GC window
		if err := c.SetLastOffset(ctx, state.req.FlowJobName, model.CdcCheckpoint{ID: wallNanos, Text: resolved}); err != nil {
			c.logger.Error("[cockroachdb] failed to persist resolved timestamp",
				slog.String("resolved", resolved), slog.Any("error", err))
		}
		return false, nil
	}
}

func (c *CockroachDBConnector) processChangefeedRow(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	state *changefeedPullState,
	emittedTableName string,
	key []byte,
	envelope *changefeedEnvelope,
) error {
	source, ok := state.lookupSource(emittedTableName)
	if !ok {
		if _, logged := state.unmappedTables[emittedTableName]; !logged {
			state.unmappedTables[emittedTableName] = struct{}{}
			c.logger.Error("[cockroachdb] dropping changefeed messages for unmapped table",
				slog.String("table", emittedTableName))
		}
		return nil
	}
	schema, ok := state.schemas[source]
	if !ok {
		c.logger.Warn("[cockroachdb] skipping changefeed message for table without cached schema",
			slog.String("table", source))
		return nil
	}
	if state.dedupSeen(c.logger, emittedTableName, key, envelope.Updated) {
		return nil
	}
	operation := envelope.operation()

	commitWallNanos, err := parseHLCWallNanos(envelope.Updated)
	if err != nil {
		return fmt.Errorf("changefeed message for %s missing updated timestamp: %w", source, err)
	}

	nameAndExclude := state.req.TableNameMapping[source]
	if unknown := schema.unknownColumns(envelope.After, envelope.Before); len(unknown) > 0 {
		if err := c.emitSchemaDelta(ctx, state, source, nameAndExclude.Name, schema, unknown); err != nil {
			return err
		}
	}

	baseRecord := model.BaseRecord{CommitTimeNano: commitWallNanos}
	var record model.Record[model.RecordItems]
	switch operation {
	case changefeedOpInsert:
		items, err := changefeedRecordItems(envelope.After, schema, nameAndExclude.Exclude)
		if err != nil {
			return fmt.Errorf("failed to convert insert for %s: %w", source, err)
		}
		record = &model.InsertRecord[model.RecordItems]{
			BaseRecord:           baseRecord,
			Items:                items,
			SourceTableName:      source,
			DestinationTableName: nameAndExclude.Name,
		}
	case changefeedOpUpdate:
		newItems, err := changefeedRecordItems(envelope.After, schema, nameAndExclude.Exclude)
		if err != nil {
			return fmt.Errorf("failed to convert update for %s: %w", source, err)
		}
		oldItems, err := changefeedRecordItems(envelope.Before, schema, nameAndExclude.Exclude)
		if err != nil {
			return fmt.Errorf("failed to convert update for %s: %w", source, err)
		}
		record = &model.UpdateRecord[model.RecordItems]{
			BaseRecord:           baseRecord,
			NewItems:             newItems,
			OldItems:             oldItems,
			SourceTableName:      source,
			DestinationTableName: nameAndExclude.Name,
		}
	case changefeedOpDelete:
		items, err := changefeedRecordItems(envelope.Before, schema, nameAndExclude.Exclude)
		if err != nil {
			return fmt.Errorf("failed to convert delete for %s: %w", source, err)
		}
		record = &model.DeleteRecord[model.RecordItems]{
			BaseRecord:           baseRecord,
			Items:                items,
			SourceTableName:      source,
			DestinationTableName: nameAndExclude.Name,
		}
	case changefeedOpSkip:
		// delete without a before image: rebuild the row key from the
		// changefeed key column, a JSON array in primary-key column order
		items, err := changefeedKeyItems(key, schema, nameAndExclude.Exclude)
		if err != nil {
			c.logger.Error("[cockroachdb] skipping changefeed data message with no row image and unusable key",
				slog.String("table", source), slog.Any("error", err))
			return nil
		}
		record = &model.DeleteRecord[model.RecordItems]{
			BaseRecord:           baseRecord,
			Items:                items,
			SourceTableName:      source,
			DestinationTableName: nameAndExclude.Name,
			// only key columns are known; the sentinel keeps normalize from
			// touching non-key columns, mirroring sparse postgres deletes
			UnchangedToastColumns: map[string]struct{}{"_peerdb_not_backfilled_delete": {}},
		}
	}

	if err := state.req.RecordStream.AddRecord(ctx, record); err != nil {
		return err
	}
	state.recordCount++
	if state.recordCount == 1 {
		state.req.RecordStream.SignalAsNotEmpty()
		state.batchDeadline = time.Now().Add(state.req.IdleTimeout)
	}
	if state.recordCount >= state.req.MaxBatchSize && state.graceDeadline.IsZero() {
		// full batch: allow a short grace period for a final resolved
		// timestamp so the checkpoint covers the pulled records
		state.graceDeadline = time.Now().Add(2 * state.resolvedInterval)
	}
	if state.recordCount%50000 == 0 {
		c.logger.Info("[cockroachdb] PullRecords streaming",
			slog.Uint64("records", uint64(state.recordCount)),
			slog.Int64("bytes", state.totalBytes.Load()))
	}

	commitTime := time.Unix(0, commitWallNanos)
	otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, commitTime.Unix())
	otelManager.Metrics.SourceLagGauge.Record(ctx, time.Since(commitTime).Milliseconds())
	return nil
}

// emitSchemaDelta re-reads the source table schema when a changefeed row
// contains columns the cached schema does not know about (changefeeds do not
// announce DDL) and emits a TableSchemaDelta for the added columns.
func (c *CockroachDBConnector) emitSchemaDelta(
	ctx context.Context,
	state *changefeedPullState,
	source string,
	destination string,
	schema *changefeedTableSchema,
	unknown []string,
) error {
	freshSchemas, err := c.GetTableSchema(ctx, state.req.Env, state.req.InternalVersion, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: source}})
	if err != nil {
		return fmt.Errorf("failed to refresh schema for %s: %w", source, err)
	}
	freshColumns := make(map[string]*protos.FieldDescription)
	for _, col := range freshSchemas[source].Columns {
		freshColumns[col.Name] = col
	}

	delta := &protos.TableSchemaDelta{
		SrcTableName:    source,
		DstTableName:    destination,
		System:          protos.TypeSystem_Q,
		NullableEnabled: schema.nullableEnabled,
	}
	for _, colName := range unknown {
		col, ok := freshColumns[colName]
		if !ok {
			// column vanished between the row event and the schema read
			// (e.g. added then dropped); don't look it up again
			schema.ignored[colName] = struct{}{}
			c.logger.Warn("[cockroachdb] ignoring unknown changefeed column absent from source schema",
				slog.String("table", source), slog.String("column", colName))
			continue
		}
		delta.AddedColumns = append(delta.AddedColumns, col)
		schema.fields[colName] = types.QField{
			Name:     col.Name,
			Type:     types.QValueKind(col.Type),
			Nullable: col.Nullable,
		}
	}
	if len(delta.AddedColumns) > 0 {
		state.req.RecordStream.AddSchemaDelta(state.req.TableNameMapping, delta)
		c.logger.Info("[cockroachdb] detected added columns from changefeed",
			slog.String("table", source), slog.Any("delta", delta))
	}
	return nil
}
