package conncockroachdb

import (
	"context"
	"errors"
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
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// changefeedSlotName is a placeholder slot identifier: CockroachDB has no
// replication slots, but a non-empty name makes the snapshot workflow carry
// the captured system time into the QRep AS OF SYSTEM TIME reads.
const changefeedSlotName = "changefeed"

const resolvedLagWarnThreshold = time.Hour

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
	return &protos.ExportTxSnapshotOutput{SnapshotName: systemTime.String()}, nil, nil
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
	systemTimeText := systemTime.String()
	if err := c.SetLastOffset(ctx, req.FlowJobName,
		model.CdcCheckpoint{ID: systemTime.WallNanos, Text: systemTimeText}); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to store initial changefeed cursor: %w", err)
	}
	c.logger.Info("[cockroachdb] SetupReplication stored initial changefeed cursor", slog.String("cursor", systemTimeText))
	if shouldProtectSnapshotHistory(c.config, req.DoInitialSnapshot) {
		c.protectSnapshotHistory(ctx, req.FlowJobName, systemTimeText)
	}
	return model.SetupReplicationResult{SlotName: changefeedSlotName, SnapshotName: systemTimeText}, nil
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
	// sinkless changefeeds end with their connection; only the snapshot
	// history protection may still be alive
	c.releaseSnapshotHistoryProtection(ctx, jobName)
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
	emittedSources    [][2]string
	schemas           map[string]*changefeedTableSchema
	seenSinceResolved map[changefeedDedupKey]crdbHLC
	cursor            crdbHLC
	resolvedInterval  time.Duration
	recordCount       uint32
	batchDeadline     time.Time
	graceDeadline     time.Time
	lastLagWarnAt     time.Time
	deltaBytes        atomic.Int64
	totalBytes        atomic.Int64
}

// indexSource registers a source table under the exact and quote-stripped
// forms of its emitted full_table_name. Lowercased aliases are added by
// finishIndexing once every table's exact forms are known, so that tables
// whose names collide when lowercased (public.data vs public."Data") never
// shadow each other regardless of registration order.
func (state *changefeedPullState) indexSource(emitted string, source string) {
	state.emittedSources = append(state.emittedSources, [2]string{emitted, source})
	for _, key := range []string{emitted, stripTableNameQuotes(emitted)} {
		if _, present := state.sourceByEmitted[key]; !present {
			state.sourceByEmitted[key] = source
		}
	}
}

// finishIndexing adds lowercased alias forms for case-tolerant routing. An
// alias is skipped when it collides with an exact form of any table, and
// dropped entirely when two different tables produce it, leaving only the
// unambiguous exact-match routing for such names.
func (state *changefeedPullState) finishIndexing() {
	exactForms := make(map[string]struct{}, 2*len(state.emittedSources))
	for _, pair := range state.emittedSources {
		exactForms[pair[0]] = struct{}{}
		exactForms[stripTableNameQuotes(pair[0])] = struct{}{}
	}
	aliasOwner := make(map[string]string)
	for _, pair := range state.emittedSources {
		emitted, source := pair[0], pair[1]
		for _, key := range []string{strings.ToLower(emitted), strings.ToLower(stripTableNameQuotes(emitted))} {
			if _, isExact := exactForms[key]; isExact {
				continue
			}
			if owner, claimed := aliasOwner[key]; claimed {
				if owner != source {
					delete(state.sourceByEmitted, key)
					aliasOwner[key] = ""
				}
				continue
			}
			aliasOwner[key] = source
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

// alreadySeen reports whether this data message was already processed in this
// batch. handleResolved prunes entries the resolved timestamp covers, since a
// reconnect never replays past the last resolved cursor.
func (state *changefeedPullState) alreadySeen(table string, key []byte, updated string) bool {
	_, seen := state.seenSinceResolved[changefeedDedupKey{table: table, key: string(key), updated: updated}]
	return seen
}

// markSeen records a data message in the replay dedup set together with its
// parsed commit timestamp. It must run only after the message was fully
// processed (its record accepted by the stream): marking earlier would let a
// replay after a processing failure skip the message for good, silently
// losing the row once the next resolved timestamp advances the checkpoint
// past it.
func (state *changefeedPullState) markSeen(
	logger log.Logger, table string, key []byte, updated string, updatedTs crdbHLC,
) {
	if len(state.seenSinceResolved) >= maxChangefeedDedupEntries {
		logger.Warn("[cockroachdb] changefeed replay dedup set overflowed, clearing;"+
			" a reconnect before the next resolved timestamp may emit duplicates",
			slog.Int("entries", len(state.seenSinceResolved)))
		clear(state.seenSinceResolved)
	}
	state.seenSinceResolved[changefeedDedupKey{table: table, key: string(key), updated: updated}] = updatedTs
}

// pruneSeen drops dedup entries the resolved timestamp covers: a reconnect
// resumes at the resolved cursor, so rows at or below it can never be
// delivered again. Entries with newer commit timestamps must survive, as the
// catch-up scan after a reconnect re-emits those rows (observed live on the
// multi-node cluster, where resolved timestamps lag furthest behind).
func (state *changefeedPullState) pruneSeen(resolved crdbHLC) {
	for key, entryTs := range state.seenSinceResolved {
		if !entryTs.After(resolved) {
			delete(state.seenSinceResolved, key)
		}
	}
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

	if req.LastOffset.Text == "" {
		// SetupReplication seeds the offset before the first pull; an empty
		// cursor here means the stored offset was lost. Falling back to the
		// current system time would silently skip everything in between.
		return fmt.Errorf("no stored changefeed cursor for mirror %s:"+
			" the replication offset is missing from the catalog, resync the mirror", req.FlowJobName)
	}
	cursor, err := parseHLC(req.LastOffset.Text)
	if err != nil {
		return err
	}

	state := &changefeedPullState{
		req:               req,
		tables:            make([]*common.QualifiedTable, 0, len(req.TableNameMapping)),
		sourceByEmitted:   make(map[string]string, len(req.TableNameMapping)),
		schemas:           make(map[string]*changefeedTableSchema, len(req.TableNameMapping)),
		seenSinceResolved: make(map[changefeedDedupKey]crdbHLC),
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
		state.indexSource(fmt.Sprintf("%s.%s.%s", c.config.Database, parsed.Namespace, parsed.Table), source)
		if schema, ok := req.TableNameSchemaMapping[target.Name]; ok {
			state.schemas[source] = newChangefeedTableSchema(schema)
		}
	}
	state.finishIndexing()
	slices.SortFunc(state.tables, func(a, b *common.QualifiedTable) int {
		return strings.Compare(a.String(), b.String())
	})
	if len(state.tables) >= changefeedManyTablesThreshold {
		c.logger.Warn("[cockroachdb] a single changefeed covers many tables, coupling their throughput and checkpointing",
			slog.Int("tables", len(state.tables)))
	}

	// seed the checkpoint so a batch that ends without a fresh resolved
	// timestamp re-persists the prior cursor instead of clearing it
	req.RecordStream.UpdateLatestCheckpointText(cursor.String())
	req.RecordStream.UpdateLatestCheckpointID(cursor.WallNanos)

	c.logger.Info("[cockroachdb] started PullRecords for mirror "+req.FlowJobName,
		slog.String("cursor", cursor.String()),
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
			slog.String("cursor", state.cursor.String()),
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

	maxRetries, baseDelay := changefeedRetryConfig(c.config)
	var attempt uint32
	for {
		cursorBefore := state.cursor
		recordsBefore := state.recordCount
		done, err := c.runChangefeedSession(ctx, otelManager, state)
		if done {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if state.recordCount > recordsBefore || state.cursor != cursorBefore {
			// only actual progress (records accepted or the resolved cursor
			// advancing) resets the retry budget: raw messages received also
			// cover deterministically failing replays, which must not be able
			// to keep the loop alive forever
			attempt = 0
		}
		if err == nil {
			// watchdog fired without records: reconnect and keep waiting
			continue
		}
		if isCursorTooOldError(err) {
			return exceptions.NewCockroachChangefeedIrrecoverableError("CURSOR_PAST_GC",
				fmt.Errorf("changefeed cursor %s is older than the replica GC threshold and cannot resume;"+
					" increase the CockroachDB gc.ttlseconds zone setting on the source tables or resync the mirror: %w",
					state.cursor, err))
		}
		if isTableTruncatedError(err) {
			return exceptions.NewCockroachChangefeedIrrecoverableError("TABLE_TRUNCATED",
				fmt.Errorf("a table watched by mirror %s was truncated; changefeeds cannot resume across a truncate"+
					" and the destination still holds the pre-truncate rows, resync the mirror: %w",
					req.FlowJobName, err))
		}
		if isTableDroppedError(err) {
			return exceptions.NewCockroachChangefeedIrrecoverableError("TABLE_DROPPED",
				fmt.Errorf("a table watched by mirror %s was dropped; remove it from the mirror or resync: %w",
					req.FlowJobName, err))
		}
		if isTargetNotAtCursorError(err) {
			return exceptions.NewCockroachChangefeedIrrecoverableError("TABLE_NOT_AT_CURSOR",
				fmt.Errorf("a table watched by mirror %s cannot be resolved at the changefeed cursor %s,"+
					" typically because it was created or recreated after the cursor; resync the mirror: %w",
					req.FlowJobName, state.cursor, err))
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
			slog.String("cursor", state.cursor.String()),
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

	// the watchdog cancels with a dedicated cause so the classification below
	// can tell a fired watchdog apart from parent-context cancellation
	queryCtx, cancelQuery := context.WithCancelCause(ctx)
	defer cancelQuery(nil)

	connConfig, err := ParseConfig(c.connStr, c.config)
	if err != nil {
		return false, err
	}
	conn, err := NewCockroachDBConnFromConfig(queryCtx, connConfig, c.ssh)
	if err != nil {
		return false, fmt.Errorf("failed to open changefeed connection: %w", err)
	}
	defer func() {
		cancelQuery(nil)
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := conn.Close(closeCtx); err != nil {
			c.logger.Debug("[cockroachdb] failed to close changefeed connection", slog.Any("error", err))
		}
	}()

	watchdog := time.AfterFunc(state.watchdogWindow(), func() { cancelQuery(errChangefeedWatchdog) })
	defer watchdog.Stop()

	rows, err := conn.Query(queryCtx, stmt)
	if err != nil {
		return false, fmt.Errorf("failed to create changefeed: %w", err)
	}
	defer rows.Close()

	if c.historyProtectionChecked.CompareAndSwap(false, true) {
		// the changefeed accepted the cursor, so the snapshot timestamp no
		// longer needs protection from garbage collection
		c.releaseSnapshotHistoryProtection(ctx, state.req.FlowJobName)
	}

	for rows.Next() {
		var tableName pgtype.Text
		var key, value []byte
		if err := rows.Scan(&tableName, &key, &value); err != nil {
			return false, fmt.Errorf("failed to scan changefeed row: %w", err)
		}
		messageBytes := int64(len(value))
		state.deltaBytes.Add(messageBytes)
		state.totalBytes.Add(messageBytes)

		envelope, err := parseChangefeedEnvelope(value)
		if err != nil {
			return false, err
		}

		if envelope.Resolved != "" {
			done, err := c.handleResolved(ctx, otelManager, state, envelope.Resolved)
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

	return classifySessionEnd(ctx, queryCtx, rows.Err(), state.recordCount > 0)
}

// errChangefeedWatchdog is the cancellation cause the session watchdog uses,
// so a fired watchdog is never confused with parent-context cancellation.
var errChangefeedWatchdog = errors.New("changefeed watchdog fired: stream silent past the stall window")

// classifySessionEnd decides how a finished changefeed session run resolves:
// parent cancellation propagates as an error, a fired watchdog completes the
// batch (with records) or asks for a reconnect (without), and anything else
// is the stream's own error.
func classifySessionEnd(ctx context.Context, queryCtx context.Context, rowsErr error, hasRecords bool) (bool, error) {
	switch {
	case ctx.Err() != nil:
		return false, ctx.Err()
	case errors.Is(context.Cause(queryCtx), errChangefeedWatchdog):
		// watchdog fired: with records the batch is complete on the last
		// checkpoint, without records reconnect and keep waiting
		return hasRecords, nil
	case rowsErr != nil:
		return false, rowsErr
	default:
		return false, errors.New("changefeed stream ended unexpectedly")
	}
}

// handleResolved advances the checkpoint: resolved timestamps are the only
// safe resume points, as row timestamps between them may arrive out of order.
func (c *CockroachDBConnector) handleResolved(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	state *changefeedPullState,
	resolved string,
) (bool, error) {
	ts, err := parseHLC(resolved)
	if err != nil {
		return false, err
	}
	otelManager.Metrics.CockroachDBResolvedLagGauge.Record(ctx, time.Since(time.Unix(0, ts.WallNanos)).Seconds())
	state.cursor = ts
	state.req.RecordStream.UpdateLatestCheckpointText(ts.String())
	state.req.RecordStream.UpdateLatestCheckpointID(ts.WallNanos)
	// a reconnect replays at most back to this resolved timestamp: entries it
	// covers can never be delivered again, while newer ones may replay and
	// must stay deduplicated
	state.pruneSeen(ts)

	if lag := time.Since(time.Unix(0, ts.WallNanos)); lag > resolvedLagWarnThreshold &&
		time.Since(state.lastLagWarnAt) > time.Minute {
		state.lastLagWarnAt = time.Now()
		c.logger.Warn("[cockroachdb] changefeed resolved timestamp lags far behind;"+
			" if it falls behind the CockroachDB gc.ttlseconds zone setting the mirror cannot resume",
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
		if err := c.SetLastOffset(ctx, state.req.FlowJobName,
			model.CdcCheckpoint{ID: ts.WallNanos, Text: ts.String()}); err != nil {
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
		// the changefeed only watches mapped tables, so an unmapped emission
		// means broken routing (e.g. a watched table was renamed); dropping
		// the message would silently diverge once the checkpoint advances
		return newTerminalChangefeedError(fmt.Errorf(
			"changefeed emitted table %s which maps to no source table of mirror %s;"+
				" if a watched table was renamed, rename it back or resync the mirror",
			emittedTableName, state.req.FlowJobName))
	}
	schema, ok := state.schemas[source]
	if !ok {
		return newTerminalChangefeedError(fmt.Errorf(
			"no cached schema for source table %s of mirror %s; resync the mirror", source, state.req.FlowJobName))
	}
	if state.alreadySeen(emittedTableName, key, envelope.Updated) {
		return nil
	}
	operation := envelope.operation()

	updatedTs, err := parseHLC(envelope.Updated)
	if err != nil {
		return newTerminalChangefeedError(
			fmt.Errorf("changefeed message for %s missing updated timestamp: %w", source, err))
	}
	commitWallNanos := updatedTs.WallNanos

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
			return newTerminalChangefeedError(fmt.Errorf("failed to convert insert for %s: %w", source, err))
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
			return newTerminalChangefeedError(fmt.Errorf("failed to convert update for %s: %w", source, err))
		}
		oldItems, err := changefeedRecordItems(envelope.Before, schema, nameAndExclude.Exclude)
		if err != nil {
			return newTerminalChangefeedError(fmt.Errorf("failed to convert update for %s: %w", source, err))
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
			return newTerminalChangefeedError(fmt.Errorf("failed to convert delete for %s: %w", source, err))
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
			// dropping the delete would leave the destination row alive for
			// good while the checkpoint advances past it
			return newTerminalChangefeedError(fmt.Errorf(
				"changefeed delete for %s carries no row image and its key is unusable: %w", source, err))
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
	// only now is the message fully processed; marking earlier would let a
	// replay after any of the failures above skip the row permanently
	state.markSeen(c.logger, emittedTableName, key, envelope.Updated, updatedTs)
	otelManager.Metrics.CockroachDBRecordsReceivedCounter.Add(ctx, 1)
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
		precision, scale := common.ParseNumericTypmod(col.TypeModifier)
		schema.fields[colName] = types.QField{
			Name:      col.Name,
			Type:      types.QValueKind(col.Type),
			Precision: precision,
			Scale:     scale,
			Nullable:  col.Nullable,
		}
	}
	if len(delta.AddedColumns) > 0 {
		state.req.RecordStream.AddSchemaDelta(state.req.TableNameMapping, delta)
		c.logger.Info("[cockroachdb] detected added columns from changefeed",
			slog.String("table", source), slog.Any("delta", delta))
	}
	return nil
}
