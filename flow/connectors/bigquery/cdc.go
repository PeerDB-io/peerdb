package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	// Pseudo-columns APPENDS()/CHANGES() add on top of the base table's real
	// columns. These are metadata, not data columns, and must not be copied into
	// the record.
	bigQueryChangeTypeColumn        = "_CHANGE_TYPE"
	bigQueryChangeTimestampColumn   = "_CHANGE_TIMESTAMP"
	bigQueryChangeIsForUpdateColumn = "_CHANGE_IS_FOR_UPDATE"

	// _CHANGE_TYPE values.
	bigQueryChangeTypeInsert = "INSERT"
	bigQueryChangeTypeUpdate = "UPDATE"
	bigQueryChangeTypeDelete = "DELETE"
)

// SetupReplConn is a no-op for BigQuery. c.client is a single long-lived connection
func (c *BigQueryConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

// UpdateReplStateLastOffset persists the checkpoint once a batch has been confirmed
// synced to the destination.
func (c *BigQueryConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

// PullFlowCleanup is a no-op. BigQuery has no server-side replication resource
// analogous to a Postgres replication slot/publication for this method to drop.
func (c *BigQueryConnector) PullFlowCleanup(context.Context, string) error {
	return nil
}

// EnsurePullability is a no-op. ValidateMirrorSource (source.go), run at
// mirror-creation time.
func (c *BigQueryConnector) EnsurePullability(
	context.Context, *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

// pollWindow computes the upper bound of the next APPENDS()/CHANGES() poll window
// given the last-scanned checkpoint and BigQuery's current clock (now).
func pollWindow(checkpoint, now time.Time, safetyLag, maxQueryWindow time.Duration) (time.Time, bool) {
	upper := checkpoint.Add(maxQueryWindow)
	if safe := now.Add(-safetyLag); safe.Before(upper) {
		upper = safe
	}
	return upper, upper.After(checkpoint)
}

// waitForNextPoll self-paces PullRecords, park it until
// time since the last poll hasn't crossed idleTimeout
func (c *BigQueryConnector) waitForNextPoll(ctx context.Context, idleTimeout time.Duration) error {
	if wait := idleTimeout - time.Since(c.lastPollAt); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// PullRecords polls every mapped table from its own synced-through checkpoint.
// A table failure leaves only that table behind; successful siblings continue
// and the resulting per-table checkpoint is confirmed with the shared stream.
func (c *BigQueryConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()
	// Recorded regardless of how this call returns below (including the
	// "nothing new to scan yet" early return) so pacing always advances.
	defer func() { c.lastPollAt = time.Now() }()

	if err := c.waitForNextPoll(ctx, req.IdleTimeout); err != nil {
		return err
	}

	sourceTables := make([]string, 0, len(req.TableNameMapping))
	for sourceTableIdentifier := range req.TableNameMapping {
		sourceTables = append(sourceTables, sourceTableIdentifier)
	}
	slices.Sort(sourceTables)

	checkpoint, err := parseBigQueryCDCCheckpoint(req.LastOffset.Text, sourceTables)
	if err != nil {
		return err
	}

	now, err := c.currentBigQueryTimestamp(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current BigQuery timestamp: %w", err)
	}

	safetyLag, err := internal.PeerDBBigQueryCDCSafetyLag(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get BigQuery CDC safety lag: %w", err)
	}
	maxQueryWindow, err := internal.PeerDBBigQueryCDCMaxQueryWindow(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get BigQuery CDC max query window: %w", err)
	}

	cfg, err := internal.FetchConfigFromDB(ctx, catalogPool, req.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to fetch flow config from db: %w", err)
	}
	eventsFunctionByTable := make(map[string]protos.BigqueryCdcEventsFunction, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		eventsFunctionByTable[tableMapping.SourceTableIdentifier] = tableMapping.GetBigqueryCdcEventsFunction()
	}

	var recordCount int
	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		if recordCount == 0 {
			req.RecordStream.SignalAsNotEmpty()
		}
		recordCount++
		return req.RecordStream.AddRecord(ctx, record)
	}

	var bytesProcessed int64
	var tableErrors []error
	type tablePullError struct {
		err   error
		table string
	}
	var customerVisibleTableErrors []tablePullError
	healthyTables := 0
	for _, sourceTableIdentifier := range sourceTables {
		start, err := checkpoint.SyncedThrough(sourceTableIdentifier)
		if err != nil {
			return err
		}
		upper, ok := pollWindow(start, now, safetyLag, maxQueryWindow)
		if !ok {
			// This table has no safe window to scan yet. It is healthy, and a
			// sibling failure must not turn that into a flow-wide failure.
			healthyTables++
			continue
		}

		pullTable := c.pullTableAppends
		if eventsFunctionByTable[sourceTableIdentifier] == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES {
			pullTable = c.pullTableChanges
		}
		recordCountBeforeTable := recordCount
		tableBytesProcessed, err := pullTable(
			ctx, sourceTableIdentifier, req.TableNameMapping[sourceTableIdentifier], start, upper, addRecord,
		)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if recordCount > recordCountBeforeTable {
				return fmt.Errorf("BigQuery CDC table %s failed after emitting records: %w", sourceTableIdentifier, err)
			}
			if checkpoint.RecordFailure(sourceTableIdentifier, upper) {
				customerVisibleTableErrors = append(customerVisibleTableErrors, tablePullError{
					table: sourceTableIdentifier,
					err:   err,
				})
			}
			tableErrors = append(tableErrors, err)
			c.logger.Error("[bigquery] CDC table poll failed; other tables will continue",
				slog.String("table", sourceTableIdentifier), slog.Any("error", err))
			continue
		}
		checkpoint.RecordSuccess(sourceTableIdentifier, upper)
		healthyTables++
		bytesProcessed += tableBytesProcessed
	}
	if len(sourceTables) > 0 && healthyTables == 0 {
		return errors.Join(tableErrors...)
	}
	if len(customerVisibleTableErrors) > 0 {
		alerter := alerting.NewAlerter(ctx, catalogPool, otelManager)
		for _, tableError := range customerVisibleTableErrors {
			_ = alerter.LogFlowError(ctx, req.FlowJobName, fmt.Errorf(
				"BigQuery CDC failed to poll source table %s; replication for other tables will continue: %w",
				tableError.table, exceptions.NewBigQueryError(tableError.err),
			))
		}
	}

	// All tables queried here are mapped tables
	otelManager.Metrics.FetchedBytesCounter.Add(ctx, bytesProcessed)
	otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, bytesProcessed)

	checkpointText, err := checkpoint.Marshal()
	if err != nil {
		return err
	}
	// Each successful table advances regardless of whether its window contained
	// changes. Failed tables retain their prior synced-through position and the
	// attempted target, so they retry without holding healthy tables back.
	req.RecordStream.UpdateLatestCheckpointText(checkpointText)

	if recordCount == 0 {
		req.RecordStream.SignalAsEmpty()
	}

	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)),
		attribute.Int64(otel_metrics.BytesPulledKey, bytesProcessed),
	)
	c.logger.Info("[bigquery] PullRecords polled tables",
		slog.Int("tables", len(sourceTables)), slog.Int("failedTables", len(tableErrors)),
		slog.Int("records", recordCount), slog.Int64("bytes", bytesProcessed))

	return nil
}

// pullTableAppends runs SELECT * FROM APPENDS(TABLE <table>, @start, @end) for one
// source table over [start, end), converting and pushing each row via addRecord.
// Returns the approximate byte size of the RecordItems actually forwarded
func (c *BigQueryConnector) pullTableAppends(
	ctx context.Context,
	sourceTableIdentifier string,
	nameAndExclude model.NameAndExclude,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	it, err := c.runPullQuery(ctx, "APPENDS", dsTable.stringQuoted(), sourceTableIdentifier, nameAndExclude.Exclude, "", start, end)
	if err != nil {
		return 0, fmt.Errorf("failed to run APPENDS query for table %s: %w", sourceTableIdentifier, err)
	}

	var qfields []types.QField
	var changeCols bigQueryChangeColumns
	var bytesForwarded int64
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return bytesForwarded, nil
			}
			return 0, fmt.Errorf("failed to read APPENDS row for table %s: %w", sourceTableIdentifier, err)
		}

		// it.Schema is only guaranteed populated after the first Next() call
		if qfields == nil {
			qfields = make([]types.QField, len(it.Schema))
			for i, field := range it.Schema {
				qfields[i] = BigQueryFieldToQField(field)
			}
			changeCols = locateBigQueryChangeColumns(it.Schema)
		}

		// _CHANGE_TIMESTAMP is APPENDS()'s own commit-time signal for the row;
		// used as this record's CommitTimeNano. Falls back to the poll window's
		// end if, unexpectedly, the column isn't present.
		commitTimeNano := end.UnixNano()
		if changeCols.changeTimestamp >= 0 {
			if ts, ok := row[changeCols.changeTimestamp].(time.Time); ok {
				commitTimeNano = ts.UnixNano()
			}
		}

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}

		itemBytes, err := recordItemsApproxBytes(items)
		if err != nil {
			return 0, fmt.Errorf("failed to size row for table %s: %w", sourceTableIdentifier, err)
		}
		bytesForwarded += itemBytes

		if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: commitTimeNano},
			Items:                items,
			SourceTableName:      sourceTableIdentifier,
			DestinationTableName: nameAndExclude.Name,
		}); err != nil {
			return 0, err
		}
	}
}

// recordItemsApproxBytes estimates the storage size of one converted row
func recordItemsApproxBytes(items model.RecordItems) (int64, error) {
	b, err := items.MarshalJSON()
	if err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

// bigQueryChangePseudoColumns are the metadata columns APPENDS()/CHANGES() add on top
// of the base table's real columns -- never copied into the record.
var bigQueryChangePseudoColumns = map[string]struct{}{
	bigQueryChangeTypeColumn:        {},
	bigQueryChangeTimestampColumn:   {},
	bigQueryChangeIsForUpdateColumn: {},
}

// buildPullQuery renders "SELECT * [EXCEPT (...)] FROM fn(TABLE dsTable, @start, @end)
// [ORDER BY orderBy]" for the APPENDS()/CHANGES() table-valued functions.
func buildPullQuery(fn string, dsTable string, exclude map[string]struct{}, orderBy string) string {
	q := fmt.Sprintf("SELECT *%s FROM %s(TABLE %s, @start, @end)", exceptClause(exclude), fn, dsTable)
	if orderBy != "" {
		q += " ORDER BY " + orderBy
	}
	return q
}

// exceptClause renders a "SELECT * EXCEPT (...)" suffix for the given excluded
// column names, or "" if there are none.
func exceptClause(exclude map[string]struct{}) string {
	if len(exclude) == 0 {
		return ""
	}
	names := slices.Sorted(maps.Keys(exclude))
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quotedIdentifier(name)
	}
	return fmt.Sprintf(" EXCEPT (%s)", strings.Join(quoted, ", "))
}

// runPullQuery runs the APPENDS()/CHANGES() query for sourceTableIdentifier,
// retrying with a shrunk EXCEPT clause if BigQuery rejects a column that no longer
// exists on the source table (BigQuery reports one such column per error, so this
// may loop more than once).
func (c *BigQueryConnector) runPullQuery(
	ctx context.Context, fn string, dsTable string, sourceTableIdentifier string,
	exclude map[string]struct{}, orderBy string, start, end time.Time,
) (*bigquery.RowIterator, error) {
	effective := c.effectiveExclude(sourceTableIdentifier, exclude)
	for {
		q := c.client.Query(buildPullQuery(fn, dsTable, effective, orderBy))
		q.Parameters = []bigquery.QueryParameter{
			{Name: "start", Value: start},
			{Name: "end", Value: end},
		}

		it, err := q.Read(ctx)
		if err == nil {
			return it, nil
		}

		missing := missingExceptColumns(err, effective)
		if len(missing) == 0 {
			return nil, err
		}
		dropped := c.droppedExcludeColumns[sourceTableIdentifier]
		if dropped == nil {
			dropped = make(map[string]struct{}, len(missing))
			c.droppedExcludeColumns[sourceTableIdentifier] = dropped
		}
		next := make(map[string]struct{}, len(effective))
		for col := range effective {
			if _, gone := missing[col]; gone {
				dropped[col] = struct{}{}
				c.logger.Warn("[bigquery] excluded column no longer exists on source table, dropping from EXCEPT clause",
					slog.String("table", sourceTableIdentifier), slog.String("column", col))
				continue
			}
			next[col] = struct{}{}
		}
		effective = next
	}
}

// effectiveExclude returns exclude minus any columns already known (from a prior
// runPullQuery retry) to no longer exist on sourceTableIdentifier.
func (c *BigQueryConnector) effectiveExclude(sourceTableIdentifier string, exclude map[string]struct{}) map[string]struct{} {
	dropped := c.droppedExcludeColumns[sourceTableIdentifier]
	if len(dropped) == 0 {
		return exclude
	}
	effective := make(map[string]struct{}, len(exclude))
	for col := range exclude {
		if _, isDropped := dropped[col]; !isDropped {
			effective[col] = struct{}{}
		}
	}
	return effective
}

// Matches BigQuery's invalid-query error for a "SELECT * EXCEPT (col)" column that
// doesn't exist on the source, e.g. "Column foo in SELECT * EXCEPT list does not
// exist at [1:18]" (verified against a live table).
var bqMissingExceptColRe = regexp.MustCompile(`Column (\S+) in SELECT \* EXCEPT list does not exist`)

// missingExceptColumns returns the column named in err's EXCEPT-clause error, as a
// single-element set, if it's one of candidates. Returns nil otherwise.
func missingExceptColumns(err error, candidates map[string]struct{}) map[string]struct{} {
	apiErr, ok := errors.AsType[*googleapi.Error](err)
	if !ok || apiErr.Code != 400 {
		return nil
	}
	match := bqMissingExceptColRe.FindStringSubmatch(apiErr.Message)
	if match == nil {
		return nil
	}
	col := match[1]
	if _, isCandidate := candidates[col]; !isCandidate {
		return nil
	}
	return map[string]struct{}{col: {}}
}

func bigQueryRowToRecordItems(
	schema bigquery.Schema, qfields []types.QField, row []bigquery.Value,
) (model.RecordItems, error) {
	items := model.NewRecordItems(len(row))
	for i, field := range schema {
		if _, isPseudo := bigQueryChangePseudoColumns[field.Name]; isPseudo {
			continue
		}

		qval, err := qvalueFromBigQueryValue(qfields[i], row[i], field)
		if err != nil {
			return model.RecordItems{}, fmt.Errorf("failed to convert column %s: %w", field.Name, err)
		}
		items.AddColumn(field.Name, qval)
	}
	return items, nil
}

type bigQueryChangeColumns struct {
	changeType      int
	changeTimestamp int
	isForUpdate     int
}

func locateBigQueryChangeColumns(schema bigquery.Schema) bigQueryChangeColumns {
	cols := bigQueryChangeColumns{changeType: -1, changeTimestamp: -1, isForUpdate: -1}
	for i, field := range schema {
		switch field.Name {
		case bigQueryChangeTypeColumn:
			cols.changeType = i
		case bigQueryChangeTimestampColumn:
			cols.changeTimestamp = i
		case bigQueryChangeIsForUpdateColumn:
			cols.isForUpdate = i
		}
	}
	return cols
}

// pullTableChanges runs SELECT * FROM CHANGES(TABLE <table>, @start, @end) ORDER BY
// _CHANGE_TIMESTAMP for one source table over [start, end), single-pass streaming
// like pullTableAppends, and pushes the resulting Insert/Update/DeleteRecords via
// addRecord.
//
// CHANGES() represents an UPDATE as two rows sharing one _CHANGE_TIMESTAMP: a
// _CHANGE_TYPE=DELETE with _CHANGE_IS_FOR_UPDATE=true carrying the old values,
// immediately followed by a _CHANGE_TYPE=UPDATE with _CHANGE_IS_FOR_UPDATE=false
// carrying the new values. The old-values half is skipped -- OldItems isn't needed
// downstream (see model.UpdateRecord usage), so there's nothing to pair it with; the
// UPDATE row alone is forwarded as the UpdateRecord.
// Returns the approximate byte size of the RecordItems actually forwarded
func (c *BigQueryConnector) pullTableChanges(
	ctx context.Context,
	sourceTableIdentifier string,
	nameAndExclude model.NameAndExclude,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	it, err := c.runPullQuery(
		ctx, "CHANGES", dsTable.stringQuoted(), sourceTableIdentifier, nameAndExclude.Exclude,
		quotedIdentifier(bigQueryChangeTimestampColumn), start, end,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to run CHANGES query for table %s: %w", sourceTableIdentifier, err)
	}

	var qfields []types.QField
	var changeCols bigQueryChangeColumns
	var bytesForwarded int64
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return bytesForwarded, nil
			}
			return 0, fmt.Errorf("failed to read CHANGES row for table %s: %w", sourceTableIdentifier, err)
		}

		// it.Schema is only guaranteed populated after the first Next() call
		if qfields == nil {
			qfields = make([]types.QField, len(it.Schema))
			for i, field := range it.Schema {
				qfields[i] = BigQueryFieldToQField(field)
			}
			changeCols = locateBigQueryChangeColumns(it.Schema)
		}

		var changeType string
		if changeCols.changeType >= 0 {
			changeType, _ = row[changeCols.changeType].(string)
		}
		var isForUpdate bool
		if changeCols.isForUpdate >= 0 {
			isForUpdate, _ = row[changeCols.isForUpdate].(bool)
		}
		if changeType == bigQueryChangeTypeDelete && isForUpdate {
			continue
		}

		// _CHANGE_TIMESTAMP is CHANGES()'s own commit-time signal for the row; used
		// as this record's CommitTimeNano. Falls back to the poll window's end if,
		// unexpectedly, the column isn't present.
		commitTimeNano := end.UnixNano()
		if changeCols.changeTimestamp >= 0 {
			if ts, ok := row[changeCols.changeTimestamp].(time.Time); ok {
				commitTimeNano = ts.UnixNano()
			}
		}

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}
		itemBytes, err := recordItemsApproxBytes(items)
		if err != nil {
			return 0, fmt.Errorf("failed to size row for table %s: %w", sourceTableIdentifier, err)
		}
		bytesForwarded += itemBytes

		baseRecord := model.BaseRecord{CommitTimeNano: commitTimeNano}
		var record model.Record[model.RecordItems]
		switch changeType {
		case bigQueryChangeTypeInsert:
			record = &model.InsertRecord[model.RecordItems]{
				BaseRecord: baseRecord, Items: items,
				SourceTableName: sourceTableIdentifier, DestinationTableName: nameAndExclude.Name,
			}
		case bigQueryChangeTypeUpdate:
			record = &model.UpdateRecord[model.RecordItems]{
				BaseRecord: baseRecord, NewItems: items,
				SourceTableName: sourceTableIdentifier, DestinationTableName: nameAndExclude.Name,
			}
		default: // bigQueryChangeTypeDelete, not flagged for update: a genuine delete
			record = &model.DeleteRecord[model.RecordItems]{
				BaseRecord: baseRecord, Items: items,
				SourceTableName: sourceTableIdentifier, DestinationTableName: nameAndExclude.Name,
			}
		}
		if err := addRecord(ctx, record); err != nil {
			return 0, err
		}
	}
}
