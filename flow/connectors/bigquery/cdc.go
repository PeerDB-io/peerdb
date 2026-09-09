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
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

func EncodeBigQueryTableCursor(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func DecodeBigQueryTableCursor(cursor string) (time.Time, error) {
	if cursor == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, cursor)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse BigQuery CDC table cursor %q: %w", cursor, err)
	}
	return t, nil
}

// PullTableRecords implements connectors.QueryCDCPullConnector. It pulls a
// single source table's due window, reusing the same window/dispatch logic
// PullRecords uses across all its tables, scoped to just req.SourceTableIdentifier.
func (c *BigQueryConnector) PullTableRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullTableRecordsRequest,
) (model.PullTableRecordsResult, error) {
	signaledNotEmpty := false
	defer func() {
		if !signaledNotEmpty {
			req.Stream.SignalAsEmpty()
		}
	}()

	now, err := c.currentBigQueryTimestamp(ctx)
	if err != nil {
		return model.PullTableRecordsResult{}, fmt.Errorf("failed to get current BigQuery timestamp: %w", err)
	}

	start, err := DecodeBigQueryTableCursor(req.Cursor)
	if err != nil {
		return model.PullTableRecordsResult{}, err
	}
	if start.IsZero() {
		// seed from now if cursor is empty (first poll for this table).
		start = now
	}

	safetyLag, err := internal.PeerDBBigQueryCDCSafetyLag(ctx, req.Env)
	if err != nil {
		return model.PullTableRecordsResult{}, fmt.Errorf("failed to get BigQuery CDC safety lag: %w", err)
	}
	maxQueryWindow, err := internal.PeerDBBigQueryCDCMaxQueryWindow(ctx, req.Env)
	if err != nil {
		return model.PullTableRecordsResult{}, fmt.Errorf("failed to get BigQuery CDC max query window: %w", err)
	}

	upper, ok := pollWindow(start, now, safetyLag, maxQueryWindow)
	if !ok {
		// No safe window to scan yet; cursor is unchanged.
		return model.PullTableRecordsResult{NextCursor: req.Cursor}, nil
	}

	cfg, err := internal.FetchConfigFromDB(ctx, catalogPool, req.FlowJobName)
	if err != nil {
		return model.PullTableRecordsResult{}, fmt.Errorf("failed to fetch flow config from db: %w", err)
	}

	var tm *protos.TableMapping
	for _, tableMapping := range cfg.TableMappings {
		if tableMapping.SourceTableIdentifier == req.SourceTableIdentifier {
			tm = tableMapping
			break
		}
	}

	// The activity waits on this signal before starting sync for this poll, so
	// a query-based source's schema - known as soon as the first row is read
	addRecord := func(addCtx context.Context, record model.Record[model.RecordItems]) error {
		err := req.Stream.AddRecord(addCtx, record)
		if err != nil {
			return err
		}
		if !signaledNotEmpty {
			signaledNotEmpty = true
			req.Stream.SignalAsNotEmpty()
		}
		return nil
	}

	var bytesProcessed int64
	if cfg.GetBigqueryCdcConfig().ReplicationMode == protos.BigQueryReplicationMode_BIGQUERY_REPLICATION_MODE_QUERY {
		bytesProcessed, err = c.pullTableQuery(ctx, tm.QueryCdcWatermarkColumn, req.SourceTableIdentifier,
			req.NameAndExclude, start, upper, addRecord)
	} else if tm.BigqueryCdcEventsFunction == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES {
		bytesProcessed, err = c.pullTableChanges(ctx, req.SourceTableIdentifier, req.NameAndExclude, start, upper, addRecord)
	} else if tm.BigqueryCdcEventsFunction == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS {
		bytesProcessed, err = c.pullTableAppends(ctx, req.SourceTableIdentifier, req.NameAndExclude, start, upper, addRecord)
	} else {
		// unreachable, but just in case throw an error instead of silently returning an empty result
		return model.PullTableRecordsResult{}, fmt.Errorf("unsupported BigQuery CDC events function: %v", tm.BigqueryCdcEventsFunction)
	}
	if err != nil {
		return model.PullTableRecordsResult{}, err
	}

	return model.PullTableRecordsResult{
		NextCursor:     EncodeBigQueryTableCursor(upper),
		BytesProcessed: bytesProcessed,
	}, nil
}

// pullTableAppends runs SELECT * FROM APPENDS(TABLE <table>, @start, @end) for one
// source table over [start, end), converting and pushing each row via addRecord.
// Returns the HTTP response bytes BigQuery transferred for this table's query,
// including pagination fetches (see withByteCounter).
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

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, nameAndExclude.Exclude, start, end,
		func(exclude map[string]struct{}) string {
			return buildPullQuery("APPENDS", dsTable.stringQuoted(), exclude, "")
		})
	if err != nil {
		return 0, fmt.Errorf("failed to run APPENDS query for table %s: %w", sourceTableIdentifier, err)
	}

	var qfields []types.QField
	var changeCols bigQueryChangeColumns
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return bytesTransferred.Load(), nil
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
		// start if, unexpectedly, the column isn't present.
		commitTimeNano := start.UnixNano()
		if changeCols.changeTimestamp >= 0 {
			if ts, ok := row[changeCols.changeTimestamp].(time.Time); ok {
				commitTimeNano = ts.UnixNano()
			}
		}

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}

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
	ctx context.Context, sourceTableIdentifier string, exclude map[string]struct{}, start, end time.Time,
	buildQuery func(exclude map[string]struct{}) string,
) (*bigquery.RowIterator, error) {
	effective := c.effectiveExclude(sourceTableIdentifier, exclude)
	for {
		q := c.client.Query(buildQuery(effective))
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
		c.droppedExcludeColumnsMu.Lock()
		dropped := c.droppedExcludeColumns[sourceTableIdentifier]
		if dropped == nil {
			dropped = make(map[string]struct{}, len(missing))
			c.droppedExcludeColumns[sourceTableIdentifier] = dropped
		}
		c.droppedExcludeColumnsMu.Unlock()
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
	c.droppedExcludeColumnsMu.Lock()
	dropped := c.droppedExcludeColumns[sourceTableIdentifier]
	c.droppedExcludeColumnsMu.Unlock()
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
// Returns the HTTP response bytes BigQuery transferred for this table's query,
// including pagination fetches (see withByteCounter).
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

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, nameAndExclude.Exclude, start, end,
		func(exclude map[string]struct{}) string {
			return buildPullQuery("CHANGES", dsTable.stringQuoted(), exclude, quotedIdentifier(bigQueryChangeTimestampColumn))
		})
	if err != nil {
		return 0, fmt.Errorf("failed to run CHANGES query for table %s: %w", sourceTableIdentifier, err)
	}

	var qfields []types.QField
	var changeCols bigQueryChangeColumns
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return bytesTransferred.Load(), nil
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
		// as this record's CommitTimeNano. Falls back to the poll window's start if,
		// unexpectedly, the column isn't present.
		commitTimeNano := start.UnixNano()
		if changeCols.changeTimestamp >= 0 {
			if ts, ok := row[changeCols.changeTimestamp].(time.Time); ok {
				commitTimeNano = ts.UnixNano()
			}
		}

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}

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
		case bigQueryChangeTypeDelete:
			// bigQueryChangeTypeDelete, not flagged for update: a genuine delete
			record = &model.DeleteRecord[model.RecordItems]{
				BaseRecord: baseRecord, Items: items,
				SourceTableName: sourceTableIdentifier, DestinationTableName: nameAndExclude.Name,
			}
		default:
			return 0, fmt.Errorf("unexpected _CHANGE_TYPE %q for table %s", changeType, sourceTableIdentifier)
		}
		if err := addRecord(ctx, record); err != nil {
			return 0, err
		}
	}
}

// pullTableQuery runs SELECT * FROM <table> WHERE watermarkColumn > @start AND
// watermarkColumn <= @end ORDER BY watermarkColumn for one source table
// Returns the HTTP response body bytes consumed by BigQuery for this table's
// query
func (c *BigQueryConnector) pullTableQuery(
	ctx context.Context,
	watermarkColumn string,
	sourceTableIdentifier string,
	nameAndExclude model.NameAndExclude,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	buildQueryModePullQuery := func(dsTable string, watermarkColumn string, exclude map[string]struct{}) string {
		col := quotedIdentifier(watermarkColumn)
		return fmt.Sprintf("SELECT *%s FROM %s WHERE TIMESTAMP(%s) > @start AND TIMESTAMP(%s) <= @end ORDER BY %s",
			exceptClause(exclude), dsTable, col, col, col)
	}

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, nameAndExclude.Exclude, start, end,
		func(exclude map[string]struct{}) string {
			return buildQueryModePullQuery(dsTable.stringQuoted(), watermarkColumn, exclude)
		})
	if err != nil {
		return 0, fmt.Errorf("failed to run watermark query for table %s: %w", sourceTableIdentifier, err)
	}

	var qfields []types.QField
	watermarkColIdx := -1
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return bytesTransferred.Load(), nil
			}
			return 0, fmt.Errorf("failed to read row for table %s: %w", sourceTableIdentifier, err)
		}

		// it.Schema is only guaranteed populated after the first Next() call
		if qfields == nil {
			qfields = make([]types.QField, len(it.Schema))
			for i, field := range it.Schema {
				qfields[i] = BigQueryFieldToQField(field)
			}
			watermarkColIdx = slices.IndexFunc(it.Schema, func(field *bigquery.FieldSchema) bool {
				return field.Name == watermarkColumn
			})
		}

		// The watermark column is this row's own commit-time signal, used as
		// CommitTimeNano. Falls back to the poll window's start if, unexpectedly, the
		// column isn't present.
		commitTimeNano := start.UnixNano()
		if watermarkColIdx >= 0 {
			switch v := row[watermarkColIdx].(type) {
			case time.Time:
				commitTimeNano = v.UnixNano()
			case civil.Date:
				commitTimeNano = v.In(time.UTC).UnixNano()
			}
		}

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}

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
