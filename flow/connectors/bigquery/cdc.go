package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	pullTableProgressLogInterval = 10_000

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
	logger := internal.LoggerFromCtx(ctx)
	pullStartedAt := time.Now()
	var pulledRecords int64
	var bytesProcessed int64
	signaledNotEmpty := false
	defer func() {
		if !signaledNotEmpty {
			req.Stream.SignalAsEmpty()
		}
		logger.Info("[bigquery] PullTableRecords finished",
			slog.String("table", req.SourceTableIdentifier),
			slog.Int64("records", pulledRecords),
			slog.Int64("bytes", bytesProcessed),
			slog.Int("channelLen", req.Stream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStartedAt).Minutes()))
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
		pulledRecords++
		if pulledRecords%pullTableProgressLogInterval == 0 {
			elapsed := time.Since(pullStartedAt)
			logger.Info("[bigquery] pulled records",
				slog.String("table", req.SourceTableIdentifier),
				slog.Int64("records", pulledRecords),
				slog.Duration("elapsed", elapsed),
				slog.Float64("recordsPerSecond", float64(pulledRecords)/elapsed.Seconds()),
				slog.Int("channelLen", req.Stream.ChannelLen()))
		}
		return nil
	}

	if req.TableSchema == nil {
		return model.PullTableRecordsResult{}, fmt.Errorf("no table schema mapping found for destination table %s", req.NameAndExclude.Name)
	}
	columns := pullColumnNames(req.TableSchema, req.NameAndExclude.Exclude)
	columns, err = c.initializeSourceTableColumns(ctx, req.SourceTableIdentifier, columns)
	if err != nil {
		return model.PullTableRecordsResult{}, err
	}

	if cfg.GetBigqueryCdcConfig().GetReplicationMethod() == protos.BigQueryReplicationMethod_BIGQUERY_REPLICATION_METHOD_QUERY {
		bytesProcessed, err = c.pullTableQuery(ctx, tm.QueryCdcWatermarkColumn,
			req.SourceTableIdentifier, req.NameAndExclude.Name, columns, start, upper, addRecord)
	} else if tm.BigqueryCdcEventsFunction == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES {
		bytesProcessed, err = c.pullTableChanges(ctx, req.SourceTableIdentifier,
			req.NameAndExclude.Name, columns, start, upper, addRecord)
	} else if tm.BigqueryCdcEventsFunction == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS {
		bytesProcessed, err = c.pullTableAppends(ctx, req.SourceTableIdentifier,
			req.NameAndExclude.Name, columns, start, upper, addRecord)
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

// pullTableAppends runs SELECT <columns> FROM APPENDS(TABLE <table>, @start, @end) for one
// source table over [start, end), converting and pushing each row via addRecord.
// Returns the HTTP response bytes BigQuery transferred for this table's query,
// including pagination fetches (see withByteCounter).
func (c *BigQueryConnector) pullTableAppends(
	ctx context.Context,
	sourceTableIdentifier string,
	destinationTableName string,
	columns []string,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, columns,
		start, end, func(cols []string) string {
			selectCols := append(slices.Clone(cols), bigQueryChangeTypeColumn, bigQueryChangeTimestampColumn)
			return buildEventsPullQuery("APPENDS", dsTable.stringQuoted(), selectCols, "")
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
			DestinationTableName: destinationTableName,
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

// buildEventsPullQuery renders "SELECT col1, col2, ... FROM fn(TABLE dsTable, @start, @end)
// [ORDER BY orderBy]" for the APPENDS()/CHANGES() table-valued functions.
func buildEventsPullQuery(fn string, dsTable string, columns []string, orderBy string) string {
	q := fmt.Sprintf("SELECT %s FROM %s(TABLE %s, @start, @end)", quotedColumnList(columns), fn, dsTable)
	if orderBy != "" {
		q += " ORDER BY " + orderBy
	}
	return q
}

// buildWatermarkPullQuery renders "SELECT col1, col2, ... FROM dsTable WHERE
// watermarkColumn > @start AND watermarkColumn <= @end". Results are
// intentionally unordered so Storage Read API can consume multiple streams in
// parallel.
func buildWatermarkPullQuery(dsTable string, watermarkColumn string, columns []string) string {
	col := quotedIdentifier(watermarkColumn)
	return fmt.Sprintf("SELECT %s FROM %s WHERE TIMESTAMP(%s) > @start AND TIMESTAMP(%s) <= @end",
		quotedColumnList(columns), dsTable, col, col)
}

// quotedColumnList renders columns as a comma-separated list of quoted identifiers.
func quotedColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, name := range columns {
		quoted[i] = quotedIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

// pullColumnNames returns tableSchema's column names, in schema order, minus any
// in exclude.
func pullColumnNames(tableSchema *protos.TableSchema, exclude map[string]struct{}) []string {
	columns := make([]string, 0, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		if _, excluded := exclude[col.Name]; excluded {
			continue
		}
		columns = append(columns, col.Name)
	}
	return columns
}

type pullQueryBuilder func(columns []string) string

// runPullQuery runs buildQuery(columns) for the [start, end) window, retrying with a
// shrunk column list if BigQuery rejects a column that no longer exists on the source
// table (BigQuery reports one such column per error, so this may loop more than once).
// Columns found missing are remembered per sourceTableIdentifier so later polls don't
// have to rediscover them during this connector's lifetime.
func (c *BigQueryConnector) runPullQuery(
	ctx context.Context, sourceTableIdentifier string, columns []string,
	start, end time.Time,
	buildQuery pullQueryBuilder,
) (*bigquery.RowIterator, error) {
	effective := columns
	for {
		q := c.client.Query(buildQuery(effective))
		q.Parameters = []bigquery.QueryParameter{
			{Name: "start", Value: start},
			{Name: "end", Value: end},
		}

		// When client has Storage Read enabled, Query.Read decides whether to
		// consume cached REST rows or use Storage Read for the query result.
		it, err := q.Read(ctx)
		if err == nil {
			return it, nil
		}

		missingCol, ok := missingSourceColumn(err, effective)
		if !ok {
			return nil, err
		}
		c.logger.Warn("[bigquery] column no longer exists on source table, dropping from SELECT list",
			slog.String("table", sourceTableIdentifier), slog.String("column", missingCol))
		c.recordMissingSourceColumn(sourceTableIdentifier, missingCol)
		effective = slices.DeleteFunc(slices.Clone(effective), func(col string) bool { return col == missingCol })
	}
}

func filterMissingColumns(columns []string, missing map[string]struct{}) []string {
	effective := make([]string, 0, len(columns))
	for _, col := range columns {
		if _, gone := missing[col]; !gone {
			effective = append(effective, col)
		}
	}
	return effective
}

func missingColumnsFromSchema(columns []string, schema bigquery.Schema) map[string]struct{} {
	sourceColumns := make(map[string]struct{}, len(schema))
	for _, field := range schema {
		sourceColumns[field.Name] = struct{}{}
	}
	missing := make(map[string]struct{})
	for _, column := range columns {
		if _, present := sourceColumns[column]; !present {
			missing[column] = struct{}{}
		}
	}
	return missing
}

// initializeSourceTableColumns fetches table metadata on the first pull and returns
// the configured mirror columns that still exist on the source. The result is kept
// for the connector's lifetime.
func (c *BigQueryConnector) initializeSourceTableColumns(
	ctx context.Context, sourceTableIdentifier string, columns []string,
) ([]string, error) {
	effective, initialized := func() ([]string, bool) {
		c.missingSourceColumnsMu.Lock()
		defer c.missingSourceColumnsMu.Unlock()
		missing, ok := c.missingSourceColumns[sourceTableIdentifier]
		return filterMissingColumns(columns, missing), ok
	}()
	if initialized {
		return effective, nil
	}

	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize source columns for table %s: %w", sourceTableIdentifier, err)
	}
	projectID := dsTable.project
	if projectID == "" {
		projectID = c.projectID
	}
	metadata, err := c.client.DatasetInProject(projectID, dsTable.dataset).Table(dsTable.table).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize source columns for table %s: %w", sourceTableIdentifier, err)
	}
	detectedMissing := missingColumnsFromSchema(columns, metadata.Schema)

	effective, newlyMissing := func() ([]string, []string) {
		c.missingSourceColumnsMu.Lock()
		defer c.missingSourceColumnsMu.Unlock()

		missing, ok := c.missingSourceColumns[sourceTableIdentifier]
		var newlyMissing []string
		if !ok {
			c.missingSourceColumns[sourceTableIdentifier] = detectedMissing
			missing = detectedMissing
			newlyMissing = make([]string, 0, len(detectedMissing))
			for column := range detectedMissing {
				newlyMissing = append(newlyMissing, column)
			}
		}
		return filterMissingColumns(columns, missing), newlyMissing
	}()

	for _, column := range newlyMissing {
		c.logger.Warn("[bigquery] mirrored column no longer exists on source table, dropping from SELECT list",
			slog.String("table", sourceTableIdentifier), slog.String("column", column))
	}
	return effective, nil
}

// recordMissingSourceColumn remembers that column no longer exists on the source
// table, so later polls omit it for the rest of this connector's lifetime.
func (c *BigQueryConnector) recordMissingSourceColumn(sourceTableIdentifier, column string) {
	c.missingSourceColumnsMu.Lock()
	defer c.missingSourceColumnsMu.Unlock()
	missing := c.missingSourceColumns[sourceTableIdentifier]
	if missing == nil {
		missing = make(map[string]struct{}, 1)
		c.missingSourceColumns[sourceTableIdentifier] = missing
	}
	missing[column] = struct{}{}
}

// Matches BigQuery's error for a SELECT list column that doesn't exist on the source.
// Verified against live BigQuery error messages.
var bqUnrecognizedNameRe = regexp.MustCompile("Unrecognized name: (?:`([^`]+)`|([^\\s;]+))")

// missingSourceColumn returns the column named in err's "unrecognized name" error, if
// it's one of candidates. Returns "", false otherwise.
func missingSourceColumn(err error, candidates []string) (string, bool) {
	apiErr, ok := errors.AsType[*googleapi.Error](err)
	if !ok || apiErr.Code != 400 {
		return "", false
	}
	match := bqUnrecognizedNameRe.FindStringSubmatch(apiErr.Message)
	if match == nil {
		return "", false
	}
	// match[1] is the backtick-quoted form, match[2] the bare one; exactly one is set.
	col := match[1]
	if col == "" {
		col = match[2]
	}
	if !slices.Contains(candidates, col) {
		return "", false
	}
	return col, true
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

// pullTableChanges runs SELECT <columns> FROM CHANGES(TABLE <table>, @start, @end)
// ORDER BY _CHANGE_TIMESTAMP for one source table over [start, end), single-pass streaming
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
	destinationTableName string,
	columns []string,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, columns,
		start, end, func(cols []string) string {
			selectCols := append(slices.Clone(cols),
				bigQueryChangeTypeColumn, bigQueryChangeTimestampColumn, bigQueryChangeIsForUpdateColumn)
			return buildEventsPullQuery("CHANGES", dsTable.stringQuoted(), selectCols,
				quotedIdentifier(bigQueryChangeTimestampColumn))
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
				SourceTableName:      sourceTableIdentifier,
				DestinationTableName: destinationTableName,
			}
		case bigQueryChangeTypeUpdate:
			record = &model.UpdateRecord[model.RecordItems]{
				BaseRecord: baseRecord, NewItems: items,
				SourceTableName:      sourceTableIdentifier,
				DestinationTableName: destinationTableName,
			}
		case bigQueryChangeTypeDelete:
			// bigQueryChangeTypeDelete, not flagged for update: a genuine delete
			record = &model.DeleteRecord[model.RecordItems]{
				BaseRecord: baseRecord, Items: items,
				SourceTableName:      sourceTableIdentifier,
				DestinationTableName: destinationTableName,
			}
		default:
			return 0, fmt.Errorf("unexpected _CHANGE_TYPE %q for table %s", changeType, sourceTableIdentifier)
		}
		if err := addRecord(ctx, record); err != nil {
			return 0, err
		}
	}
}

// pullTableQuery runs SELECT <columns> FROM <table> WHERE watermarkColumn > @start AND
// watermarkColumn <= @end for one source table. Results are intentionally
// unordered so Storage Read API can consume multiple streams in parallel.
// Returns the HTTP response body bytes consumed by BigQuery for this table's
// query
func (c *BigQueryConnector) pullTableQuery(
	ctx context.Context,
	watermarkColumn string,
	sourceTableIdentifier string,
	destinationTableName string,
	columns []string,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return 0, fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	var bytesTransferred atomic.Int64
	it, err := c.runPullQuery(withByteCounter(ctx, &bytesTransferred), sourceTableIdentifier, columns,
		start, end, func(cols []string) string {
			return buildWatermarkPullQuery(dsTable.stringQuoted(), watermarkColumn, cols)
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
			DestinationTableName: destinationTableName,
		}); err != nil {
			return 0, err
		}
	}
}
