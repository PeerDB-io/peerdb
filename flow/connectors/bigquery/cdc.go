package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"cloud.google.com/go/bigquery"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

// PullRecords polls every mapped table over one window, [checkpoint, upper), and
// pushes the resulting records onto the stream.
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

	checkpoint, err := time.Parse(time.RFC3339Nano, req.LastOffset.Text)
	if err != nil {
		return fmt.Errorf("failed to parse BigQuery CDC checkpoint %q: %w", req.LastOffset.Text, err)
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

	upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
	if !ok {
		// Nothing new to scan yet (e.g. the safety lag hasn't cleared). Don't
		// advance the checkpoint: nothing was scanned to protect from a re-scan,
		// and the same window is simply recomputed on the next poll.
		req.RecordStream.SignalAsEmpty()
		return nil
	}

	cfg, err := internal.FetchConfigFromDB(ctx, catalogPool, req.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to fetch flow config from db: %w", err)
	}
	pullTable := c.pullTableAppends
	if cfg.GetBigqueryCdcConfig().GetCdcMode() == protos.BigqueryCdcMode_BIGQUERY_CDC_MODE_CHANGES {
		pullTable = c.pullTableChanges
	}

	var recordCount int
	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		if recordCount == 0 {
			req.RecordStream.SignalAsNotEmpty()
		}
		recordCount++
		return req.RecordStream.AddRecord(ctx, record)
	}

	// Sequential per table, not concurrent, within this poll cycle -- bounds
	// BigQuery job/slot usage (see https://docs.cloud.google.com/bigquery/docs/slots).
	// Sorted so polls process tables in a deterministic order.
	sourceTables := make([]string, 0, len(req.TableNameMapping))
	for sourceTableIdentifier := range req.TableNameMapping {
		sourceTables = append(sourceTables, sourceTableIdentifier)
	}
	slices.Sort(sourceTables)

	var bytesProcessed int64
	for _, sourceTableIdentifier := range sourceTables {
		tableBytesProcessed, err := pullTable(
			ctx, sourceTableIdentifier, req.TableNameMapping[sourceTableIdentifier], checkpoint, upper, addRecord,
		)
		if err != nil {
			return err
		}
		bytesProcessed += tableBytesProcessed
	}

	// All tables queried here are mapped tables
	otelManager.Metrics.FetchedBytesCounter.Add(ctx, bytesProcessed)
	otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, bytesProcessed)

	// The window advances regardless of whether it contained changes.
	req.RecordStream.UpdateLatestCheckpointText(
		upper.Format(time.RFC3339Nano),
	)

	if recordCount == 0 {
		req.RecordStream.SignalAsEmpty()
	}

	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)),
		attribute.Int64(otel_metrics.BytesPulledKey, bytesProcessed),
	)
	c.logger.Info("[bigquery] PullRecords polled window",
		slog.Time("start", checkpoint), slog.Time("end", upper),
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

	q := c.client.Query(fmt.Sprintf("SELECT * FROM APPENDS(TABLE %s, @start, @end)", dsTable.stringQuoted()))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
	}

	it, err := q.Read(ctx)
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

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row, nameAndExclude.Exclude)
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

func bigQueryRowToRecordItems(
	schema bigquery.Schema, qfields []types.QField, row []bigquery.Value, exclude map[string]struct{},
) (model.RecordItems, error) {
	items := model.NewRecordItems(len(row))
	for i, field := range schema {
		if _, isPseudo := bigQueryChangePseudoColumns[field.Name]; isPseudo {
			continue
		}
		if _, excluded := exclude[field.Name]; excluded {
			continue
		}

		qval, err := qvalueFromBigQueryValue(qfields[i], row[i])
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

	q := c.client.Query(fmt.Sprintf(
		"SELECT * FROM CHANGES(TABLE %s, @start, @end) ORDER BY %s",
		dsTable.stringQuoted(), quotedIdentifier(bigQueryChangeTimestampColumn),
	))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
	}

	it, err := q.Read(ctx)
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

		items, err := bigQueryRowToRecordItems(it.Schema, qfields, row, nameAndExclude.Exclude)
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
