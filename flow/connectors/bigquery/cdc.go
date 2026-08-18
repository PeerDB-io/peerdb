package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
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

// pullTableChanges runs
// SELECT * FROM CHANGES(TABLE <table>, @start, @end)
//
//	ORDER BY _CHANGE_TIMESTAMP, <pk columns>, _CHANGE_IS_FOR_UPDATE DESC
//
// for one source table over [start, end), pairs up delete+insert rows that CHANGES()
// represents as one logical UPDATE (see pairBigQueryChanges), and pushes the
// resulting Insert/Update/DeleteRecords via addRecord.
//
// Unlike pullTableAppends' single-pass streaming, this buffers one window's full,
// already-ordered result set into memory before emitting anything: pairing a delete
// with the insert that immediately follows it needs one row of lookahead, and reading
// the whole result into a slice is the simplest correct way to get that (see
// pairBigQueryChanges' doc comment for why it's structured as a pure function over a
// slice). Poll windows are capped at maxQueryWindow, so this is bounded, not unbounded,
// per-table memory.
// Returns the approximate byte size of the RecordItems actually forwarded (see
// recordItemsApproxBytes), for FetchedBytesCounter.
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

	// Re-checked here, not just trusted from mirror-creation-time validation
	// (source.go's ValidateMirrorSource, which already requires this): PullRecords
	// runs long after validation, on every poll, and the table's PK constraint
	// could have been dropped since. Metadata() is also how the PK column names for
	// the ORDER BY/pairing below are obtained -- no separate lookup to keep in sync
	// with source.go's tableHasPrimaryKey, this calls that same helper.
	metadata, err := c.client.DatasetInProject(c.projectID, dsTable.dataset).Table(dsTable.table).Metadata(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata for table %s: %w", sourceTableIdentifier, err)
	}
	if !tableHasPrimaryKey(metadata) {
		return 0, fmt.Errorf("table %s has no primary key constraint configured on BigQuery; "+
			"CHANGES mode requires one", sourceTableIdentifier)
	}
	pkColumns := metadata.TableConstraints.PrimaryKey.Columns

	orderBy := make([]string, 0, len(pkColumns)+2)
	orderBy = append(orderBy, quotedIdentifier(bigQueryChangeTimestampColumn))
	for _, col := range pkColumns {
		orderBy = append(orderBy, quotedIdentifier(col))
	}
	orderBy = append(orderBy, quotedIdentifier(bigQueryChangeIsForUpdateColumn)+" DESC")

	q := c.client.Query(fmt.Sprintf(
		"SELECT * FROM CHANGES(TABLE %s, @start, @end) ORDER BY %s",
		dsTable.stringQuoted(), strings.Join(orderBy, ", "),
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
	var pkIdx []int
	var rows [][]bigquery.Value
	var changeRows []bigQueryChangeRow
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
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
			pkIdx = make([]int, len(pkColumns))
			for i, col := range pkColumns {
				idx := slices.IndexFunc(it.Schema, func(f *bigquery.FieldSchema) bool { return f.Name == col })
				if idx < 0 {
					return 0, fmt.Errorf("primary key column %s not found in CHANGES result for table %s", col, sourceTableIdentifier)
				}
				pkIdx[i] = idx
			}
		}

		changeRow := bigQueryChangeRow{pk: make([]bigquery.Value, len(pkIdx))}
		if changeCols.changeType >= 0 {
			changeRow.changeType, _ = row[changeCols.changeType].(string)
		}
		if changeCols.changeTimestamp >= 0 {
			changeRow.changeTime, _ = row[changeCols.changeTimestamp].(time.Time)
		}
		if changeCols.isForUpdate >= 0 {
			changeRow.isForUpdate, _ = row[changeCols.isForUpdate].(bool)
		}
		for i, idx := range pkIdx {
			changeRow.pk[i] = row[idx]
		}

		rows = append(rows, row)
		changeRows = append(changeRows, changeRow)
	}

	var bytesForwarded int64
	for _, change := range pairBigQueryChanges(changeRows) {
		changeBytes, err := c.emitBigQueryChange(
			ctx, change, rows, changeRows, it.Schema, qfields, sourceTableIdentifier, nameAndExclude, addRecord,
		)
		if err != nil {
			return 0, err
		}
		bytesForwarded += changeBytes
	}
	return bytesForwarded, nil
}

// emitBigQueryChange converts one pairBigQueryChanges result into the corresponding
// Insert/Update/DeleteRecord and pushes it via addRecord. rows and changeRows are the
// same index-aligned, parallel slices pairBigQueryChanges was given. Returns the
// approximate byte size of the RecordItems forwarded (see recordItemsApproxBytes) --
// both OldItems and NewItems for an update, since both traverse the pipeline.
func (c *BigQueryConnector) emitBigQueryChange(
	ctx context.Context,
	change bigQueryPairedChange,
	rows [][]bigquery.Value,
	changeRows []bigQueryChangeRow,
	schema bigquery.Schema,
	qfields []types.QField,
	sourceTableIdentifier string,
	nameAndExclude model.NameAndExclude,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (int64, error) {
	switch change.kind {
	case bigQueryChangeUpdate:
		oldItems, err := bigQueryRowToRecordItems(schema, qfields, rows[change.deleteIdx], nameAndExclude.Exclude)
		if err != nil {
			return 0, fmt.Errorf("failed to convert old row for table %s: %w", sourceTableIdentifier, err)
		}
		newItems, err := bigQueryRowToRecordItems(schema, qfields, rows[change.insertIdx], nameAndExclude.Exclude)
		if err != nil {
			return 0, fmt.Errorf("failed to convert new row for table %s: %w", sourceTableIdentifier, err)
		}
		oldBytes, err := recordItemsApproxBytes(oldItems)
		if err != nil {
			return 0, fmt.Errorf("failed to size old row for table %s: %w", sourceTableIdentifier, err)
		}
		newBytes, err := recordItemsApproxBytes(newItems)
		if err != nil {
			return 0, fmt.Errorf("failed to size new row for table %s: %w", sourceTableIdentifier, err)
		}
		if err := addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: changeRows[change.insertIdx].changeTime.UnixNano()},
			OldItems:             oldItems,
			NewItems:             newItems,
			SourceTableName:      sourceTableIdentifier,
			DestinationTableName: nameAndExclude.Name,
		}); err != nil {
			return 0, err
		}
		return oldBytes + newBytes, nil
	case bigQueryChangeInsert:
		items, err := bigQueryRowToRecordItems(schema, qfields, rows[change.insertIdx], nameAndExclude.Exclude)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}
		itemBytes, err := recordItemsApproxBytes(items)
		if err != nil {
			return 0, fmt.Errorf("failed to size row for table %s: %w", sourceTableIdentifier, err)
		}
		if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: changeRows[change.insertIdx].changeTime.UnixNano()},
			Items:                items,
			SourceTableName:      sourceTableIdentifier,
			DestinationTableName: nameAndExclude.Name,
		}); err != nil {
			return 0, err
		}
		return itemBytes, nil
	default: // bigQueryChangeDelete
		items, err := bigQueryRowToRecordItems(schema, qfields, rows[change.deleteIdx], nameAndExclude.Exclude)
		if err != nil {
			return 0, fmt.Errorf("failed to convert row for table %s: %w", sourceTableIdentifier, err)
		}
		itemBytes, err := recordItemsApproxBytes(items)
		if err != nil {
			return 0, fmt.Errorf("failed to size row for table %s: %w", sourceTableIdentifier, err)
		}
		if err := addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: changeRows[change.deleteIdx].changeTime.UnixNano()},
			Items:                items,
			SourceTableName:      sourceTableIdentifier,
			DestinationTableName: nameAndExclude.Name,
		}); err != nil {
			return 0, err
		}
		return itemBytes, nil
	}
}

type bigQueryChangeRow struct {
	changeTime  time.Time
	changeType  string
	pk          []bigquery.Value
	isForUpdate bool
}

type bigQueryChangeKind int

const (
	bigQueryChangeInsert bigQueryChangeKind = iota
	bigQueryChangeDelete
	bigQueryChangeUpdate
)

// bigQueryPairedChange is one pairing result, indexing into the rows slice
// pairBigQueryChanges was given: a lone insert/delete leaves the other index at -1; a
// matched update sets both.
type bigQueryPairedChange struct {
	kind      bigQueryChangeKind
	deleteIdx int
	insertIdx int
}

// pairBigQueryChanges scans rows -- expected in CHANGES()'s own
// ORDER BY _CHANGE_TIMESTAMP, _CHANGE_IS_FOR_UPDATE DESC, <pk> order -- and pairs each
// _CHANGE_IS_FOR_UPDATE delete with the insert immediately following it when they
// share the same _CHANGE_TIMESTAMP and PK columns: that's CHANGES()'s own
// representation of a single UPDATE as a delete+insert pair sharing the same PK and
// timestamp. Any row not part of such a pair -- including a _CHANGE_IS_FOR_UPDATE
// delete with no matching insert right after it, e.g. one whose pair fell in a
// different poll window -- passes through as a lone insert/delete, exactly like a
// genuine standalone insert/delete would.
func pairBigQueryChanges(rows []bigQueryChangeRow) []bigQueryPairedChange {
	changes := make([]bigQueryPairedChange, 0, len(rows))
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		if row.changeType == bigQueryChangeTypeDelete && row.isForUpdate && i+1 < len(rows) {
			next := rows[i+1]
			if next.changeType == bigQueryChangeTypeInsert && next.isForUpdate &&
				next.changeTime.Equal(row.changeTime) && bigQueryPKEqual(row.pk, next.pk) {
				changes = append(changes, bigQueryPairedChange{kind: bigQueryChangeUpdate, deleteIdx: i, insertIdx: i + 1})
				i++
				continue
			}
		}
		switch row.changeType {
		case bigQueryChangeTypeDelete:
			changes = append(changes, bigQueryPairedChange{kind: bigQueryChangeDelete, deleteIdx: i, insertIdx: -1})
		case bigQueryChangeTypeInsert:
			changes = append(changes, bigQueryPairedChange{kind: bigQueryChangeInsert, deleteIdx: -1, insertIdx: i})
		}
	}
	return changes
}

// bigQueryPKEqual compares two rows' primary key column values for
// pairBigQueryChanges' match check. time.Time (a PK column could, in principle, be a
// TIMESTAMP) uses Equal rather than reflect.DeepEqual, which can differ on
// monotonic-clock representation for an otherwise-identical instant; every other
// value type the BigQuery Go SDK returns (bool, int64, float64, string, []byte,
// civil.Date, civil.Time, civil.DateTime, *big.Rat) is compared with
// reflect.DeepEqual.
func bigQueryPKEqual(a, b []bigquery.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		at, aIsTime := a[i].(time.Time)
		bt, bIsTime := b[i].(time.Time)
		if aIsTime || bIsTime {
			if !aIsTime || !bIsTime || !at.Equal(bt) {
				return false
			}
			continue
		}
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
