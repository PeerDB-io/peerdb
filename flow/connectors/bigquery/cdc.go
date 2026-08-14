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
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	// safetyLag keeps a poll window's upper bound behind BigQuery's own clock.
	// APPENDS()/CHANGES() consistency for writes in the last few seconds is
	// undocumented, so staying safetyLag behind now() avoids querying right up to
	// the instant of now() and risking rows that land just after the window closes.
	// Hardcoded per the series' "Decisions locked in" -- not made configurable
	// unless a customer actually needs it tuned.
	safetyLag = 1 * time.Minute
	// queryWindow caps how much time a single poll can cover, bounding one
	// BigQuery job's row-scan cost even if a mirror falls far behind. Hardcoded,
	// same rationale as safetyLag.
	queryWindow = 24 * time.Hour

	// Pseudo-columns APPENDS() (and, later, CHANGES()) add on top of the base
	// table's real columns. These are metadata, not data columns, and must not be
	// copied into the record.
	bigQueryChangeTypeColumn      = "_CHANGE_TYPE"
	bigQueryChangeTimestampColumn = "_CHANGE_TIMESTAMP"
)

// SetupReplConn is a no-op for BigQuery. c.client is a single long-lived connection
// pool created once in NewBigQueryConnector and reused for every query -- unlike
// Postgres, which re-derives a fresh RDS IAM token here per activity attempt,
// BigQuery credential refresh is handled transparently underneath by
// auth.Credentials, so there's no per-activity resource to (re)establish.
func (c *BigQueryConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

// UpdateReplStateLastOffset persists the checkpoint once a batch has been confirmed
// synced to the destination. Thin wrapper over SetLastOffset, same shape as
// MySQL's: the flow name travels via context rather than a parameter, per
// CDCPullConnectorCore.UpdateReplStateLastOffset's signature.
func (c *BigQueryConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

// PullFlowCleanup is a no-op. BigQuery has no server-side replication resource
// analogous to a Postgres replication slot/publication for this method to drop.
// The persisted CDC checkpoint itself (the metadata_last_sync_state row keyed by
// job name, backed by the embedded PostgresMetadata) is already cleaned up
// centrally by FlowableActivity.RemoveFlowDetailsFromCatalog (which calls
// connmetadata.SyncFlowCleanupInTx) regardless of source connector type, so
// there's nothing left here for BigQuery specifically to do.
func (c *BigQueryConnector) PullFlowCleanup(context.Context, string) error {
	return nil
}

// EnsurePullability is a no-op. ValidateMirrorSource (source.go), run at
// mirror-creation time, already confirms every mapped table exists and, for CDC
// mirrors, satisfies its mode's requirements (a real PK constraint plus
// enable_change_history for CHANGES; a safe destination engine choice when no PK
// is present for APPENDS). There's nothing left for BigQuery to check at this
// later setup_flow stage that creation-time validation didn't already gate.
func (c *BigQueryConnector) EnsurePullability(
	context.Context, *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

// pollWindow computes the upper bound of the next APPENDS()/CHANGES() poll window
// given the last-scanned checkpoint and BigQuery's current clock (now). upper is
// capped at queryWindow past checkpoint (bounding one poll's row-scan cost) and at
// safetyLag behind now (see safetyLag's doc comment). ok is false when upper
// doesn't move past checkpoint -- e.g. the safety lag hasn't cleared yet -- meaning
// there's nothing new to scan this cycle.
func pollWindow(checkpoint, now time.Time) (time.Time, bool) {
	upper := checkpoint.Add(queryWindow)
	if safe := now.Add(-safetyLag); safe.Before(upper) {
		upper = safe
	}
	return upper, upper.After(checkpoint)
}

// waitForNextPoll self-paces PullRecords. SyncFlow's outer loop
// (flow/activities/flowable.go) calls PullRecords back-to-back with no pacing of
// its own -- every other CDC connector blocks on a live stream instead of polling,
// so the loop never needed one. Without this, BigQuery would issue a fresh, billed
// query every loop iteration instead of respecting IdleTimeout.
func (c *BigQueryConnector) waitForNextPoll(ctx context.Context, idleTimeout time.Duration) error {
	// time.Since(zero-value lastPollAt) clamps to the max representable Duration,
	// which is always >= idleTimeout, so the first-ever call naturally polls
	// immediately without a separate IsZero check.
	if wait := idleTimeout - time.Since(c.lastPollAt); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// PullRecords polls APPENDS() for every mapped table over one window,
// (checkpoint, upper], and pushes the resulting rows onto the stream as
// InsertRecords -- there's no delete/insert pairing here, that's CHANGES mode
// (chunk 5), so every row is a plain insert.
//
// Known open item, deliberately not resolved here (see the series plan): whether
// APPENDS()'s window bounds are inclusive/exclusive on either end is undocumented.
// This uses (checkpoint, upper] as the straightforward reading and defers empirical
// verification against a live instance to chunk 5.
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

	upper, ok := pollWindow(checkpoint, now)
	if !ok {
		// Nothing new to scan yet (e.g. the safety lag hasn't cleared). Don't
		// advance the checkpoint: nothing was scanned to protect from a re-scan,
		// and the same window is simply recomputed on the next poll.
		req.RecordStream.SignalAsEmpty()
		return nil
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
	// BigQuery job/slot usage (see the series' "Decisions locked in"). Sorted so
	// polls process tables in a deterministic order (map iteration isn't).
	sourceTables := make([]string, 0, len(req.TableNameMapping))
	for sourceTableIdentifier := range req.TableNameMapping {
		sourceTables = append(sourceTables, sourceTableIdentifier)
	}
	slices.Sort(sourceTables)

	for _, sourceTableIdentifier := range sourceTables {
		if err := c.pullTableAppends(
			ctx, sourceTableIdentifier, req.TableNameMapping[sourceTableIdentifier], checkpoint, upper, addRecord,
		); err != nil {
			return err
		}
	}

	if recordCount == 0 {
		req.RecordStream.SignalAsEmpty()
	}
	// The window advances past what was scanned regardless of whether it
	// contained changes, so the next poll doesn't re-scan it.
	req.RecordStream.UpdateLatestCheckpointText(upper.Format(time.RFC3339Nano))

	trace.SpanFromContext(ctx).SetAttributes(attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)))
	c.logger.Info("[bigquery] PullRecords polled APPENDS window",
		slog.Time("start", checkpoint), slog.Time("end", upper), slog.Int("records", recordCount))

	return nil
}

// pullTableAppends runs SELECT * FROM APPENDS(TABLE <table>, @start, @end) for one
// source table over (start, end], converting and pushing each row via addRecord.
func (c *BigQueryConnector) pullTableAppends(
	ctx context.Context,
	sourceTableIdentifier string,
	nameAndExclude model.NameAndExclude,
	start, end time.Time,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) error {
	dsTable, err := c.convertToDatasetTable(sourceTableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to parse table identifier %s: %w", sourceTableIdentifier, err)
	}

	q := c.client.Query(fmt.Sprintf("SELECT * FROM APPENDS(TABLE %s, @start, @end)", dsTable.stringQuoted()))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to run APPENDS query for table %s: %w", sourceTableIdentifier, err)
	}

	// it.Schema is only guaranteed populated after the first Next() call (the Go
	// SDK may run the query via a fast path that defers schema resolution until
	// the first page is fetched), so qfields is built lazily off the first row
	// rather than right after Read().
	var qfields []types.QField
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				return nil
			}
			return fmt.Errorf("failed to read APPENDS row for table %s: %w", sourceTableIdentifier, err)
		}

		if qfields == nil {
			qfields = make([]types.QField, len(it.Schema))
			for i, field := range it.Schema {
				qfields[i] = BigQueryFieldToQField(field)
			}
		}

		// _CHANGE_TIMESTAMP is APPENDS()'s own commit-time signal for the row;
		// used as this record's CommitTimeNano. Falls back to the poll window's
		// end if, unexpectedly, the column isn't present.
		commitTimeNano := end.UnixNano()
		items := model.NewRecordItems(len(row))
		for i, field := range it.Schema {
			switch field.Name {
			case bigQueryChangeTypeColumn:
				continue
			case bigQueryChangeTimestampColumn:
				if ts, ok := row[i].(time.Time); ok {
					commitTimeNano = ts.UnixNano()
				}
				continue
			}
			if _, excluded := nameAndExclude.Exclude[field.Name]; excluded {
				continue
			}

			qval, err := qvalueFromBigQueryValue(qfields[i], row[i])
			if err != nil {
				return fmt.Errorf("failed to convert column %s for table %s: %w", field.Name, sourceTableIdentifier, err)
			}
			items.AddColumn(field.Name, qval)
		}

		if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
			BaseRecord:           model.BaseRecord{CommitTimeNano: commitTimeNano},
			Items:                items,
			SourceTableName:      sourceTableIdentifier,
			DestinationTableName: nameAndExclude.Name,
		}); err != nil {
			return err
		}
	}
}
