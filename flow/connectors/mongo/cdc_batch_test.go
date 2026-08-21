package connmongo

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// pullHarness wires a MongoConnector up to a scripted change stream and an in-memory
// metadata store, so a whole PullRecords call can be driven without a server.
type pullHarness struct {
	connector *MongoConnector
	store     *mockMetadataStore
	stream    *mockChangeStream
	req       *model.PullRecordsRequest[model.RecordItems]
	// cancel cancels the context PullRecords runs under, so a test can cancel the
	// activity mid-pull. It is set by run, and so is only safe to call from inside a
	// pull.
	cancel context.CancelFunc
	// streamCreations counts createChangeStream calls: one for the initial stream plus
	// one per recreation.
	streamCreations int
}

// pullOutcome is everything the sync workflow gets to observe from one pull: the
// records handed to the record stream, the offset left on it, and the error, if any.
type pullOutcome struct {
	err        error
	checkpoint string
	ids        []string
}

func newPullHarness(t *testing.T, iterations ...iterationType) *pullHarness {
	t.Helper()
	h := &pullHarness{
		store:  &mockMetadataStore{},
		stream: newMockChangeStream(t, iterations...),
	}
	h.connector = &MongoConnector{
		logger: internal.LoggerFromCtx(t.Context()),
		createChangeStream: func(
			context.Context, mongo.Pipeline, ...options.Lister[options.ChangeStreamOptions],
		) (ChangeStream, error) {
			h.streamCreations++
			return h.stream, nil
		},
		metadataStore: h.store,
	}
	h.req = &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName: t.Name(),
		// Buffered past any batch these tests pull, so that a blocked AddRecord means
		// a real deadlock rather than a slow drain goroutine.
		RecordStream:           model.NewCDCStream[model.RecordItems](4096),
		TableNameMapping:       map[string]model.NameAndExclude{"db.coll": {Name: "db_coll"}},
		TableNameSchemaMapping: map[string]*protos.TableSchema{},
		MaxBatchSize:           10000,
		IdleTimeout:            time.Minute,
		InternalVersion:        shared.InternalVersion_Latest,
	}
	return h
}

// run drives PullRecords to completion, draining the record stream concurrently the way
// the sync side of the activity does.
func (h *pullHarness) run(t *testing.T) pullOutcome {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.cancel = cancel

	ids := []string{}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for record := range h.req.RecordStream.GetRecords() {
			if record == nil {
				ids = append(ids, "<nil record>")
				continue
			}
			value, err := record.GetItems().GetValueByColName(DefaultDocumentKeyColumnName)
			if err != nil {
				ids = append(ids, fmt.Sprintf("<no _id column: %v>", err))
				continue
			}
			id, ok := value.Value().(string)
			if !ok {
				ids = append(ids, fmt.Sprintf("<non-string _id: %v>", value.Value()))
				continue
			}
			ids = append(ids, id)
		}
	}()

	otelManager, err := otel_metrics.NewOtelManager(t.Context(), "test", false)
	require.NoError(t, err)
	pullErr := h.connector.PullRecords(ctx, shared.CatalogPool{}, otelManager, h.req)
	<-drained

	return pullOutcome{err: pullErr, checkpoint: h.req.RecordStream.GetLastCheckpoint().Text, ids: ids}
}

// persistedOffsets is every offset the pull wrote straight to the catalog.
func (h *pullHarness) persistedOffsets() []string {
	offsets := make([]string, 0, len(h.store.persisted))
	for _, persisted := range h.store.persisted {
		offsets = append(offsets, persisted.Text)
	}
	return offsets
}

// requireOffsetsCoverOnlyDeliveredRecords asserts the safety property PullRecords owes
// the sync workflow: a resume token once every replicable event up to
// it has been handed to the RecordStream. The next batch resumes strictly after the
// committed offset, so an offset that runs ahead of the delivered records skips those
// events for good.
//
// Two kinds of offset are checked, and only these two are ever committed:
//   - offsets PullRecords writes straight to the catalog, which are committed the moment
//     they are written and so are never exempt;
//   - the offset left on the record stream by a pull that returned no error, which the
//     sync workflow commits once the destination has taken the batch.
func (h *pullHarness) requireOffsetsCoverOnlyDeliveredRecords(t *testing.T, out pullOutcome) {
	t.Helper()

	offsets := h.persistedOffsets()
	if out.err == nil && out.checkpoint != "" {
		offsets = append(offsets, out.checkpoint)
	}

	for _, offset := range offsets {
		want := h.stream.replicableThrough(h.stream.iterationOfToken(offset))
		require.LessOrEqual(t, len(want), len(out.ids),
			"offset covers %d replicable events but only %d records reached the record stream", len(want), len(out.ids))
		require.Equal(t, want, out.ids[:len(want)],
			"offset commits past events that never reached the record stream")
	}
}

// repeatInserts scripts n insert iterations.
func repeatInserts(n int) []iterationType {
	return slices.Repeat([]iterationType{insert}, n)
}

// mockEventIDs is the _id of the first n insert events.
func mockEventIDs(n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, mockEventID(i))
	}
	return ids
}

func TestPullRecordsOffsetNeverRunsAheadOfDeliveredRecords(t *testing.T) {
	for _, tc := range []struct {
		name         string
		iterations   []iterationType
		maxBatchSize uint32
	}{
		{name: "cut by max batch size", iterations: repeatInserts(8), maxBatchSize: 5},
		{
			name:         "cut by max batch size across decode batches",
			iterations:   repeatInserts(pullRecordsItemsBatchSize + 9),
			maxBatchSize: pullRecordsItemsBatchSize + 3,
		},
		{name: "cut by sync interval", iterations: []iterationType{insert, insert, idle}, maxBatchSize: 100},
		{name: "idle before any record", iterations: []iterationType{idle, idle, insert, idle}, maxBatchSize: 100},
		{name: "idle after every record", iterations: []iterationType{insert, idle}, maxBatchSize: 100},
		{name: "fatal error mid batch", iterations: []iterationType{insert, insert, fatal}, maxBatchSize: 100},
		{name: "fatal error before any record", iterations: []iterationType{fatal}, maxBatchSize: 100},
		{name: "undecodable document", iterations: []iterationType{insert, insert, nullIdInsert}, maxBatchSize: 3},
		{
			name:         "undecodable document after a full decode batch",
			iterations:   append(repeatInserts(pullRecordsItemsBatchSize), nullIdInsert),
			maxBatchSize: pullRecordsItemsBatchSize + 1,
		},
		// A batch that never sees a record keeps recreating its stream on every idle
		// timeout, so these cases have to be ended by a hard failure instead.
		{name: "unsupported operations only", iterations: []iterationType{unsupportedOp, unsupportedOp, fatal}, maxBatchSize: 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPullHarness(t, tc.iterations...)
			h.req.MaxBatchSize = tc.maxBatchSize

			out := h.run(t)
			// Whatever reached the stream must be a gap-free prefix of the emitted
			// inserts: a hole would mean an offset skipped an event even if the
			// offset itself looks sane.
			require.Equal(t, mockEventIDs(len(out.ids)), out.ids, "records reached the stream out of order or with gaps")
			h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
		})
	}
}

// MaxBatchSize is a hard cap: PullRecords must stop pulling the moment it is reached,
// because an event read past the cap is neither delivered nor covered by the offset, and
// the change stream it came from is closed on the way out.
func TestPullRecordsTruncatesBatchAtMaxBatchSize(t *testing.T) {
	const maxBatchSize = 4
	h := newPullHarness(t, repeatInserts(maxBatchSize+6)...)
	h.req.MaxBatchSize = maxBatchSize

	out := h.run(t)
	require.NoError(t, out.err)
	require.Equal(t, mockEventIDs(maxBatchSize), out.ids)
	require.Equal(t, maxBatchSize, h.stream.idx, "pulled events past MaxBatchSize")
	require.Equal(t, h.stream.tokenAt(maxBatchSize-1), out.checkpoint)
	require.Empty(t, h.persistedOffsets(), "a full batch is committed by the sync workflow, not the puller")
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// A batch larger than pullRecordsItemsBatchSize is handed to the decode workers in
// several chunks that are decoded in parallel. Every event must still arrive exactly
// once and in change stream order, and the offset must land on the last one.
func TestPullRecordsTruncatesBatchSpanningManyDecodeBatches(t *testing.T) {
	maxBatchSize := 2*pullRecordsItemsBatchSize + 37
	h := newPullHarness(t, repeatInserts(maxBatchSize+1)...)
	h.req.MaxBatchSize = uint32(maxBatchSize)

	out := h.run(t)
	require.NoError(t, out.err)
	require.Equal(t, mockEventIDs(maxBatchSize), out.ids)
	require.Equal(t, maxBatchSize, h.stream.idx, "pulled events past MaxBatchSize")
	require.Equal(t, h.stream.tokenAt(maxBatchSize-1), out.checkpoint)
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// The sync interval bounds the batch from the arrival of its first record, not the gap
// between consecutive records: once the interval elapses the batch is cut and handed
// over, however busy the stream still is.
func TestPullRecordsSyncIntervalBoundsBatchFromFirstRecord(t *testing.T) {
	const syncInterval = 90 * time.Second
	h := newPullHarness(t, insert, insert, idle)
	h.req.IdleTimeout = syncInterval

	out := h.run(t)
	require.NoError(t, out.err)
	require.Equal(t, mockEventIDs(2), out.ids)

	// Before any record arrives the pull waits on the long, hour-long budget rather
	// than the sync interval, so an idle mirror does not spin recreating its stream.
	require.Greater(t, h.stream.deadlines[0], time.Hour-time.Minute)
	// Once a record has landed, the remaining budget is the configured sync interval,
	// and it keeps counting down rather than being reset per record.
	require.LessOrEqual(t, h.stream.deadlines[1], syncInterval)
	require.Greater(t, h.stream.deadlines[1], syncInterval-time.Minute)
	require.Less(t, h.stream.deadlines[2], h.stream.deadlines[1], "sync interval restarted mid-batch")

	// Cutting a non-empty batch advances the offset to the post-batch resume token,
	// which is safe because every record is already on the stream, but it must be left
	// for the sync workflow to commit rather than persisted here.
	require.Equal(t, h.stream.tokenAt(2), out.checkpoint)
	require.Empty(t, h.persistedOffsets())
	require.Equal(t, 1, h.streamCreations, "cutting a batch should not recreate the change stream")
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// An idle stream with nothing pulled yet is the one case where the puller commits an
// offset itself. That is safe because no records are in flight, and it is what
// stops an idle mirror's resume token from ageing out. A batch that has seen no record
// keeps waiting rather than returning empty, so this one only ends once a record shows up.
func TestPullRecordsIdleWithEmptyBatchPersistsOffsetAndRecreatesStream(t *testing.T) {
	h := newPullHarness(t, idle, idle, insert, idle)

	out := h.run(t)
	require.NoError(t, out.err)
	require.Equal(t, mockEventIDs(1), out.ids)
	// One offset per idle timeout that found the batch empty, and none for the timeout
	// that cut the batch holding a record: committing that one is the sync workflow's job.
	require.Equal(t, []string{h.stream.tokenAt(0), h.stream.tokenAt(1)}, h.persistedOffsets())
	require.Equal(t, h.stream.tokenAt(3), out.checkpoint)
	// One initial stream plus one recreation per idle timeout: a deadline-exceeded
	// stream is not resumable.
	require.Equal(t, 3, h.streamCreations)
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// Events the connector cannot replicate must not consume batch budget, and must not
// advance the offset on their own: the offset stays on the last event that was replicated.
func TestPullRecordsSkipsUnsupportedOperations(t *testing.T) {
	h := newPullHarness(t, insert, unsupportedOp, unsupportedOp, insert, idle)
	h.req.MaxBatchSize = 2

	out := h.run(t)
	require.NoError(t, out.err)
	require.Equal(t, mockEventIDs(2), out.ids)
	// Five iterations were scripted; the two inserts plus the two skipped events is
	// where MaxBatchSize is reached, so the trailing idle is never reached.
	require.Equal(t, 4, h.stream.idx)
	require.Equal(t, h.stream.tokenAt(3), out.checkpoint)
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// A change stream error that is neither a timeout nor a stale resume token ends the
// pull. The caller throws the batch away, so the one thing that must not happen is an
// offset reaching the catalog.
func TestPullRecordsFatalStreamErrorCommitsNoOffset(t *testing.T) {
	h := newPullHarness(t, insert, insert, fatal)

	out := h.run(t)
	require.ErrorIs(t, out.err, errFatalChangeStream)
	require.ErrorContains(t, out.err, "change stream error")
	// The workers are drained before the error is surfaced, so the records pulled
	// before the failure are intact on the stream rather than half-written.
	require.Equal(t, mockEventIDs(2), out.ids)
	require.Empty(t, h.persistedOffsets(), "a failed pull must not commit an offset")
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// A document the decode workers reject must fail the pull. Silently dropping it would
// leave the offset covering an event that never reached the destination.
func TestPullRecordsUndecodableDocumentFailsPull(t *testing.T) {
	h := newPullHarness(t, insert, insert, nullIdInsert)
	h.req.MaxBatchSize = 3

	out := h.run(t)
	require.ErrorContains(t, out.err, "_id field is missing or null in table db.coll")
	// The bad document is in the only sub-batch, so the whole batch dies with it.
	require.Empty(t, out.ids)
	require.Empty(t, h.persistedOffsets(), "a failed pull must not commit an offset")
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// The same failure in a sub-batch the pull loop has already moved past has to be noticed
// too: the loop is still pulling events when a worker dies behind it, and finishing the
// batch as if nothing happened would hand the sync workflow an offset spanning the
// records that worker dropped.
func TestPullRecordsUndecodableDocumentInEarlierSubBatchFailsPull(t *testing.T) {
	// The bad document heads the first sub-batch; MaxBatchSize is two sub-batches, so
	// the loop keeps pulling well past the point the worker rejects it.
	iterations := append([]iterationType{nullIdInsert}, repeatInserts(3*pullRecordsItemsBatchSize)...)
	h := newPullHarness(t, iterations...)
	h.req.MaxBatchSize = uint32(2 * pullRecordsItemsBatchSize)

	out := h.run(t)
	require.ErrorContains(t, out.err, "_id field is missing or null in table db.coll")
	require.Empty(t, h.persistedOffsets(), "a failed pull must not commit an offset")
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}

// Cancelling the activity mid-pull must surface as an error. Returning nil would let the
// sync workflow commit the offset for a batch that was cut off part-way through.
func TestPullRecordsContextCancellationMidBatchFailsPull(t *testing.T) {
	h := newPullHarness(t, repeatInserts(4*pullRecordsItemsBatchSize)...)
	h.req.MaxBatchSize = 3 * pullRecordsItemsBatchSize
	// Cancel once a couple of sub-batches are in flight, so the cancellation races the
	// decode workers rather than arriving before any work started.
	h.stream.beforeNext = func(idx int) {
		if idx == 2*pullRecordsItemsBatchSize {
			h.cancel()
		}
	}

	out := h.run(t)
	require.ErrorIs(t, out.err, context.Canceled)
	require.Empty(t, h.persistedOffsets(), "a cancelled pull must not commit an offset")
	// Whatever did reach the stream is still a gap-free prefix; cancellation drops
	// in-flight records rather than reordering or interleaving them.
	require.Equal(t, mockEventIDs(len(out.ids)), out.ids)
	h.requireOffsetsCoverOnlyDeliveredRecords(t, out)
}
