package connmongo

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/model"
)

func (h *pullHarness) nextBatch(offset string) {
	h.req.RecordStream = model.NewCDCStream[model.RecordItems](4096)
	h.req.LastOffset = model.CdcCheckpoint{Text: offset}
}

func TestPullRecordsReusesAcknowledgedCursorAcrossBatches(t *testing.T) {
	for _, boundary := range []string{"row limit", "sync interval"} {
		t.Run(boundary, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newPullHarness(t, repeatInserts(6)...)
				h.req.MaxBatchSize = 3
				if boundary == "sync interval" {
					h = newPullHarness(t, insert, idle, insert, idle)
				}

				first := h.run(t)
				require.NoError(t, first.err)
				require.Zero(t, h.stream.closeCalls)
				require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
				h.nextBatch(first.checkpoint)

				second := h.run(t)
				require.NoError(t, second.err)
				allIDs := slices.Concat(first.ids, second.ids)
				require.Equal(t, mockEventIDs(len(allIDs)), allIDs, "unread events must survive the batch boundary exactly once")
				wantRecords := 6
				if boundary == "sync interval" {
					wantRecords = 2
				}
				require.Len(t, allIDs, wantRecords)
				require.Equal(t, 1, h.streamCreations)
				require.Zero(t, h.stream.closeCalls)
				require.Empty(t, h.persistedOffsets(), "nonempty batches are committed by the destination")
				require.NoError(t, h.connector.Close())
				require.Equal(t, 1, h.stream.closeCalls)
				require.NoError(t, h.connector.Close())
				require.Equal(t, 1, h.stream.closeCalls, "connector cleanup must close the retained cursor only once")
			})
		})
	}
}

func TestPullRecordsIncludesRecordReturnedAfterSoftBatchDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newPullHarness(t, insert, insert, insert)
		h.req.IdleTimeout = time.Minute
		h.stream.beforeNext = func(idx int) {
			if idx == 1 {
				time.Sleep(90 * time.Second)
			}
		}

		first := h.run(t)
		require.NoError(t, first.err)
		require.Equal(t, mockEventIDs(2), first.ids, "a successful slow poll must not discard the returned event")
		require.Equal(t, h.stream.tokenAt(1), first.checkpoint)
		require.NoError(t, h.stream.Err())
		require.Zero(t, h.stream.closeCalls)
		require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
		h.nextBatch(first.checkpoint)
		h.req.MaxBatchSize = 1

		second := h.run(t)
		require.NoError(t, second.err)
		require.Equal(t, mockEventIDs(3), slices.Concat(first.ids, second.ids))
		require.Equal(t, 1, h.streamCreations)
		require.NoError(t, h.connector.Close())
	})
}

func TestPullRecordsRequiresAcknowledgementForEveryBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newPullHarness(t, insert, insert, insert)
		h.req.MaxBatchSize = 1
		first := h.run(t)
		require.NoError(t, first.err)
		require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
		h.nextBatch(first.checkpoint)
		second := h.run(t)
		require.NoError(t, second.err)
		require.Equal(t, 1, h.streamCreations)

		// The previous acknowledgement must not authorize reuse of the second batch.
		h.nextBatch(second.checkpoint)
		third := h.run(t)
		require.NoError(t, third.err)
		require.Equal(t, 2, h.streamCreations)
		require.Equal(t, 1, h.stream.closeCalls)
		require.Equal(t, mockEventIDs(3), slices.Concat(first.ids, second.ids, third.ids))
		require.NoError(t, h.connector.Close())
	})
}

func TestPullRecordsReadTimeoutSealsBatchAndClosesCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newPullHarness(t, insert, idleNoReply, insert)
		first := h.run(t)
		require.NoError(t, first.err)
		require.Equal(t, mockEventIDs(1), first.ids)
		require.Equal(t, h.stream.tokenAt(0), first.checkpoint)
		require.ErrorIs(t, h.stream.Err(), context.DeadlineExceeded)
		require.Equal(t, 1, h.stream.closeCalls, "a successfully sealed batch must not retain an errored cursor")
		require.Empty(t, h.persistedOffsets())

		require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
		h.nextBatch(first.checkpoint)
		h.req.MaxBatchSize = 1
		second := h.run(t)
		require.NoError(t, second.err)
		require.Equal(t, 2, h.streamCreations)
		require.Equal(t, mockEventIDs(2), slices.Concat(first.ids, second.ids))
		require.NoError(t, h.connector.Close())
	})
}

func TestPullRecordsReopensFromRequestedCheckpointWithoutMatchingAcknowledgement(t *testing.T) {
	for _, acknowledgement := range []string{"missing", "missing with matching offset", "matching but offset changed", "wrong"} {
		t.Run(acknowledgement, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newPullHarness(t, repeatInserts(4)...)
				h.req.MaxBatchSize = 2
				startToken := toResumeToken(time.Now().Add(-time.Hour))
				expectedResumeToken := startToken
				h.req.LastOffset = model.CdcCheckpoint{Text: b64(startToken)}
				firstStream := h.stream
				replayStream := newMockChangeStream(t, insert, insert)
				h.connector.createChangeStream = func(
					_ context.Context, _ mongo.Pipeline, opts ...options.Lister[options.ChangeStreamOptions],
				) (ChangeStream, error) {
					h.streamCreations++
					var applied options.ChangeStreamOptions
					for _, builder := range opts {
						for _, setter := range builder.List() {
							require.NoError(t, setter(&applied))
						}
					}
					require.Equal(t, expectedResumeToken, applied.ResumeAfter, "reopening must use the durable request, not the consumed token")
					require.NotNil(t, applied.MaxAwaitTime)
					require.Equal(t, time.Second, *applied.MaxAwaitTime)
					if h.streamCreations == 1 {
						return firstStream, nil
					}
					return replayStream, nil
				}

				first := h.run(t)
				require.NoError(t, first.err)
				switch acknowledgement {
				case "matching but offset changed":
					require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
				case "wrong":
					require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: b64(startToken)}))
					require.Equal(t, 1, firstStream.closeCalls)
				}
				h.nextBatch(b64(startToken))
				wantIDs := first.ids
				if acknowledgement == "missing with matching offset" {
					// Isolate the acknowledgement gate: the requested token matches,
					// but without the callback the consumed cursor must still be replaced.
					h.nextBatch(first.checkpoint)
					expectedResumeToken = firstStream.ResumeToken()
					replayStream.inserts = len(first.ids)
					wantIDs = mockEventIDs(4)[2:]
				}

				replay := h.run(t)
				require.NoError(t, replay.err)
				require.Equal(t, wantIDs, replay.ids, "reopening must respect the requested checkpoint")
				require.Equal(t, 2, h.streamCreations)
				require.Equal(t, 1, firstStream.closeCalls)
				require.Zero(t, replayStream.closeCalls)
				require.NoError(t, h.connector.Close())
				require.Equal(t, 1, replayStream.closeCalls)
			})
		})
	}
}

func TestPullRecordsFailureClosesCursor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		iterations []iterationType
		wantError  string
	}{
		{name: "server error", iterations: []iterationType{insert, fatal}, wantError: "mock change stream failure"},
		{name: "decode error", iterations: []iterationType{insert, nullIdInsert}, wantError: "_id field is missing or null"},
		{name: "exhaustion", iterations: []iterationType{insert, exhausted}, wantError: "cursor exhausted"},
		{name: "cancellation", iterations: []iterationType{insert, insert}, wantError: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newPullHarness(t, tc.iterations...)
				h.req.MaxBatchSize = 2
				if tc.name == "cancellation" {
					h.stream.beforeNext = func(idx int) {
						if idx == 1 {
							h.cancel()
						}
					}
				}
				out := h.run(t)
				require.ErrorContains(t, out.err, tc.wantError)
				require.Equal(t, 1, h.stream.closeCalls)
				require.Zero(t, h.stream.ID())
				require.Empty(t, h.persistedOffsets())
				require.NoError(t, h.connector.Close())
				require.Equal(t, 1, h.stream.closeCalls, "failed pull must not leave a cached cursor")
			})
		})
	}
}

func TestPullRecordsChangedStreamConfigurationReopensCursor(t *testing.T) {
	for _, changed := range []string{"flow", "mapping", "excluded operations", "internal version"} {
		t.Run(changed, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newPullHarness(t, insert, insert)
				h.req.MaxBatchSize = 1
				first := h.run(t)
				require.NoError(t, first.err)
				require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
				h.nextBatch(first.checkpoint)
				switch changed {
				case "flow":
					h.req.FlowJobName += "-different"
				case "mapping":
					h.req.TableNameMapping["db.coll"] = model.SourceTableMapping{Name: "different_destination"}
				case "excluded operations":
					h.connector.excludedOps = []operationType{operationTypeDelete}
				case "internal version":
					h.req.InternalVersion--
				}
				second := h.run(t)
				require.NoError(t, second.err)
				require.Equal(t, 2, h.streamCreations)
				require.Equal(t, 1, h.stream.closeCalls)
				require.NoError(t, h.connector.Close())
			})
		})
	}
}

func TestPullRecordsEquivalentMappingOrderKeepsCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newPullHarness(t, insert, insert)
		h.req.MaxBatchSize = 1
		h.req.TableNameMapping["other.coll"] = model.SourceTableMapping{Name: "other_coll"}
		first := h.run(t)
		require.NoError(t, first.err)
		require.NoError(t, h.connector.UpdateReplStateLastOffset(t.Context(), model.CdcCheckpoint{Text: first.checkpoint}))
		h.nextBatch(first.checkpoint)
		h.req.TableNameMapping = map[string]model.SourceTableMapping{
			"other.coll": {Name: "other_coll"},
			"db.coll":    {Name: "db_coll"},
		}
		second := h.run(t)
		require.NoError(t, second.err)
		require.Equal(t, 1, h.streamCreations)
		require.Equal(t, mockEventIDs(2), slices.Concat(first.ids, second.ids))
		require.NoError(t, h.connector.Close())
	})
}
