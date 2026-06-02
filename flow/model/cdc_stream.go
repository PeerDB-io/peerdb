package model

import (
	"context"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

type CDCStream[T Items] struct {
	// empty signal to indicate if the records are going to be empty or not.
	emptySignal chan struct{}
	records     chan Record[T]
	// lastCheckpointText is used for mysql GTID and MongoDB ResumeToken
	lastCheckpointText string
	// Schema changes from slot
	SchemaDeltas []*protos.TableSchemaDelta
	// CDC v2: XIDs whose commit was fully observed in this batch.
	// Set by source after PullRecords drains, before Close; read by sync.
	committedXIDs []uint32
	// lastCheckpointID is the last ID of the commit that corresponds to this batch.
	lastCheckpointID  int64
	lastCheckpointSet bool
	needsNormalize    bool
	// CDC v2 protocol was actually used on the source side for this batch
	v2Active bool
	empty    bool
	emptySet bool
}

type CdcCheckpoint struct {
	Text string
	ID   int64
}

func NewCDCStream[T Items](channelBuffer int) *CDCStream[T] {
	return &CDCStream[T]{
		records:            make(chan Record[T], channelBuffer),
		SchemaDeltas:       make([]*protos.TableSchemaDelta, 0),
		emptySignal:        make(chan struct{}),
		lastCheckpointID:   0,
		lastCheckpointText: "",
		needsNormalize:     false,
		lastCheckpointSet:  false,
		empty:              true,
		emptySet:           false,
	}
}

func (r *CDCStream[T]) UpdateLatestCheckpointID(val int64) {
	r.lastCheckpointID = max(r.lastCheckpointID, val)
}

func (r *CDCStream[T]) UpdateLatestCheckpointText(val string) {
	r.lastCheckpointText = val
}

func (r *CDCStream[T]) GetLastCheckpoint() CdcCheckpoint {
	if !r.lastCheckpointSet {
		panic("last checkpoint not set, stream is still active")
	}
	return CdcCheckpoint{ID: r.lastCheckpointID, Text: r.lastCheckpointText}
}

func (r *CDCStream[T]) AddRecord(ctx context.Context, record Record[T]) error {
	if !r.needsNormalize {
		switch record.(type) {
		case *InsertRecord[T], *UpdateRecord[T], *DeleteRecord[T]:
			r.needsNormalize = true
		}
	}

	logger := internal.LoggerFromCtx(ctx)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case r.records <- record:
			return nil
		case <-ticker.C:
			logger.Warn("waiting on adding record to stream", slog.String("dstTableName", record.GetDestinationTableName()))
		case <-ctx.Done():
			logger.Warn("context cancelled while adding record to stream", slog.String("dstTableName", record.GetDestinationTableName()))
			return ctx.Err()
		}
	}
}

func (r *CDCStream[T]) SignalAsEmpty() {
	r.emptySet = true
	close(r.emptySignal)
}

func (r *CDCStream[T]) SignalAsNotEmpty() {
	r.empty = false
	r.emptySet = true
	close(r.emptySignal)
}

func (r *CDCStream[T]) WaitAndCheckEmpty() bool {
	<-r.emptySignal
	return r.empty
}

func (r *CDCStream[T]) Close() {
	if !r.lastCheckpointSet {
		r.lastCheckpointSet = true
		close(r.records)
		if !r.emptySet {
			r.emptySet = true
			close(r.emptySignal)
		}
	}
}

func (r *CDCStream[T]) GetRecords() <-chan Record[T] {
	return r.records
}

func (r *CDCStream[T]) ChannelLen() int {
	return len(r.records)
}

func (r *CDCStream[T]) AddSchemaDelta(
	tableNameMapping map[string]NameAndExclude,
	delta *protos.TableSchemaDelta,
) {
	r.SchemaDeltas = append(r.SchemaDeltas, delta)
}

func (r *CDCStream[T]) NeedsNormalize() bool {
	return r.needsNormalize
}

// SetV2Active is called by the source at the start of pull, before any DML
// records arrive. The sync path needs to know v2 mode before it picks an avro
// destination table, which happens concurrently with pull — leaving this to
// the post-pull defer races against the sync goroutine and routes records to
// the v1 raw table while normalize reads from the v2 WAL sink.
func (r *CDCStream[T]) SetV2Active(active bool) {
	r.v2Active = active
}

// SetCommittedXIDs is called by the source after pull drains. Receiving any
// committed XID forces a normalize pass even with zero DML records in this
// batch: prior batches' WAL sink rows still need promotion.
func (r *CDCStream[T]) SetCommittedXIDs(committedXIDs []uint32) {
	r.committedXIDs = committedXIDs
	if r.v2Active && len(committedXIDs) > 0 {
		r.needsNormalize = true
	}
}

func (r *CDCStream[T]) V2Active() bool {
	return r.v2Active
}

func (r *CDCStream[T]) CommittedXIDs() []uint32 {
	return r.committedXIDs
}
