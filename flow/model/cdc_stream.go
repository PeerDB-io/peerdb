package model

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type CDCStream[T Items] struct {
	// empty signal to indicate if the records are going to be empty or not.
	emptySignal chan bool
	records     chan Record[T]
	// Schema changes from slot
	SchemaDeltas      []*protos.TableSchemaDelta
	lastCheckpointSet bool
	needsNormalize    atomic.Bool
	// lastCheckpointID is the last ID of the commit that corresponds to this batch.
	lastCheckpointID atomic.Int64
}

func NewCDCStream[T Items](channelBuffer int) *CDCStream[T] {
	return &CDCStream[T]{
		records:           make(chan Record[T], channelBuffer),
		SchemaDeltas:      make([]*protos.TableSchemaDelta, 0),
		emptySignal:       make(chan bool, 1),
		lastCheckpointSet: false,
		lastCheckpointID:  atomic.Int64{},
		needsNormalize:    atomic.Bool{},
	}
}

func (r *CDCStream[T]) UpdateLatestCheckpoint(val int64) {
	shared.AtomicInt64Max(&r.lastCheckpointID, val)
}

func (r *CDCStream[T]) GetLastCheckpoint() int64 {
	if !r.lastCheckpointSet {
		panic("last checkpoint not set, stream is still active")
	}
	return r.lastCheckpointID.Load()
}

func (r *CDCStream[T]) AddRecord(ctx context.Context, record Record[T]) error {
	if !r.needsNormalize.Load() {
		switch record.(type) {
		case *InsertRecord[T], *UpdateRecord[T], *DeleteRecord[T]:
			r.needsNormalize.Store(true)
		}
	}

	logger := shared.LoggerFromCtx(ctx)
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
	r.emptySignal <- true
}

func (r *CDCStream[T]) SignalAsNotEmpty() {
	r.emptySignal <- false
}

func (r *CDCStream[T]) WaitAndCheckEmpty() bool {
	isEmpty := <-r.emptySignal
	return isEmpty
}

func (r *CDCStream[T]) Close() {
	if !r.lastCheckpointSet {
		close(r.emptySignal)
		close(r.records)
		r.lastCheckpointSet = true
	}
}

func (r *CDCStream[T]) GetRecords() <-chan Record[T] {
	return r.records
}

func (r *CDCStream[T]) AddSchemaDelta(
	tableNameMapping map[string]NameAndExclude,
	delta *protos.TableSchemaDelta,
) {
	r.SchemaDeltas = append(r.SchemaDeltas, delta)
}

func (r *CDCStream[T]) NeedsNormalize() bool {
	return r.needsNormalize.Load()
}
