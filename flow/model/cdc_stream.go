package model

import (
	"sync/atomic"

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

func (r *CDCStream[T]) AddRecord(record Record[T]) {
	r.records <- record
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
