package model

import (
	"sync/atomic"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type CDCRecordStream struct {
	// empty signal to indicate if the records are going to be empty or not.
	emptySignal chan bool
	// Records are a list of json objects.
	records chan Record
	// Schema changes from the slot
	SchemaDeltas []*protos.TableSchemaDelta
	// Indicates if the last checkpoint has been set.
	lastCheckpointSet bool
	// lastCheckpointID is the last ID of the commit that corresponds to this batch.
	lastCheckpointID atomic.Int64
}

func NewCDCRecordStream() *CDCRecordStream {
	channelBuffer := peerdbenv.PeerDBCDCChannelBufferSize()
	return &CDCRecordStream{
		records:           make(chan Record, channelBuffer),
		SchemaDeltas:      make([]*protos.TableSchemaDelta, 0),
		emptySignal:       make(chan bool, 1),
		lastCheckpointSet: false,
		lastCheckpointID:  atomic.Int64{},
	}
}

func (r *CDCRecordStream) UpdateLatestCheckpoint(val int64) {
	shared.AtomicInt64Max(&r.lastCheckpointID, val)
}

func (r *CDCRecordStream) GetLastCheckpoint() int64 {
	if !r.lastCheckpointSet {
		panic("last checkpoint not set, stream is still active")
	}
	return r.lastCheckpointID.Load()
}

func (r *CDCRecordStream) AddRecord(record Record) {
	r.records <- record
}

func (r *CDCRecordStream) SignalAsEmpty() {
	r.emptySignal <- true
}

func (r *CDCRecordStream) SignalAsNotEmpty() {
	r.emptySignal <- false
}

func (r *CDCRecordStream) WaitAndCheckEmpty() bool {
	isEmpty := <-r.emptySignal
	return isEmpty
}

func (r *CDCRecordStream) Close() {
	if !r.lastCheckpointSet {
		close(r.emptySignal)
		close(r.records)
		r.lastCheckpointSet = true
	}
}

func (r *CDCRecordStream) GetRecords() <-chan Record {
	return r.records
}

func (r *CDCRecordStream) AddSchemaDelta(tableNameMapping map[string]NameAndExclude,
	delta *protos.TableSchemaDelta,
) {
	r.SchemaDeltas = append(r.SchemaDeltas, delta)
}
