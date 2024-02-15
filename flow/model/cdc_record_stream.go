package model

import (
	"errors"
	"sync/atomic"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type CDCRecordStream struct {
	// Records are a list of json objects.
	records chan Record
	// Schema changes from the slot
	SchemaDeltas []*protos.TableSchemaDelta
	// Indicates if the last checkpoint has been set.
	lastCheckpointSet bool
	// lastCheckpointID is the last ID of the commit that corresponds to this batch.
	lastCheckpointID atomic.Int64
	// empty signal to indicate if the records are going to be empty or not.
	emptySignal chan bool
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
	// TODO update with https://github.com/golang/go/issues/63999 once implemented
	// r.lastCheckpointID.Max(val)
	oldLast := r.lastCheckpointID.Load()
	for oldLast < val && !r.lastCheckpointID.CompareAndSwap(oldLast, val) {
		oldLast = r.lastCheckpointID.Load()
	}
}

func (r *CDCRecordStream) GetLastCheckpoint() (int64, error) {
	if !r.lastCheckpointSet {
		return 0, errors.New("last checkpoint not set, stream is still active")
	}
	return r.lastCheckpointID.Load(), nil
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
	close(r.emptySignal)
	close(r.records)
	r.lastCheckpointSet = true
}

func (r *CDCRecordStream) GetRecords() <-chan Record {
	return r.records
}

func (r *CDCRecordStream) AddSchemaDelta(tableNameMapping map[string]NameAndExclude, delta *protos.TableSchemaDelta) {
	if tm, ok := tableNameMapping[delta.SrcTableName]; ok && len(tm.Exclude) != 0 {
		added := make([]*protos.DeltaAddedColumn, 0, len(delta.AddedColumns))
		for _, column := range delta.AddedColumns {
			if _, has := tm.Exclude[column.ColumnName]; !has {
				added = append(added, column)
			}
		}
		if len(added) != 0 {
			r.SchemaDeltas = append(r.SchemaDeltas, &protos.TableSchemaDelta{
				SrcTableName: delta.SrcTableName,
				DstTableName: delta.DstTableName,
				AddedColumns: added,
			})
		}
	} else {
		r.SchemaDeltas = append(r.SchemaDeltas, delta)
	}
}
