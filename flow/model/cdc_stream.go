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
	// lastCheckpointText is used for mysql GTID
	lastCheckpointText string
	// Schema changes from slot
	SchemaDeltas []*protos.TableSchemaDelta
	// lastCheckpointID is the last ID of the commit that corresponds to this batch.
	lastCheckpointID  int64
	lastCheckpointSet bool
	needsNormalize    bool
	empty             bool
	emptySet          bool
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

func (r *CDCStream[T]) AddSchemaDelta(
	tableNameMapping map[string]NameAndExclude,
	delta *protos.TableSchemaDelta,
) {
	r.SchemaDeltas = append(r.SchemaDeltas, delta)
}

func (r *CDCStream[T]) NeedsNormalize() bool {
	return r.needsNormalize
}
