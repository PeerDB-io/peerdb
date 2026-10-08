package model

import (
	"context"
	"sync"

	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// QRecordStream is the general-purpose stream used by snapshots and connectors.
type QRecordStream = RecordStream[[]types.QValue]

// RecordStream shares schema, cancellation and error handling across row types.
type RecordStream[T any] struct {
	schemaLatch *concurrency.Latch[types.QRecordSchema]
	Records     chan T
	schemaDebug *types.NullableSchemaDebug
	err         error
	closeOnce   sync.Once
}

func NewQRecordStream(buffer int) *QRecordStream {
	return NewRecordStream[[]types.QValue](buffer)
}

func NewRecordStream[T any](buffer int) *RecordStream[T] {
	return &RecordStream[T]{
		schemaLatch: concurrency.NewLatch[types.QRecordSchema](),
		Records:     make(chan T, buffer),
		err:         nil,
	}
}

func (s *RecordStream[T]) Schema() (types.QRecordSchema, error) {
	return s.schemaLatch.Wait(), s.Err()
}

func (s *RecordStream[T]) SetSchema(schema types.QRecordSchema) {
	s.schemaLatch.Set(schema)
}

func (s *RecordStream[T]) IsSchemaSet() bool {
	return s.schemaLatch.IsSet()
}

func (s *RecordStream[T]) SetSchemaDebug(debug *types.NullableSchemaDebug) {
	s.schemaDebug = debug
}

func (s *RecordStream[T]) SchemaDebug() *types.NullableSchemaDebug {
	return s.schemaDebug
}

func (s *RecordStream[T]) SchemaChan() <-chan struct{} {
	return s.schemaLatch.Chan()
}

func (s *RecordStream[T]) HandleQRepSyncError(err error) {
	// no-op for QRecordStream
}

// Sends the record into the channel, erroring out on context cancellation instead of waiting for the reader indefinitely
func (s *RecordStream[T]) Send(ctx context.Context, record T) error {
	if s.err != nil {
		return s.err
	}
	select {
	case s.Records <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *RecordStream[T]) Err() error {
	return s.err
}

// Set error and close stream. Calling Close multiple times tracks only the first error.
func (s *RecordStream[T]) Close(err error) {
	s.closeOnce.Do(func() {
		s.err = err
		close(s.Records)
		if !s.schemaLatch.IsSet() {
			s.SetSchema(types.QRecordSchema{})
		}
	})
}
