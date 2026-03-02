package model

import (
	"context"
	"sync"

	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type QRecordStream struct {
	schemaLatch *concurrency.Latch[types.QRecordSchema]
	Records     chan []types.QValue
	schemaDebug *types.NullableSchemaDebug
	err         error
	closeOnce   sync.Once
}

func NewQRecordStream(buffer int) *QRecordStream {
	return &QRecordStream{
		schemaLatch: concurrency.NewLatch[types.QRecordSchema](),
		Records:     make(chan []types.QValue, buffer),
		err:         nil,
	}
}

func (s *QRecordStream) Schema() (types.QRecordSchema, error) {
	return s.schemaLatch.Wait(), s.Err()
}

func (s *QRecordStream) SetSchema(schema types.QRecordSchema) {
	s.schemaLatch.Set(schema)
}

func (s *QRecordStream) IsSchemaSet() bool {
	return s.schemaLatch.IsSet()
}

func (s *QRecordStream) SetSchemaDebug(debug *types.NullableSchemaDebug) {
	s.schemaDebug = debug
}

func (s *QRecordStream) SchemaDebug() *types.NullableSchemaDebug {
	return s.schemaDebug
}

func (s *QRecordStream) SchemaChan() <-chan struct{} {
	return s.schemaLatch.Chan()
}

func (s *QRecordStream) ReportQRepSyncError(err error) {
	// no-op for QRecordStream
}

// Sends the record into the channel, erroring out on context cancellation instead of waiting for the reader indefinitely
func (s *QRecordStream) Send(ctx context.Context, record []types.QValue) error {
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

func (s *QRecordStream) Err() error {
	return s.err
}

// Set error and close stream. Calling Close multiple times tracks only the first error.
func (s *QRecordStream) Close(err error) {
	s.closeOnce.Do(func() {
		s.err = err
		close(s.Records)
		if !s.schemaLatch.IsSet() {
			s.SetSchema(types.QRecordSchema{})
		}
	})
}
