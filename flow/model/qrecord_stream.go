package model

import (
	"sync/atomic"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type RecordTypeCounts struct {
	InsertCount atomic.Int32
	UpdateCount atomic.Int32
	DeleteCount atomic.Int32
}

type RecordStream[T any] struct {
	schemaLatch chan struct{}
	Records     chan []T
	err         error
	schema      qvalue.QRecordSchema
	schemaSet   bool
}

type (
	QRecordStream  = RecordStream[qvalue.QValue]
	PgRecordStream = RecordStream[any]
)

type RecordsToStreamRequest[T Items] struct {
	records      <-chan Record[T]
	TableMapping map[string]*RecordTypeCounts
	BatchID      int64
}

func NewRecordsToStreamRequest[T Items](
	records <-chan Record[T],
	tableMapping map[string]*RecordTypeCounts,
	batchID int64,
) *RecordsToStreamRequest[T] {
	return &RecordsToStreamRequest[T]{
		records:      records,
		TableMapping: tableMapping,
		BatchID:      batchID,
	}
}

func (r *RecordsToStreamRequest[T]) GetRecords() <-chan Record[T] {
	return r.records
}

func NewRecordStream[T any](buffer int) *RecordStream[T] {
	return &RecordStream[T]{
		schemaLatch: make(chan struct{}),
		Records:     make(chan []T, buffer),
		schema:      qvalue.QRecordSchema{},
		err:         nil,
		schemaSet:   false,
	}
}

func NewQRecordStream(buffer int) *QRecordStream {
	return NewRecordStream[qvalue.QValue](buffer)
}

func (s *RecordStream[T]) Schema() qvalue.QRecordSchema {
	<-s.schemaLatch
	return s.schema
}

func (s *RecordStream[T]) SetSchema(schema qvalue.QRecordSchema) {
	if !s.schemaSet {
		s.schema = schema
		close(s.schemaLatch)
		s.schemaSet = true
	}
}

func (s *RecordStream[T]) IsSchemaSet() bool {
	return s.schemaSet
}

func (s *RecordStream[T]) SchemaChan() <-chan struct{} {
	return s.schemaLatch
}

func (s *RecordStream[T]) Err() error {
	return s.err
}

// Set error & close stream. Calling with multiple errors only tracks first error & does not panic.
// Close(nil) after an error won't panic, but Close after Close(nil) will panic,
// this is enough to be able to safely `defer stream.Close(nil)`.
func (s *RecordStream[T]) Close(err error) {
	if s.err == nil {
		s.err = err
		close(s.Records)
	}
}
