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

type QRecordStream struct {
	schemaLatch chan struct{}
	Records     chan []qvalue.QValue
	err         error
	schema      qvalue.QRecordSchema
	schemaSet   bool
}

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

func NewQRecordStream(buffer int) *QRecordStream {
	return &QRecordStream{
		schemaLatch: make(chan struct{}),
		Records:     make(chan []qvalue.QValue, buffer),
		schema:      qvalue.QRecordSchema{},
		err:         nil,
		schemaSet:   false,
	}
}

func (s *QRecordStream) Schema() qvalue.QRecordSchema {
	<-s.schemaLatch
	return s.schema
}

func (s *QRecordStream) SetSchema(schema qvalue.QRecordSchema) {
	if !s.schemaSet {
		s.schema = schema
		close(s.schemaLatch)
		s.schemaSet = true
	}
}

func (s *QRecordStream) IsSchemaSet() bool {
	return s.schemaSet
}

func (s *QRecordStream) SchemaChan() <-chan struct{} {
	return s.schemaLatch
}

func (s *QRecordStream) Err() error {
	return s.err
}

// Set error & close stream. Calling with multiple errors only tracks first error & does not panic.
// Close(nil) after an error won't panic, but Close after Close(nil) will panic,
// this is enough to be able to safely `defer stream.Close(nil)`.
func (s *QRecordStream) Close(err error) {
	if s.err == nil {
		s.err = err
		close(s.Records)
	}
}
