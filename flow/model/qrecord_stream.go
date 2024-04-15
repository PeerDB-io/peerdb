package model

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type RecordTypeCounts struct {
	InsertCount int
	UpdateCount int
	DeleteCount int
}

type QRecordOrError struct {
	Err    error
	Record []qvalue.QValue
}

type QRecordSchemaOrError struct {
	Schema *qvalue.QRecordSchema
}

type QRecordStream struct {
	schemaLatch chan struct{}
	Records     chan QRecordOrError
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
		Records:     make(chan QRecordOrError, buffer),
		schema:      qvalue.QRecordSchema{},
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
