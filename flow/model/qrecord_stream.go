package model

import (
	"errors"

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
	Err    error
}

type QRecordStream struct {
	schema      chan QRecordSchemaOrError
	Records     chan QRecordOrError
	schemaCache *qvalue.QRecordSchema
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
		schema:      make(chan QRecordSchemaOrError, 1),
		Records:     make(chan QRecordOrError, buffer),
		schemaSet:   false,
		schemaCache: nil,
	}
}

func (s *QRecordStream) Schema() (*qvalue.QRecordSchema, error) {
	if s.schemaCache != nil {
		return s.schemaCache, nil
	}

	schemaOrError := <-s.schema
	s.schemaCache = schemaOrError.Schema
	return schemaOrError.Schema, schemaOrError.Err
}

func (s *QRecordStream) SetSchema(schema *qvalue.QRecordSchema) error {
	if s.schemaSet {
		return errors.New("Schema already set")
	}

	s.schema <- QRecordSchemaOrError{
		Schema: schema,
	}
	s.schemaSet = true
	return nil
}

func (s *QRecordStream) IsSchemaSet() bool {
	return s.schemaSet
}

func (s *QRecordStream) SchemaChan() <-chan QRecordSchemaOrError {
	return s.schema
}
