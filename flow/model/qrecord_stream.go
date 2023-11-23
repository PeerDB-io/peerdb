package model

import (
	"fmt"
)

type QRecordOrError struct {
	Record *QRecord
	Err    error
}

type QRecordSchemaOrError struct {
	Schema *QRecordSchema
	Err    error
}

type QRecordStream struct {
	schema      chan *QRecordSchemaOrError
	Records     chan *QRecordOrError
	schemaSet   bool
	schemaCache *QRecordSchema
}

type RecordsToStreamRequest struct {
	records      chan Record
	TableMapping map[string]uint32
	BatchID      int64
}

func NewRecordsToStreamRequest(
	records chan Record,
	tableMapping map[string]uint32,
	batchID int64,
) *RecordsToStreamRequest {
	return &RecordsToStreamRequest{
		records:      records,
		TableMapping: tableMapping,
		BatchID:      batchID,
	}
}

func (r *RecordsToStreamRequest) GetRecords() chan Record {
	return r.records
}

type RecordsToStreamResponse struct {
	Stream *QRecordStream
}

func NewQRecordStream(buffer int) *QRecordStream {
	return &QRecordStream{
		schema:      make(chan *QRecordSchemaOrError, 1),
		Records:     make(chan *QRecordOrError, buffer),
		schemaSet:   false,
		schemaCache: nil,
	}
}

func (s *QRecordStream) Schema() (*QRecordSchema, error) {
	if s.schemaCache != nil {
		return s.schemaCache, nil
	}

	schemaOrError := <-s.schema
	s.schemaCache = schemaOrError.Schema
	return schemaOrError.Schema, schemaOrError.Err
}

func (s *QRecordStream) SetSchema(schema *QRecordSchema) error {
	if s.schemaSet {
		return fmt.Errorf("Schema already set")
	}

	s.schema <- &QRecordSchemaOrError{
		Schema: schema,
	}
	s.schemaSet = true
	return nil
}

func (s *QRecordStream) IsSchemaSet() bool {
	return s.schemaSet
}

func (s *QRecordStream) SchemaChan() chan *QRecordSchemaOrError {
	return s.schema
}
