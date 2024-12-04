package model

import (
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QRecordStream struct {
	schemaLatch chan struct{}
	Records     chan []qvalue.QValue
	err         error
	schema      qvalue.QRecordSchema
	schemaSet   bool
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
		s.schemaSet = true
		close(s.schemaLatch)
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
