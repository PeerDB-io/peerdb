package model

import (
	"github.com/PeerDB-io/peer-flow/shared"
)

type PgRecordCopyFromSource struct {
	stream        *PgRecordStream
	currentRecord [][]byte
}

func NewPgRecordCopyFromSource(
	stream *PgRecordStream,
) *PgRecordCopyFromSource {
	return &PgRecordCopyFromSource{
		stream:        stream,
		currentRecord: nil,
	}
}

func (src *PgRecordCopyFromSource) Next() bool {
	rec, ok := <-src.stream.Records
	src.currentRecord = rec
	return ok
}

func (src *PgRecordCopyFromSource) Values() ([]interface{}, error) {
	if err := src.Err(); err != nil {
		return nil, err
	}

	values := make([]interface{}, len(src.currentRecord))
	for i, val := range src.currentRecord {
		values[i] = shared.UnsafeFastReadOnlyBytesToString(val)
	}
	return values, nil
}

func (src *PgRecordCopyFromSource) Err() error {
	return src.stream.Err()
}
