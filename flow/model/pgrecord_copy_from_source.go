package model

type PgRecordCopyFromSource struct {
	stream        *PgRecordStream
	currentRecord []any
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
	return ok || src.Err() != nil
}

func (src *PgRecordCopyFromSource) Values() ([]interface{}, error) {
	return src.currentRecord, src.Err()
}

func (src *PgRecordCopyFromSource) Err() error {
	return src.stream.Err()
}
