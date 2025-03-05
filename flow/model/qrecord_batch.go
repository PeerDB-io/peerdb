package model

import (
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

// QRecordBatch holds a batch of []QValue slices
type QRecordBatch struct {
	Schema  qvalue.QRecordSchema
	Records [][]qvalue.QValue
}

func (q *QRecordBatch) ToQRecordStream(buffer int) *QRecordStream {
	stream := NewQRecordStream(min(buffer, len(q.Records)))
	go q.FeedToQRecordStream(stream)
	return stream
}

func (q *QRecordBatch) FeedToQRecordStream(stream *QRecordStream) {
	stream.SetSchema(q.Schema)

	for _, record := range q.Records {
		stream.Records <- record
	}
	close(stream.Records)
}
