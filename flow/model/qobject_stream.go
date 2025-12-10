package model

import (
	"net/http"

	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type QObjectStreamFormat string

const (
	// QObjectStreamBigQueryExportAvroFormat is a representation of Avro format with some BigQuery Avro export
	// specific conventions. For more details, see:
	// https://cloud.google.com/bigquery/docs/exporting-data#avro_export_details
	QObjectStreamBigQueryExportAvroFormat    QObjectStreamFormat = "bigquery_export_avro"
	QObjectStreamBigQueryExportParquetFormat QObjectStreamFormat = "bigquery_export_parquet"
)

func QObjectStreamFormatNormalized(format QObjectStreamFormat) string {
	switch format {
	case QObjectStreamBigQueryExportAvroFormat:
		return "avro"
	case QObjectStreamBigQueryExportParquetFormat:
		return "parquet"
	default:
		return "unknown"
	}
}

type Object struct {
	Headers http.Header
	URL     string
	Size    int64
}

// QObjectStream is a stream of HTTP objects represented with URLs
// that are directly consumable by any HTTP client.
// This means the URLs are either public URLs or authenticated
// with a header (e.g., token in header).
type QObjectStream struct {
	Objects     chan *Object
	formatLatch *concurrency.Latch[QObjectStreamFormat]
	schemaLatch *concurrency.Latch[types.QRecordSchema]
	err         error
}

func NewQObjectStream(buffer int) *QObjectStream {
	return &QObjectStream{
		schemaLatch: concurrency.NewLatch[types.QRecordSchema](),
		formatLatch: concurrency.NewLatch[QObjectStreamFormat](),
		Objects:     make(chan *Object, buffer),
		err:         nil,
	}
}

func (s *QObjectStream) Schema() (types.QRecordSchema, error) {
	return s.schemaLatch.Wait(), s.Err()
}

func (s *QObjectStream) SetSchema(schema types.QRecordSchema) {
	s.schemaLatch.Set(schema)
}

func (s *QObjectStream) Format() (QObjectStreamFormat, error) {
	return s.formatLatch.Wait(), s.Err()
}

func (s *QObjectStream) SetFormat(format QObjectStreamFormat) {
	s.formatLatch.Set(format)
}

func (s *QObjectStream) Err() error {
	return s.err
}

func (s *QObjectStream) Close(err error) {
	if s.err == nil {
		s.err = err
		close(s.Objects)
	}
}
