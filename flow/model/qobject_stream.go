package model

import (
	"context"
	"net/http"

	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type QObjectStreamFormat string

const (
	// QObjectStreamBigQueryExportParquetFormat is the Parquet format used by BigQuery export.
	// More details: https://docs.cloud.google.com/bigquery/docs/exporting-data#parquet_export_details
	QObjectStreamBigQueryExportParquetFormat QObjectStreamFormat = "parquet"
)

// HeaderProvider provides HTTP headers for authenticating object requests.
type HeaderProvider interface {
	GetHeaders(ctx context.Context) (http.Header, error)
	InvalidateToken()
}

type Object struct {
	URL  string
	Size int64
}

// QObjectStream is a stream of HTTP objects represented with URLs
// that are directly consumable by any HTTP client when combined
// with authentication headers from the HeaderProvider.
type QObjectStream struct {
	Objects             chan *Object
	formatLatch         *concurrency.Latch[QObjectStreamFormat]
	schemaLatch         *concurrency.Latch[types.QRecordSchema]
	headerProviderLatch *concurrency.Latch[HeaderProvider]
	err                 error
}

func NewQObjectStream(buffer int) *QObjectStream {
	return &QObjectStream{
		schemaLatch:         concurrency.NewLatch[types.QRecordSchema](),
		formatLatch:         concurrency.NewLatch[QObjectStreamFormat](),
		headerProviderLatch: concurrency.NewLatch[HeaderProvider](),
		Objects:             make(chan *Object, buffer),
		err:                 nil,
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

func (s *QObjectStream) HeaderProvider() (HeaderProvider, error) {
	return s.headerProviderLatch.Wait(), s.Err()
}

func (s *QObjectStream) SetHeaderProvider(provider HeaderProvider) {
	s.headerProviderLatch.Set(provider)
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
