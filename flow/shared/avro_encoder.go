package shared

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

type WatchWriter struct {
	w       io.Writer
	written *atomic.Int64
}

type AvroChunkSizeTracker struct {
	TrackUncompressed bool
	Bytes             atomic.Int64
}

func NewWatchWriter(w io.Writer, size *atomic.Int64) *WatchWriter {
	return &WatchWriter{w: w, written: size}
}

func (w *WatchWriter) Write(p []byte) (int, error) {
	written, err := w.w.Write(p)
	w.written.Add(int64(written))
	return written, err
}

type AvroEncoder interface {
	Encode(v any) error
	Close() error
}

type PeerDBAvroEncoder struct {
	encoder        *ocf.Encoder
	discardEncoder *ocf.Encoder
}

func NewAvroEncoder(
	schema *avro.RecordSchema,
	writer io.Writer,
	codec ocf.CodecName,
	sizeTracker *AvroChunkSizeTracker,
) (*PeerDBAvroEncoder, error) {
	// discard writer's purpose is to track uncompressed bytes, if sizeTracker asked for it
	var discardEncoder *ocf.Encoder
	if sizeTracker != nil {
		if sizeTracker.TrackUncompressed {
			discardWriter := NewWatchWriter(io.Discard, &sizeTracker.Bytes)
			var err error
			discardEncoder, err = ocf.NewEncoderWithSchema(
				schema, discardWriter, ocf.WithCodec(ocf.Null),
				ocf.WithBlockLength(8192), ocf.WithBlockSize(1<<26),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create tracker encoder: %w", err)
			}
		} else {
			writer = NewWatchWriter(writer, &sizeTracker.Bytes)
		}
	}

	encoder, err := ocf.NewEncoderWithSchema(
		schema, writer, ocf.WithCodec(codec),
		ocf.WithBlockLength(8192), ocf.WithBlockSize(1<<26),
	)
	if err != nil {
		if discardEncoder != nil {
			discardEncoder.Close()
		}
		return nil, fmt.Errorf("failed to create OCF encoder: %w", err)
	}

	return &PeerDBAvroEncoder{
		encoder:        encoder,
		discardEncoder: discardEncoder,
	}, nil
}

func (t *PeerDBAvroEncoder) Encode(v any) error {
	if err := t.encoder.Encode(v); err != nil {
		return err
	}
	if t.discardEncoder != nil {
		if err := t.discardEncoder.Encode(v); err != nil {
			return err
		}
	}
	return nil
}

func (t *PeerDBAvroEncoder) Close() error {
	var err1, err2 error
	if t.encoder != nil {
		err1 = t.encoder.Close()
	}
	if t.discardEncoder != nil {
		err2 = t.discardEncoder.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}
