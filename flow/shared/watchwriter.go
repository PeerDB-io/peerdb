package shared

import (
	"io"
	"sync/atomic"
)

type WatchWriter struct {
	w       io.Writer
	written *atomic.Int64
}

func NewWatchWriter(w io.Writer, size *atomic.Int64) *WatchWriter {
	return &WatchWriter{w: w, written: size}
}

func (w *WatchWriter) Write(p []byte) (int, error) {
	written, err := w.w.Write(p)
	w.written.Add(int64(written))
	return written, err
}
