package model

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (r *CDCStream[T]) addRecordOld(ctx context.Context, record Record[T]) error {
	if !r.needsNormalize {
		switch record.(type) {
		case *InsertRecord[T], *UpdateRecord[T], *DeleteRecord[T]:
			r.needsNormalize = true
		}
	}

	logger := internal.LoggerFromCtx(ctx)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case r.records <- record:
			return nil
		case <-ticker.C:
			logger.Warn("waiting on adding record to stream", slog.String("dstTableName", record.GetDestinationTableName()))
		case <-ctx.Done():
			logger.Warn("context cancelled while adding record to stream", slog.String("dstTableName", record.GetDestinationTableName()))
			return ctx.Err()
		}
	}
}

func benchmarkAddRecord(b *testing.B, add func(*CDCStream[RecordItems], context.Context, Record[RecordItems]) error) {
	ctx := context.WithValue(b.Context(), shared.FlowNameKey, "bench-flow")
	stream := NewCDCStream[RecordItems](1)
	record := &InsertRecord[RecordItems]{
		SourceTableName:      "public.bench",
		DestinationTableName: "public.bench",
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := add(stream, ctx, record); err != nil {
			b.Fatal(err)
		}
		<-stream.records
	}
}

func BenchmarkAddRecordOld(b *testing.B) {
	benchmarkAddRecord(b, (*CDCStream[RecordItems]).addRecordOld)
}

func BenchmarkAddRecordNew(b *testing.B) {
	benchmarkAddRecord(b, (*CDCStream[RecordItems]).AddRecord)
}
